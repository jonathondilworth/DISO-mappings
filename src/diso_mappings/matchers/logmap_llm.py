"""
LogMap-LLM matcher adapter (thin driver)

Drives LogMap-LLM by spawning diso_mappings.matchers._workers._logmap_llm_worker

Per-pair output layout under the run dir:

    runs/logmap_llm/<run-id>/
        alignments/<pair_tag>.rdf        final lifted alignment
        logs/per-pair/<pair_tag>.log     worker stdout/stderr
        intermediates/<pair_tag>/
            logmap-llm.toml              config
            initial/                     LogMap initial mappings + M_ask
            refined/                     LogMap refined alignment
            outputs/                     prompts JSON, predictions CSV, few-shot, run logs

Config keys (configs/logmap_llm.yaml; required keys marked *):

    logmap_dir:                    str    matchers/logmap (default)
    java:                          str    'java' or absolute path
    heap:                          str    JVM -Xmx for worker; default '8g'
    extra_jvm_args:                list   extras after base set
    oracle_model_name:             str *  HF model id or local model name
    oracle_base_url:               str    LLM endpoint (127.0.0.1:8000 for vLLM)
    oracle_api_key_env:            str    reccomended: use 'EMPTY' for local w/ vLLM
    oracle_interaction_style:      str    vllm | openrouter
    oracle_temperature, oracle_top_p, oracle_max_completion_tokens, etc.
    cls_dev_prompt_template_name:  str    'class_equivalence' (default)
    cls_usr_prompt_template_name:  str    'one_level_of_parents_and_synonyms' (default)
    sibling_strategy:              str    'sbert' / 'sapbert' / 'alphanumeric' / 'shortest_label'
    few_shot_k:                    int    0 disables (default)
    refinement_strategy:           str    'logmap' (default; required for RDF output)
    ontology_domain:               str    'security' for DISO

References:
see: https://github.com/jonathondilworth/logmap-llm
&    https://github.com/city-artificial-intelligence/logmap-llm
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from diso_mappings.matchers.base import Matcher, MatchResult, register
from diso_mappings.matchers._subprocess_runner import run_subprocess_with_timeout
from diso_mappings.io.terminal import LiveStatusLine, debug, info, warn
from diso_mappings.paths import MATCHERS_DIR


_DEFAULT_LOGMAP_DIR        = MATCHERS_DIR / "logmap"
_DEFAULT_LOGMAP_JAR_NAME   = "logmap-matcher-4.0.jar"
_DEFAULT_JAVA_DEPS_DIRNAME = "java-dependencies"
_DEFAULT_HEAP              = "8g"
_DEFAULT_JAVA_BINARY       = "java"

_REQUIRED_LOGMAP_LLM_INTERFACE_CLASS = "uk.ac.ox.krr.logmap2.LogMapLLM_Interface"

_WORKER_MODULE = "diso_mappings.matchers._workers._logmap_llm_worker"

_JAR_CLASS_CHECK_CACHE: dict[str, bool] = {}



def _format_timer_suffix(elapsed_seconds: float) -> str:
    minutes, seconds = divmod(int(elapsed_seconds), 60)
    return f"[{minutes:02d}:{seconds:02d}]"



def _resolve_java_binary(matcher_config: dict) -> str:
    """java binary resolution"""
    return matcher_config.get("java") or os.environ.get("JAVA") or _DEFAULT_JAVA_BINARY



def _resolve_logmap_dir(matcher_config: dict) -> Path:
    logmap_dir = Path(
        matcher_config.get("logmap_dir") or _DEFAULT_LOGMAP_DIR
    ).expanduser().resolve()

    logmap_jar      = logmap_dir / _DEFAULT_LOGMAP_JAR_NAME
    java_deps_dir   = logmap_dir / _DEFAULT_JAVA_DEPS_DIRNAME

    if not logmap_jar.exists():
        raise FileNotFoundError(f"LogMap JAR not found at {logmap_jar}.")
    if not java_deps_dir.is_dir():
        raise FileNotFoundError(f"LogMap java-dependencies directory not found at {java_deps_dir}.")
    return logmap_dir



def _verify_logmap_llm_interface_in_jar(logmap_dir: Path, java_binary: str) -> None:
    """
    Confirm that the JAR contains uk.ac.ox.krr.logmap2.LogMapLLM_Interface
    """
    logmap_jar = logmap_dir / _DEFAULT_LOGMAP_JAR_NAME
    cache_key  = str(logmap_jar)

    if cache_key in _JAR_CLASS_CHECK_CACHE:
        return

    java_path = Path(java_binary)
    if java_path.is_absolute() and java_path.parent.is_dir():
        javap_binary = str(java_path.parent / "javap")
    else:
        javap_binary = "javap"

    try:
        result = subprocess.run(
            [javap_binary, "-classpath", str(logmap_jar), _REQUIRED_LOGMAP_LLM_INTERFACE_CLASS],
            capture_output=True,
            timeout=30,
        )
    except FileNotFoundError:
        warn(f"javap not available; cannot pre-verify {_REQUIRED_LOGMAP_LLM_INTERFACE_CLASS} in {logmap_jar}.")
        _JAR_CLASS_CHECK_CACHE[cache_key] = True   # cache the skipped state
        return
    except subprocess.TimeoutExpired:
        warn(f"javap timed out on {logmap_jar}; skipping JAR class pre-check.")
        _JAR_CLASS_CHECK_CACHE[cache_key] = True
        return

    if result.returncode != 0:
        raise RuntimeError(f"LogMap JAR at {logmap_jar} does not contain the required class {_REQUIRED_LOGMAP_LLM_INTERFACE_CLASS}.")

    _JAR_CLASS_CHECK_CACHE[cache_key] = True
    debug(f"Verified {_REQUIRED_LOGMAP_LLM_INTERFACE_CLASS} present in {logmap_jar}")



@register
class LogMapLLM(Matcher):
    """
    LogMap + LLM oracle matcher
    """
    name       = "logmap_llm"
    _family    = "LogMap"
    show_timer = True

    @property
    def version(self) -> str:
        return "4.0"


    def run(self, source: Path, target: Path, out_dir: Path, config: dict | None = None, timeout: float | None = None) -> MatchResult:

        ##
        # CONFIG + INPUT VALIDATION
        ##

        matcher_config = config or {}

        if "oracle_model_name" not in matcher_config:
            raise ValueError("LogMap-LLM requires oracle_model_name in config.")

        java_binary = _resolve_java_binary(matcher_config)
        logmap_dir  = _resolve_logmap_dir(matcher_config)

        _verify_logmap_llm_interface_in_jar(logmap_dir, java_binary)

        if not source.exists():
            raise FileNotFoundError(f"Source ontology not found: {source}")
        if not target.exists():
            raise FileNotFoundError(f"Target ontology not found: {target}")

        ##
        # PER-PAIR PATHS
        ##

        out_dir.mkdir(parents=True, exist_ok=True)

        pair_tag        = f"{source.stem}__{target.stem}"
        final_rdf_path  = out_dir / f"{pair_tag}.rdf"
        per_pair_logdir = out_dir.parent / "logs" / "per-pair"
        log_path        = per_pair_logdir / f"{pair_tag}.log"

        intermediates_dir = out_dir.parent / "intermediates" / pair_tag
        intermediates_dir.mkdir(parents=True, exist_ok=True)

        matcher_config_path = self._resolve_matcher_config_path()

        ##
        # WORKER COMMAND
        ##

        worker_cmd: list[str] = [
            sys.executable, "-m", _WORKER_MODULE,
            "--source",            str(source.resolve()),
            "--target",            str(target.resolve()),
            "--final-rdf-path",    str(final_rdf_path),
            "--intermediates-dir", str(intermediates_dir),
            "--pair-tag",          pair_tag,
            "--matcher-config",    str(matcher_config_path),
            "--logmap-dir",        str(logmap_dir),
        ]

        ##
        # ENVIRONMENT
        ##

        worker_env = os.environ.copy()
        debug(f"LogMapLLM command: {' '.join(worker_cmd)}")

        ##
        # RUN + LIVE STATUS
        ##

        prefix = f"LogMapLLM: matching {source.name} -> {target.name}"
        t_start = time.time()

        with LiveStatusLine(prefix) as status:

            def _on_tick(elapsed: float) -> None:
                if self.show_timer:
                    status.update(_format_timer_suffix(elapsed))

            if self.show_timer:
                status.update(_format_timer_suffix(0.0))

            worker_exit_code = run_subprocess_with_timeout(
                worker_cmd,
                cwd=out_dir,
                timeout=timeout,
                log_path=log_path,
                on_tick=_on_tick,
                matcher_display_name="LogMapLLM",
                env=worker_env,
            )

        elapsed = time.time() - t_start

        ##
        # POST-RUN: VERIFY + LIFT
        ##

        if worker_exit_code != 0:
            raise RuntimeError(f"LogMapLLM worker exited with status {worker_exit_code} on {pair_tag}. See {log_path}.")

        worker_refined_rdf = intermediates_dir / "refined" / f"{pair_tag}-logmap_mappings.rdf"
        if not worker_refined_rdf.exists():
            raise RuntimeError(f"LogMapLLM worker exited cleanly but refined RDF is missing at {worker_refined_rdf}. See {log_path}.")

        shutil.copy2(str(worker_refined_rdf), str(final_rdf_path))

        return MatchResult(alignment_path=final_rdf_path, duration_seconds=elapsed)


    def _resolve_matcher_config_path(self) -> Path:
        """
        Find the YAML config file on disk so the worker can re-read it
        """
        candidate_path = Path("configs") / "logmap_llm.yaml"
        if not candidate_path.is_file():
            raise FileNotFoundError(f"LogMap-LLM matcher config not found at {candidate_path.resolve()}.")
        return candidate_path.resolve()