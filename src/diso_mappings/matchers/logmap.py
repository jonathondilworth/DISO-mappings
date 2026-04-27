"""
LogMap matcher adapter (LogMap2_core) - invokes standalone JAR as a subprocess:

    java -Xms500M -Xmx<heap> -DentityExpansionLimit=10000000 \
         --add-opens=java.base/java.lang=ALL-UNNAMED \
         -jar <LOGMAP_DIR>/logmap-matcher-4.0.jar \
         MATCHER <src_file_uri> <tgt_file_uri> <output_dir>/ <classify>

References:
    - LogMap repo:  https://github.com/ernestojimenezruiz/logmap-matcher
    - LogMap paper: Jimenez-Ruiz & Cuenca Grau, ISWC 2011
"""
from __future__ import annotations

import os
import shutil
import tempfile
import time
from pathlib import Path

from diso_mappings.matchers.base import Matcher, MatchResult, register
from diso_mappings.matchers._subprocess_runner import run_subprocess_with_timeout
from diso_mappings.paths import MATCHERS_DIR
from diso_mappings.io.terminal import debug, info


_DEFAULT_LOGMAP_DIR       = MATCHERS_DIR / "logmap"
_DEFAULT_LOGMAP_JAR_NAME  = "logmap-matcher-4.0.jar"
_DEFAULT_PARAMETERS_NAME  = "parameters.txt"
_DEFAULT_HEAP             = "8g"
_DEFAULT_JAVA_BINARY      = "java"
_LOGMAP_RDF_OUTPUT_NAME = "logmap2_mappings.rdf"
_LOGMAP_JVM_BASE_ARGS = [
    "-Xms500M",
    "-DentityExpansionLimit=10000000",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
]



def _resolve_java_binary(logmap_config: dict) -> str:
    """Java binary resolution order: config['java'] > $JAVA env var > 'java' on PATH"""
    return logmap_config.get("java") or os.environ.get("JAVA") or _DEFAULT_JAVA_BINARY



def _resolve_logmap_dir(logmap_config: dict) -> Path:
    """
    Resolve and verify the directory containing logmap-matcher-4.0.jar +
    parameters.txt. Fails loudly if either is missing.
    """
    logmap_dir         = Path(logmap_config.get("logmap_dir") or _DEFAULT_LOGMAP_DIR).expanduser().resolve()
    logmap_jar         = logmap_dir / _DEFAULT_LOGMAP_JAR_NAME
    logmap_parameters  = logmap_dir / _DEFAULT_PARAMETERS_NAME
    if not logmap_jar.exists():
        raise FileNotFoundError(f"LogMap JAR not found at {logmap_jar}.")
    if not logmap_parameters.exists():
        raise FileNotFoundError(f"LogMap parameters file not found at {logmap_parameters}.")
    return logmap_dir



@register
class LogMap(Matcher):
    """
    LogMap 2; see module docstring.
    """
    name    = "logmap"
    _family = "LogMap"

    @property
    def version(self) -> str:
        return "4.0" # LogMap does not have a reliable version number


    def run(self, source: Path, target: Path, out_dir: Path, config: dict | None = None, timeout: float | None = None) -> MatchResult:

        ##
        # CONFIG + INPUT VALIDATION
        ##

        logmap_config  = config or {}
        classify_flag  = bool(logmap_config.get("classify", False))
        extra_jvm_args = list(logmap_config.get("extra_jvm_args") or [])

        logmap_dir     = _resolve_logmap_dir(logmap_config)
        java_binary    = _resolve_java_binary(logmap_config)
        jvm_heap       = logmap_config.get("heap", _DEFAULT_HEAP)

        if not source.exists():
            raise FileNotFoundError(f"Source ontology not found: {source}")

        if not target.exists():
            raise FileNotFoundError(f"Target ontology not found: {target}")

        out_dir.mkdir(parents=True, exist_ok=True)

        pair_tag        = f"{source.stem}__{target.stem}"
        final_path      = out_dir / f"{pair_tag}.rdf"
        per_pair_logdir = out_dir.parent / "logs" / "per-pair"
        log_path        = per_pair_logdir / f"{pair_tag}.log"

        ##
        # SUBPROCESS INVOCATION
        ##

        with tempfile.TemporaryDirectory(prefix="logmap-", suffix=f"-{pair_tag}") as tmp_cwd:

            tmp_cwd_path = Path(tmp_cwd)

            shutil.copy2(logmap_dir / _DEFAULT_PARAMETERS_NAME, tmp_cwd_path / _DEFAULT_PARAMETERS_NAME)
            
            logmap_output_dir_arg = str(tmp_cwd_path) + os.sep # WARNING: trailing separator is required

            source_uri = source.resolve().as_uri() # must have 'file:' URI
            target_uri = target.resolve().as_uri() # must have 'file:' URI

            logmap_cmd = [
                java_binary,
                f"-Xmx{jvm_heap}",
                *_LOGMAP_JVM_BASE_ARGS,
                *extra_jvm_args,
                "-jar", str(logmap_dir / _DEFAULT_LOGMAP_JAR_NAME),
                "MATCHER",
                source_uri,
                target_uri,
                logmap_output_dir_arg,
                "true" if classify_flag else "false",
            ]

            debug(f"LogMap command: {' '.join(logmap_cmd)}")
            info(f"LogMap: matching {source.name} -> {target.name}")

            start_time = time.time()
            logmap_exit_code = run_subprocess_with_timeout(
                logmap_cmd,
                cwd=tmp_cwd_path,
                timeout=timeout,
                log_path=log_path,
                matcher_display_name="LogMap",
            )
            elapsed = time.time() - start_time

            ##
            # POST-RUN VALIDATION
            ##

            if logmap_exit_code != 0:
                raise RuntimeError(f"LogMap exited with status {logmap_exit_code} on {pair_tag}. See {log_path}.")

            # check output files exist
            logmap_rdf_output = tmp_cwd_path / _LOGMAP_RDF_OUTPUT_NAME
            if not logmap_rdf_output.exists():
                raise RuntimeError(f"LogMap exited cleanly but produced no alignment file at {logmap_rdf_output}.")

            shutil.move(str(logmap_rdf_output), str(final_path))

        return MatchResult(alignment_path=final_path, duration_seconds=elapsed)