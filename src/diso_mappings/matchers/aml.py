"""
AML (AgreementMakerLight) adapter.

Invokes AML v3.2 as a subprocess:

    java -Xmx<heap> -jar <AML_DIR>/AgreementMakerLight.jar -s <source> -t <target> -o <out.rdf> -a

Notes on AML's filesystem behaviour (learned from aml/Main.java + running it):

    * AML launches its Swing GUI if invoked with no args; we pass -a (automatic
      match mode) to force CLI behaviour.
    * AML resolves 'store/config.ini' and 'store/knowledge/' relative to the
      JAR's location, NOT the process CWD — so we don't need to run from the
      JAR's directory.
    * log4j writes 'store/error.log' relative to the CWD. Concurrent runs
      sharing a CWD race on that file. We sidestep this by running each pair
      from a per-pair tempdir.
    * AML already emits OAEI RDF in our canonical plain-text-IRI form
      (see aml/match/Alignment.saveRDF), so we move its output into place
      rather than using our writer; AML's output IS the canonical form we need.

Basic configs/aml.yaml:

    aml_dir:        matchers/aml/AML_v3.2  # directory containing AgreementMakerLight.jar
    java:           java                   # or override via $JAVA env var
    heap:           8g                     # passed as -Xmx
    mode:           auto                   # only 'auto' is supported in v1 of this adapter
    extra_jvm_args: []                     # e.g. ['-Dfile.encoding=UTF-8']

References:
    - AML repo:     https://github.com/AgreementMakerLight/AML-Project
    - AML paper:    Faria et al., ODBASE 2013
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
from diso_mappings.io.terminal import debug, info, warn


_DEFAULT_AML_DIR         = MATCHERS_DIR / "aml" / "AML_v3.2"
_DEFAULT_HEAP            = "8g"
_DEFAULT_JAVA_BINARY     = "java"
_TERMINATE_GRACE_SECONDS = 10.0



def _resolve_java_binary(aml_config: dict) -> str:
    """Java binary resolution order: config['java'] > $JAVA env var > 'java' on PATH"""
    return aml_config.get("java") or os.environ.get("JAVA") or _DEFAULT_JAVA_BINARY



def _resolve_aml_dir(aml_config: dict) -> Path:
    """
    Resolve and verify the directory containing AgreementMakerLight.jar
    Fails loudly if the JAR is missing.
    """
    aml_dir = Path(aml_config.get("aml_dir") or _DEFAULT_AML_DIR).expanduser().resolve()
    aml_jar = aml_dir / "AgreementMakerLight.jar"
    if not aml_jar.exists():
        raise FileNotFoundError(f"AML JAR not found at {aml_jar}.")
    return aml_dir



@register
class AML(Matcher):
    """
    AgreementMakerLight matcher. See module docstring for details.
    """
    name = "aml"
    _family = "AML"

    @property
    def version(self) -> str:
        return "3.2" # TODO: hard-coded for now


    def run(self, source: Path, target: Path, out_dir: Path, config: dict | None = None, timeout: float | None = None) -> MatchResult:
        """
        Since AML is an external JAR, the run method mirrors many of the patterns seen in
        the bertmap workers; basically the logic just lives in this run method instead.
        We seperate BERTMap into a subprocess, to maintain this split between the harness
        and the matching system process. Not required for AML or LogMap, since they are
        by their nature subprocesses (which run from an executable JAR).
        """
        aml_config     = config or {}
        aml_mode       = aml_config.get("mode", "auto")
        extra_jvm_args = list(aml_config.get("extra_jvm_args") or [])

        if aml_mode != "auto": # we only support auto mode for now (TODO: look into repair mode/other)
            raise NotImplementedError(f"AML adapter only supports mode='auto' in v1; got {aml_mode!r}")

        aml_dir     = _resolve_aml_dir(aml_config)
        java_binary = _resolve_java_binary(aml_config)
        jvm_heap    = aml_config.get("heap", _DEFAULT_HEAP)

        if not source.exists():
            raise FileNotFoundError(f"Source ontology not found: {source}")

        if not target.exists():
            raise FileNotFoundError(f"Target ontology not found: {target}")

        out_dir.mkdir(parents=True, exist_ok=True) # ensure output dir exists

        pair_tag        = f"{source.stem}__{target.stem}"
        final_path      = out_dir / f"{pair_tag}.rdf"
        per_pair_logdir = out_dir.parent / "logs" / "per-pair"
        log_path        = per_pair_logdir / f"{pair_tag}.log"

        # per-pair tmp dir as CWD: prevents concurrent runs racing on store/error.log 
        # (log4j writes this relative to CWD). cleaned up automatically on exit.
        with tempfile.TemporaryDirectory(prefix="aml-", suffix=f"-{pair_tag}") as tmp_cwd:

            tmp_cwd_path   = Path(tmp_cwd)
            aml_tmp_output = tmp_cwd_path / "alignment.rdf"

            aml_cmd = [
                java_binary,
                f"-Xmx{jvm_heap}",
                *extra_jvm_args,
                "-jar", str(aml_dir / "AgreementMakerLight.jar"),
                "-a", # automatic match mode
                "-s", str(source.resolve()),
                "-t", str(target.resolve()),
                "-o", str(aml_tmp_output),
            ]

            debug(f"AML command: {' '.join(aml_cmd)}")
            info(f"AML: matching {source.name} -> {target.name}")

            start_time = time.time()
            aml_exit_code = run_subprocess_with_timeout(
                aml_cmd,
                cwd=tmp_cwd_path,
                timeout=timeout,
                log_path=log_path,
                matcher_display_name="AML",
            )
            elapsed = time.time() - start_time

            if aml_exit_code != 0:
                raise RuntimeError(f"AML exited with status {aml_exit_code} on {pair_tag}. See {log_path}.")

            # AML emits "exit 0" for a save failure path (Main.java line 238 calls System.exit(0) inside the catch
            # block after printing an error); so the exit code itself is not proof of success; we check output files exist
            if not aml_tmp_output.exists():
                raise RuntimeError(f"AML exited cleanly but produced no alignment file at {aml_tmp_output}.")

            # AML writes OAEI RDF in our canonical form, so we just move it
            shutil.move(str(aml_tmp_output), str(final_path))

        return MatchResult(alignment_path=final_path, duration_seconds=elapsed)