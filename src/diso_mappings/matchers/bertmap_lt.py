"""
BERTMapLt adapter (thin driver): this adapter drives lite BERTMap (BERTMapLt)

Config keys (configs/bertmap_lt.yaml; all optional):

    heap:                     str   JVM -Xmx; default '8g'
    reasoner_type:            str   'elk' | 'hermit' | 'struct'; default 'elk'
    annotation_property_iris: list  override DeepOnto's default annotation-property IRIs

Any key not set falls back to DeepOnto's default; see:

    https://krr-oxford.github.io/DeepOnto/bertmap/

References:
    - DeepOnto: https://github.com/KRR-Oxford/DeepOnto
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from diso_mappings.matchers.base import Matcher, MatchResult, register
from diso_mappings.matchers._deeponto_common import deeponto_version
from diso_mappings.matchers._subprocess_runner import run_subprocess_with_timeout
from diso_mappings.io.terminal import LiveStatusLine, debug


_DEFAULT_HEAP     = "8g"
_DEFAULT_REASONER = "elk"
_WORKER_MODULE = "diso_mappings.matchers._workers._bertmap_lt_worker"


def _format_timer_suffix(elapsed_seconds: float) -> str:
    """42.7 -> [00:42] - integer-second resolution (matching BERTMapLt)"""
    minutes, seconds = divmod(int(elapsed_seconds), 60)
    return f"[{minutes:02d}:{seconds:02d}]"


@register
class BERTMapLt(Matcher):
    """
    Lightweight BERTMap: string-distance-based, no fine-tuning. see module docstring.
    """
    name        = "bertmap_lt"
    _family     = "BERTMap"
    show_timer  = True

    @property
    def version(self) -> str:
        return deeponto_version() # TODO: consider whether this is appropriate


    def run(self, source: Path, target: Path, out_dir: Path, config: dict | None = None, timeout: float | None = None) -> MatchResult:

        ##
        # CONFIG + INPUT VALIDATION
        ##

        bertmap_config           = config or {}

        jvm_heap                 = bertmap_config.get("heap", _DEFAULT_HEAP)
        reasoner_type            = bertmap_config.get("reasoner_type", _DEFAULT_REASONER)
        annotation_property_iris = bertmap_config.get("annotation_property_iris")

        if not source.exists():
            raise FileNotFoundError(f"Source ontology not found: {source}")
        
        if not target.exists():
            raise FileNotFoundError(f"Target ontology not found: {target}")

        out_dir.mkdir(parents=True, exist_ok=True)

        pair_tag        = f"{source.stem}__{target.stem}"
        final_rdf_path  = out_dir / f"{pair_tag}.rdf"
        per_pair_logdir = out_dir.parent / "logs" / "per-pair"
        log_path        = per_pair_logdir / f"{pair_tag}.log"
        
        ##
        # COMMAND ASSEMBLY
        ##

        worker_cmd: list[str] = [
            sys.executable, "-m", _WORKER_MODULE,
            "--source",     str(source.resolve()),
            "--target",     str(target.resolve()),
            "--output-dir", str(out_dir.resolve()),
            "--pair-tag",   pair_tag,
            "--heap",       jvm_heap,
            "--reasoner",   reasoner_type,
        ]

        if annotation_property_iris is not None:
            worker_cmd.extend(["--annotation-property-iris", json.dumps(annotation_property_iris)])

        debug(f"BERTMapLt command: {' '.join(worker_cmd)}")

        ##
        # RUN + LIVE STATUS LINE
        ##

        prefix = f"BERTMapLt: matching {source.name} -> {target.name}"

        start_time = time.time()

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
                matcher_display_name="BERTMapLt",
            )
        
        elapsed = time.time() - start_time

        ##
        # POST-RUN VERIFICATION
        ##

        if worker_exit_code != 0:
            raise RuntimeError(f"BERTMapLt worker exited with status {worker_exit_code} on {pair_tag}. See {log_path}.")

        return MatchResult(alignment_path=final_rdf_path, duration_seconds=elapsed)