"""
BERTMap adapter (suppose to be a thin driver): this adapter drives full BERTMap

Config keys (configs/bertmap.yaml; all optional):

    heap:                          str    JVM -Xmx; default '8g'
    reasoner_type:                 str    'elk' | 'hermit' | 'struct'; default 'elk'
    device:                        str    'cuda:0' | 'cuda:1' | ... ; default 'cuda:0'
    bert_model:                    str    HF model id (default 'bert-base-uncased')
    max_length:                    int    BERT max_length (default 128||256) - depends on impl
    num_epochs:                    float  fine-tune epochs (default 3.0)
    batch_size_training:           int    training batch size (default 32)
    batch_size_prediction:         int    prediction batch size (default 128)
    mapping_extension_threshold:   float  (default 0.9)
    mapping_filtered_threshold:    float  (default 0.9995)
    annotation_property_iris:      list   override DeepOnto defaults (optional)
    logmap_jar:                    str    override the project-default LogMap JAR path (optional)

Requires:
    * the patched DeepOnto fork (see environment.yml: deeponto @ git+...)
      this allows us to specify a custom LogMap build with DEEPONTO_LOGMAP_JAR
    * a recently compiled (from source) LogMap JAR at matchers/logmap/logmap-matcher-4.0.jar
    * CUDA

References:
    - DeepOnto: https://github.com/KRR-Oxford/DeepOnto  (fork: jonathondilworth/DeepOnto)
    - BERTMap paper: He et al., AAAI 2022
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from diso_mappings.matchers.base import Matcher, MatchResult, register
from diso_mappings.matchers._deeponto_common import deeponto_version
from diso_mappings.matchers._subprocess_runner import run_subprocess_with_timeout
from diso_mappings.io.terminal import LiveStatusLine, warn, debug, info
from diso_mappings.paths import MATCHERS_DIR


_DEFAULT_HEAP                          = "8g"
_DEFAULT_REASONER                      = "elk"
_DEFAULT_DEVICE                        = "cuda:0"
_DEFAULT_BERT_MODEL                    = "bert-base-uncased"
_DEFAULT_MAX_LENGTH                    = 256
_DEFAULT_NUM_EPOCHS                    = 3.0
_DEFAULT_BATCH_SIZE_TRAINING           = 32
_DEFAULT_BATCH_SIZE_PREDICTION         = 128
_DEFAULT_MAPPING_EXTENSION_THRESHOLD   = 0.9
_DEFAULT_MAPPING_FILTERED_THRESHOLD    = 0.9995

# default project-local location of the patched LogMap JAR, resolved against
# the matchers/ dir (any user can override via the 'logmap_jar' config key)
# TODO: ensure the environment variable DEEPONTO_LOGMAP_JAR overrides this

_DEFAULT_LOGMAP_JAR = MATCHERS_DIR / "logmap" / "logmap-matcher-4.0.jar"

_WORKER_MODULE = "diso_mappings.matchers._workers._bertmap_worker"



def _format_timer_suffix(elapsed_seconds: float) -> str:
    """42.7 -> [00:42] - integer-second resolution (matching BERTMapLt)"""
    minutes, seconds = divmod(int(elapsed_seconds), 60)
    return f"[{minutes:02d}:{seconds:02d}]"



def _resolve_cuda_device_index(device_spec: str) -> str:
    """translate a config 'device' string into the CUDA_VISIBLE_DEVICES value"""
    if device_spec == "cpu":
        raise NotImplementedError("BERTMap on CPU is not currently supported.")

    if device_spec == "cuda":
        return "0" # TODO: check if this is standard (I think it is)

    if device_spec.startswith("cuda:"):
        device_index_str = device_spec[len("cuda:"):]
        if not device_index_str.isdigit(): # should also be non negative
            raise ValueError(f"device={device_spec!r} is not a valid 'cuda:N' specifier.")
        return device_index_str

    warn(f"Supported: 'cuda:N' (e.g. 'cuda:0', 'cuda:1'). 'cpu' is reserved for future support.")
    raise ValueError(f"Unknown device spec: {device_spec!r}.")



def _resolve_logmap_jar(bertmap_config: dict) -> Path:
    """
    Return the absolute path to the LogMap JAR the DeepOnto run_logmap_repair should use.
    Resolution order:
        1. explicit 'logmap_jar' key in configs/bertmap.yaml
        2. project convention path: matchers/logmap/logmap-matcher-4.0.jar
    Fails loudly. TODO: check env variable override works propertly.
    """
    configured = bertmap_config.get("logmap_jar", _DEFAULT_LOGMAP_JAR.resolve())
    logmap_jar_path = Path(configured).expanduser().resolve()
    if not logmap_jar_path.is_file():
        raise FileNotFoundError(f"BERTMap needs LogMap JAR for repair; not found at {logmap_jar_path}")
    return logmap_jar_path



@register
class BERTMap(Matcher):
    """
    Full BERTMap (BERT fine-tuning + LogMap repair); see module docstring.
    """
    name       = "bertmap"
    _family    = "BERTMap"
    show_timer = True

    @property
    def version(self) -> str:
        return deeponto_version() # TODO: consider whether this is appropriate


    def run(self, source: Path, target: Path, out_dir: Path, config: dict | None = None, timeout: float | None = None) -> MatchResult:

        ##
        # CONFIG + INPUT VALIDATION
        ##

        bertmap_config = config or {}

        jvm_heap                    = bertmap_config.get("heap",                        _DEFAULT_HEAP)
        reasoner_type               = bertmap_config.get("reasoner_type",               _DEFAULT_REASONER)
        device_spec                 = bertmap_config.get("device",                      _DEFAULT_DEVICE)
        bert_model                  = bertmap_config.get("bert_model",                  _DEFAULT_BERT_MODEL)
        max_length                  = bertmap_config.get("max_length",                  _DEFAULT_MAX_LENGTH)
        num_epochs                  = bertmap_config.get("num_epochs",                  _DEFAULT_NUM_EPOCHS)
        batch_size_training         = bertmap_config.get("batch_size_training",         _DEFAULT_BATCH_SIZE_TRAINING)
        batch_size_prediction       = bertmap_config.get("batch_size_prediction",       _DEFAULT_BATCH_SIZE_PREDICTION)
        mapping_extension_threshold = bertmap_config.get("mapping_extension_threshold", _DEFAULT_MAPPING_EXTENSION_THRESHOLD)
        mapping_filtered_threshold  = bertmap_config.get("mapping_filtered_threshold",  _DEFAULT_MAPPING_FILTERED_THRESHOLD)
        annotation_property_iris    = bertmap_config.get("annotation_property_iris")

        if not source.exists():
            raise FileNotFoundError(f"Source ontology not found: {source}")
        
        if not target.exists():
            raise FileNotFoundError(f"Target ontology not found: {target}")

        logmap_jar_path  = _resolve_logmap_jar(bertmap_config)
        cuda_device_idx  = _resolve_cuda_device_index(device_spec)

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
            "--source",                      str(source.resolve()),
            "--target",                      str(target.resolve()),
            "--output-dir",                  str(out_dir.resolve()),
            "--pair-tag",                    pair_tag,
            "--heap",                        jvm_heap,
            "--reasoner",                    reasoner_type,
            "--bert-model",                  bert_model,
            "--max-length",                  str(max_length),
            "--num-epochs",                  str(num_epochs),
            "--batch-size-training",         str(batch_size_training),
            "--batch-size-prediction",       str(batch_size_prediction),
            "--mapping-extension-threshold", str(mapping_extension_threshold),
            "--mapping-filtered-threshold",  str(mapping_filtered_threshold),
        ]

        if annotation_property_iris is not None:
            worker_cmd.extend(["--annotation-property-iris", json.dumps(annotation_property_iris)])

        ##
        # ENVIRONMENT ASSEMBLY
        # --------------------
        # DEEPONTO_LOGMAP_JAR -- patched DeepOnto's run_logmap_repair reads this
        # CUDA_VISIBLE_DEVICES -- masks all but the selected GPU for the subprocess
        ##

        worker_env = os.environ.copy()
        worker_env["DEEPONTO_LOGMAP_JAR"]  = str(logmap_jar_path)
        worker_env["CUDA_VISIBLE_DEVICES"] = cuda_device_idx

        debug(f"BERTMap command: {' '.join(worker_cmd)}")
        debug(f"BERTMap env overrides: DEEPONTO_LOGMAP_JAR={logmap_jar_path}, CUDA_VISIBLE_DEVICES={cuda_device_idx}")

        ##
        # RUN + LIVE STATUS LINE
        ##

        prefix = f"BERTMap: matching {source.name} -> {target.name}"

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
                matcher_display_name="BERTMap",
                env=worker_env,
            )

        elapsed = time.time() - start_time

        ##
        # POST-RUN VERIFICATION
        ##
        
        if worker_exit_code != 0:
            raise RuntimeError(f"BERTMap worker exited with status {worker_exit_code} on {pair_tag}. See {log_path}.")

        if not final_rdf_path.exists():
            raise RuntimeError(f"BERTMap worker exited cleanly but produced no alignment file at {final_rdf_path}. See {log_path}.")

        return MatchResult(alignment_path=final_rdf_path, duration_seconds=elapsed)