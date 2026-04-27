"""
BERTMap worker — the subprocess entry point for full BERTMap matching.

The adapter (diso_mappings.matchers.bertmap.BERTMap) spawns this worker via
    python -m diso_mappings.matchers._workers._bertmap_worker ...
and captures std::out and std::err to a per-pair log file.

The worker should complete one pair, write '<output-dir>/<pair-tag>.rdf' and exit

ENVIORNMENT VARIABLES TO BE AWARE OF:

DEEPONTO_LOGMAP_JAR
-------------------

DEEPONTO_LOGMAP_JAR is an enviornment variable that can provide the absolute
path to a (patched) LogMap JAR that DeepOntos 'run_logmap_repair' should use.
Using the official LogMap release JAR has been known to cause NPE issues, so
we suggest re-compiling from source and then using this env variable to specify
a version of LogMap that will not crash with an NPE. It should also be noted
that this is why we MUST include our own vendor fork of DeepOnto (since DeepOnto
itself does not include this functionality; and their JAR will crash on some of
our DISO ontologies).

CUDA_VISIBLE_DEVICES
--------------------

CUDA_VISIBLE_DEVICES essentially decides which 'device' is in use when running 
any CUDA-based script. It should be noted that the adapter is responsible for 
two setting this variable so the 'device' key can be used to specify which GPU 
the user wants to use with DeepOnto, since the library (in its current state) 
will ALWAYS default to (CUDA) device="cuda:0" -- and may even be misleading in
some cases; so we bypass this missing feature with this 'workaround'

CLI ARGUMENTS
-------------

--source PATH       (source ontology file path)
--target PATH       (target ontology file path)
--output-dir PATH   (where to write <pair-tag>.rdf)

--pair-tag TAG
--heap JVM_HEAP
--reasoner TYPE     (reccomended: ELK)

--annotation-property-iris JSON
--bert-model STR
--max-length INT
--num-epochs FLOAT
--batch-size-training INT
--batch-size-prediction INT
--mapping-extension-threshold FLOAT
--mapping-filtered-threshold FLOAT
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

from diso_mappings.io.alignment import Alignment, Mapping, write_alignment
from diso_mappings.io.terminal import debug, info, warn
from diso_mappings.matchers._deeponto_common import (
    deeponto_tsv_to_mappings,
    init_deeponto_jvm,
)
from diso_mappings.matchers._workers._common_workers import (
    add_common_args,
    worker_main,
)

# BERTMap writes to <tmp>/bertmap/match/ -- the pipeline order is:
#   raw -> extended -> filtered -> repaired
# and when model=="bertmap" all four should complete
# care: if latest version of LogMap is not built from source and used
# then BERTMap can fail @ repair via LogMap (for ontos \not\in BioML)

# PRIVATE CONSTANTS -- consider moving to constants.py (beware: coupling? probably OKAY)
# plus, the config overwrites these values; so... there is an argument to made to say that
# actually, the configs should just describe the defaults; similar to how to use pydantic
# BaseModel as a schema in LogMapLLM

# for now, keep it simple

# TODO

_DEFAULT_HEAP                          = "8g"
_DEFAULT_REASONER                      = "elk"
_DEFAULT_BERT_MODEL                    = "bert-base-uncased"
_DEFAULT_MAX_LENGTH                    = 128
_DEFAULT_NUM_EPOCHS                    = 3.0
_DEFAULT_BATCH_SIZE_TRAINING           = 32
_DEFAULT_BATCH_SIZE_PREDICTION         = 128
_DEFAULT_MAPPING_EXTENSION_THRESHOLD   = 0.9
_DEFAULT_MAPPING_FILTERED_THRESHOLD    = 0.9995

_CANDIDATE_TSV_FILENAMES: tuple[str, ...] = (
    "repaired_mappings.tsv",   # post logmap repair
    "filtered_mappings.tsv",   # post threshold filter
    "extended_mappings.tsv",   # post mapping extension
    "raw_mappings.tsv",        # raw candidate selection
)



def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_common_args(parser)
    parser.add_argument(
        "--heap", default=_DEFAULT_HEAP,
        help=f"JVM -Xmx value (default {_DEFAULT_HEAP!r})",
    )
    parser.add_argument(
        "--reasoner", default=_DEFAULT_REASONER,
        help=f"DeepOnto reasoner type (passed verbatim to Ontology(reasoner_type=...)). "
             f"Default {_DEFAULT_REASONER!r}.",
    )
    parser.add_argument(
        "--annotation-property-iris", default=None,
        help="JSON list of annotation-property IRIs; omit to use DeepOnto's defaults.",
    )
    parser.add_argument(
        "--bert-model", default=_DEFAULT_BERT_MODEL,
        help=f"HF model id or local path for the base BERT (default {_DEFAULT_BERT_MODEL!r}).",
    )
    parser.add_argument(
        "--max-length", type=int, default=_DEFAULT_MAX_LENGTH,
        help=f"BERT max_length_for_input (default {_DEFAULT_MAX_LENGTH}).",
    )
    parser.add_argument(
        "--num-epochs", type=float, default=_DEFAULT_NUM_EPOCHS,
        help=f"Number of fine-tuning epochs (default {_DEFAULT_NUM_EPOCHS}).",
    )
    parser.add_argument(
        "--batch-size-training", type=int, default=_DEFAULT_BATCH_SIZE_TRAINING,
        help=f"Training batch size per device (default {_DEFAULT_BATCH_SIZE_TRAINING}).",
    )
    parser.add_argument(
        "--batch-size-prediction", type=int, default=_DEFAULT_BATCH_SIZE_PREDICTION,
        help=f"Prediction batch size per device (default {_DEFAULT_BATCH_SIZE_PREDICTION}).",
    )
    parser.add_argument(
        "--mapping-extension-threshold", type=float,
        default=_DEFAULT_MAPPING_EXTENSION_THRESHOLD,
        help=f"global_matching.mapping_extension_threshold "
             f"(default {_DEFAULT_MAPPING_EXTENSION_THRESHOLD}).",
    )
    parser.add_argument(
        "--mapping-filtered-threshold", type=float,
        default=_DEFAULT_MAPPING_FILTERED_THRESHOLD,
        help=f"global_matching.mapping_filtered_threshold "
             f"(default {_DEFAULT_MAPPING_FILTERED_THRESHOLD}).",
    )
    return parser



def _require_cuda() -> None:
    """
    Fail loudly if CUDA is unavailable (TODO: reconsider this..?)
    we import torch LAZILY -- precisely because its a faily heavy dep
    """
    import torch
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is not available in this worker process. BERTMap requires a GPU (FOR NOW).")
    debug(f"CUDA available!")
    debug(f"  device_count={torch.cuda.device_count()}")
    debug(f"  selected={torch.cuda.get_device_name(0)}")



def _resolve_chosen_tsv(deeponto_output_dir: Path) -> Path:
    """
    (Ideally) Return the final TSV produced by the pipeline:
        'repaired_mappings.tsv'
    Note that BERTMap produces intermediate TSV files; so we can
    fallthrough to return a TSV \in _CANDIDATE_TSV_FILENAMES with
    a warning if neccesary
    """
    for this_candidate_name in _CANDIDATE_TSV_FILENAMES:
        candidate_path = deeponto_output_dir / this_candidate_name
        if candidate_path.exists():
            if this_candidate_name != "repaired_mappings.tsv":
                warn(f"BERTMap: expected {_CANDIDATE_TSV_FILENAMES[0]!r} but found {this_candidate_name!r}.")
                warn("  (possible cause: failed during LogMap repair, check logs)")
            # finally:
            info(f"BERTMap: using {this_candidate_name}")
            return candidate_path
    # should not be possible to reach this point, unless:
    if not deeponto_output_dir.exists():
        raise RuntimeError(f"SDeepOnto directory: {deeponto_output_dir} does not exist.")
    # else:
    raise RuntimeError(f"No usable TSV found in {deeponto_output_dir}.")



def _implements(args: argparse.Namespace) -> int:
    
    if not args.source.exists():
        raise FileNotFoundError(f"Source ontology not found: {args.source}")
    if not args.target.exists():
        raise FileNotFoundError(f"Target ontology not found: {args.target}")
    
    args.output_dir.mkdir(parents=True, exist_ok=True) # mkdir if !exists

    annotation_property_iris: list[str] | None = None
    if args.annotation_property_iris is not None:
        try:
            annotation_property_iris = json.loads(args.annotation_property_iris)
        except json.JSONDecodeError:
            raise ValueError(f"--annotation-property-iris must be valid JSON.")
        if not isinstance(annotation_property_iris, list):
            raise ValueError(f"--annotation-property-iris must be a JSON list.")

    ##
    # CUDA CHECK
    ##

    _require_cuda()

    ##
    # JVM + DEEPONTO BRING-UP
    # DEFER SINCE DEEPONTO REQUIRES JVM
    ##

    init_deeponto_jvm(args.heap)
    from deeponto.onto import Ontology
    from deeponto.align.bertmap import (
        DEFAULT_CONFIG_FILE,
        BERTMapPipeline,
    )

    ##
    # PIPELINE RUN
    ##

    final_rdf_path = args.output_dir / f"{args.pair_tag}.rdf"
    time_at_start = time.time()

    with tempfile.TemporaryDirectory(prefix="bertmap-", suffix=f"-{args.pair_tag}") as tmp_dir:

        tmp_dir_path = Path(tmp_dir)

        # load the DeepOnto default config, then override where appropriate
        # note: the nested structure (config.bert.* and config.global_matching.*) 
        # is DeepOnto's convention; we flatten it at CLI / YAML (easier to read)

        deeponto_config = BERTMapPipeline.load_bertmap_config(DEFAULT_CONFIG_FILE)
        deeponto_config.model       = "bertmap"
        deeponto_config.output_path = str(tmp_dir_path)

        if annotation_property_iris is not None:
            deeponto_config.annotation_property_iris = annotation_property_iris

        # (expected) bert block
        deeponto_config.bert.pretrained_path           = args.bert_model
        deeponto_config.bert.max_length_for_input      = args.max_length
        deeponto_config.bert.num_epochs_for_training   = args.num_epochs
        deeponto_config.bert.batch_size_for_training   = args.batch_size_training
        deeponto_config.bert.batch_size_for_prediction = args.batch_size_prediction

        # global_matching block
        deeponto_config.global_matching.mapping_extension_threshold = args.mapping_extension_threshold
        deeponto_config.global_matching.mapping_filtered_threshold  = args.mapping_filtered_threshold
        
        # leaves num_raw_candidates, num_best_predictions, for_oaei at DeepOnto defaults

        info(f"BERTMap: matching {args.source.name} -> {args.target.name}")

        debug(f"BERTMap tempdir: {tmp_dir_path}")
        debug(f"BERTMap reasoner_type={args.reasoner}, bert_model={args.bert_model}, num_epochs={args.num_epochs}")

        try:
            src_onto = Ontology(str(args.source.resolve()), reasoner_type=args.reasoner)
            tgt_onto = Ontology(str(args.target.resolve()), reasoner_type=args.reasoner)

            ##
            # BERTMapPipeline:
            # Runs on construction:
            #   annotation index -> corpus -> BERT fine-tune 
            #   -> prediction -> extension -> LogMap repair
            ##
            
            BERTMapPipeline(src_onto, tgt_onto, deeponto_config)

        except RuntimeError as matcher_exception:
            exception_msg = str(matcher_exception)
            if "No class annotations found" in exception_msg:
                raise RuntimeError(
                    f"DeepOnto found no class annotations in at least one of the two ontologies: {exception_msg}"
                ) from matcher_exception
            # else:
            raise matcher_exception # all other RuntimeErrors

        elapsed_time = time.time() - time_at_start
        # preserve the chosen TSV alongside the canonical RDF for audit (see if repair ran)
        chosen_tsv = _resolve_chosen_tsv(tmp_dir_path / "bertmap" / "match")
        tsv_sidecar_path = args.output_dir / f"{args.pair_tag}.bertmap-{chosen_tsv.stem}.tsv"
        shutil.copy2(chosen_tsv, tsv_sidecar_path)
        tsv_mappings = deeponto_tsv_to_mappings(chosen_tsv)

    # it is deliberate that we do not preserve fine-tuned BERT weights
    # since they're only reusable across the same pairs (though, it could be nice
    # to introduce this as a feature if many users want to do several runs
    # or we could ship the weights seperately -- in which case we could make
    # the device=cpu an option... probably?) -- in any case, at present we
    # discard the fine-tuned weights (TODO: think about this)

    ##
    # WRITE OAEI RDF (AND COMPLETE)
    ##

    resulting_alignment = Alignment(
        onto1_iri=args.source.resolve().as_uri(),
        onto2_iri=args.target.resolve().as_uri(),
        mappings=[
            Mapping(entity1=src_iri, entity2=tgt_iri, relation="=", measure=score)
            for src_iri, tgt_iri, score in tsv_mappings
        ],
    )
    write_alignment(resulting_alignment, final_rdf_path)
    info(f"BERTMap: wrote {len(tsv_mappings)} mappings to {final_rdf_path.name} (elapsed {elapsed_time:.1f}s)")
    return 0 # success



def main() -> int:
    """
    main: we compose anything from _common_workers and any locally private functions
    then defer to _implements (wrapped by worker_main) -- this may be slightly over
    engineered -- it was suppose to be simple functional composition, but as it turns
    out, there may be less 'things of relevance' shared across workers as was initially
    expected; however, this could change if this continues to be extended...
    """
    parser = _build_arg_parser()
    return worker_main(_implements, parser)



if __name__ == "__main__":
    sys.exit(main())