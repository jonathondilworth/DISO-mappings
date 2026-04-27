"""
BERTMapLt worker (diso_mappings.matchers._workers._bertmap_lt_worker) is
the subprocess entry point for BERTMapLt matching.

The adapter (diso_mappings.matchers.bertmap_lt.BERTMapLt) spawns this worker via
    python -m diso_mappings.matchers._workers._bertmap_lt_worker ...
and captures std::out and std::err to a per-pair log file.

The worker should complete one pair, write '<output-dir>/<pair-tag>.rdf' and exit

SEE: _bertmap_worker.py for complementary documentation.
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

# NOTE
# ----
# there is a fair amount of common code shared between BERTMap
# and BERTMapLt (could probably do some refactoring; but at present
# wish to avoid coupling wherever possible; and keep code complexity
# fairly low -- so some duplicate code) -- so you can also refer to 
# _bertmap_worker.py for additional documentation / comments

_DEFAULT_HEAP      = "8g"
_DEFAULT_REASONER  = "elk"

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
    return parser



def _resolve_chosen_tsv(deeponto_output_dir: Path) -> Path:
    """
    Return the final TSV produced by the pipeline
    """
    for this_candidate_name in _CANDIDATE_TSV_FILENAMES:
        candidate_path = deeponto_output_dir / this_candidate_name
        if candidate_path.exists():
            info(f"BERTMapLt: using {this_candidate_name}")
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

    with tempfile.TemporaryDirectory(prefix="bertmaplt-", suffix=f"-{args.pair_tag}") as tmp_dir:

        tmp_dir_path = Path(tmp_dir)

        deeponto_config = BERTMapPipeline.load_bertmap_config(DEFAULT_CONFIG_FILE)
        deeponto_config.model       = "bertmaplt" # <-- disables BERT training phase
        deeponto_config.output_path = str(tmp_dir_path)

        if annotation_property_iris is not None:
            deeponto_config.annotation_property_iris = annotation_property_iris
        
        # leaves num_raw_candidates, num_best_predictions, for_oaei at DeepOnto defaults

        info(f"BERTMapLt: matching {args.source.name} -> {args.target.name}")
        debug(f"BERTMapLt tempdir: {tmp_dir_path}")
        debug(f"BERTMapLt reasoner_type={args.reasoner}")
        
        try:
            src_onto = Ontology(str(args.source.resolve()), reasoner_type=args.reasoner)
            tgt_onto = Ontology(str(args.target.resolve()), reasoner_type=args.reasoner)
            
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
        chosen_tsv = _resolve_chosen_tsv(tmp_dir_path / "bertmaplt" / "match")
        tsv_sidecar_path = args.output_dir / f"{args.pair_tag}.bertmaplt-{chosen_tsv.stem}.tsv"
        shutil.copy2(chosen_tsv, tsv_sidecar_path)
        tsv_mappings = deeponto_tsv_to_mappings(chosen_tsv)

    ##
    # WRITE OAEI RDF
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
    info(f"BERTMapLt: wrote {len(tsv_mappings)} mappings to {final_rdf_path.name} (elapsed {elapsed_time:.1f}s)")
    return 0



def main() -> int:
    parser = _build_arg_parser()
    return worker_main(_implements, parser)



if __name__ == "__main__":
    sys.exit(main())