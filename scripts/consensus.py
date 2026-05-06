"""
SCRIPT: consensus.py
--------------------

Builds the consensus reference alignment over a set of ontology pairs by combining each matchers latest run results. 

Typical invocations:

    1. auto-discovery: pick the latest run for every named matcher under 'runs' dir:

        python scripts/consensus.py --pairs configs/pairs.paper.yaml --matchers aml logmap_lt logmap bertmap_lt bertmap

    2. include a partial-coverage matcher (allowed to fail on some pairs):

        python scripts/consensus.py --pairs configs/pairs.paper.yaml \\
                                    --matchers aml logmap_lt logmap bertmap_lt bertmap \\
                                              matcha logmap_llm \\
                                    --partial-coverage-matchers logmap_llm

    3. explicit override: specify exact run dirs (e.g. for a non-latest reproduction)

        python scripts/consensus.py --pairs configs/pairs.paper.yaml \\
                                    --runs runs/aml/<UTC1> runs/logmap/<UTC2> ...

Most users will should enter via Make: 'make consensus PAIRS=configs/pairs.paper.yaml'.

Output layout
-------------
Each invocation creates a timestamped run directory:

    consensus/<UTC-timestamp>/
    ├── manifest.yaml                    # run metadata + per-pair summary
    └── pairs/
        └── <source>__<target>/
            ├── consensus.tsv            # consolidated; family_votes column
            ├── per-vote/
            │   ├── consensus-2.tsv      # >= 2 votes
            │   ├── consensus-2.rdf      # OAEI RDF, >= 2 votes
            │   └── consensus-N.{tsv,rdf} up to max observed
            ├── unique-by-system/        # one TSV per system (incl. empties)
            ├── unique-by-family/        # one TSV per family (incl. empties)
            └── stats.txt                # OAEI-evaluation format Vote-N statistics

Discovery rules
---------------
Default mode picks the lexicographically-latest run under runs/<matcher>/ for each named matcher. Validation refuses to proceed if:

    * any latest run has a non-empty failed/ directory
    * any expected <src>__<tgt>.rdf is missing from any runs alignments dir
    * the set of alignment filenames differs across runs

Pass --runs explicitly to override (useful for reproducing a specific historical state).

Partial coverage
----------------
Some matchers are expected to fail on a subset of pairs (e.g. LLM-based matchers that exceed context windows on large ontologies). 
Pass them via --partial-coverage-matchers to relax discovery rules: those matchers are allowed to have non-empty `failed/` dirs
and missing alignment files for the failed pairs. They contribute votes for the pairs they did succeed on, and are simply absent 
from the vote table for pairs they failed on. Their per-system and per-family unique-mapping outputs reflect only the pairs they ran on.

Exit Codes
----------
  0 : consensus written successfully for every pair
  1 : configuration error (missing files, invalid pairs, etc.)
  2 : validation failed against a discovered run set
"""
from __future__ import annotations

import argparse
import logging
import sys
import yaml
from datetime import datetime, timezone
from pathlib import Path

from diso_mappings.constants import VERBOSE_LOGGING_PKGS
from diso_mappings.io.alignment import read_alignment
from diso_mappings.io.terminal import (
    configure, debug, error, highlight, info, success, warn,
)
from diso_mappings.matchers import list_matchers
from diso_mappings.pairs import Pair, load_pairs
from diso_mappings.paths import CONSENSUS_DIR, ONTO_REGISTRY_FILE, RUNS_DIR
from diso_mappings.registry import OntologyRegistry

from diso_mappings.consensus import (
    MatcherRun,
    OntologyLabelResolver,
    build_consensus,
    build_runs_from_explicit,
    discover_latest_runs,
    extract_unique_per_family,
    extract_unique_per_system,
    load_vote_tables,
    write_pair_outputs,
    write_pair_stats,
)

##
# CONSTANTS
##

_PAIRS_SUBDIR:        str = "pairs"
_PAIR_STATS_FILENAME: str = "stats.txt"
_MANIFEST_FILENAME:   str = "manifest.yaml"
_DEFAULT_READ_MODE:   str = "lenient"
_RECURSION_LIMIT:     int = 48000   # see run_matcher.py

##
# CLI ENTRY POINT
##

def main() -> int:
    parser = _build_argument_parser()
    args   = parser.parse_args()

    ##
    # SETUP
    ##

    sys.setrecursionlimit(_RECURSION_LIMIT)

    configure(level = logging.DEBUG if args.verbose else logging.INFO, stream = sys.stdout)
    for this_noisy_logger_name in VERBOSE_LOGGING_PKGS:
        logging.getLogger(this_noisy_logger_name).setLevel(logging.WARNING)

    highlight("Starting consensus procedure")

    ##
    # REGISTRY + PAIRS LOAD
    ##

    if not ONTO_REGISTRY_FILE.exists():
        error(f"Registry not found at {ONTO_REGISTRY_FILE}. Run 'make compact-ontos' first.")
        return 1

    onto_registry = OntologyRegistry.load(ONTO_REGISTRY_FILE)

    try:
        pairs = load_pairs(args.pairs, onto_registry)
    except Exception as pair_load_exception:
        error(f"Pair file invalid: {pair_load_exception}")
        return 1

    info(f"Loaded {len(pairs)} pair(s) from {args.pairs}")

    ##
    # RUN DISCOVERY
    ##

    partial_coverage_matchers = frozenset(args.partial_coverage_matchers or ())

    try:
        runs = _resolve_runs(args, partial_coverage_matchers)
    except (FileNotFoundError, ValueError) as discovery_exception:
        error(f"Run discovery failed: {discovery_exception}")
        return 1

    info(f"Resolved {len(runs)} matcher run(s):")
    for this_run in runs:
        registered_marker = "" if this_run.was_registered else " [unregistered fallback]"
        partial_marker = " [partial coverage]" if this_run.partial_coverage else ""
        info(f"  {this_run.matcher_name:14s}  family={this_run.family_name:10s}  {this_run.run_dir}{registered_marker}{partial_marker}")

    ##
    # LOAD VOTE TABLES 
    # (validates internally, registry-aware filename resolution)
    ##

    try:
        vote_tables = load_vote_tables(runs, pairs, onto_registry, read_mode=args.read_mode)
    except (FileNotFoundError, RuntimeError, ValueError) as load_exception:
        error(f"Validation/load failed: {load_exception}")
        return 2

    ##
    # OUTPUT DIRECTORY
    ##

    run_id = _generate_run_id()
    out_dir = (args.output_dir if args.output_dir else (CONSENSUS_DIR / run_id)).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    info(f"Output directory: {out_dir}")

    ##
    # OPTIONAL: LABEL RESOLVER
    ##

    label_resolver: OntologyLabelResolver | None = None
    if not args.no_labels:
        involved_ontology_names = sorted(
            {this_pair.source for this_pair in pairs} | {this_pair.target for this_pair in pairs}
        )
        info(f"Building label resolver over {len(involved_ontology_names)} ontology file(s)")
        label_resolver = OntologyLabelResolver(
            onto_registry,
            ontology_names = involved_ontology_names,
            lowercase_labels = args.lowercase_labels,
        )
    else:
        info("Label resolver disabled (--no-labels)")

    ##
    # PER-PAIR PROCESSING
    ##

    pair_summaries: list[dict] = []

    for this_pair_idx, this_pair in enumerate(pairs, start=1):

        this_vote_table = vote_tables[this_pair]

        info(f"[{this_pair_idx}/{len(pairs)}] {this_pair.source} -> {this_pair.target}: ")
        info(f"    vote_table size={len(this_vote_table)}")
        info(f"    max_family_votes={this_vote_table.max_family_votes()}")

        # build consensus, unique sets
        this_consensus = build_consensus(
            this_vote_table,
            min_family_votes = 2,
            label_resolver = label_resolver,
        )
        this_unique_by_system = extract_unique_per_system(this_vote_table)
        this_unique_by_family = extract_unique_per_family(this_vote_table)

        # ontology IRIs for the OAEI RDF outputs
        this_onto1_iri, this_onto2_iri = _resolve_pair_ontology_iris(
            this_pair, runs, onto_registry,
        )

        # per-pair output dir + write the lot
        this_pair_dir = out_dir / _PAIRS_SUBDIR / f"{this_pair.source}__{this_pair.target}"

        write_pair_outputs(
            consensus_mappings = this_consensus,
            unique_by_system   = this_unique_by_system,
            unique_by_family   = this_unique_by_family,
            onto1_iri          = this_onto1_iri,
            onto2_iri          = this_onto2_iri,
            pair_out_dir       = this_pair_dir,
            label_resolver     = label_resolver,
        )

        write_pair_stats(
            this_consensus,
            this_vote_table,
            this_pair_dir / _PAIR_STATS_FILENAME,
        )

        # collect summary for the manifest
        this_unique_system_total = sum(len(this_keys) for this_keys in this_unique_by_system.values())
        this_unique_family_total = sum(len(this_keys) for this_keys in this_unique_by_family.values())

        pair_summaries.append({
            "source":                 this_pair.source,
            "target":                 this_pair.target,
            "consensus_count":        len(this_consensus),
            "max_family_votes":       this_vote_table.max_family_votes(),
            "unique_by_system_total": this_unique_system_total,
            "unique_by_family_total": this_unique_family_total,
        })

        debug(f"  consensus={len(this_consensus)}, unique-by-system={this_unique_system_total}, unique-by-family={this_unique_family_total}")

    ##
    # MANIFEST
    ##

    manifest = _build_manifest(
        run_id           = run_id,
        pairs_file       = args.pairs,
        out_dir          = out_dir,
        runs             = runs,
        pair_summaries   = pair_summaries,
        read_mode        = args.read_mode,
        label_resolver   = label_resolver,
        lowercase_labels = args.lowercase_labels,
    )
    manifest_path = out_dir / _MANIFEST_FILENAME
    with open(manifest_path, "w") as manifest_file:
        yaml.safe_dump(manifest, manifest_file, sort_keys=False)
    
    debug(f"Manifest: {manifest_path}")

    ##
    # SUMMARY
    ##

    total_consensus = sum(this_summary["consensus_count"] for this_summary in pair_summaries)
    success(f"Done. {len(pairs)} pair(s) processed; {total_consensus} total consensus mappings written. Output: {out_dir}")

    return 0



##
# ARGPARSE
##

def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description = __doc__.split("Output layout")[0],
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--pairs", required=True, type=Path,
        help="Path to pair YAML (see diso_mappings.pairs for schema).",
    )

    # START: matcher_source

    matcher_source = parser.add_mutually_exclusive_group()
    
    matcher_source.add_argument(
        "--matchers", nargs="+", default=None,
        help="Auto-discover the latest runs in runs/<matcher>/ - defaults to every registered matcher.",
    )
    
    matcher_source.add_argument(
        "--runs", nargs="+", type=Path, default=None,
        help="Explicit run directories (one per matcher)."
    )

    # END: matcher_source

    parser.add_argument(
        "--partial-coverage-matchers", nargs="+", default=None, metavar="MATCHER",
        help="Names of matchers permitted to have failures on some pairs (e.g. logmap_llm)."
    )

    parser.add_argument(
        "--extra-runs", nargs="+", default=None, type=Path, metavar="PATH",
        help="Additional matcher run directories to include as distinct systems beyond the auto-discovered set."
    )

    parser.add_argument(
        "--runs-root", type=Path, default=RUNS_DIR,
        help=f"Root directory for auto-discovery (default: {RUNS_DIR}).",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help=f"Override default output directory (default: {CONSENSUS_DIR}/<UTC-timestamp>).",
    )

    parser.add_argument(
        "--no-labels", action="store_true",
        help="Skip rdfs:label resolution. Output TSVs will have empty label cells.",
    )
    parser.add_argument(
        "--lowercase-labels", action="store_true",
        help="Lower-case every resolved label. Default: preserve original case.",
    )

    parser.add_argument(
        "--read-mode", default=_DEFAULT_READ_MODE,
        choices=["lenient", "strict", "draconian"],
        help=f"Tolerance for non-spec-compliant matcher alignment files (default: {_DEFAULT_READ_MODE}).",
    )

    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable DEBUG-level logging.",
    )

    return parser



##
# RUN RESOLUTION
##

def _resolve_runs(args: argparse.Namespace, partial_coverage_matchers: frozenset[str]) -> list[MatcherRun]:
    """
    Resolve the matcher run set from CLI args. Either explicit --runs or auto-discovery.
    """
    if args.runs: # explicit
        runs = build_runs_from_explicit(args.runs, partial_coverage_matchers = partial_coverage_matchers)
    else: # auto-discovery
        matcher_names = args.matchers or list_matchers()
        if not matcher_names:
            raise ValueError("no matchers to resolve; pass --matchers explicitly or ensure at least one matcher is registered")
        runs = discover_latest_runs(
            args.runs_root, matcher_names,
            partial_coverage_matchers = partial_coverage_matchers,
        )

    # append experiment variants, if any
    if args.extra_runs:
        already_resolved_names = { run.matcher_name for run in runs }
        extra_runs = _build_extra_matcher_runs(
            extra_run_dirs            = args.extra_runs,
            partial_coverage_matchers = partial_coverage_matchers,
            already_resolved_names    = already_resolved_names,
        )
        runs.extend(extra_runs)

    return runs



##
# ONTOLOGY IRI RESOLUTION
##

def _resolve_pair_ontology_iris(pair: Pair, runs: list[MatcherRun], registry: OntologyRegistry) -> tuple[str, str]:
    """
    Pick clean ontology iris for pair; OAEI header slots can vary; we are tolerant in lenient mode.
    A small re-parse per pair (already loaded once by load_vote_tables); avoiding it complex iri threading through VoteTable.
    The on-disk filename is constructed via stems of the resolved ontology files (see discovery.py TODO).
    Partial-coverage matchers that have no alignment for this pair are skipped silently (no header/file produced, by design).
    """
    source_stem = registry.resolve(pair.source).path.stem
    target_stem = registry.resolve(pair.target).path.stem
    align_filename = f"{source_stem}__{target_stem}.rdf"

    candidate_iris: list[tuple[str, str]] = []

    for this_run in runs:
        this_align_path = this_run.run_dir / "alignments" / align_filename
        if not this_align_path.exists():
            continue   # partial-coverage matcher, expected
        try:
            this_alignment = read_alignment(this_align_path, mode="lenient")
        except Exception as parse_exception:
            debug(f"  IRI peek: {this_run.matcher_name} alignment unparseable: {parse_exception}")
            continue
        candidate_iris.append((this_alignment.onto1_iri, this_alignment.onto2_iri))

    if not candidate_iris:
        warn(f"could not read ontology IRIs for {pair.source}__{pair.target}; using empty placeholders")
        return ("", "")

    clean_candidates = [
        (this_o1, this_o2) for (this_o1, this_o2) in candidate_iris
        if _is_clean_iri(this_o1) and _is_clean_iri(this_o2)
    ]
    if clean_candidates:
        return clean_candidates[0]

    # all candidates have funky markers; use the first and warn
    warn(f"all matcher alignments for {pair.source}__{pair.target} have non-clean ontology IRIs...")
    warn(f"    ... using first available: {candidate_iris[0]}")
    
    return candidate_iris[0]



def _is_clean_iri(iri: str) -> bool:
    """Detect the AML 'null' and LogMap header bugs"""
    if not iri:
        return False
    if iri == "null":
        return False
    if iri.startswith("Optional.of(") and iri.endswith(")"):
        return False
    return True



##
# MANIFEST
##

def _build_manifest(
    *,
    run_id:           str,
    pairs_file:       Path,
    out_dir:          Path,
    runs:             list[MatcherRun],
    pair_summaries:   list[dict],
    read_mode:        str,
    label_resolver:   OntologyLabelResolver | None,
    lowercase_labels: bool,
) -> dict:
    """
    Compose the manifest.yaml content for the run. Captures enough metadata that a 
    future reader  can trace the consensus output back to its constituent matcher runs
    """
    label_resolver_section: dict
    if label_resolver is None:
        label_resolver_section = {"enabled": False}
    else:
        label_resolver_section = {
            "enabled":          True,
            "lowercase_labels": lowercase_labels,
            "label_count":      len(label_resolver),
        }

    return {
        "run_id":         run_id,
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "pairs_file":     str(pairs_file),
        "output_dir":     str(out_dir),
        "read_mode":      read_mode,
        "label_resolver": label_resolver_section,
        "matchers": [
            {
                "name":             this_run.matcher_name,
                "family":           this_run.family_name,
                "registered":       this_run.was_registered,
                "partial_coverage": this_run.partial_coverage,
                "run_dir":          str(this_run.run_dir),
            }
            for this_run in runs
        ],
        "pairs": pair_summaries,
    }



##
# MISC HELPERS
##

def _build_extra_matcher_runs(
        extra_run_dirs: list[Path], partial_coverage_matchers: frozenset[str], already_resolved_names: set[str]
    ) -> list[MatcherRun]:
    """
    Build MatcherRun entries for experiment-variant run directories.
    Each path must be of the form runs/<parent>/<UTC>__<variant>/. 
    The synthesised system name is '<parent>_<variant>'; 
    the family is inherited from the parent matchers registry entry 
    (or falls back to <parent> with a warning if unregistered).

    Variants are tagged partial-coverage iff their synthesised name is
    in 'partial_coverage_matchers'; the same flag the user already passes
    for other partial matchers — no separate mechanism.
    """
    extras: list[MatcherRun] = []
    seen_names: set[str] = set(already_resolved_names)

    for this_run_dir in extra_run_dirs:
        run_dir_resolved = this_run_dir.expanduser().resolve()
        if not run_dir_resolved.is_dir(): # does not exist
            raise FileNotFoundError(f"--extra-runs path does not exist: {this_run_dir}")

        parent_matcher_name = run_dir_resolved.parent.name
        run_dir_basename = run_dir_resolved.name

        if "__" not in run_dir_basename: # unexpected file name variant
            raise ValueError(f"--extra-runs path {this_run_dir} has no '__<variant>'.")

        variant_suffix = run_dir_basename.split("__", 1)[1]
        synthesised_name = f"{parent_matcher_name}_{variant_suffix}"

        if synthesised_name in seen_names:
            raise ValueError(f"--extra-runs synthesised matcher name {synthesised_name!r} collides with an existing matcher.")
        
        seen_names.add(synthesised_name)

        ##
        # family inheritance from the parent matcher's registry entry
        ##

        try:
            from diso_mappings.matchers import get_matcher
            parent_matcher_instance = get_matcher(parent_matcher_name)
            family_name = parent_matcher_instance.family
        except KeyError:
            warn(f"--extra-runs: parent matcher {parent_matcher_name!r} not registered.") 
            warn(f"  ...falling back to family={parent_matcher_name!r} for {synthesised_name!r}")
            family_name = parent_matcher_name

        is_partial = synthesised_name in partial_coverage_matchers

        extras.append(MatcherRun(
            matcher_name     = synthesised_name,
            family_name      = family_name,
            run_dir          = run_dir_resolved,
            was_registered   = False,   # synthesised name is not in registry
            partial_coverage = is_partial,
        ))

    return extras


def _generate_run_id() -> str:
    """UTC timestamp suitable for filenames/run ids; mirrors run_matcher.py"""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")



if __name__ == "__main__":
    sys.exit(main())