"""
SCRIPT: run_matcher.py
----------------------

Runs an ontology matcher over a list of ontology pairs. Usage:

    python scripts/run_matcher.py --matcher aml --pairs configs/pairs.example.yaml

Typically, this would be (most easily) called from the included Makefile by:

    make mappings MATCHER=aml PAIRS=configs/pairs.examples.yaml

Briefly, the script loads the OntologyRegistry, the list of ontology pairs,
and the specified matcher. It then iterates through each ontology pair by
invoking the matcher run() function on each, writing alignments, run logs,
errors and so forth to disk. The run directory layout:

    runs/<MATCHER>/<RUN_ID>/
        alignments/*    RDF files produced by the matcher
        failed/         err files for pairs that raised exceptions
        logs/           per-pair matcher logs for debugging/review

"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

from diso_mappings.constants import VERBOSE_LOGGING_PKGS
from diso_mappings.paths import ONTO_REGISTRY_FILE, RUNS_DIR
from diso_mappings.pairs import load_pairs
from diso_mappings.registry import OntologyRegistry
from diso_mappings.matchers import get_matcher, list_matchers, MatchResult
from diso_mappings.io.terminal import configure, highlight, info, success, error, warn, debug
from diso_mappings.io.alignment import read_alignment



def _parse_timeout(timeout_arg: str | None) -> float | None:
    """
    Parse --timeout accepting either a float-like string or:
        {'', 'none', 'null', 'off', 'false'} 
    all of which mean 'no timeout'
    """
    if timeout_arg is None:
        return None
    normalised_arg = timeout_arg.strip().lower()
    if normalised_arg in {"", "none", "null", "off", "false"}:
        return None
    try:
        return float(normalised_arg)
    except ValueError as raised_exception:
        raise argparse.ArgumentTypeError(f"Invalid timeout value: {timeout_arg!r}") from raised_exception



def _generate_run_id() -> str:
    """generates a timestamp suitable for filenames/run ids"""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")



def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--matcher", required=True,
        help=f"Matcher name. Registered: {list_matchers()}",
    )
    parser.add_argument(
        "--pairs", required=True, type=Path, 
        help="Path to pair YAML (see diso_mappings.pairs for schema)"
    )
    parser.add_argument(
        "--config", type=Path, default=None,
        help="Matcher config YAML (matcher-specific keys)"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help="Override default run directory (runs/<matcher>/<run-id>/)"
    )
    parser.add_argument(
        "--timeout", default="3600",
        help="Per-pair timeout in seconds. Use 'none' to disable. Default: 3600.",
    )
    parser.add_argument(
        "--force", action="store_true", 
        help="Re-run pairs that already have an alignment file"
    )
    parser.add_argument(
        "--verbose", action="store_true"
    )
    args = parser.parse_args()

    ##
    # SETUP
    ##

    sys.setrecursionlimit(48000) # this is required for FacilityOntology

    ##
    # LOGGING SETUP
    ##

    configure(
        level=logging.DEBUG if args.verbose else logging.INFO,
        stream=sys.stdout
    )

    for this_noisy_logger_name in VERBOSE_LOGGING_PKGS:
        logging.getLogger(this_noisy_logger_name).setLevel(logging.WARNING)

    highlight("Starting OM procedure!")

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

    info(f"Loaded {len(pairs)} pairs from {args.pairs}")

    ##
    # MATCHER RESOLUTION + CONFIG
    ##

    try:
        matcher = get_matcher(args.matcher)
    except KeyError as unknown_matcher_exception:
        error(str(unknown_matcher_exception))
        return 1

    # NOTE: clean this mess up:

    matcher_config: dict = {}
    config_path: Path | None = None
    if args.config:
        if not args.config.exists():
            error(f"Config file not found: {args.config}")
            return 1
        config_path = args.config
    else:
        default_config_path = Path("configs") / f"{args.matcher}.yaml"
        if default_config_path.is_file():
            config_path = default_config_path

        if config_path is not None:
            info(f"Loading matcher config from {config_path}")
            with open(config_path) as config_file:
                matcher_config = yaml.safe_load(config_file) or {}

        else:
            debug(f"No config file for matcher {args.matcher!r}; using built-in defaults.")

    try:
        per_pair_timeout = _parse_timeout(args.timeout)
    except argparse.ArgumentTypeError as timeout_parse_exception:
        error(str(timeout_parse_exception))
        return 1

    ##
    # RUN DIRECTORY PREPARATION
    ##

    run_id = _generate_run_id()
    run_dir = args.output_dir if args.output_dir else (RUNS_DIR / args.matcher / run_id)
    alignments_dir = run_dir / "alignments"
    logs_dir       = run_dir / "logs"
    failed_dir     = run_dir / "failed"

    for this_dir in (alignments_dir, logs_dir, failed_dir):
        this_dir.mkdir(parents=True, exist_ok=True)

    info(f"Run directory: {run_dir}")
    info(f"Matcher: {matcher.name} (family={matcher.family}, version={matcher.version})")
    info(f"Timeout per pair: {'none' if per_pair_timeout is None else f'{per_pair_timeout:g}s'}")

    ##
    # PAIR ITERATION
    ##

    # outcomes are tracked in a dict rather than a proper manifest for now;
    # TODO: replace with a Manifest object (runs/.../manifest.json) that records
    # config hash, git sha, per-pair duration + mapping count, start/finish times

    pair_outcomes: dict[str, int] = {
        "success": 0, "failed": 0, "timeout": 0, "skipped": 0,
    }

    for pair_idx, this_pair in enumerate(pairs, start=1):

        info(f"[{pair_idx}/{len(pairs)}] {this_pair.source} -> {this_pair.target}")

        src_onto = onto_registry.resolve(this_pair.source)
        tgt_onto = onto_registry.resolve(this_pair.target)

        alignment_filename = f"{this_pair.source}__{this_pair.target}.rdf"
        alignment_out_path = alignments_dir / alignment_filename

        # skip-if-exists is cheap re-run protection; --force overrides
        if alignment_out_path.exists() and not args.force:
            info(f"  already exists, skipping")
            pair_outcomes["skipped"] += 1
            continue

        t_start = time.time()

        try:
            match_result: MatchResult = matcher.run(
                source=src_onto.path,
                target=tgt_onto.path,
                out_dir=alignments_dir,
                config=matcher_config,
                timeout=per_pair_timeout,
            )
            elapsed = time.time() - t_start
            pair_outcomes["success"] += 1

            try:
                resulting_alignment = read_alignment(match_result.alignment_path, mode="lenient")
                mapping_count = len(resulting_alignment.mappings)
                highlight(f"  [√]  {this_pair.source} -> {this_pair.target}  ({match_result.duration_seconds:.1f}s, {mapping_count} mappings)")
            except Exception as read_exception:
                # matcher succeeded but cant parse the output (warn the user: completed but some kind of issue)
                warn(f"  [√]  {this_pair.source} -> {this_pair.target}  ({match_result.duration_seconds:.1f}s, count unavailable: {read_exception})")

        except TimeoutError as timeout_exception:
            elapsed = time.time() - t_start
            failure_msg = f"TimeoutError: {timeout_exception}"
            (failed_dir / f"{this_pair.source}__{this_pair.target}.err").write_text(failure_msg)
            pair_outcomes["timeout"] += 1
            warn(f"  [TIMEOUT]  {this_pair.source} -> {this_pair.target}  timeout after {elapsed:.1f}s")

        except NotImplementedError as not_impl_exception:
            elapsed = time.time() - t_start
            failure_msg = str(not_impl_exception)[:2000]
            (failed_dir / f"{this_pair.source}__{this_pair.target}.err").write_text(failure_msg)
            pair_outcomes["failed"] += 1
            first_line = failure_msg.splitlines()[0] if failure_msg else ""
            error(f"  [X]  {this_pair.source} -> {this_pair.target}  not implemented: {first_line}")

        except Exception as matcher_exception:
            # broad catch: any matcher fails, its recorded 'per-pair' (we dont abort the whole run)
            elapsed = time.time() - t_start
            failure_msg = f"{type(matcher_exception).__name__}: {str(matcher_exception)[:2000]}"
            (failed_dir / f"{this_pair.source}__{this_pair.target}.err").write_text(failure_msg)
            pair_outcomes["failed"] += 1
            error(f"  ✗ {this_pair.source} -> {this_pair.target}  {failure_msg}")

    # END: forall pairs

    ##
    # END-OF-RUN SUMMARY
    ##

    info(
        f"Done. success={pair_outcomes['success']} "
        f"failed={pair_outcomes['failed']} "
        f"timeout={pair_outcomes['timeout']} "
        f"skipped={pair_outcomes['skipped']}"
    )

    info(f"Results: {run_dir}")

    if pair_outcomes["failed"] == 0 and pair_outcomes["timeout"] == 0:
        success(
            f"Done. {pair_outcomes['success']} succeeded"
            + (f", {pair_outcomes['skipped']} skipped" if pair_outcomes['skipped'] else "")
            + "."
        )
    else:
        warn(
            f"Done. success={pair_outcomes['success']} "
            f"failed={pair_outcomes['failed']} "
            f"timeout={pair_outcomes['timeout']} "
            f"skipped={pair_outcomes['skipped']}"
        )

    # non-zero on hard failures so Make can react
    return 0 if (pair_outcomes["failed"] == 0 and pair_outcomes["timeout"] == 0) else 2



if __name__ == "__main__":
    sys.exit(main())