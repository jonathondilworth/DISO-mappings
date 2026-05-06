"""
diso_mappings.consensus.discovery -- run discovery and per-pair VoteTable loading

Bridges the gap between a directory of matcher run results and a populated
VoteTable per ontology pair. This module allows for:

    1. Auto-discovery (default for the consensus CLI):
        works if: expectation for users to follow conventions is met.
        in which case the latest run is determined lexicographically for
        each named matcher. Run IDs are UTC timestamps:
            %Y%m%dT%H%M%SZ (see run_matcher.py)  <-- convention
        so lex sort yields chronological ordering (see docs for details).
    
    2. Explicit run paths: use --runs; for example:
        --runs runs/aml/<UTC-TIMESTAMP> runs/logmap/<UTC-TIMESTAMP> ...

(full coverage) validation is run as follows:

    * any full coverage run has any .err files within failed (raises)
    * any full coverage run has a missing alignment expected from pairs.yaml (raises)
    * the set of alignment filenames differs cross full-coverage runs (raises)

(partial coverage) validation is run as follows:

    * matchers passed via partial_coverage_matchers are permitted to:
        * have a non-empty failed/ subdirectory (under their timstamped run dir)
        * have missing alignment files for any failed pairs
    * still require:
        * alignments UNION failed (subdirs under timestamped run dir) must cover
          every expected pair, else we raise
        * alignments subdir under timestamped run dir must be a subset of the
          full coverage set (which is determined by the first full coverage matcher)

for cases where all matchers are partial coverage, cross-run subset check is skipped

Note: validation is implemented fairly rigorously, as it is neccesary to catch any
issues which may cause problems with the consensus output (since it is then used for
a 'silver-standard' reference alignment downstream; eg. in diso-oaei).

Note on filename resolution: adapters (ie. subclassing the Matcher class to add a
custom matcher) should write alignments using the convention:

        {src.stem}__{tgt.stem}.rdf  (w/ OAEI compliant Alignment API format)

where src and tgt stems are resolved via the ontology filenames. In most cases this
matches the filename stem in _registry.yaml; however, there are some instances where
this may not be the case, therefore, we resolve pair endpoints via OntologyRegistry
responsible for constructing and loading _registry.yaml

TODO: review inconsistency between matchers using stem in run_matcher.py and the
      registry design assumtion where name_id == stem; can be resolved upstream
      by either making matchers use name_id-deriverd filenames or by enforcing 
      the name_id == stem invariant in the registry (currently, we work-around).

References
----------
- Run layout (run_matcher.py):  runs/<matcher>/<UTC>/{alignments,logs,failed}/
- OAEI Alignment Format:        https://moex.gitlabpages.inria.fr/alignapi/format.html
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from diso_mappings.consensus.voting import VoteTable
from diso_mappings.io.alignment import AlignmentReadMode, read_alignment
from diso_mappings.io.terminal import debug, info, warn
from diso_mappings.matchers import get_matcher
from diso_mappings.pairs import Pair
from diso_mappings.registry import OntologyRegistry

##
# PRIVATE CONSTANTS
##

_ALIGNMENTS_SUBDIR: str = "alignments"
_FAILED_SUBDIR:     str = "failed"
_ALIGNMENT_SUFFIX:  str = ".rdf"
_FAILURE_SUFFIX:    str = ".err"
_DIAGNOSTIC_LIST_HEAD: int = 5 # diagnostic trunaction

##
# MATCHER RUN DESCRIPTOR
##

@dataclass(frozen=True)
class MatcherRun:
    """
    Resolved descriptor for a single matcher's run.
    """
    matcher_name:     str           # canonical (registered) matcher identifier (eg. 'aml')
    family_name:      str           # family names registered by _family on extended Matcher cls
    run_dir:          Path          # abs path to run directory, expects '<run_dir>/alignments/<src>__<tgt>.rdf'
    was_registered:   bool          # true iff matcher cls \in registry (use @register decorator on Matcher subclass)
    partial_coverage: bool = False  # true iff matcher is permitted to fail on subset of ontology pairs



##
# DISCOVERY
##

def discover_latest_runs(
    runs_root: Path,
    matcher_names: list[str], *,
    partial_coverage_matchers: frozenset[str] = frozenset(),
) -> list[MatcherRun]:
    """
    Auto-discover the latest run per named matcher under 'runs_root/<matcher>/'.
    partial_coverage_matchers: names of matchers permitted to have expected pair failures.
    raises FileNotFoundError if runs root dir or matcher missing. ValueError if matcher_names is empty.
    """
    if not runs_root.is_dir():
        raise FileNotFoundError(f"runs root does not exist: {runs_root}")

    if not matcher_names:
        raise ValueError("discover_latest_runs: matcher_names must be non-empty")

    discovered_runs: list[MatcherRun] = []

    for this_matcher_name in matcher_names:
        matcher_runs_dir = runs_root / this_matcher_name
        if not matcher_runs_dir.is_dir():
            raise FileNotFoundError(
                f"no runs directory for matcher {this_matcher_name!r} at {matcher_runs_dir}"
            )

        latest_run_dir              = _resolve_latest_run(matcher_runs_dir)
        family_name, was_registered = _resolve_matcher_family(this_matcher_name)
        is_partial                  = this_matcher_name in partial_coverage_matchers

        discovered_runs.append(MatcherRun(
            matcher_name     = this_matcher_name,
            family_name      = family_name,
            run_dir          = latest_run_dir,
            was_registered   = was_registered,
            partial_coverage = is_partial,
        ))

        debug(f"resolved latest run for {this_matcher_name!r}: {latest_run_dir}")

    return discovered_runs



def build_runs_from_explicit(
    run_dirs: list[Path], *,
    partial_coverage_matchers: frozenset[str] = frozenset(),
) -> list[MatcherRun]:
    """
    Build the same MatcherRun list from explicit run dirs given on the CLI.
      * partial_coverage_matchers: names of matchers permitted to have expected pair failures.
      * The matcher name is taken from 'run_dir.parent.name'.
      * We expect users to preserve the <root>/<matcher>/<run_id> convention.
      * Family-based processes resolve via the matcher registery.
      * Fallsback to matcher_name if _family is not set on the subclassed matcher.
      * Duplicate matchers (same matcher_name appearing twice) raise.
    """
    if not run_dirs:
        raise ValueError("build_runs_from_explicit: run_dirs must be non-empty")

    constructed_runs: list[MatcherRun] = []
    seen_matcher_names: set[str] = set()

    for this_run_dir in run_dirs:
        run_dir_resolved = this_run_dir.expanduser().resolve()
        if not run_dir_resolved.is_dir():
            raise FileNotFoundError(f"explicit run path does not exist: {this_run_dir}")

        derived_matcher_name = run_dir_resolved.parent.name

        if not derived_matcher_name:
            raise ValueError(f"underivable matcher name from {this_run_dir} (parent dir name empty)")
        
        if derived_matcher_name in seen_matcher_names:
            raise ValueError(f"matcher {derived_matcher_name!r} appears more than once in --runs.")
        
        seen_matcher_names.add(derived_matcher_name)

        family_name, was_registered = _resolve_matcher_family(derived_matcher_name)
        is_partial                  = derived_matcher_name in partial_coverage_matchers

        constructed_runs.append(MatcherRun(
            matcher_name     = derived_matcher_name,
            family_name      = family_name,
            run_dir          = run_dir_resolved,
            was_registered   = was_registered,
            partial_coverage = is_partial,
        ))

    return constructed_runs



##
# VALIDATION
##

def validate_runs(runs: list[MatcherRun], pairs: list[Pair], registry: OntologyRegistry) -> None:
    """
    Fail loudly and ealry during validation of a MatcherRun list against the expected pair set.

    For full-coverage runs:
      * 'alignments/' must contain every expected file
      * 'failed/' must be empty
      * Cross-run: filename sets across all full-coverage runs must agree

    For partial-coverage runs:
      * 'failed/' may be non-empty
      * 'alignments/' may be a subset of the expected pair set
      * 'alignments/ UNION failed/' must cover the expected pair set
      * Cross-run: 'alignments/' must be a subset of the full-coverage
        reference set (skipped if every run is partial)

    registry is used to resolve each pair's endpoints to filename stems
    Does not parse the alignment files (that happens in load_vote_tables)
    """
    if not runs:
        raise ValueError("validate_runs: runs must be non-empty")
    if not pairs:
        raise ValueError("validate_runs: pairs must be non-empty")

    expected_filenames: frozenset[str] = frozenset(
        _expected_alignment_filename(this_pair, registry) for this_pair in pairs
    )
    expected_failure_basenames: frozenset[str] = frozenset(
        f.removesuffix(_ALIGNMENT_SUFFIX) for f in expected_filenames
    )

    per_run_alignment_filenames: dict[str, frozenset[str]] = {}

    for this_run in runs: # alignments/ must exist on every run, partial or not
        alignments_dir = this_run.run_dir / _ALIGNMENTS_SUBDIR
        if not alignments_dir.is_dir():
            raise FileNotFoundError(
                f"matcher {this_run.matcher_name!r}: alignments directory missing at {alignments_dir}"
            )

        present_alignments = frozenset(
            file.name for file in alignments_dir.iterdir()
            if file.is_file() and file.suffix == _ALIGNMENT_SUFFIX
        )
        
        present_failures = _list_failure_basenames(this_run)

        if this_run.partial_coverage:
            _validate_partial_coverage_run(
                this_run,
                expected_filenames,
                expected_failure_basenames,
                present_alignments,
                present_failures,
            )
        else:
            _validate_full_coverage_run(
                this_run,
                expected_filenames,
                present_alignments,
                present_failures,
            )

        per_run_alignment_filenames[this_run.matcher_name] = present_alignments

    ##
    # cross-run: full-coverage runs must agree exactly; partial-coverage 
    # runs must be a subset of the full-coverage reference. If every run 
    # is partial-coverage there's no reference to compare to, so we skip.
    # we assume the first matcher that is not partial coverage sets the 
    # standard for full coverage runs.
    ##

    full_coverage_names = [r.matcher_name for r in runs if not r.partial_coverage]
    if not full_coverage_names:
        debug("validate_runs: every run is partial-coverage; skipping cross-run subset check")
        return # successful validation

    # first full coverage matcher sets the standard
    reference_matcher = full_coverage_names[0]
    reference_set     = per_run_alignment_filenames[reference_matcher]

    # check (validate) full-coverage runs
    for this_full_name in full_coverage_names[1:]:
        this_set = per_run_alignment_filenames[this_full_name]
        if this_set == reference_set:
            continue # skip (identity)
        in_other_only     = sorted(this_set - reference_set)[:_DIAGNOSTIC_LIST_HEAD]
        in_reference_only = sorted(reference_set - this_set)[:_DIAGNOSTIC_LIST_HEAD]
        raise ValueError(
            f"alignment filename sets differ between full-coverage runs:\n"
            f"  {reference_matcher!r} has {len(reference_set)} files\n"
            f"  {this_full_name!r} has {len(this_set)} files\n"
            f"  in {this_full_name!r} only: {in_other_only}\n"
            f"  in {reference_matcher!r} only: {in_reference_only}"
        )

    # check (validate) partial coverage runs: (partial \subseteq full)
    for this_run in runs:
        if not this_run.partial_coverage:
            continue # encountered a full coverage run; skip
        this_set = per_run_alignment_filenames[this_run.matcher_name]
        excess   = this_set - reference_set
        if excess:
            sample_excess  = sorted(excess)[:_DIAGNOSTIC_LIST_HEAD]
            more_indicator = (
                f" (and {len(excess) - _DIAGNOSTIC_LIST_HEAD} more)"
                if len(excess) > _DIAGNOSTIC_LIST_HEAD else ""
            )
            raise ValueError(
                f"partial-coverage matcher {this_run.matcher_name!r}: "
                f"alignments/ contains {len(excess)} file(s) not in the "
                f"full-coverage reference set ({reference_matcher!r}): "
                f"{sample_excess}{more_indicator}"
            )



##
# LOAD VOTE TABLES
##

def load_vote_tables(
    runs: list[MatcherRun], pairs: list[Pair], registry: OntologyRegistry, 
    *, read_mode: AlignmentReadMode = "lenient", # TODO: cretae Enum for AlignmentReadModes
) -> dict[Pair, VoteTable]:
    """
    Per-pair VoteTable construction; note: 
      * validates 'runs' and 'pairs'.
      * missing alignments are permitted for partial coverage matchers; 
          these contribute 0 votes to the missing pair.
      * resolves each pair via filename stem-based registry (see: _registry.yaml).
      * due to the 'liberal' payloads provided by matchers, we default to 'read_mode: lenient'
        (see diso_mappings.io.alignment for modes: [lenient, strict, draconian])
    """
    validate_runs(runs, pairs, registry)

    # warn once per unregistered matcher 
    # (see backlog TODO)
    for this_run in runs:
        if not this_run.was_registered:
            warn(f"matcher {this_run.matcher_name!r} is not in the registry; fallback to {this_run.family_name!r}")

    # log partial-coverage participants once
    partial_names = sorted(r.matcher_name for r in runs if r.partial_coverage)
    if partial_names:
        info(f"partial-coverage matchers: {partial_names}")

    info(f"loading {len(pairs)} pair(s) across {len(runs)} matcher(s) in mode {read_mode!r}")

    vote_tables: dict[Pair, VoteTable] = {}

    ##
    # LOAD: CUMULATIVE VOTE TABLE CONSTRUCTION
    # ----------------------------------------
    # construct a VoteTable for each pair (for which a set of runs belongs to)
    # check each run for full vs partial coverage, skip empty partial coverage
    # otherwise: load alignment and construct VoteTable (iteratively)
    ##

    for this_pair in pairs:
        this_table     = VoteTable()
        align_filename = _expected_alignment_filename(this_pair, registry)

        for this_run in runs:
            align_path = this_run.run_dir / _ALIGNMENTS_SUBDIR / align_filename

            if not align_path.exists():
                if this_run.partial_coverage:
                    debug(f"{this_pair.source}__{this_pair.target}: {this_run.matcher_name!r} has no alignment.")
                    debug(f"  ^->  (permitted — partial coverage) ... ")
                    continue # do not raise, see ^^^ we expect to hit here
                # else: (full-coverage) -> something is wrong (raise)
                raise FileNotFoundError(
                    f"matcher {this_run.matcher_name!r}: {this_pair.source} -> {this_pair.target}"
                    f" filepath has mutated since validation. Expected: {align_path} to exist."
                )

            try:
                this_alignment = read_alignment(align_path, mode=read_mode)
            except Exception as parse_exception:
                raise RuntimeError(
                    f"matcher {this_run.matcher_name!r}: failed to parse alignment for pair: "
                    f"{this_pair.source} -> {this_pair.target} ({align_path}): {parse_exception}"
                ) from parse_exception

            this_table.add(
                system    = this_run.matcher_name,
                family    = this_run.family_name,
                alignment = this_alignment,
            ) # iterative vote table construction (loop)

        vote_tables[this_pair] = this_table # assign & loop until all pairs
        
        debug(f"VOTE TABLE ASSIGNMENT.")
        debug(f"  {this_pair.source}__{this_pair.target} successful, check:")
        debug(f"    vote_table size:  {len(this_table)}")
        debug(f"    max_family_votes: {this_table.max_family_votes()}")
        debug(f"END OF: VOTE TABLE ASSIGNMENT.")

    return vote_tables



##
# PRIVATE HELPERS — VALIDATION
##

def _validate_full_coverage_run(
    matcher_run: MatcherRun,
    expected_filenames: frozenset[str],
    present_alignments: frozenset[str],
    present_failures: list[Path],
) -> None:
    """
    Per-run validation for full-coverage runs: failed/ must be empty,
    alignments/ must contain every expected file.
    """
    if present_failures: # take the first (in this case, 5) failures and raise w/ details
        sample_failures = [failure.name for failure in present_failures[:_DIAGNOSTIC_LIST_HEAD]]
        more_indicator = (
            f" (and {len(present_failures) - _DIAGNOSTIC_LIST_HEAD} more)"
            if len(present_failures) > _DIAGNOSTIC_LIST_HEAD else ""
        )
        raise RuntimeError(
            f"matcher {matcher_run.matcher_name!r}: latest run has "
            f"{len(present_failures)} failure(s) in {matcher_run.run_dir / _FAILED_SUBDIR}: "
            f"{sample_failures}{more_indicator}. Resolve the failures. "
            f"Invoke with --runs explicitly, or pass --partial-coverage-matchers: "
            f"{matcher_run.matcher_name} if this matcher is expected to fail on some pairs."
        )

    if not present_alignments:
        raise FileNotFoundError(
            f"matcher {matcher_run.matcher_name!r}: "
            f"no alignment files in {matcher_run.run_dir / _ALIGNMENTS_SUBDIR}"
        )

    missing_for_run = expected_filenames - present_alignments
    if missing_for_run: # take the HEAD instances and report
        sample_missing = sorted(missing_for_run)[:_DIAGNOSTIC_LIST_HEAD]
        more_indicator = (
            f" (and {len(missing_for_run) - _DIAGNOSTIC_LIST_HEAD} more)"
            if len(missing_for_run) > _DIAGNOSTIC_LIST_HEAD else ""
        )
        raise FileNotFoundError(
            f"matcher {matcher_run.matcher_name!r}: "
            f"missing {len(missing_for_run)} expected alignment file(s) in "
            f"{matcher_run.run_dir / _ALIGNMENTS_SUBDIR}: "
            f"{sample_missing}{more_indicator}"
        )



def _validate_partial_coverage_run(
    matcher_run:                MatcherRun,
    expected_filenames:         frozenset[str],
    expected_failure_basenames: frozenset[str],
    present_alignments:         frozenset[str],
    present_failures:           list[Path],
) -> None:
    """
    Per-run validation for partial-coverage runs: 
        alignments/ UNION failed/ 
    must cover the expected pair set; each pair must be counted 
    as either: successful alignment || recorded failure.
    """
    present_alignment_basenames = frozenset(
        file.removesuffix(_ALIGNMENT_SUFFIX) for file in present_alignments
    )
    present_failure_basenames = frozenset(file.stem for file in present_failures)

    accounted_basenames = present_alignment_basenames | present_failure_basenames
    unaccounted = expected_failure_basenames - accounted_basenames

    if unaccounted: # discrepency between expected and actual (validation)
        sample_unaccounted = sorted(unaccounted)[:_DIAGNOSTIC_LIST_HEAD]
        more_indicator = (
            f" (and {len(unaccounted) - _DIAGNOSTIC_LIST_HEAD} more)"
            if len(unaccounted) > _DIAGNOSTIC_LIST_HEAD else ""
        )
        raise FileNotFoundError(
            f"partial-coverage matcher {matcher_run.matcher_name!r}: "
            f"{len(unaccounted)} expected pair(s) have neither an alignment "
            f"in {_ALIGNMENTS_SUBDIR}/ nor a failure record in {_FAILED_SUBDIR}/: "
            f"{sample_unaccounted}{more_indicator}. Either re-run the matcher on "
            f"those pairs or supply --runs explicitly."
        )

    # check whether an alignment file AND a failure file exists for pairs
    overlap = present_alignment_basenames & present_failure_basenames
    if overlap: # something is wrong, report:
        sample_overlap = sorted(overlap)[:_DIAGNOSTIC_LIST_HEAD]
        raise RuntimeError(
            f"partial-coverage matcher {matcher_run.matcher_name!r}: "
            f"{len(overlap)} pair(s) have both an alignment and a failure "
            f"record (which should be mutually exclusive): {sample_overlap}"
        )



def _list_failure_basenames(matcher_run: MatcherRun) -> list[Path]:
    failed_dir = matcher_run.run_dir / _FAILED_SUBDIR
    if not failed_dir.is_dir():
        return [] # no .err files present
    return sorted(
        file for file in failed_dir.iterdir()
        if file.is_file() and file.suffix == _FAILURE_SUFFIX
    ) # list of all .err files



##
# PRIVATE HELPERS — DISCOVERY
##

def _resolve_latest_run(matcher_runs_dir: Path) -> Path:
    """
    Pick the latest subdir of 'matcher_runs_dir' (determined lexiographically) as
    that matcher's latest run. Raise FileNotFoundError if there are no subdirs.
    """
    candidate_runs = sorted(p for p in matcher_runs_dir.iterdir() if p.is_dir())
    if not candidate_runs:
        raise FileNotFoundError(f"no runs found under {matcher_runs_dir} (expected subdirs)")
    return candidate_runs[-1].resolve() # rotate backwards to final tail entry



def _resolve_matcher_family(matcher_name: str) -> tuple[str, bool]:
    """
    Look up the family declared by the matcher's class. Returns (family_name, was_registered). 
    Falls back to (matcher_name, False) when the matcher is not in the registry.
    """
    try:
        matcher_instance = get_matcher(matcher_name)
        return matcher_instance.family, True
    except KeyError:
        return matcher_name, False



def _expected_alignment_filename(pair: Pair, registry: OntologyRegistry) -> str:
    """
    The alignment filename produced by the matcher adapters for 'pair'.
    Note: matcher adapters (ie. subclassed Matcher classes) use:
        {src.stem}__{tgt.stem}.rdf  (w/ OAEI compliant Alignment API format)
    where src and tgt stems are resolved via the ontology filenames.
    This has caused some 'transient' errors previously (TODO: reconsider
    how the registry functions in this respect); so we explicitly search
    the registry for the actual (registered) stem.
    """
    source_stem = registry.resolve(pair.source).path.stem
    target_stem = registry.resolve(pair.target).path.stem
    return f"{source_stem}__{target_stem}{_ALIGNMENT_SUFFIX}"