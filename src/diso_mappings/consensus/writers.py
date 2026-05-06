"""
diso_mappings.consensus.writers: TSV and OAEI RDF emitters for consensus and unique outputs.

Per-pair output layout produced by write_pair_outputs:

    <pair_out_dir>/
    ├── consensus.tsv                # consolidated; family_votes is a column
    ├── per-vote/
    │   ├── consensus-2.tsv          # Java-faithful >= 2 family votes
    │   ├── consensus-2.rdf          # OAEI RDF      >= 2 family votes
    │   ├── consensus-3.tsv / .rdf   #               >= 3 family votes
    │   └── consensus-N.tsv / .rdf   # up to max observed in the input
    ├── unique-by-system/
    │   └── <system>.tsv             # one per system; empty TSVs included
    └── unique-by-family/
        └── <family>.tsv             # one per family; empty TSVs included


Unique TSV is 5 columns (URI1, label1, URI2, label2, relation).
TODO: include CLS/INST/OPROP/DPROP/UNKNN. Requires additional parse.
(probably not required right now?) -- will implement later when/if neccesary.

see: 
  * https://github.com/ernestojimenezruiz/oaei-evaluation
    - CreateConsensusAlignments.storeReadableAlignments
    - ExtendedFlatTSVAlignmentFormat (LogMap2)
  * https://moex.gitlabpages.inria.fr/alignapi/format.html

"""
from __future__ import annotations

from pathlib import Path

from diso_mappings.consensus.consensus import ConsensusMapping, LabelResolver
from diso_mappings.consensus.voting import VoteKey
from diso_mappings.io.alignment import Alignment, Mapping, write_alignment
from diso_mappings.io.terminal import debug, info


##
# PRIVATE CONSTANTS
# -----------------
# Column headers, filenames, and separators.
##

_CONSENSUS_TSV_HEADER: str = (
    "#URI 1\tLabel 1\tURI 2\tLabel 2\t"
    "Confidence\tFamily votes\tFamilies\tSystem votes\tSystems"
)

_UNIQUE_TSV_HEADER:    str = "#URI 1\tLabel 1\tURI 2\tLabel 2\tRelation"

_MULTI_VALUE_SEPARATOR:    str = ";"   # within families/systems cells
_DEFAULT_UNIQUE_RELATION:  str = "="   # relation handling: always consider as \equiv (by design)
_MIN_REQUIRED_VOTES:       int = 2     # per-vote loop start; matches build_consensus threshold

_PER_VOTE_SUBDIR:          str = "per-vote"
_CONSOLIDATED_TSV_NAME:    str = "consensus.tsv"
_UNIQUE_BY_SYSTEM_SUBDIR:  str = "unique-by-system"
_UNIQUE_BY_FAMILY_SUBDIR:  str = "unique-by-family"
_TSV_SUFFIX:               str = ".tsv"
_RDF_SUFFIX:               str = ".rdf"



##
# CONSENSUS TSV WRITERS
##

def write_consensus_tsv(consensus_mappings: list[ConsensusMapping], out_path: Path) -> Path:
    """
    Emit the consolidated consensus TSV at out_path.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines_to_write: list[str] = [_CONSENSUS_TSV_HEADER]
    for this_mapping in consensus_mappings:
        lines_to_write.append(_format_consensus_row(this_mapping))
    out_path.write_text("\n".join(lines_to_write) + "\n")
    return out_path



def write_consensus_tsv_per_vote(consensus_mappings: list[ConsensusMapping], out_dir: Path) -> list[Path]:
    """
    Emit one TSV per family-vote level under out_dir/consensus-N.tsv
    """
    if not consensus_mappings:
        return []

    out_dir.mkdir(parents=True, exist_ok=True)

    table_max_votes = max(this_mapping.family_votes for this_mapping in consensus_mappings)
    written_paths: list[Path] = []

    for this_min_votes in range(_MIN_REQUIRED_VOTES, table_max_votes + 1):
        filtered_mappings = _filter_by_min_votes(consensus_mappings, this_min_votes)
        this_out_path     = out_dir / f"consensus-{this_min_votes}{_TSV_SUFFIX}"
        write_consensus_tsv(filtered_mappings, this_out_path)
        written_paths.append(this_out_path)

    return written_paths



##
# CONSENSUS RDF WRITER
##

def write_consensus_rdf_per_vote(consensus_mappings: list[ConsensusMapping], out_dir: Path, onto1_iri: str, onto2_iri: str) -> list[Path]:
    """
    Emit one OAEI RDF alignment per family-vote level under out_dir/consensus-N.rdf 
    Reuses `diso_mappings.io.alignment.write_alignment`, so output is in the canonical 
    level-0 OAEI form consumable by MELT and OAEI track evaluators.
    """
    if not consensus_mappings:
        return []

    out_dir.mkdir(parents=True, exist_ok=True)

    table_max_votes = max(this_mapping.family_votes for this_mapping in consensus_mappings)
    written_paths: list[Path] = []

    for this_min_votes in range(_MIN_REQUIRED_VOTES, table_max_votes + 1):
        filtered_mappings = _filter_by_min_votes(consensus_mappings, this_min_votes)
        filtered_alignment = _consensus_to_alignment(filtered_mappings, onto1_iri, onto2_iri)
        this_out_path = out_dir / f"consensus-{this_min_votes}{_RDF_SUFFIX}"
        write_alignment(filtered_alignment, this_out_path)
        written_paths.append(this_out_path)

    return written_paths



##
# UNIQUE TSV WRITER
##

def write_unique_tsv(
    unique_by_key: dict[str, list[VoteKey]], out_dir: Path, *, 
    label_resolver: LabelResolver | None = None,
) -> list[Path]:
    """
    Emit one TSV per key (system or family) under out_dir/<key>.tsv.
    Empty entries (keys with no unique mappings) still produce a header-only TSV
    The same function services both unique-by-system and unique-by-family
    outputs; the caller picks the directory and supplies the appropriate
    dict from extract_unique_per_system or extract_unique_per_family
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    written_paths: list[Path] = []

    for this_key_name in sorted(unique_by_key):
        this_unique_keys = unique_by_key[this_key_name]
        this_out_path    = out_dir / f"{this_key_name}{_TSV_SUFFIX}"

        lines_to_write: list[str] = [_UNIQUE_TSV_HEADER]
        for this_vote_key in this_unique_keys:
            this_label1 = label_resolver.label_for(this_vote_key.entity1) if label_resolver else None
            this_label2 = label_resolver.label_for(this_vote_key.entity2) if label_resolver else None
            lines_to_write.append(_format_unique_row(
                this_vote_key,
                this_label1,
                this_label2,
                _DEFAULT_UNIQUE_RELATION,
            ))

        this_out_path.write_text("\n".join(lines_to_write) + "\n")
        written_paths.append(this_out_path)

    return written_paths



##
# ORCHESTRATOR
##

def write_pair_outputs(
    consensus_mappings: list[ConsensusMapping],
    unique_by_system:   dict[str, list[VoteKey]],
    unique_by_family:   dict[str, list[VoteKey]],
    onto1_iri:          str,
    onto2_iri:          str,
    pair_out_dir:       Path,
    *,
    label_resolver:     LabelResolver | None = None,
) -> None:
    """
    Lay down the full per-pair output structure under pair_out_dir; used
    by the consensus CLI driver; component callers can use the lower-level
    write_* functions individually if they need only a subset.
    """
    pair_out_dir.mkdir(parents=True, exist_ok=True)

    # consolidated consensus TSV at the pair root
    consolidated_path = pair_out_dir / _CONSOLIDATED_TSV_NAME
    write_consensus_tsv(consensus_mappings, consolidated_path)
    debug(f"  wrote {consolidated_path.name} ({len(consensus_mappings)} rows)")

    # per-vote TSVs and RDFs in the same subdir
    per_vote_dir = pair_out_dir / _PER_VOTE_SUBDIR
    tsv_per_vote_paths = write_consensus_tsv_per_vote(consensus_mappings, per_vote_dir)
    rdf_per_vote_paths = write_consensus_rdf_per_vote(consensus_mappings, per_vote_dir, onto1_iri, onto2_iri)
    debug(f"  wrote {len(tsv_per_vote_paths)} per-vote TSV(s) and {len(rdf_per_vote_paths)} per-vote RDF(s) under {per_vote_dir.name}/")

    # unique-by-system
    by_system_paths = write_unique_tsv(
        unique_by_system,
        pair_out_dir / _UNIQUE_BY_SYSTEM_SUBDIR,
        label_resolver = label_resolver,
    )
    debug(f"  wrote {len(by_system_paths)} unique-by-system TSV(s)")

    # unique-by-family
    by_family_paths = write_unique_tsv(
        unique_by_family,
        pair_out_dir / _UNIQUE_BY_FAMILY_SUBDIR,
        label_resolver = label_resolver,
    )
    debug(f"  wrote {len(by_family_paths)} unique-by-family TSV(s)")



##
# PRIVATE HELPERS
##

def _filter_by_min_votes(consensus_mappings: list[ConsensusMapping], min_votes: int) -> list[ConsensusMapping]:
    """Return only mappings with family_votes >= min_votes"""
    return [
        this_mapping for this_mapping in consensus_mappings
        if this_mapping.family_votes >= min_votes
    ]



def _consensus_to_alignment(consensus_mappings: list[ConsensusMapping], onto1_iri: str, onto2_iri: str) -> Alignment:
    """
    Convert a list of ConsensusMapping into the Alignment shape expected by
        diso_mappings.io.alignment.write_alignment
    Confidence becomes the OAEI align:measure.
    Relation passes through as-is (will get cast to '=' in any case).
    """
    return Alignment(
        onto1_iri = onto1_iri,
        onto2_iri = onto2_iri,
        mappings  = [
            Mapping(
                entity1  = this_mapping.entity1,
                entity2  = this_mapping.entity2,
                relation = this_mapping.relation,
                measure  = this_mapping.confidence,
            )
            for this_mapping in consensus_mappings
        ],
    )



def _format_voting_set(voting_set: tuple[str, ...]) -> str:
    """join voting set together as a string for reporting"""
    return _MULTI_VALUE_SEPARATOR.join(voting_set)



def _format_consensus_row(consensus_mapping: ConsensusMapping) -> str:
    """
    Build a single tab-separated row for the consensus TSV. 
    The column order matches `_CONSENSUS_TSV_HEADER`. 
    Confidence is serialised via str(...).
    """
    return "\t".join((
        consensus_mapping.entity1,
        consensus_mapping.label1 or "",
        consensus_mapping.entity2,
        consensus_mapping.label2 or "",
        str(consensus_mapping.confidence),
        str(consensus_mapping.family_votes),
        _format_voting_set(consensus_mapping.voting_families),
        str(consensus_mapping.system_votes),
        _format_voting_set(consensus_mapping.voting_systems),
    ))



def _format_unique_row(vote_key: VoteKey, label1: str | None, label2: str | None, relation: str) -> str:
    """
    Build a single tab-separated row for the unique TSV. 
    Column order matches `_UNIQUE_TSV_HEADER`. 
    Type column omitted (TODO).
    """
    return "\t".join((
        vote_key.entity1,
        label1 or "",
        vote_key.entity2,
        label2 or "",
        relation,
    ))