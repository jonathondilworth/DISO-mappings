"""
diso_mappings.consensus -- consensus and unique-mapping extraction
"""
from diso_mappings.consensus.voting import VoteKey, VoteTable
from diso_mappings.consensus.unique import (
    extract_unique_per_family,
    extract_unique_per_system,
)
from diso_mappings.consensus.consensus import (
    ConsensusMapping,
    LabelResolver,
    build_consensus,
)
from diso_mappings.consensus.discovery import (
    MatcherRun,
    build_runs_from_explicit,
    discover_latest_runs,
    load_vote_tables,
    validate_runs,
)
from diso_mappings.consensus.writers import (
    write_consensus_rdf_per_vote,
    write_consensus_tsv,
    write_consensus_tsv_per_vote,
    write_pair_outputs,
    write_unique_tsv,
)
from diso_mappings.consensus.labels import OntologyLabelResolver
from diso_mappings.consensus.stats import write_pair_stats

__all__ = [
    "VoteKey",
    "VoteTable",
    "extract_unique_per_family",
    "extract_unique_per_system",
    "ConsensusMapping",
    "LabelResolver",
    "build_consensus",
    "MatcherRun",
    "build_runs_from_explicit",
    "discover_latest_runs",
    "load_vote_tables",
    "validate_runs",
    "write_consensus_rdf_per_vote",
    "write_consensus_tsv",
    "write_consensus_tsv_per_vote",
    "write_pair_outputs",
    "write_unique_tsv",
    "OntologyLabelResolver",
    "write_pair_stats",
]