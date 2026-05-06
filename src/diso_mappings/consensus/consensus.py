"""
diso_mappings.consensus.consensus -- consensus-mapping construction

Given a populated VoteTable, produce a list of ConsensusMapping records.
Filter by a minimum family-vote threshold; decorate w/ confidence scores,
vote provenance and (optionally) human-readable labels.

Confidence is computed by linear interpolation on family-votes:

    confidence = 0.7 + (family_votes - 2) * 0.3 / (max_family_votes - 2)

see: ConsensusMapping.setConfidenceConsensusMapping in oaei-evaluation [1].

NOTES:
------
    * max_family_votes < 2 => reject
    * clamps confidence calc @ / 0 (avoids devision by zero)
    * interprets any relation as \equiv (by design)

ROADMAP:
--------
    * lift relation into vote key for future consideration
    * configurable confidence score weightings (not hardcoded)

References:
-----------
[1] Reference implementation: https://github.com/ernestojimenezruiz/oaei-evaluation

"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from diso_mappings.consensus.voting import VoteKey, VoteTable

##
# CONFIDENCE FORMULA
# ------------------
# Hardcoded constants for the v1 linear-interpolation rule. Made configurable
# in ROADMAP (TODO) (pluggable scorers: mean of voter confidences, QV-style
# \Sigma \sqrt{p_m}, etc); the pair of values below corresponds to the java 
# reference's y1_min_confidence / y2_max_confidence in ConsensusMapping.java
##

_CONFIDENCE_AT_MIN_VOTES: float = 0.7   # confidence when family_votes == 2
_CONFIDENCE_AT_MAX_VOTES: float = 1.0   # confidence when family_votes == max_family_votes
_MIN_REQUIRED_VOTES:      int   = 2     # min admissible threshold; below this the formula breaks


def _consensus_confidence(family_votes: int, max_family_votes: int) -> float:
    """
    voting consensus mechanism w/ linear interpolation
    TODO: allow injection of alternative scoring schemes (eg. QV)
    """
    if max_family_votes <= _MIN_REQUIRED_VOTES:
        return _CONFIDENCE_AT_MAX_VOTES
    interpolation_coef = (_CONFIDENCE_AT_MAX_VOTES - _CONFIDENCE_AT_MIN_VOTES) / (max_family_votes - _MIN_REQUIRED_VOTES)
    return interpolation_coef * (family_votes - _MIN_REQUIRED_VOTES) + _CONFIDENCE_AT_MIN_VOTES



##
# LABEL RESOLVER PROTOCOL
# -----------------------
# Defines the contract that 'diso_mappings.consensus.labels' rdflib resolver implements
# Pinned here so that build_consensus has a typed slot for the dependency without needing 
# to import labels.py (which will itself depend on rdflib + OntologyRegistry)
##

@runtime_checkable
class LabelResolver(Protocol):
    """
    Resolves : IRI -> human-readable label (or None).
    see: diso_mappings.consensus.labels
    """
    def label_for(self, iri: str) -> str | None: ...



##
# CONSENSUS MAPPING
# -----------------
# The structured per-mapping output record. Frozen so callers can treat
# build_consensus's return value as read-only data (the writers and stats
# modules will consume this; nothing should mutate it after construction).
##

@dataclass(frozen=True)
class ConsensusMapping:
    """
    single consensus mapping w/ vote provenance and a confidence score
    derived from family-vote agreement
    """
    entity1:         str                    # src IRI
    entity2:         str                    # tgt IRI
    relation:        str                    # =
    confidence:      float                  # _consensus_confidence
    family_votes:    int                    # distinct family votes
    system_votes:    int                    # distinct system votes
    voting_families: tuple[str, ...]        # sorted tuples families that voted
    voting_systems:  tuple[str, ...]        # sorted tuples systems that voted
    label1:          str | None = None      # human-readable label (if available)
    label2:          str | None = None      # human-readable label (if available)



##
# BUILD CONSENSUS
# ---------------
# Driver function. Takes a populated VoteTable, applies the family-vote
# threshold, computes confidences against the table-wide max_family_votes,
# resolves labels if a LabelResolver is supplied, and returns a deterministic-
# ordered list of ConsensusMapping records.
##

def build_consensus(
        vote_table: VoteTable, *, 
        min_family_votes: int = _MIN_REQUIRED_VOTES, 
        label_resolver: LabelResolver | None = None
    ) -> list[ConsensusMapping]:
    """
    Build the consensus mapping list from a populated VoteTable: given a populated 
    VoteTable for a single ontology pair, the minimum number of family votes required,
    and an optional label resolver, produce (return) a list of ConsensusMapping records
    sorted by (entity1, entity2) for determinism.
    """
    if min_family_votes < _MIN_REQUIRED_VOTES:
        raise ValueError(f"build_consensus: min_family_votes must be >= {_MIN_REQUIRED_VOTES}.")

    table_max_family_votes = vote_table.max_family_votes()

    consensus_mappings: list[ConsensusMapping] = []

    for vote_key in vote_table.keys():

        this_family_votes = vote_table.family_votes(vote_key)
        if this_family_votes < min_family_votes:
            continue   # below threshold; skip

        # we have a qualifying mapping; gather the rest of its provenance
        this_system_votes    = vote_table.system_votes(vote_key)
        this_voting_families = tuple(sorted(vote_table.voting_families(vote_key)))
        this_voting_systems  = tuple(sorted(vote_table.voting_systems(vote_key)))
        this_confidence      = _consensus_confidence(this_family_votes, table_max_family_votes)

        # label resolution is optional
        this_label1: str | None = None
        this_label2: str | None = None
        if label_resolver is not None:
            this_label1 = label_resolver.label_for(vote_key.entity1)
            this_label2 = label_resolver.label_for(vote_key.entity2)

        consensus_mappings.append(ConsensusMapping(
            entity1         = vote_key.entity1,
            entity2         = vote_key.entity2,
            relation        = "=", # interpret all relations as = (by design)
            confidence      = this_confidence,
            family_votes    = this_family_votes,
            system_votes    = this_system_votes,
            voting_families = this_voting_families,
            voting_systems  = this_voting_systems,
            label1          = this_label1,
            label2          = this_label2,
        ))

    # cheap sort to ensure deterministic output
    consensus_mappings.sort(key=lambda this_mapping: (this_mapping.entity1, this_mapping.entity2))

    return consensus_mappings