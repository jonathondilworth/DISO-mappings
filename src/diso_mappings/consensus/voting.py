"""
diso_mappings.consensus.voting -- vote-table primitives for consensus construction

Systems vote; families are credited once per mapping regardless of how many 
systems within the family voted (unionised voting across families).

    * vote keys are (entity1, entity2) ordered pairs
    * relation, confidence and entity type are discarded (by desing)
    * faithful to the reference implementation in [1]
    * _make_vote_key : hook for richer keying schemes (TODO)
    * self-mappings (entity1 == entity2) skipped @ insertion [1]

References:
-----------
[1] reference implementation: https://github.com/ernestojimenezruiz/oaei-evaluation
[2] OAEI Alignment Format:  https://moex.gitlabpages.inria.fr/alignapi/format.html
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from diso_mappings.io.alignment import Alignment


##
# VOTE KEY
# --------
# Frozen dataclass representing the vote-tally key. v1: (entity1, entity2).
# All construction goes through `_make_vote_key` so that backlog hooks (#1, #2)
# plug in at a single seam.
##

@dataclass(frozen=True)
class VoteKey:
    """
    Vote-table key. v1 uses an ordered pair (entity1, entity2). Relation,
    confidence and entity type are deliberately not part of the key — see
    module docstring for the rationale.
    """
    entity1: str
    entity2: str



def _make_vote_key(entity1: str, entity2: str, relation: str = "=") -> VoteKey:
    """
    construction seam for VoteKey; drops relation [1], but accepts 'relation'
    so call sites do not need to change in future (if we want to deal with subs
    relations in future when evaluating ranking tasks, etc).
    TODO (backlog):
        * relation-aware keying for richer schemes
        * symmetric = normalisation : collapse (A, B) and (B, A) into a single key
          when relation := '=' (in future, directional subs keys, etc).
        * allow for configurable relation handling rules
    """
    return VoteKey(entity1=entity1, entity2=entity2)



##
# VOTE TABLE
# ----------
# Accumulates per-mapping votes from one or more system alignments. Operates
# entirely on Alignment objects (already parsed by `diso_mappings.io.alignment`);
# this module does not touch the filesystem or any ontology files.
##

class VoteTable:
    """
    Tally (per-pair) of which systems and families voted for which mappings.
    A single VoteTable instance corresponds to a single ontology pair.
    """

    def __init__(self) -> None:
        self._systems_for_key:  dict[VoteKey, set[str]] = {}
        self._families_for_key: dict[VoteKey, set[str]] = {}


    def add(self, system: str, family: str, alignment: Alignment) -> None:
        """
        Register every mapping in 'alignment', A, as a vote from 'system' s
            (s \in F : F := the set of systems in family F)
        Skip self-mappings (where entity1 == entity2) [1], idempotent set-based 
        operations on (system, alignment).
        """
        if not system:
            raise ValueError("VoteTable.add: system must be a non-empty string")
        if not family:
            raise ValueError("VoteTable.add: family must be a non-empty string")

        for this_mapping in alignment.mappings:
            if this_mapping.entity1 == this_mapping.entity2:
                continue   # mirrors [1] self-mappings are dropped
            this_key = _make_vote_key(
                this_mapping.entity1,
                this_mapping.entity2,
                this_mapping.relation,
            )
            self._systems_for_key.setdefault(this_key,  set()).add(system)
            self._families_for_key.setdefault(this_key, set()).add(family)


    def system_votes(self, key: VoteKey) -> int:
        """Number of distinct systems that voted for 'key'"""
        return len(self._systems_for_key.get(key, ()))


    def family_votes(self, key: VoteKey) -> int:
        """Number of distinct families that voted for 'key'"""
        return len(self._families_for_key.get(key, ()))


    def voting_systems(self, key: VoteKey) -> frozenset[str]:
        """Set of systems that voted for 'key' (empty if 'key' is unknown)."""
        return frozenset(self._systems_for_key.get(key, ()))


    def voting_families(self, key: VoteKey) -> frozenset[str]:
        """Set of families that voted for 'key' (empty if 'key' is unknown)."""
        return frozenset(self._families_for_key.get(key, ()))


    def all_systems(self) -> frozenset[str]:
        """Set of every system that contributed at least one mapping."""
        all_seen_systems: set[str] = set()
        for system_set in self._systems_for_key.values():
            all_seen_systems |= system_set
        return frozenset(all_seen_systems)


    def all_families(self) -> frozenset[str]:
        """Set of every family that contributed at least one mapping."""
        all_seen_families: set[str] = set()
        for family_set in self._families_for_key.values():
            all_seen_families |= family_set
        return frozenset(all_seen_families)


    def keys(self) -> Iterator[VoteKey]:
        """Deterministic iteration over all keys; sorted by (entity1, entity2)"""
        yield from sorted(
            self._systems_for_key,
            key=lambda this_key: (this_key.entity1, this_key.entity2),
        )


    def max_family_votes(self) -> int:
        """
        Maximum family-vote count across the whole table. Returns 0 for an
        empty table (since we use this in the downstream confidence calc).
        """
        if not self._families_for_key:
            return 0
        return max(len(family_set) for family_set in self._families_for_key.values())


    def __len__(self) -> int:
        return len(self._systems_for_key)


    def __contains__(self, key: VoteKey) -> bool:
        return key in self._systems_for_key