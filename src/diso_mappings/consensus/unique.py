"""
diso_mappings.consensus.unique -- unique-mapping extraction from a populated VoteTable

Two related but distinct notions of uniqueness:

  * System-unique:
      
      M_S = { keys voted by exactly one system, where that system is S }
      That is, a mapping is unique to system S iff S is the sole voter.

  * Family-unique: 
  
      U_F = M_F \backslash \bigcup_{F^\prime \not= F} M_{F^\prime}
      where M_F denotes the set of vote keys with at least one voter from family F.
      A mapping appears in U_F iff F is the only family that voted for it.
      Singleton families (|F| = 1) reduce to extract_unique_per_system

both functions return dict[str, list[VoteKey]] keyed on system or family name
with deterministic ordering (keys sorted alphabetically); per-key lists are
sorted by (entity1, entity2). Every known system recieves an entry even when
its unique mapping lsit is empty.
"""
from __future__ import annotations

from diso_mappings.consensus.voting import (
    VoteKey, 
    VoteTable
)


def extract_unique_per_system(vote_table: VoteTable) -> dict[str, list[VoteKey]]:
    """
    System-unique mappings: M_S as defined in the above doc string.
    """
    unique_by_system: dict[str, list[VoteKey]] = {
        system_name: [] for system_name in sorted(vote_table.all_systems())
    }

    for vote_key in vote_table.keys():
        voting_systems = vote_table.voting_systems(vote_key)
        if len(voting_systems) != 1:
            continue   # not unique to any single system
        # else: exactly one voter — credit the unique mapping to that system
        sole_voting_system = next(iter(voting_systems))
        unique_by_system[sole_voting_system].append(vote_key)

    # canonicalise per-system list ordering for deterministic output
    for system_name in unique_by_system:
        unique_by_system[system_name].sort(
            key=lambda this_key: (this_key.entity1, this_key.entity2),
        )

    return unique_by_system



def extract_unique_per_family(vote_table: VoteTable) -> dict[str, list[VoteKey]]:
    """
    Family-unique mappings: 
      U_F = M_F \backslash \bigcup_{F^\prime \not= F} M_{F^\prime}
    """
    unique_by_family: dict[str, list[VoteKey]] = {
        family_name: [] for family_name in sorted(vote_table.all_families())
    }

    for vote_key in vote_table.keys():
        voting_families = vote_table.voting_families(vote_key)
        if len(voting_families) != 1:
            continue   # voted by >= 2 families; not family-unique
        # else: exactly one family voted — its mapping is unique to that family
        sole_voting_family = next(iter(voting_families))
        unique_by_family[sole_voting_family].append(vote_key)

    for family_name in unique_by_family:
        unique_by_family[family_name].sort(
            key=lambda this_key: (this_key.entity1, this_key.entity2),
        )

    return unique_by_family