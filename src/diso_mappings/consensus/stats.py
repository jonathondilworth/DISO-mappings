"""
diso_mappings.consensus.stats: Per-pair vote-statistics text writer.

See reference implementation:

    https://github.com/ernestojimenezruiz/oaei-evaluation

for writing family-vote level blocks. For example:

    Vote 2
        Mappings: 48
        Families:
            AML: 64.6% (31)
            BertMap: 45.8% (22)
            ...
        Systems:
            AML: 64.6% (31/32)
            BertMap: 25.0% (12/12)
            ...

See: CreateConsensusAlignments.keepStatistics
"""
from __future__ import annotations

from pathlib import Path

from diso_mappings.consensus.consensus import ConsensusMapping
from diso_mappings.consensus.voting import VoteTable
from diso_mappings.io.terminal import debug

##
# CONSTANTS
##

_MIN_REQUIRED_VOTES: int = 2  # bottom of the vote range; matches build_consensus
_INDENT_LEVEL_1:    str = "\t"
_INDENT_LEVEL_2:    str = "\t\t"
_BLOCK_SEPARATOR:   str = "\n\n"  # blank line between vote blocks

##
# PUBLIC
##

def write_pair_stats(consensus_mappings: list[ConsensusMapping], vote_table: VoteTable, out_path: Path) -> Path:
    """
    Write the per-pair Vote-N statistics text file (see reference implementation)
    one block per family-vote level from 2 up to the maximum observed in consensus_mappings
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not consensus_mappings:
        out_path.write_text("")
        debug(f"  wrote {out_path.name} (empty: no consensus mappings)")
        return out_path

    table_max_votes   = max(this_mapping.family_votes for this_mapping in consensus_mappings)
    per_system_totals = _per_system_totals(vote_table)

    formatted_blocks: list[str] = []
    for this_min_votes in range(_MIN_REQUIRED_VOTES, table_max_votes + 1):
        this_block = _format_vote_block(
            consensus_mappings,
            this_min_votes,
            per_system_totals,
        )
        formatted_blocks.append(this_block)

    out_path.write_text(_BLOCK_SEPARATOR.join(formatted_blocks) + "\n")
    debug(f"  wrote {out_path.name} ({len(formatted_blocks)} vote block(s))")

    return out_path



##
# PRIVATE HELPERS
##

def _format_vote_block(consensus_mappings: list[ConsensusMapping], min_votes: int, per_system_totals: dict[str, int]) -> str:
    """
    Build the text of a single Vote-N block; 
    caller should join multiple blocks w/ breakline between.
    """
    qualifying_mappings = [
        this_mapping for this_mapping in consensus_mappings
        if this_mapping.family_votes >= min_votes
    ]
    qualifying_count = len(qualifying_mappings)

    ##
    # tally per-family and per-system contributions 
    # across the qualifying subset
    ##

    family_contributions: dict[str, int] = {}
    system_contributions: dict[str, int] = {}
    for this_mapping in qualifying_mappings:
        for this_family in this_mapping.voting_families:
            family_contributions[this_family] = family_contributions.get(this_family, 0) + 1
        for this_system in this_mapping.voting_systems:
            system_contributions[this_system] = system_contributions.get(this_system, 0) + 1

    output_lines: list[str] = []
    output_lines.append(f"Vote {min_votes}")
    output_lines.append(f"{_INDENT_LEVEL_1}Mappings: {qualifying_count}")

    # families: name + percentage + raw count (no denominator)
    output_lines.append(f"{_INDENT_LEVEL_1}Families:")
    for this_family in sorted(family_contributions):
        this_count      = family_contributions[this_family]
        this_percentage = _format_percentage(this_count, qualifying_count)
        output_lines.append(f"{_INDENT_LEVEL_2}{this_family}: {this_percentage}% ({this_count})")

    # systems: name + percentage + count / total
    output_lines.append(f"{_INDENT_LEVEL_1}Systems:")
    for this_system in sorted(system_contributions):
        this_count      = system_contributions[this_system]
        this_percentage = _format_percentage(this_count, qualifying_count)
        this_total      = per_system_totals.get(this_system, 0)
        output_lines.append(f"{_INDENT_LEVEL_2}{this_system}: {this_percentage}% ({this_count}/{this_total})")

    return "\n".join(output_lines)



def _per_system_totals(vote_table: VoteTable) -> dict[str, int]:
    """
    For each system that contributed at least one mapping, return the count
    of distinct mappings it produced (after self-mapping filtering at VoteTable.add).
    """
    per_system_totals: dict[str, int] = {
        this_system: 0 for this_system in vote_table.all_systems()
    }
    for this_key in vote_table.keys():
        for this_system in vote_table.voting_systems(this_key):
            per_system_totals[this_system] += 1
    return per_system_totals



def _java_half_up_rounding(value: float) -> int:
    """replicates java semantics for method call: Math.round(double)"""
    return int(value + 0.5)



def _format_percentage(numerator: int, denominator: int) -> str:
    """
    Format a percentage to one decimal place 
    replicates behaviour from reference implementation
    Returns 0.0 when denom is zero
    """
    if denominator == 0:
        return "0.0"
    raw_per_mille     = numerator * 1000.0 / denominator
    rounded_per_mille = _java_half_up_rounding(raw_per_mille)
    percent_value     = rounded_per_mille / 10.0
    return str(percent_value)