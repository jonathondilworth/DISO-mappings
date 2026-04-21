"""
Simple module that defines the 'pair' schema: it is simply a pair of ontologies.
We represent each pair as a source-target pair (by convention). Generally, they
would by identifiable via their IRI. However, while we might integrate that approach
in the future, for now we simply register (and refer) to them via their stemmed file
name, as is discussed in `registry.y` (diso_mappings.registry).

Pair, per the YAML schema:

    pairs:
      - source: <canonical-id>
        target: <canonical-id>
      - ...

A concrete example would be:

    pairs:
    - source: uco2
      target: stix-spec-merged
    - source: JC3IEDM
      target: STO

When running `make mappings MATCHER=aml PAIRS=configs/pairs.example.yaml`, the python
script will load the set of pairs (you can have as many as you like) from the provided
pairs.example.yaml; and then run the OM system (in this case 'AML') for each pair, and
save the mappings (and the terminal output, std::out, std::err, etc.). You get log files
and output mappings; and this can then be repeated for any number of pairs, and any number
of matchers. 

Anyway, that's the purpose of this module, to define what it means to be a 'pair'.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from diso_mappings.registry import OntologyRegistry

import yaml



##
# (ONTOLOGY) PAIR CONTRACT
# -------------
# source, target; both strings (point to ontology or KG files on disk, 
# relative to the registry, resolve to abs paths for our internal representation).
##

@dataclass(frozen=True)
class Pair:
    source: str
    target: str



##
# LOAD PAIRS
# ----------
# Function that accepts a path to pairs and validates each pair agaisnt
# an OntologyRegistry (see: diso_mappings.registry). Does:
# 
#     1. checks the file exists (implicitly via open)
#     2. loads the YAML 'pair' content
#     3. checks that each pair conforms to the _'pair schema'_
#     4. ensures that all of the associated ontology pairs are resolvable through OntologyRegistry.
#     5. returns the set of pairs (to then be processed by an OM system).
# 
# Note that much of this function can probably be broken into a set of 'validators'. However, for the
# short term, we opt for simplicity and keep all of the potential exceptions that may be @raised inline.
##

def load_pairs(yaml_path: Path, registry: OntologyRegistry) -> list[Pair]:

    with open(yaml_path) as yaml_file:
        pair_data = yaml.safe_load(yaml_file)

    if not isinstance(pair_data, dict) or "pairs" not in pair_data:
        raise ValueError(f"{yaml_path}: must be a YAML configuration with a top-level 'pairs' key.")
    
    if not isinstance(pair_data['pairs'], list):
        raise ValueError(f"{yaml_path}: 'pairs' must be parseable from a YAML [list] to a Python list.")

    src_tgt_pairs = pair_data['pairs']
    observed_pairs: set[tuple[str, str]] = set()
    final_pairs_xs: list[Pair] = [] # xs ~:= list

    for idx_pair_pointer, src_tgt_pair in enumerate(src_tgt_pairs):

        if not isinstance(src_tgt_pair, dict) or "source" not in src_tgt_pair or "target" not in src_tgt_pair:
            raise ValueError(f"{yaml_path} pair #{idx_pair_pointer}: expected an (ontology) pair with both: 'source' and 'target'")
        
        src, tgt = str(src_tgt_pair["source"]), str(src_tgt_pair["target"])

        if src not in registry:
            raise ValueError(f"{yaml_path} pair #{idx_pair_pointer}: unknown source ontology ID {src!r}")
        
        if tgt not in registry:
            raise ValueError(f"{yaml_path} pair #{idx_pair_pointer}: unknown target ontology ID {tgt!r}")
        
        if src == tgt:
            raise ValueError(f"{yaml_path} pair #{idx_pair_pointer}: self-pair ({src} -> {tgt}) not allowed")
        
        this_pair: tuple[str, str] = (src, tgt)

        if this_pair in observed_pairs:
            raise ValueError(f"{yaml_path} pair #{idx_pair_pointer}: duplicate pair {src} -> {tgt}")
        
        observed_pairs.add(this_pair)

        final_pairs_xs.append(Pair(source=src, target=tgt))

    return final_pairs_xs

