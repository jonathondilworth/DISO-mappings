"""
diso_mappings.registry: An Ontology Registry Module

Registers on-disk ontologies, stores via the ontology name (canonical, stemmed),
and its associated path and cluster membership.

Specifically, this module registers on-disk ontologies using a 'bind-resolve' 
type pattern, similar to that of an ontology catelogue. In our case, however, 
our tooling is suppose to be intuitive to human users, so each ontology is 
identified by its 'cannonical_name' (string), which is its filename stem.
The filename stem is simply the filename without its extension.

Cross-cluster duplicates in the DISO compact set will share filenames in cases
and are therefore collapsed into a single entry. The entries 'clusters' list 
records every cluster that the ontology appears in. Note that: the first path 
encountered during a sorted walk is treated as the canonical path for loading.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator
from diso_mappings.io.terminal import info
from diso_mappings.constants import (
    ONTOLOGY_EXTENSIONS,
    ONTOLOGY_EXCLUSIONS,
)
import yaml



@dataclass
class Ontology:
    name_id:  str           # internal ontology identifier (filename stem)
    path:     Path          # filepath to the ontology
    clusters: set[str] = field(default_factory=set)



class OntologyRegistry:
    """
    The OntologyRegistry accepts a dict of string-ontology (kv) pairs, where
    the key:str binds each Ontology (dataclass) to the registry during 'build'.
    Keys are our internal (cannonical) identifiers: each ontology's stemmed filename.
    Once built, the OntologyRegistry can then be used to resolve an individual Ontology
    dataclass, obtain its cluster membership, list all registered ontologies, etc.
    """
    def __init__(self, ontologies: dict[str, Ontology]):
        self._ontologies = ontologies # can be an empty dict

    def __contains__(self, name_id: str) -> bool:
        return name_id in self._ontologies

    def __iter__(self) -> Iterator[Ontology]:
        """we sort by keys for a deterministic iterator"""
        for name_id in sorted(self._ontologies):
            yield self._ontologies[name_id]

    def __len__(self) -> int:
        return len(self._ontologies)

    def bind(self, name_id: str, ontology_inst: Ontology) -> Ontology:
        if not isinstance(ontology_inst, Ontology):
            raise TypeError(f"The specified ontology is of the wrong type: {type(ontology_inst).__name__}")
        if name_id in self._ontologies:
            info(f"{name_id!r} is already bound.")
            info(f"Bind retains attributes from the initial call, while unionising cluster membership.")
            self._ontologies[name_id].clusters.update(ontology_inst.clusters)
            return self._ontologies[name_id]
        # else: new ontology instance
        self._ontologies[name_id] = ontology_inst
        return self._ontologies[name_id] # for method chaining

    def resolve(self, name_id: str) -> Ontology:
        if name_id not in self._ontologies:
            raise KeyError(f"Unknown ontology NAME ID: {name_id!r}")
        return self._ontologies[name_id]

    def entries(self) -> list[str]:
        return sorted(self._ontologies)

    def by_cluster(self, cluster: str) -> list[Ontology]:
        return sorted([
            ontology for ontology in self._ontologies.values() if cluster in ontology.clusters
        ], key=lambda ontology: ontology.name_id)



    @classmethod
    def build(cls, onto_dir_to_walk: Path, exclusion_matches: frozenset[str] | set[str] = ONTOLOGY_EXCLUSIONS) -> OntologyRegistry:
        """
        Walk compact DISO, construct registry keys each filename stem.
        """
        onto_registry = cls({})
        recursive_onto_paths = sorted(onto_dir_to_walk.rglob("*"))

        for this_fp in recursive_onto_paths:

            if not this_fp.is_file() or this_fp.suffix.lower() not in ONTOLOGY_EXTENSIONS:
                continue # skip: not a file, or is invalid per our exts specification
            if this_fp.name.startswith("_"):
                continue # skip: encountered a reserved file

            relative_fp_from_root = this_fp.relative_to(onto_dir_to_walk)
            path_segments_from_root: tuple[str, ...] = relative_fp_from_root.parts
            if any(segment in exclusion_matches for segment in path_segments_from_root):
                continue # skip: encountered a matching pattern satisfying exclusion

            # new (valid) ontology to bind:
            onto_name_id = this_fp.stem
            onto_cluster_membership = path_segments_from_root[0] if len(path_segments_from_root) > 1 else "root"
            resolved_onto_fp = this_fp.resolve()

            onto_registry.bind(onto_name_id, Ontology(
                name_id=onto_name_id,
                path=resolved_onto_fp,
                clusters={onto_cluster_membership},
            )) # binds the ontology for later resolution
        
        # END: forall FPs derived by onto_dir_to_walk
        return onto_registry



    def save(self, registry_fp: Path) -> None:
        """
        Save the existing ontology registry.
        """
        registry_dict_to_disk: dict = {}
        registry_root_dir = registry_fp.parent
        for name_id in sorted(self._ontologies):
            this_ontology = self._ontologies[name_id]
            try:
                this_relative_path = this_ontology.path.relative_to(registry_root_dir)
            except ValueError:
                this_relative_path = this_ontology.path             # fallback: abs path
            registry_dict_to_disk[name_id] = {
                "path"    : str(this_relative_path.as_posix()),     # SERIALISE
                "clusters": sorted(this_ontology.clusters),         # CAST (set -> list) 4 YAML
            }
        registry_fp.parent.mkdir(parents=True, exist_ok=True)
        with open(registry_fp, "w") as registry_file:
            yaml.safe_dump(registry_dict_to_disk, registry_file, sort_keys=False)



    @classmethod
    def load(cls, registry_fp: Path) -> OntologyRegistry:
        """
        Load a pre-existing ontology registry.
        Recall that since we're reading from a YAML file, we are 
        concerned with roundtripability. Note that:
        YAML null entries should be internally repr'd as None
        Filepaths should be converted from string to Path
        """
        with open(registry_fp) as registry_file:
            registry_data = yaml.safe_load(registry_file) or {}
        
        ontologies: dict[str, Ontology] = {}
        registry_root_dir = registry_fp.parent # the dir the registry exists in

        for name_id, ontology_entry in registry_data.items():
            entry_onto_fp = (registry_root_dir / ontology_entry['path']).resolve()
            entry_onto_cluster_membership = set(ontology_entry.get("clusters", []))
            ontologies[name_id] = Ontology(
                name_id=name_id,                                # REQUIRED (str)
                path=entry_onto_fp,                             # REQUIRED (str -> Path)
                clusters=entry_onto_cluster_membership,         # ALREADY CAST AS SET
            )
        
        return cls(ontologies)

