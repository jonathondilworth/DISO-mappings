"""
diso_mappings.consensus.labels - rdflib-based rdfs:label resolution

Provides an implementation of the LabelResolver Protocol declared in
    diso_mappings.consensus.consensus

Walks one or more registered source ontologies (via OntologyRegistry)
and extracts rdfs:label annotations, serves fast IRI-to-label lookups
for the consensus and unique TSV writers.

See:

[1] Java reference implementation in AbstractEvaluation :: getLabel4Entity 
      oaei-evaluation: https://github.com/ernestojimenezruiz/oaei-evaluation
      /blob/master/src/main/java/oaei/evaluation/AbstractEvaluation.java#L426

In this repo:
  * OntologyRegistry: diso_mappings.registry
  * rdflib quiet filter: diso_mappings._rdflib_common (TODO: review)
  * LabelResolver Protocol: diso_mappings.consensus.consensus
"""
from __future__ import annotations

from pathlib import Path

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDFS

from diso_mappings._rdflib_common import (
    get_rdflib_suppressed_count,
    install_rdflib_quiet_filter,
    reset_rdflib_suppressed_count,
)
from diso_mappings.io.terminal import debug, info, warn
from diso_mappings.registry import Ontology, OntologyRegistry

##
# CONSTANTS
##

_ENGLISH_LANG_TAGS: frozenset[str | None] = frozenset({None, "en"})

##
# RESOLVER
##

class OntologyLabelResolver:
    """
    rdflib-backed rdfs:label resolver. Implements LabelResolver Protocol.
    Construction performs the full ontology parse; failures on individual ontologies 
    are warned but do not raise. After construction the resolver is read-only.
    """

    def __init__(
        self,
        registry: OntologyRegistry, *,
        ontology_names: list[str] | None = None,
        lowercase_labels: bool = False,
    ) -> None:
        self._registry = registry
        self._lowercase_labels = lowercase_labels
        self._label_index: dict[str, str] = {}
        # determine scope
        if ontology_names is None:
            self._scope = list(registry.entries())
        else:
            self._scope = list(ontology_names)
        self._load_all() # loads eagerly


    def label_for(self, iri: str) -> str | None:
        """Return the label for iri, or None if no rdfs:label was found"""
        return self._label_index.get(iri)


    def __len__(self) -> int:
        """number of IRIs with a resolved label; useful diagnostic"""
        return len(self._label_index)

    ##
    # private methods
    ##

    def _load_all(self) -> None:
        """
        Parse each ontology and merge its label triples into the unified index.
        Warns and continues on individual parse failures.
        """
        info(f"label resolver: loading {len(self._scope)} ontology file(s)")
        # report the suppression count via debug() per-ontology
        install_rdflib_quiet_filter()

        successful_loads = 0
        failed_loads = 0

        for this_onto_name in self._scope:
            try:
                this_ontology = self._registry.resolve(this_onto_name)
            except KeyError as registry_error:
                # an unregistered name in the explicit list is a caller bug; warn, but proceed
                warn(f"label resolver: {registry_error}; skipping")
                failed_loads += 1
                continue

            this_index, this_load_succeeded = self._load_one(this_ontology)
            if this_load_succeeded:
                self._label_index.update(this_index)
                successful_loads += 1
            else:
                failed_loads += 1

        if failed_loads > 0:
            info(f"label resolver: indexed {len(self._label_index)} label(s)")
            info(f"  across {successful_loads}/{len(self._scope)} ontology file(s).")
            info(f"     {failed_loads} failed; see warnings above ... ")
        else:
            info(f"label resolver: indexed {len(self._label_index)} label(s)")
            info(f"across {successful_loads}/{len(self._scope)} ontology file(s)")


    def _load_one(self, ontology: Ontology) -> tuple[dict[str, str], bool]:
        """
        Parse a single ontology and return (index, success).
          * on parse failure, returns ({}, False) with a warning.
          * on success, returns the parsed {iri: label} dict and True.
            (the dict may be empty for ontologies w/o rdfs:label annotations)
        """
        graph = Graph()
        # tally any literal-conversion warnings emitted
        reset_rdflib_suppressed_count()
        try:
            graph.parse(str(ontology.path))
        except Exception as parse_exception:
            warn(f"label resolver: could not parse {ontology.path}: {type(parse_exception).__name__}: {parse_exception}")
            return {}, False # failure

        per_iri_candidates: dict[str, list[Literal]] = {}
        for subject_node, _, label_object in graph.triples((None, RDFS.label, None)):
            if not isinstance(subject_node, URIRef):
                continue
            if not isinstance(label_object, Literal):
                continue
            per_iri_candidates.setdefault(str(subject_node), []).append(label_object)

        # collapse multi-candidate entries 
        # (apply lowercase if requested)
        chosen_labels: dict[str, str] = {}
        for this_iri, this_candidates in per_iri_candidates.items():
            preferred_label = _choose_preferred_label(this_candidates)
            if self._lowercase_labels:
                preferred_label = preferred_label.lower()
            chosen_labels[this_iri] = preferred_label

        suppressed_during_parse = get_rdflib_suppressed_count()
        debug(f"Parsed {ontology.path.name}: {len(chosen_labels)} label(s).")
        if suppressed_during_parse > 0:
            debug(f"  (suppressed {suppressed_during_parse} rdflib literal-conversion warning(s))")
        
        return chosen_labels, True



##
# PRIVATE HELPERS
##

def _choose_preferred_label(candidate_literals: list[Literal]) -> str:
    """Pick an appropriate label from a list of rdfs:label literals for an iri"""
    english_candidates = [
        this_candidate for this_candidate in candidate_literals
        if this_candidate.language in _ENGLISH_LANG_TAGS
    ]
    if english_candidates:
        return str(english_candidates[0])
    return str(candidate_literals[0])