"""
diso_mappings.io.alignment: OAEI RDF Alignment Format IO

Writes ontology alignments in the OAEI RDF Alignment Format:
ie. the .rdf (de-facto) standard consumed by: MELT and OAEI track evaluators

Currently this module provides WRITE (AND NOW) READ support:

    * write_alignment : Alignment -> <out_path>.rdf
    * read_alitnment  : <in_path>.rdf -> Alignment

READ support (round-tripping from .rdf back into an Alignment) should work

TODO: review: https://github.com/ernestojimenezruiz/oaei-evaluation
TODO: write some roudtripability tests

Relation types supported, as per the OAEI spec:

    '='   : equivalence         (e1 <=> e2)
    '<='  : subsumption         (e1 is subclass/subproperty of e2)
    '>='  : reverse subsumption (e1 is superclass/superproperty of e2)

LogMap notably weakens some equivalence mappings to subsumption during its
repair phase, so subsumption support is perhaps useful for our use case.

_(or perhaps not)_

References:
-----------
    - OAEI Alignment Format: https://moex.gitlabpages.inria.fr/alignapi/format.html
    - MELT (evaluator):      https://dwslab.github.io/melt/
    - OAEI-evaluation:       https://github.com/ernestojimenezruiz/oaei-evaluation
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from typing import Literal as _Literal

from rdflib import Graph, Literal as RDFLiteral, Namespace, URIRef, BNode
from rdflib.namespace import RDF, XSD
from rdflib.term import Node

from diso_mappings.io.terminal import warn

ALIGN = Namespace("http://knowledgeweb.semanticweb.org/heterogeneity/alignment")

AlignmentRelation = Literal["=", "<=", ">="]

_VALID_RELATIONS: frozenset[str] = frozenset({"=", "<=", ">="})

AlignmentReadMode = _Literal["lenient", "strict", "draconian"]

_KNOWN_RELATION_ALIASES: dict[str, str] = {
    ">": ">=",
    "<": "<=",
}


@dataclass(frozen=True)
class Mapping:
    entity1:  str
    entity2:  str
    relation: AlignmentRelation = "="
    measure:  float = 1.0


@dataclass
class Alignment:
    onto1_iri: str
    onto2_iri: str
    mappings:  list[Mapping] = field(default_factory=list)



def _validate_alignment(alignment: Alignment) -> None:
    """
    Pre-serialisation sanity checks on an in-memory Alignment. Writes are
    always strict; we fail loudly on bad data rather than letting it land
    in an .rdf file.
    """
    if not alignment.onto1_iri or not alignment.onto2_iri:
        raise ValueError("Alignment requires non-empty onto1_iri and onto2_iri.")

    for idx, this_mapping in enumerate(alignment.mappings):

        if this_mapping.relation not in _VALID_RELATIONS:
            raise ValueError(f"Mapping #{idx}: invalid relation {this_mapping.relation!r}.")

        if not (0.0 <= this_mapping.measure <= 1.0):
            raise ValueError(f"Mapping #{idx}: measure {this_mapping.measure!r} out of range [0.0, 1.0]")

        if not this_mapping.entity1 or not this_mapping.entity2:
            raise ValueError(f"Mapping #{idx}: entity1 and entity2 must be non-empty IRIs")



def write_alignment(alignment: Alignment, out_path: Path, pretty: bool = True) -> None:
    """
    Serialise an Alignment to out_path in OAEI RDF Alignment Format
    """
    _validate_alignment(alignment)

    alignment_graph = Graph()
    alignment_graph.bind("align", ALIGN)

    alignment_node = BNode()
    alignment_graph.add((alignment_node, RDF.type, ALIGN.Alignment))

    # level '0' = plain named-entity alignment 
    # type '??' = no claim about cardinality
    # silver-standard reference where m:n mappings are acceptable
    
    alignment_graph.add((alignment_node, ALIGN.level, RDFLiteral("0")))
    alignment_graph.add((alignment_node, ALIGN.type,  RDFLiteral("??")))

    alignment_graph.add((alignment_node, ALIGN.onto1, RDFLiteral(alignment.onto1_iri)))
    alignment_graph.add((alignment_node, ALIGN.onto2, RDFLiteral(alignment.onto2_iri)))

    for this_mapping in alignment.mappings:

        cell_node = BNode()
        alignment_graph.add((alignment_node, ALIGN.map, cell_node))
        alignment_graph.add((cell_node, RDF.type, ALIGN.Cell))
        alignment_graph.add((cell_node, ALIGN.entity1,  URIRef(this_mapping.entity1)))
        alignment_graph.add((cell_node, ALIGN.entity2,  URIRef(this_mapping.entity2)))
        alignment_graph.add((cell_node, ALIGN.relation, RDFLiteral(this_mapping.relation)))
        alignment_graph.add((
            cell_node, ALIGN.measure,
            RDFLiteral(f"{this_mapping.measure:.6f}", datatype=XSD.float),
        ))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    alignment_graph.serialize(
        destination=str(out_path),
        format="pretty-xml" if pretty else "xml",
    )



def read_alignment(alignment_path: Path, mode: AlignmentReadMode = "lenient") -> Alignment:
    """
    Parse OAEI RDF alignment file -> 'Alignment'; accepts commonly seen serialisations.

      1. plain-text IRI:  <align:onto1>http://example.org/foo</align:onto1>

      2. nested Ontology: <align:onto1>
                            <align:Ontology>
                                <align:location rdf:resource="..."/>
                            </align:Ontology>
                          </align:onto1>

    Mode controls tolerance for deviations from the OAEI spec:

    lenient (default): 'Postel's law' reading. Registered known deviations 
    ------- (e.g. LogMap's bare '>' / '<') are silently canonicalised to 
            their spec-compliant forms. Unknown deviations (unrecognised 
            relation symbols, measures outside [0,1], missing entity1/entity2) 
            skip the offending cell with no per-cell warning; a  per-file summary 
            is emitted if any cells were skipped (SILENT in lenient — see below).
            Missing measure defaults to 1.0 (spec allows). Missing relation
            defaults to '=' with a single per-file warning.
    
    NOTE: Lenient mode is silent about known deviations and per-file summary is
    NOT emitted in lenient (lenient mode takes the view that these are harmless). 
    It is, however, emitted in strict/draconian modes.
    
    TODO: perhaps consider renaming 'lenient' mode to 'tolerant' mode.
    
    strict: spec-aware reading. Registered known deviations are accepted (and are
    ------- canonicalised), but emits a per-file warning noting how many cells used 
            them. Unknown relations raise. Measure out of [0,1] raises. Missing 
            entity1/entity2 raises. Missing measure still defaults to 1.0 but is 
            warned. Missing relation raises (use lenient if you can tolerate it)
    
    draconian: any deviation raises immediately. No canonicalisation, no defaults.
    ---------  Missing measure raises. Missing relation raises. Known aliases raise. 
               Unknown relations raise. onto1/onto2 that look like 'null' (ie. AML)
               or any variation (eg. produced by LogMap or other matchers) raise. 
               Use when validating reference alignments in the most unforgiving sense
               possible!

    Regardless of mode, onto1/onto2 being absent entirely raises.

    Cell-level Behaviour Summary
    ----------------------------

        Conditional Behaviour            | Lenient (tolerant)  | Strict       | Draconian
        ---------------------------------------------------------------------------------
        relation is '>' or '<'           | canonicalise silent | warn file    | raise
        relation is something else       | skip cell (silent)  | raise        | raise
        measure absent                   | default 1.0 silent  | default+warn | raise
        measure out of [0,1]             | skip cell (silent)  | raise        | raise
        relation absent                  | default '=' warn    | raise        | raise
        entity1/entity2 missing          | skip cell (silent)  | raise        | raise
        onto1/onto2 is 'null'/'Optional' | accept verbatim     | warn         | raise
    """
    alignment_graph = Graph()
    alignment_graph.parse(alignment_path)

    alignment_nodes: list[Node] = list(alignment_graph.subjects(RDF.type, ALIGN.Alignment))
    if not alignment_nodes:
        raise ValueError(f"{alignment_path}: no align:Alignment found")
    
    alignment_node = alignment_nodes[0] # take the first alignment (appropriate?)

    onto1_iri = _extract_ontology_iri(alignment_graph, alignment_node, ALIGN.onto1)
    onto2_iri = _extract_ontology_iri(alignment_graph, alignment_node, ALIGN.onto2)

    if not onto1_iri or not onto2_iri:
        raise ValueError(f"{alignment_path}: missing onto1 or onto2 (onto1={onto1_iri!r}, onto2={onto2_iri!r})")

    # header-level sanity: AML emits 'null', LogMap wraps in 'Optional.of(...)'
    # NOTE: in strict we warn; in draconian we reject.
    
    _check_header_iri(onto1_iri, "onto1", alignment_path, mode)
    _check_header_iri(onto2_iri, "onto2", alignment_path, mode)

    # per-file state collected across cells:
    
    skip_counters: dict[str, int] = {} # {reason: count}
    missing_relation_count: int = 0    # warned once at end if > 0

    parsed_mappings: list[Mapping] = []

    for cell_node in alignment_graph.objects(alignment_node, ALIGN.map):
        extracted_mapping, had_missing_relation = _extract_mapping(
            alignment_graph, cell_node, mode, skip_counters,
        )
        if had_missing_relation:
            missing_relation_count += 1
        if extracted_mapping is not None:
            parsed_mappings.append(extracted_mapping)

    if mode in ("strict", "draconian") and skip_counters:
        summary_parts = [f"{count} {reason}" for reason, count in sorted(skip_counters.items())]
        warn(f"{alignment_path.name}: {mode} mode deviations: {', '.join(summary_parts)}")

    if missing_relation_count > 0:
        warn(f"{alignment_path.name}: {missing_relation_count} cells missing align:relation (defaulted to '=')")

    return Alignment(
        onto1_iri=onto1_iri,
        onto2_iri=onto2_iri,
        mappings=parsed_mappings,
    )



def _check_header_iri(iri_string: str, slot_name: str, alignment_path: Path, mode: AlignmentReadMode) -> None:
    """
    Validate an onto1/onto2 header IRI against known matcher-bug patterns. AML emits literal 'null' when 
    it can't determine an ontology IRI; LogMap wraps values in 'Optional.of(...)'; these are cosmetic
    upstream bugs that do not affect cell content but should surface in strict/draconian modes.
    """
    if iri_string == "null":
        if mode == "draconian":
            raise ValueError(f"{alignment_path.name}: {slot_name} is literal 'null'.")
        if mode == "strict":
            warn(f"{alignment_path.name}: {slot_name} is literal 'null'.")
        # lenient: silently accepts
        return

    if iri_string.startswith("Optional.of(") and iri_string.endswith(")"):
        if mode == "draconian":
            raise ValueError(f"{alignment_path.name}: {slot_name} wrapped in 'Optional.of(...)'.")
        if mode == "strict":
            warn(f"{alignment_path.name}: {slot_name} wrapped in 'Optional.of(...).")
        # lenient: accept silently
        return



def _extract_ontology_iri(alignment_graph: Graph, alignment_node: Node, predicate: URIRef) -> str | None:
    """
    Extract an ontology IRI from either the plain-text literal form or the nested 
    align:Ontology / align:location form. Returns None if neither is present.
    """
    onto_value = alignment_graph.value(alignment_node, predicate)
    
    if onto_value is None:
        return None

    # plain-text literal (what AML, LogMap, and our writer emit)
    if isinstance(onto_value, RDFLiteral):
        return str(onto_value)

    # nested align:Ontology with align:location
    onto_location = alignment_graph.value(onto_value, ALIGN.location)
    if onto_location is not None:
        return str(onto_location)

    # fallback: value is a URIRef; eg. <align:onto1 rdf:resource=".."/>
    if isinstance(onto_value, URIRef):
        return str(onto_value)

    return None



def _extract_mapping(alignment_graph: Graph, cell_node: Node, mode: AlignmentReadMode, skip_counters: dict[str, int]) -> tuple[Mapping | None, bool]:
    """
    Extract one Mapping from an align:Cell. Returns (mapping_or_none, had_missing_relation)
    Both lenient and strict mode return None for cells that cannot be interpreted after handling
    whereas draconian mode raises.
    """
    # entity1/entity2 are required in all modes
    
    entity1_node = alignment_graph.value(cell_node, ALIGN.entity1)
    entity2_node = alignment_graph.value(cell_node, ALIGN.entity2)
    
    if entity1_node is None or entity2_node is None:
        if mode in ("strict", "draconian"):
            raise ValueError(f"Cell {cell_node}: missing entity1 or entity2")
        skip_counters["cells_missing_entity"] = skip_counters.get("cells_missing_entity", 0) + 1
        return (None, False) # lenient mode

    # relation extraction

    relation_literal = alignment_graph.value(cell_node, ALIGN.relation)
    had_missing_relation = False # mutable
    if relation_literal is None:
        if mode in ("strict", "draconian"):
            raise ValueError(f"Cell {cell_node}: missing relation (spec requires)")
        had_missing_relation = True
        raw_relation = "="
    else:
        raw_relation = str(relation_literal)

    # known deviations are canonicalised in lenient/strict (raise in draconian)
    
    if raw_relation in _KNOWN_RELATION_ALIASES: # then convert
        canonical_relation = _KNOWN_RELATION_ALIASES[raw_relation]
        if mode == "draconian": # but raise in draconian mode
            raise ValueError(f"Cell {cell_node}: non-spec relation {raw_relation!r}.")
        if mode == "strict": # and warn in strict
            counter_key = f"cells_with_alias_{raw_relation!r}_to_{canonical_relation!r}"
            skip_counters[counter_key] = skip_counters.get(counter_key, 0) + 1
        # lenient: silent canonicalisation
        raw_relation = canonical_relation

    # unknown relations are skipped in lenient (strict/draconian raise)

    if raw_relation not in _VALID_RELATIONS:
        if mode in ("strict", "draconian"):
            raise ValueError(f"Cell {cell_node}: unrecognised relation {raw_relation!r}")
        skip_counters["cells_with_unknown_relation"] = skip_counters.get("cells_with_unknown_relation", 0) + 1
        return (None, had_missing_relation) # lenient

    # measure (or confidence) considered spec-compliant (TODO: really, draconian shouldnt raise in this instance?)

    measure_literal = alignment_graph.value(cell_node, ALIGN.measure)
    if measure_literal is None:
        if mode == "draconian":
            raise ValueError(f"Cell {cell_node}: missing measure; draconian requires explicit")
        if mode == "strict":
            skip_counters["cells_with_default_measure"] = skip_counters.get("cells_with_default_measure", 0) + 1
        mapping_measure = 1.0
    else:
        mapping_measure = float(measure_literal)

    # bounds check on 'measure' (or confidence) -- stict AND draconian raise, lenient skips with warning

    if not (0.0 <= mapping_measure <= 1.0):
        if mode in ("strict", "draconian"):
            raise ValueError(f"Cell {cell_node}: measure {mapping_measure!r} outside [0.0, 1.0]")
        skip_counters["cells_with_measure_out_of_range"] = skip_counters.get("cells_with_measure_out_of_range", 0) + 1
        return (None, had_missing_relation)

    # return extracted mapping w/ missing_relation bool

    return (
        Mapping(
            entity1=str(entity1_node),
            entity2=str(entity2_node),
            relation=raw_relation,
            measure=mapping_measure,
        ),
        had_missing_relation,
    )

