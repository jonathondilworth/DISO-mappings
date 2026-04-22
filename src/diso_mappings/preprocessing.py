"""
ONTO preprocessing — label augmentation.

Implements:

    * get_local_name : IRI -> str (for use as an rdfs:label)
    * enrich_onto_labels : onto -> onto (for label-sparse ontos)

Specifically, 'enrich_onto_labels': (1) loads an ontology, (2) iterates 
through its 'owl:Class' declarations; (3) for all classes that lack an 
rdfs:label triple it adds one, where its value is from get_local_name.

get_local_name  does not apply tokenisation, splitting of any kind,
it is preserved verbatim.

..WHY?   AML appears to work well on label-sparse ontologies where 
classes have not much in the way of annotation properties, whereas 
BERTMap and BERTMapLt appear to (at first glance) not work so well 
(if they cannot find an annotation, they raise a No Class Annotations 
Found exception). Basically, we account for this by adopting an approach
similar to what AML does. But, rather than modify BERTMap directly 
(it runs as a part of DeepOnto), we preprocess the labels in a similar 
manner prior to running BERTMap (it appears effective!). Note that the 
operation is idempotent, re-running won't add new labels & the process 
is deterministic.
"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import unquote
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from diso_mappings.io.terminal import debug



def get_local_name(uri: str) -> str:
    """
    Accepts an IRI/URI and returns the local name (for it). ie. the substr after
    the last '#' or '/' symbol, whichever appears latest. We run unquote 
    unconditionally (decoding any potential HTML spec chars, percent-escapes, etc)
    since _it should_ have no effect on a string without any such chars.
    """
    decoded_iri = unquote(uri)
    fragment_idx = decoded_iri.rfind("#")
    path_sep_idx = decoded_iri.rfind("/")
    final_sep_idx = max(fragment_idx, path_sep_idx)
    return decoded_iri[final_sep_idx + 1:] if final_sep_idx >= 0 else decoded_iri



def _sniff_onto_format(onto_path: Path) -> str:
    """
    Given that some of the files in DISO-compact are not faithful their ext/suffix,
    we can determine the (actual) file format by observing the first few bytes of the
    file, then decide on the rdflib format. NOTE: Some DISO files use a .owl extension 
    but contain Turtle (e.g. DUL.owl).
    """
    with open(onto_path, "rb") as onto_file:
        file_header = onto_file.read(256).lstrip()
    if file_header.startswith(b"<?xml") or file_header.startswith(b"<rdf:") or b"<rdf:RDF" in file_header:
        return "xml"
    # else:
    return "turtle"



def enrich_onto_labels(source_path: Path, dest_path: Path | None = None, dry_run: bool = False) -> dict:
    """
    Augment a single ontology with rdfs:label triples derived from local names.
    For every named owl:Class (or rdfs:Class) that has no existing rdfs:label,
    add one whose value is the class's IRI local name. Classes with any existing
    rdfs:label are left untouched; returns a dict of counts (for logging & summary)
    """
    # if the dest_path is not specified, overwrite the source path (file)
    output_path = dest_path if dest_path is not None else source_path
    onto_format = _sniff_onto_format(source_path)

    rdflib_graph = Graph()
    rdflib_graph.parse(source=str(source_path), format=onto_format)

    # we process named classes: owl:Class & rdfs:Class; some DISO ontos mix them
    class_iris: set[URIRef] = set()
    for rdfs_class_type in (OWL.Class, RDFS.Class):
        for class_subject in rdflib_graph.subjects(RDF.type, rdfs_class_type):
            if isinstance(class_subject, URIRef):
                class_iris.add(class_subject)

    classes_seen = len(class_iris)
    classes_without_label = 0
    classes_enriched = 0
    classes_skipped_empty_local_name = 0

    for this_class_iri in class_iris:

        already_has_label = (this_class_iri, RDFS.label, None) in rdflib_graph
        if already_has_label:
            continue

        classes_without_label += 1
        derived_local_name = get_local_name(str(this_class_iri))

        if not derived_local_name:
            classes_skipped_empty_local_name += 1
            debug(f"empty local name for {this_class_iri}; skipping")
            continue

        rdflib_graph.add((this_class_iri, RDFS.label, Literal(derived_local_name)))
        classes_enriched += 1

    # only rewrite the file if we actually added anything
    if classes_enriched > 0 and not dry_run:
        rdflib_graph.serialize(destination=str(output_path), format=onto_format)

    return {
        "classes_seen": classes_seen,
        "classes_without_label": classes_without_label,
        "classes_enriched": classes_enriched,
        "classes_skipped_empty_local_name": classes_skipped_empty_local_name,
        "onto_format": onto_format,
    }

