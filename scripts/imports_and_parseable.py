import rdflib
from rdflib.namespace import OWL
from diso_mappings.registry import OntologyRegistry, Ontology

from pathlib import Path

from diso_mappings.paths import ONTO_REGISTRY_FILE, COMPACT_DIR


##
# LOAD OntologyRegistry from disk
##

DISO_ONTO_REGISTRY_FP = Path(ONTO_REGISTRY_FILE)
diso_compact_onto_registry = OntologyRegistry.load(DISO_ONTO_REGISTRY_FP)

###
# mIOmerged
##

DEAD_IMPORT_MIOMERGED = rdflib.URIRef('http://ontologies.ezweb.morfeo-project.org/profile.owl')
MIOMERGED_ONTOLOGY_PATH = Path(COMPACT_DIR / 'context-awareness' / 'mIOmerged.ttl')
MIOMERGED_ONTOLOGY_NO_IMPORTS_PATH = Path(COMPACT_DIR / 'context-awareness' / 'mIOmerged-noimports.ttl')

miomerged_graph = rdflib.Graph()
miomerged_graph.parse(MIOMERGED_ONTOLOGY_PATH, format='turtle')

removed_miomerged = 0

for subj, pred, obj in list(miomerged_graph.triples((None, OWL.imports, DEAD_IMPORT_MIOMERGED))):
    miomerged_graph.remove((subj, pred, obj))
    removed_miomerged += 1

print(f"Stripped {removed_miomerged} owl:imports triple(s) pointint to {DEAD_IMPORT_MIOMERGED}")

miomerged_graph.serialize(destination=MIOMERGED_ONTOLOGY_NO_IMPORTS_PATH, format='xml')

print(f"Wrote {len(miomerged_graph)} triples to mIOmerged-noimports.rdf")

print(f'Registering mIOmerged-noimports.rdf with OntologyRegistry.')

m_io_merged = Ontology("mIOmerged-noimports", MIOMERGED_ONTOLOGY_NO_IMPORTS_PATH, {'context-awareness'})

diso_compact_onto_registry.bind("mIOmerged-noimports", m_io_merged)

print(f'Bound!')

###
# facilityOntology
###

DEAD_IMPORT_FACILITY  = rdflib.URIRef('https://www.commoncoreontologies.org/ArtifactOntology')
FACILITY_ONTOLOGY_PATH = Path(COMPACT_DIR / 'mid-level' / 'cco-modules' / 'FacilityOntology.ttl')
FACILITY_ONTOLOGY_NO_IMPORTS_PATH = Path(COMPACT_DIR / 'mid-level' / 'cco-modules' / 'FacilityOntology-noimports.rdf')

facility_graph = rdflib.Graph()
facility_graph.parse(FACILITY_ONTOLOGY_PATH, format='turtle')

removed_facility = 0

for subj, pred, obj in list(facility_graph.triples((None, OWL.imports, DEAD_IMPORT_FACILITY))):
    facility_graph.remove((subj, pred, obj))
    removed_facility += 1

print(f'Stripped {removed_facility} owl:imports triple(s) pointing to {DEAD_IMPORT_FACILITY}')

facility_graph.serialize(destination=FACILITY_ONTOLOGY_NO_IMPORTS_PATH, format='xml')

print(f'Wrote {len(facility_graph)} triples to FacilityOntology-noimports.rdf')

print(f'Registering FacilityOntology-noimports.rdf with OntologyRegistry.')

facility_ontology = Ontology("FacilityOntology-noimports", FACILITY_ONTOLOGY_NO_IMPORTS_PATH, {'mid-level', 'cco-modules'})

diso_compact_onto_registry.bind("FacilityOntology-noimports", facility_ontology)

print(f'Bound!')

##
# AML has problems with d3fend in its current format
# as such, we convert from TTL to RDF & re-register it as 
##

D3FEND_TTL_PATH = COMPACT_DIR / 'cyber-security' / 'd3fend.ttl'
D3FEND_RDF_PATH = COMPACT_DIR / 'cyber-security' / 'd3fend.rdf'

d3fend_graph = rdflib.Graph()
d3fend_graph.parse(str(D3FEND_TTL_PATH), format='turtle')
d3fend_graph.serialize(destination=str(D3FEND_RDF_PATH), format='xml')

print(f"Converted {len(d3fend_graph)} triples from d3fend.ttl to d3fend.rdf")

d3fend_onto = Ontology("d3fend-rdf-xml", D3FEND_RDF_PATH, clusters={'cyber-security'})
diso_compact_onto_registry.bind("d3fend-rdf-xml", d3fend_onto)

print(f"Registry updated: d3fend-rdf-xml -> {D3FEND_RDF_PATH.relative_to(COMPACT_DIR)}")

print(f'Saving updated OntologyRegistery to disk.')

diso_compact_onto_registry.save(DISO_ONTO_REGISTRY_FP)
