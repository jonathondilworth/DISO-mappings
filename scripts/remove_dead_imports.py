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
MIOMERGED_ONTOLOGY_NO_IMPORTS_PATH = Path(COMPACT_DIR / 'context-awareness' / 'FacilityOntology-noimports.rdf')

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

facility_ontology = Ontology("mIOmerged-noimports", MIOMERGED_ONTOLOGY_NO_IMPORTS_PATH, {'context-awareness'})

diso_compact_onto_registry.bind("mIOmerged-noimports", facility_ontology)

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

print(f'Saving updated OntologyRegistery to disk.')

diso_compact_onto_registry.save(DISO_ONTO_REGISTRY_FP)