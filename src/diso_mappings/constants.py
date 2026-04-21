"""
A python constants file. Similar to `paths.py`, some _constants_ are (technically) mutable, 
while others (ie. frozensets) are not; and, again, it's essentially a config file. You might
want to make some changes in here prior to running any associated tooling, but then everything
_should_ be immutable. We'll rework this when it's appropriate.
"""

##
# DISO_REPO_LOCATION
# ------------------
# When running: `make diso-download` the specific repo and branch used to
# fetch the ontologies from is set here (repo -> repo name; ref -> branch)
##

DISO_REPO_LOCATION = {
    "repo": "city-artificial-intelligence/diso",
    "ref": "main",
}



##
# ONTOLOGY_EXTENSIONS
# -------------------
# A list of acceptable file extensions that we accept during OntologyRegistry
# construction. See 'diso_mappings.registry' for more information. Importantly,
# if you are using this tool with ontologies presented in formats other than
# those listed below, you will need to modify the list to 
##
ONTOLOGY_EXTENSIONS = frozenset({
    ".owl", 
    ".ttl", 
    ".rdf"
})



##
# ONTOLOGY_EXCLUSIONS
# -------------------
# Note on ONTOLOGY_EXCLUSIONS: A list of strings that represent path-segments 
# to match for exlusion during the OntologyRegistry build process.
# 
# For instance, CCO is treated as one ontology: CommonCoreOntologiesMerged.ttl
# The individual cco-modules are OWL files stored under:
# 
#   data/diso/mid-level/CCO/cco-modules/*
# 
# However, since use the merged turtle file, we include "cco-modules" within
# the ONTOLOGY_EXCLUSIONS set, so that the registry ignores the individual modules.
# (they remain on disk for users that would like to curate their own custom pairs)
##

ONTOLOGY_EXCLUSIONS = frozenset({
    "cco-modules"
})



##
# VERBOSE_LOGGING_PKGS
# --------------------
# During OM with BERTMap and BERTMapLt, there is ALOT of detail that gets logged
# to std::out, it stems from a variety of dependencies that the code uses (in some
# cases), as well as the nature of tuning \w transformers & pyTorch. So, we include 
# a list of packages to 'suppress' the output from, but allow users to toggle off the
# suppression with a verbose flag. eg. make mappings MATCHER=bertmap ARGS='--verbose'
##

VERBOSE_LOGGING_PKGS = [
    "httpcore",
    "httpx",
    "urllib3",
    "filelock",
    "fsspec",
    "fsspec.local",
    "datasets",
    "huggingface_hub",
    "transformers",
]

