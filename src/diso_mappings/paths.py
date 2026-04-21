"""
A python paths file which manages the directory structure for the project.
While the variables are (technically) mutable, making changes will break things.

Note: this should probably be built like: PROJECT_ROOT goes into a config, then the
paths can be derived by a PathManager, instanciated as a 'paths' object, shared
between modules as a singleton (or something); but this is fine for now!
"""

from pathlib import Path

##
# GENERAL PROJECT PATHS
# ---------------------
# Since we don't expect this file to be moved (be warned!), we
# specify the project root relative to this constants.py file.
# The reamining directory structure is built according to this.
# We might consider an alternative in future; for now, this works.
##

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DISO_DIR = DATA_DIR / "diso"
COMPACT_DIR = DATA_DIR / "diso-compact"



##
# DISO ONTOLOGY REGISTRY
# ----------------------
# To identify ontologies we use the filename stem. This registers 
# them to (at least one, possibly many) clusters. We do not consider 
# the possibility where two ontologies may share the same stem.
# In such cases, we expect the user to make reasonable adjustments 
# eg. UCO1 and UCO2
##

ONTO_REGISTRY_FILE = COMPACT_DIR / "_registry.yaml"



##
# MATCHERS & CONFIGS
# ------------------
# Matchers (ie. OM systems) often come in the form of JARs (with
# their own set of dependencies, etc). We expect each matcher to
# be located within its own directory in MATCHERS_DIR. Also,
# different matchers have different parameters and configurable
# settings. These are specified within the CONFIGS_DIR.
##

MATCHERS_DIR = PROJECT_ROOT / "matchers"
CONFIGS_DIR = PROJECT_ROOT / "configs"



##
# RUNS & LOGS
# -----------
# RUNS_DIR contains results, whereas LOGS_DIR 
# contains std::out logs
##

RUNS_DIR = PROJECT_ROOT / "runs"
LOGS_DIR = PROJECT_ROOT / "logs"



##
# TODO: CONSENSUS_DIR
##
CONSENSUS_DIR = PROJECT_ROOT / "consensus"

