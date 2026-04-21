from pathlib import Path

# we don't expect this file to move
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
DISO_DIR = DATA_DIR / "diso"
COMPACT_DIR = DATA_DIR / "diso-compact"

###
# DISO ONTOLOGY REGISTRY
# ----------------------
# To identify ontologies we use the filename stem. This registers 
# them to (at least one, possibly many) clusters. We do not consider 
# the possibility where two ontologies may share the same stem.
# In such cases, we expect the user to make reasonable adjustments 
# eg. UCO1 and UCO2
###

ONTO_REGISTRY_FILE = COMPACT_DIR / "_registry.yaml"

###
# MATCHERS & CONFIGS
# ------------------
# Matchers (ie. OM systems) often come in the form of JARs (with
# their own set of dependencies, etc). We expect each matcher to
# be located within its own directory in MATCHERS_DIR. Also,
# different matchers have different parameters and configurable
# settings. These are specified within the CONFIGS_DIR.
###

MATCHERS_DIR = PROJECT_ROOT / "matchers"
CONFIGS_DIR = PROJECT_ROOT / "configs"

###
# RUNS & LOGS
# -----------
# RUNS_DIR contains results, whereas LOGS_DIR 
# contains std::out logs
###

RUNS_DIR = PROJECT_ROOT / "runs"
LOGS_DIR = PROJECT_ROOT / "logs"

###
# TODO: CONSENSUS_DIR
###

CONSENSUS_DIR = PROJECT_ROOT / "consensus"

