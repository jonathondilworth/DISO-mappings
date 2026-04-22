"""
Python script for preparing the DISO compact ontology network for matching.

This is what runs when you execute: `make compact-ontos`, broadly it:

    1. Extracts 'diso-compact.zip' from 'data/diso/diso-compact/diso-compact.zip' 
       to 'data/diso-compact'.
    
    2. Preserves the cluster directory structure, then walks the structure w/ 
       OntologyRegistry (registering the ontologies alongside their cluster 
       membership, identifiable via their 'stemmed file name' for human 
       readability; see registry.py).
    
    3. Writes the registry to disk at 'data/diso-compact/_registry.yaml'.
    
    4. [OPTIONALLY TODO] we might validate that the ontologies are parseable 
       (initial tests suggest rdflib fails for 2 of 60 ontologies; not yet
       tried replacing rdflib w/ owlready2 or just using DeepOnto).

If there are any issues with formats, then this is probably the place to
implement a conversion step _(maybe)_.
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
import zipfile
from pathlib import Path

from diso_mappings.paths import COMPACT_DIR, DISO_DIR, ONTO_REGISTRY_FILE
from diso_mappings.constants import ONTOLOGY_EXCLUSIONS
from diso_mappings.registry import OntologyRegistry
from diso_mappings.io.terminal import configure, highlight, info, error, debug, success

_ZIP_PATH = DISO_DIR / "diso-compact" / "diso-compact.zip"



def _extract_zip(zip_path: Path, zip_out_dest: Path) -> None:
    info(f"Extracting {zip_path} to {zip_out_dest}")
    zip_out_dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zipf:
        zipf.extractall(zip_out_dest)



def main() -> int:
    """
    script runs -> extracts COMPACT DISO -> builds registry
    registry is viewable @ data/diso-compact/_registry.yaml
    we then use this to reference the pairs for the matchers to use
    """
    parser = argparse.ArgumentParser(
        description="Prepares DISO compact for OM."
    )
    parser.add_argument(
        "--force", action="store_true", 
        help="Re-extract even if compact dir exists"
    )
    #############################################################################
    # TODO: rdflib is quite particular about what it can/cannot read.           #
    #       switching out for owlready2 / OWL API / DeepOnto = JVM is required. #
    #  We need to be able to do the label preprocessing with something though.. #
    #############################################################################
    # parser.add_argument(
    #     "--validate", action="store_true", 
    #     help="Skip rdflib parseability check"
    # )
    ##  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ [OPTIONAL TODO]
    parser.add_argument(
        "--include-cco-modules",
        action="store_true",
        help="Include the 11 CCO modules as registry entries (off by default)."
    )
    parser.add_argument(
        "--verbose", action="store_true"
    )
    args = parser.parse_args()

    highlight("DISO COMPACT Extraction script starting!")

    ##
    # LOGGING SETUP
    ##

    configure(
        level=logging.DEBUG if args.verbose else logging.INFO, 
        stream=sys.stdout
    )

    ##
    # DISO-COMPACT EXTRACTION
    ##

    if not _ZIP_PATH.exists():
        error(f"Expected {_ZIP_PATH}. Run 'make download-diso' first.")
        return 1

    do_extraction = (
        not COMPACT_DIR.exists() 
        or not any(COMPACT_DIR.iterdir())
    )

    if args.force and COMPACT_DIR.exists():
        info(f"Removing existing {COMPACT_DIR} (--force)")
        shutil.rmtree(COMPACT_DIR)
        do_extraction = True

    if do_extraction:
        _extract_zip(_ZIP_PATH, COMPACT_DIR)
        success("Zip Extracted!")
    else:
        info(f"{COMPACT_DIR} already extracted; rebuilding registry only.")

    ##
    # BUILD ONTOLOGY REGISTRY
    ##

    info(f"Building registry from {COMPACT_DIR}")

    exclude_ontos = ONTOLOGY_EXCLUSIONS.difference({"cco-modules"}) if args.include_cco_modules else ONTOLOGY_EXCLUSIONS

    onto_registry = OntologyRegistry.build(
        onto_dir_to_walk=COMPACT_DIR,
        exclusion_matches=exclude_ontos
    )

    onto_name_ids = onto_registry.entries()

    info(f"Found {len(onto_name_ids)} unique ontologies {'(including CCO modules)' if args.include_cco_modules else '(CCO modules excluded; see --include-cco-modules)'}.")

    for this_name_id in onto_name_ids:
        this_ontology = onto_registry.resolve(this_name_id)
        debug(f" [ONTOLOGY REGISTRY]  BOUND :: {this_name_id} -> {this_ontology.path.relative_to(COMPACT_DIR)}\t(clusters={this_ontology.clusters}")
        
    onto_registry.save(ONTO_REGISTRY_FILE)
    info(f"Wrote registry to {ONTO_REGISTRY_FILE}")
    success("Finished!")
    return 0


if __name__ == "__main__":
    sys.exit(main())

