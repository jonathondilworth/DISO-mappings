"""
Enrich a label-less DISO ontology CLS with 'rdfs:label' values derived from IRIs.
Currently only applies to classes. Should perhaps consider extending to properties. 
The process is outlined below:

    1. Walks the OntologyRegistry @ 'data/diso-compact/_registry.yaml'
    2. For each named 'owl:Class' (or 'rdfs:Class) that has no 'rdfs:label':
        - derive a local name from its IRI (see diso_mappings.preprocess::get_local_name)
        - adds that local name (unmodified) as an 'rdfs:label' literal (annotation).
    3. Writes a YAML 'enrichment' report at 'data/diso-compact/_enrichment_report.yaml'.

The extraction rule (roughly/broadly) mirrors the approach from AML:

https://github.com/AgreementMakerLight/AML-Project/blob/master/AgreementMakerLight/src/aml/ontology/Ontology.java#L1771-L1788

NOTE: THIS SCRIPT IS AN OPTIONAL PREPROCESSING STEP. If you would rather leave the ontologies 
completely unmodified, simply omit `make agnostic-labels` from your pipeline. Rerunning is 
idempotent (classes that already have an rdfs:label are left alone).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import yaml

from diso_mappings.paths import COMPACT_DIR, ONTO_REGISTRY_FILE
from diso_mappings.preprocessing import enrich_onto_labels
from diso_mappings.registry import OntologyRegistry
from diso_mappings.io.terminal import configure, info, warn, error, debug, success
from diso_mappings._rdflib_common import (
    install_rdflib_quiet_filter,
    get_rdflib_suppressed_count,
    reset_rdflib_suppressed_count,
)


_ENRICHMENT_REPORT_FILE = COMPACT_DIR / "_enrichment_report.yaml"


def main() -> int:
    """
    Configures logging -> loads registry -> runs _enrichment_ loop -> produces report
    optionally, can use --dry-run at the CLI for testing; verbose sets logging level
    """
    parser = argparse.ArgumentParser(
        description="Enrich DISO ontologies with rdfs:label from local names."
    )
    parser.add_argument(
        "--dry-run", action="store_true", 
        help="Report what would be enriched without writing any files."
    )
    parser.add_argument(
        "--verbose", action="store_true"
    )
    args = parser.parse_args()

    ###
    # SETUP
    ###

    sys.setrecursionlimit(48000) # cannot parse some ontologies otherwise!

    ##
    # LOGGING SETUP
    ##

    configure(
        level=logging.DEBUG if args.verbose else logging.INFO,
        stream=sys.stdout
    )

    ##
    # ADD FILTER FOR RDFLIB
    # (FOR PARSE ERRORS OF LOW CONSEQUENCE)
    ##

    install_rdflib_quiet_filter()

    ##
    # REGISTRY LOAD
    ##

    if not ONTO_REGISTRY_FILE.exists():
        error(f"Expected {ONTO_REGISTRY_FILE}. Run 'make compact-ontos' first.")
        return 1

    onto_registry = OntologyRegistry.load(ONTO_REGISTRY_FILE)
    onto_name_ids = onto_registry.entries()
    info(f"Loaded registry with {len(onto_name_ids)} ontologies.")

    ##
    # _ENRICHMENT_ LOOP
    ##

    per_ontology_results: dict[str, dict] = {}
    skipped_ontologies: list[dict] = []
    total_classes_seen = 0
    total_classes_without_label = 0
    total_classes_enriched = 0
    total_classes_skipped_empty = 0
    total_rdflib_suppressed = 0

    for this_name_id in onto_name_ids:

        this_ontology = onto_registry.resolve(this_name_id)
        this_onto_path = this_ontology.path

        debug(f"Enriching {this_name_id} ({this_onto_path.relative_to(COMPACT_DIR)})")

        reset_rdflib_suppressed_count()

        try:
            enrichment_result = enrich_onto_labels(this_onto_path, dry_run=args.dry_run)
        except Exception as enrichment_exception:
            # rdflib can fail on some DISO files (edge cases in OWL/RDF serialisation)
            # OWL API-based matchers (AML, LogMap) handle these natively, so we log
            # and continue rather than abort the whole run
            this_rdflib_suppressed = get_rdflib_suppressed_count()
            total_rdflib_suppressed += this_rdflib_suppressed
            failure_reason = f"{type(enrichment_exception).__name__}: {str(enrichment_exception)[:200]}"
            warn(f"Skipping {this_name_id}: {failure_reason}")
            skipped_ontologies.append({
                "name":   this_name_id,
                "path":   str(this_onto_path.relative_to(COMPACT_DIR)),
                "reason": failure_reason,
                "rdflib_suppressed": this_rdflib_suppressed,
            })
            continue

        this_rdflib_suppressed = get_rdflib_suppressed_count()
        total_rdflib_suppressed += this_rdflib_suppressed

        if this_rdflib_suppressed > 0:
            debug(f"{this_name_id}: suppressed {this_rdflib_suppressed} (possible malformed xsd:date in source)")

        per_ontology_results[this_name_id] = {
            "classes":                  enrichment_result["classes_seen"],
            "without_label":            enrichment_result["classes_without_label"],
            "enriched":                 enrichment_result["classes_enriched"],
            "skipped_empty_local_name": enrichment_result["classes_skipped_empty_local_name"],
            "rdflib_suppressed":        this_rdflib_suppressed,
        }

        total_classes_seen           += enrichment_result["classes_seen"]
        total_classes_without_label  += enrichment_result["classes_without_label"]
        total_classes_enriched       += enrichment_result["classes_enriched"]
        total_classes_skipped_empty  += enrichment_result["classes_skipped_empty_local_name"]

        if enrichment_result["classes_enriched"] > 0:
            info(f"  {this_name_id}: enriched {enrichment_result['classes_enriched']} of {enrichment_result['classes_seen']} classes")

    # END: forall ontologies in registry

    ##
    # REPORT
    ##

    report_lines = [
        f"\nCompleted! Label Enrichment Report:\n",
        f"  {len(per_ontology_results)} ontologies processed.",
        f"  {len(skipped_ontologies)} skipped.",
        f"  {total_classes_enriched} classes enriched ...",
        f"    (out of {total_classes_without_label} lacking rdfs:label, with {total_classes_seen} total classes seen)\n",
    ]
    
    info("\n".join(report_lines))

    if total_rdflib_suppressed > 0:
        warn(f"Suppressed {total_rdflib_suppressed} literal-conversion warning(s).")

    enrichment_report = {
        "totals": {
            "ontologies_processed":                 len(per_ontology_results),
            "ontologies_skipped":                   len(skipped_ontologies),
            "classes_seen":                         total_classes_seen,
            "classes_without_rdfs_label":           total_classes_without_label,
            "classes_enriched":                     total_classes_enriched,
            "classes_skipped_empty_local_name":     total_classes_skipped_empty,
            "rdflib_literal_conversion_suppressed": total_rdflib_suppressed,
        },
        "per_ontology": per_ontology_results,
        "skipped":      skipped_ontologies,
        "dry_run":      args.dry_run,
    }

    if args.dry_run:
        info(f"Dry run — not writing {_ENRICHMENT_REPORT_FILE}")
        success("Finished!")
        return 0
    
    # else: write the file to disk
    
    _ENRICHMENT_REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(_ENRICHMENT_REPORT_FILE, "w", encoding="utf-8") as report_file:
        yaml.safe_dump(enrichment_report, report_file, sort_keys=False, default_flow_style=False)
    
    info(f"Wrote enrichment report to {_ENRICHMENT_REPORT_FILE}")

    success("Finished!")
    return 0



if __name__ == "__main__":
    sys.exit(main())

