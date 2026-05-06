# convienent Makefile for diso-mappings repo  (self-documenting)

PYTHON  ?= python
MATCHER ?= aml logmap_lt logmap bertmap_lt bertmap
PAIRS   ?= configs/pairs.paper.yaml
ARGS    ?=

.PHONY: help download compact labels parseable mappings consensus clean

all: download compact labels parseable mappings consensus

help:
	@echo "DISO-mappings Makefile commands:"
	@echo ""
	@echo "  download       Fetch DISO ontologies, saves to 'data/diso'."
	@echo ""
	@echo "  compact        Extract DISO compact, saves to 'data/diso-compact'."
	@echo ""
	@echo "  labels         Extracts local names as rdfs:label annotations for classes w/o anns."
	@echo ""
	@echo "  parseable      Removes dead imports and parse issues that cause matching to hang."
	@echo ""
	@echo "  mappings       Run a MATCHER on a set of ontology PAIRS using ARGS;"
	@echo "                     accepts MATCHER=[aml, logmap_lt, logmap, bertmap_lt, bertmap]"
	@echo "                             PAIRS=configs/pairs.example.yaml"
	@echo "                             ARGS='--verbose --timeout=3600'"
	@echo ""
	@echo "  consensus      Build the consensus reference alignment over PAIRS via their latest runs."
	@echo "                 Outputs to: consensus/<UTC-timestamp>/."
	@echo "					Forward extra flags via ARGS, e.g.:"
	@echo "                     ARGS='--no-labels'                (skip rdfs:label resolution)"
	@echo "                     ARGS='--lowercase-labels'         (java-parity label casing)"
	@echo "                     ARGS='--read-mode strict'         (loud about non-spec input)"
	@echo "                     ARGS='--runs runs/aml/<UTC1> ...' (override auto-discovery)"
	@echo "                     ARGS='--verbose'				  (use verbose logging)"
	@echo ""
	@echo "  clean          Removes all runs, data, and logs."
	@echo ""
	@echo ""

download:
	$(PYTHON) scripts/download_diso.py $(ARGS)

compact:
	$(PYTHON) scripts/extract_compact_diso.py $(ARGS)

labels:
	$(PYTHON) scripts/preprocess_labels_om.py $(ARGS)

parseable:
	$(PYTHON) scripts/imports_and_parseable.py $(ARGS)

mappings:
	@for m in $(MATCHER); do \
		echo "Running matcher: $$m"; \
		$(PYTHON) scripts/run_matcher.py --matcher $$m --pairs $(PAIRS) $(ARGS); \
	done

consensus:
	$(PYTHON) scripts/consensus.py --pairs $(PAIRS) --matchers $(MATCHER) $(ARGS)

clean:
	rm -rf runs/*
	rm -rf data/*
	rm -rf logs/*
	rm -rf consensus/*
	@echo "Cleared all runs, data, logs, and consensus outputs!"