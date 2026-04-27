# convienent Makefile for diso-mappings repo  (self-documenting)

PYTHON  ?= python
MATCHER ?= aml logmap_lt logmap bertmap_lt bertmap
PAIRS   ?= configs/pairs.example.yaml
ARGS    ?=

.PHONY: help download-diso diso-compact local-labels no-dead-imports mappings clean

all: download-diso diso-compact local-labels no-dead-imports mappings

help:
	@echo "DISO-mappings Makefile commands:"
	@echo ""
	@echo "  download-diso       Fetch DISO ontologies, saves to 'data/diso'."
	@echo "  diso-compact        Extract DISO compact, saves to 'data/diso-compact'."
	@echo "  local-labels        Extracts local names as rdfs:label annotations for classes w/o anns."
	@echo "  no-dead-imports     Removes dead imports that cause matching to hang."
	@echo "  mappings 			 Run a MATCHER on a set of ontology PAIRS using ARGS;"
	@echo " 					   accepts MATCHER=[aml, logmap_lt, logmap, bertmap_lt, bertmap]"
	@echo "                                PAIRS=configs/pairs.example.yaml"
	@echo "                                ARGS='--verbose --timeout=3600'"
	@echo "  clean               Removes all runs, data, and logs."
	@echo ""
	@echo ""

download-diso:
	$(PYTHON) scripts/download_diso.py $(ARGS)

diso-compact:
	$(PYTHON) scripts/extract_compact_diso.py $(ARGS)

local-labels:
	$(PYTHON) scripts/preprocess_labels_om.py $(ARGS)

no-dead-imports:
	$(PYTHON) scripts/remove_dead_imports.py $(ARGS)

mappings:
	@for m in $(MATCHER); do \
		echo "Running matcher: $$m"; \
		$(PYTHON) scripts/run_matcher.py --matcher $$m --pairs $(PAIRS) $(ARGS); \
	done

clean:
	rm -rf runs/*
	rm -rf data/*
	rm -rf logs/*
	@echo "Cleared all runs, data, and logs!"
