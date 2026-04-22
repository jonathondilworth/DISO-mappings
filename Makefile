# convienent Makefile for diso-mappings repo  (self-documenting)

PYTHON  ?= python
MATCHER ?= "$(error MATCHER is required.)"
PAIRS   ?= configs/pairs.example.yaml
ARGS    ?=

.PHONY: help download-diso diso-compact agnostic-labels mappings clean

help:
	@echo "DISO-mappings Makefile commands:"
	@echo ""
	@echo "  download-diso           Fetch DISO ontologies, saves to 'data/diso'."
	@echo "  diso-compact            Extract DISO compact, saves to 'data/diso-compact'. TODO: add validation step."
	@echo "  agnostic-labels         Use IRI-extracted 'local names' as rdfs:label annotations (for classes w/o anns)."
	@echo "  mappings MATCHER=<OM>   TODO"
	@echo "  clean                   TODO"
	@echo ""

download-diso:
	$(PYTHON) scripts/download_diso.py $(ARGS)

diso-compact:
	$(PYTHON) scripts/extract_compact_diso.py $(ARGS)

agnostic-labels: diso-compact
	$(PYTHON) scripts/preprocess_labels_om.py $(ARGS)

mappings:
	$(PYTHON) scripts/run_matcher.py --matcher $(MATCHER) --pairs $(PAIRS) $(ARGS)

clean:
	rm -rf runs/*
	rm -rf data/*
	rm -rf logs/*
	@echo "Cleared all runs, data, and logs!"
