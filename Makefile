# convienent Makefile for diso-mappings repo  (self-documenting)

PYTHON  ?= python
MATCHER ?= "$(error MATCHER is required.)"
PAIRS   ?= configs/pairs.example.yaml
ARGS    ?=

.PHONY: help download-ontologies normalise-ontologies augment-onto-labels mappings test clean list-matchers

help:
	@echo "DISO-mappings Makefile commands:"
	@echo ""
	@echo "  download-diso           Fetch DISO ontologies, save to 'data/diso'."
	@echo "  compact-ontos           TODO"
	@echo "  agnostic-labels         TODO"
	@echo "  mappings MATCHER=<OM>   TODO"
	@echo "  clean                   TODO"
	@echo ""

download-diso:
	$(PYTHON) scripts/download_diso.py $(ARGS)

compact-ontos:
	$(PYTHON) scripts/extract_compact_diso.py $(ARGS)

agnostic-labels: compact-ontos
	$(PYTHON) scripts/preprocess_labels_om.py $(ARGS)

mappings:
	$(PYTHON) scripts/run_matcher.py --matcher $(MATCHER) --pairs $(PAIRS) $(ARGS)

clean:
	rm -rf runs/*
	rm -rf data/*
	rm -rf logs/*
	@echo "Cleared all runs, data, and logs!"
