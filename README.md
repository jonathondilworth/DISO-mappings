# DISO-mappings

Experimental repository for exploring DISO ontologies with OM systems. The specific OM tooling in use within this repository is AgreementMakerLight (AML) [1], LogMap [2], and BERTMap [3].

> "DISO is a collection of publicly available OWL ontologies relating to the domains of defence, intelligence and security."
> Refer to [4] for further detail (original author: David Herron).

This repository provides a convienent way to (1) download, (2) extract, (4) pre-process and (4) align these domain ontologies. 

It can also be used to provide a _silver standard reference alignment **(TODO)**_ for a subset of the ontologies.

## Setup

Setup your environment using the provided `environment.yml` file:

```sh
conda env create --file environment.yml
conda activate diso-mappings
pip install -e .
```

### Download, Extract & Pre-process DISO

For convienence, we provide a [`Makefile`](./Makefile) which provides commands:

1. `make download-diso` downloads the **full set of** DISO ontologies to `./data/diso`.
2. `make compact-ontos` extracts DISO compact$^1$ to `./data/diso-compact` and builds an [OntologyRegistry](./src/diso_mappings/registry.py) at `./data/diso-compact/_registry.yaml`.

## Usage

_TODO_

**Footnotes:**

1. A compact version of DISO — one with less structure and no documentation, and hence better suited to machine processing:

> "To further facilitate reuse of the DISO collection as a research resource, a condensed version of the DISO collection is available within the DISO repository. We call this condensed version ‘DISO compact’. DISO compact has a simpler folder structure and no documentation. In DISO compact, there is one folder for each DISO cluster, with each cluster folder containing one ontology file for each ontology assigned to that cluster. DISO compact is presented as an archive (.zip) file within the DISO repository. The general idea is that researchers would download DISO compact for use in software applications, while using the online DISO repository for context and documentation." [4]

**References:**

* [1] [https://github.com/AgreementMakerLight/AML-Project](https://github.com/AgreementMakerLight/AML-Project)
* [2] [https://github.com/ernestojimenezruiz/logmap-matcher](https://github.com/ernestojimenezruiz/logmap-matcher)
* [3] [https://github.com/krr-oxford/bertmap](https://github.com/krr-oxford/bertmap)
* [4] [https://github.com/city-artificial-intelligence/diso](https://github.com/city-artificial-intelligence/diso)

