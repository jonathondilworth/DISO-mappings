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

## Usage

_TODO_

**References:**

[1] [https://github.com/AgreementMakerLight/AML-Project](https://github.com/AgreementMakerLight/AML-Project)
[2] [https://github.com/ernestojimenezruiz/logmap-matcher](https://github.com/ernestojimenezruiz/logmap-matcher)
[3] [https://github.com/krr-oxford/bertmap](https://github.com/krr-oxford/bertmap)
[4] [https://github.com/city-artificial-intelligence/diso](https://github.com/city-artificial-intelligence/diso)

