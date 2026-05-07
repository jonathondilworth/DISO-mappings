# DISO-mappings

DISO-mappings, a reproducible Ontology Matching (OM) pipeline for pairwise alignment generation across the [DISO ontologies](https://github.com/city-artificial-intelligence/diso): over 60 public OWL ontologies in defence, intelligence, and security. The pipeline includes adapters for AML [[1]](#references), LogMap & LogMapLt [[2]](#references), and BERTMap & BERTMapLt [[3, 4]](#references). Each adapter produces correspondences in the OAEI RDF Alignment Format. This effort directly supports the establishment of a [new OAEI track for defence and security](https://github.com/city-artificial-intelligence/diso-oaei).

## Project Status

The matching pipeline is complete and can be readily extended. The consensus-derived silver-standard reference alignment has been obtained. Users can reproduce this preliminary reference alignment using the [Makefile](./Makefile) shipped within this repository or by using [the OAEI evaluation tools repository](https://github.com/ernestojimenezruiz/oaei-evaluation). AML and LogMap are currently distributed as precompiled JARs in this repository. However, future releases will allow matchers to be installable from a _distributed registry_. BERTMap is supported via [DeepOnto](https://github.com/KRR-Oxford/DeepOnto) [[4]](#references); we include a [vendor fork of the project](https://github.com/jonathondilworth/DeepOnto) as a dependency. Regarding shipped adaptors: Matcha is proprietary and not publicly released. The LogMapLLM adapter is available to use under the [feature/logmap-llm](https://github.com/city-artificial-intelligence/DISO-mappings/tree/feature/logmap-llm) branch, but should be considered experimental.

## Requirements

- **OS:** Linux or macOS. _Windows is untested._
- **Python 3.10+** _(environment file pins 3.11)_.
- **JDK 11** for AML, LogMap, LogMapLt, and the LogMap repair step in BERTMap. Provided by `openjdk=11` in `environment.yml`.
- BERTMap requires a CUDA GPU; CPUs are not supported (see `_resolve_cuda_device_index` in `src/diso_mappings/matchers/bertmap.py`). BERTMapLt does **not** require a GPU.
- **Disk:** ~2GB for DISO compact, model caches, and outputs.
- **Network:** required for the initial DISO download and Hugging Face model download (BERTMap).

_(Tested on Ubuntu 24.04.4 LTS and MacOS Tahoe 26.4)_

## Installation

```
conda env create --file environment.yml
conda activate diso-mappings
pip install -e .
```

`pip install -e .` installs the project, making `diso_mappings` importable from anywhere. The matcher adapters self-register via the `@register` decorator the first time `diso_mappings.matchers` is imported (each adapter module is imported by the package's [`__init__.py`](src/diso_mappings/matchers/__init__.py)).

Users are currently advised to use the precompiled AML and LogMap JARs in the [matchers](matchers) directory. Alternatively, you can compile these JARs from source, but we provide no guarantees of compatibility for user-built JARs (since actively recompiled open-source research code is often difficult to version pin).

## Reproducing alignments

To reproduce alignments with a freshly cloned repo and an activated environment, run:

```sh
make
```

The default `make` pipeline performs the following actions (see the [Makefile](Makefile)):

1. `make download` fetches the DISO release tarball into `data/diso/`.
2. `make compact` extracts `diso-compact.zip`, walks the cluster directories, and writes the ontology registry to `data/diso-compact/_registry.yaml`.
3. `make labels` derives labels from the class local name for any classes lacking an `rdfs:label` annotation. The process is idempotent; classes with existing labels remain unchanged.
4. `make parseable` removes obsolete `owl:imports` triples and converts `d3fend.ttl` to RDF/XML for AML compatibility. This step registers `*-noimports` and `d3fend-rdf-xml` ontologies as referenced in the [paper pairs](./configs/pairs.paper.yaml).
5. `make mappings PAIRS=configs/pairs.paper.yaml` runs each matcher over the eight ontology pairs in `configs/pairs.paper.yaml`. Writes results to `runs/<matcher>/<UTC-timestamp>/.` with subdirectories: `alignments`, `failed`, and `logs`.
6. `make consensus` builds the consensus reference alignment over `PAIRS` via their latest runs. Writes consensus output to `consensus/<UTC-timestamp>/.`.

To run a single matcher over a custom pair list:

```
make mappings MATCHER=aml PAIRS=configs/pairs.example.yaml
```

To pass extra arguments to `scripts/run_matcher.py`:

```
make mappings MATCHER=bertmap PAIRS=configs/pairs.paper.yaml ARGS='--verbose --timeout=7200 --force'
```

`ARGS` are forwarded as specified. We recommend you run `make help` for a summary of available Make targets.

## Project Structure

```
.
├── configs                 
│   ├── aml.yaml            # one config file per matcher
│   ├── bertmap_lt.yaml         
│   ├── bertmap.yaml            
│   ├── logmap_lt.yaml          
│   ├── logmap.yaml             
│   ├── pairs.example.yaml  # two illustrative ontology pairs
│   └── pairs.paper.yaml    # the eight pairs reported in the paper
├── environment.yml         # conda env (Python, JDK 11, Torch, DeepOnto)
├── Makefile                # entry points; run `make help`
├── matchers                # external JARs and their dependencies
│   ├── aml                     
│   └── logmap                  
├── pyproject.toml              
├── README.md                   
├── scripts                 # CLI entry points (called by the Makefile)
│   ├── consensus.py             
│   ├── download_diso.py         
│   ├── extract_compact_diso.py  
│   ├── imports_and_parseable.py 
│   ├── preprocess_labels_om.py  
│   └── run_matcher.py           
└── src
    └── diso_mappings
        ├── consensus
        │   ├── consensus.py
        │   ├── discovery.py
        │   ├── __init__.py
        │   ├── labels.py
        │   ├── stats.py
        │   ├── unique.py
        │   ├── voting.py
        │   └── writers.py
        ├── constants.py        # Configurable project-wide constants
        ├── __init__.py         
        ├── io                  
        │   ├── alignment.py    # OAEI RDF read/write
        │   ├── __init__.py     
        │   └── terminal.py     
        ├── matchers            # adapter implementations + base class
        │   ├── aml.py           
        │   ├── base.py         # Matcher ABC, @register, get_matcher
        │   ├── bertmap_lt.py   
        │   ├── bertmap.py      
        │   ├── _deeponto_common.py
        │   ├── __init__.py     
        │   ├── logmap_lt.py    
        │   ├── logmap.py       
        │   ├── _subprocess_runner.py
        │   └── _workers        # subprocess workers dir for python-driven matchers
        ├── pairs.py            # Pair schema and YAML loader
        ├── paths.py            # project-relative paths
        ├── preprocessing.py
        ├── _rdflib_common.py
        └── registry.py         # OntologyRegistry: filename-stem -> Ontology
```

Note that you can expect three additional directories to be created during use:
  1. The `data` directory is populated by running `make download`.
  2. The `runs` directory is populated by running `make mappings`.
  3. The `consensus` directory is populated by running `make consensus`.

## Matchers

| Name         | Family  | Shipped                                                                                                       | Confidence                                | GPU required | Notes                                         |
| ------------ | ------- | ------------------------------------------------------------------------------------------------------------- | ----------------------------------------- | ------------ | --------------------------------------------- |
| `aml`        | AML     | Yes                                                                                                           | Yes                                       | No           | AgreementMakerLight 3.2; auto mode only       |
| `logmap`     | LogMap  | Yes                                                                                                           | Yes                                       | No           | LogMap 2 with logical repair                  |
| `logmap_lt`  | LogMap  | Yes                                                                                                           | No                                        | No           | LogMap-Lite; lexical only, scores are uniform |
| `logmap_llm` | LogMap  | [Branch](https://github.com/city-artificial-intelligence/DISO-mappings/tree/feature/logmap-llm) | Yes                                       | Yes          | LogMap + use of vLLM to aid as an oracle      |
| `bertmap`    | BERTMap | Yes                                                                                                           | Yes  <small>*(high thresholding)*</small> | Yes          | BERT fine-tuning + LogMap repair (DeepOnto)   |
| `bertmap_lt` | BERTMap | Yes                                                                                                           | Yes  <small>*(high thresholding)*</small> | No           | String-distance variant (DeepOnto)            |
| `matcha`     | Matcha  | No                                                                                                            | Yes                                       | No           | Proprietary closed source (as of May 2026).   |

The [`family` attribute](src/diso_mappings/matchers/base.py) is used by the [consensus component](src/diso_mappings/consensus/) to prevent correlated matchers from artificially inflating agreement unfairly. For instance, a mapping produced by both BERTMap and BERTMapLt is counted as a single entry toward the "BERTMap family" rather than as two separate entries. Singleton families (`aml`, `matcha`) cast one vote each — this is by design, not a count error.

Configuration for each matcher is specified in `configs/<matcher>.yaml`. The configuration keys are documented within these files and in the corresponding adapter module's doc-string.

## Pairs

A pair list is a YAML file with a single top-level key named `pairs`. Sources and targets are identified by their **registered filename stem in** `_registry.yaml` (such as `JC3IEDM` or `Brick+imports`); i.e., these serve as the canonical identifiers used by `OntologyRegistry`. Any identifier registered in `data/diso-compact/_registry.yaml` is valid. **Self-pairs and duplicate pairs result in an error.**

An example pair:

```yaml
pairs:
  - source: stix-spec-merged
    target: uco2
  - source: d3fend-rdf-xml
    target: stix-spec-merged
```

See [`configs/pairs.example.yaml`](configs/pairs.example.yaml) and [`configs/pairs.paper.yaml`](configs/pairs.paper.yaml) for additional illustrative examples.

## Outputs

### Mappings

Each invocation of `scripts/run_matcher.py` (called from `make mappings`) creates a timestamped run directory:

```
runs/<matcher>/<UTC-timestamp>/
├── alignments/
│   └── <source>__<target>.rdf      # OAEI RDF Alignment Format
├── logs/
│   └── per-pair/
│       └── <source>__<target>.log  # full matcher stdout/stderr
└── failed/
    └── <source>__<target>.err      # exception text for any failures
```

Output directories are preserved and not overwritten. Each re-run creates a new timestamped output directory. The skip-if-exists check runs within a directory, allowing resumption of failed runs by passing `--output-dir` to `scripts/run_matcher.py`.

The OAEI RDF output serves as the canonical format. To inspect an alignment programmatically, apply the following approach:

```python
from diso_mappings.io.alignment import read_alignment

alignment = read_alignment("runs/aml/<RUN_ID>/alignments/<ONTO_ONE_NAME>__<ONTO_TWO_NAME>.rdf")

print(f"{len(alignment.mappings)} mappings between {alignment.onto1_iri} and {alignment.onto2_iri}")

for m in alignment.mappings[:5]:
    print(f"  {m.entity1}  {m.relation}  {m.entity2}  ({m.measure:.3f})")
```

### Consensus

Each invocation of `scripts/consensus.py` (called from `make consensus`) creates a timestamped run directory:

```
consensus/<UTC-timestamp>/
├── manifest.yaml                       # run metadata + per-pair summary
└── pairs/
    └── <source>__<target>/
        ├── consensus.tsv               # consolidated table; family_votes column
        ├── per-vote/
        │   ├── consensus-2.tsv         # mappings agreed by >= 2 families
        │   ├── consensus-2.rdf         # OAEI RDF, >= 2 families
        │   └── consensus-N.{tsv,rdf}   # up to the maximum observed vote count
        ├── unique-by-system/           # one TSV per matcher (incl. empties)
        ├── unique-by-family/           # one TSV per family (incl. empties)
        └── stats.txt                   # OAEI-evaluation format Vote-N statistics
```

The silver-standard reference alignment is `consensus-N.rdf` for whichever vote threshold is appropriate. For our case, we opt to use `consensus-2.rdf` as the default for the eight pairs reported in the paper (a mapping is accepted if at least two matcher families agree). The `unique-by-system/` and `unique-by-family/` outputs report mappings produced by exactly one matcher or family; these are the candidates flagged for manual review.

## Extending the Pipeline: Adding a New Matcher

A matcher must be implemented as a subclass of `diso_mappings.matchers.base.Matcher` and decorated with `@register`. To add your own matcher, the following requirements must be met:

- A `name: str` class attribute — the unique CLI identifier passed to `--matcher <name>`.
- An _optional_ `_family: str` — used by the consensus rule to group correlated matchers.
- A `version` @property, which returns a human-readable _(reliable or approximate)_ version string.
- A `run()` method that performs the match and returns a `MatchResult`.

The `run()` method writes an OAEI RDF alignment file to out_dir/f`"{source.stem}__{target.stem}.rdf"` and returns the path along with the wall-clock duration. Timeouts must raise `TimeoutError`; other errors should raise exceptions (the harness logs them under `failed/`).

A minimal stub for generating an empty alignment file (sufficient boilerplate for registering a new matcher and verifying end-to-end integration) is provided below. Save the following code under `src/diso_mappings/matchers/mymatcher.py`:

```python
from __future__ import annotations

import time
from pathlib import Path

from diso_mappings.matchers.base import Matcher, MatchResult, register
from diso_mappings.io.alignment import Alignment, write_alignment


@register
class MyMatcher(Matcher):
    name = "mymatcher"
    _family = "MyMatcher"   # optional; defaults to `name`

    @property
    def version(self) -> str:
        return "0.1.0"

    def run(
        self,
        source: Path,
        target: Path,
        out_dir: Path,
        config: dict | None = None,
        timeout: float | None = None,
    ) -> MatchResult:
        if not source.exists():
            raise FileNotFoundError(source)
        if not target.exists():
            raise FileNotFoundError(target)

        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{source.stem}__{target.stem}.rdf"

        t0 = time.time()

        ##                                                           ##
        # --> ...your matching logic populates `mappings` here... <-- #
        ##                                                           ##

        alignment = Alignment(
            onto1_iri=source.resolve().as_uri(),
            onto2_iri=target.resolve().as_uri(),
            mappings=[],
        )
        write_alignment(alignment, out_path)
        return MatchResult(alignment_path=out_path, duration_seconds=time.time() - t0)
```

Next, add the import statement to `src/diso_mappings/matchers/__init__.py` to ensure registration occurs at import time:

```python
from . import mymatcher
```

Verify that the matcher has been registered:

```
python scripts/run_matcher.py --matcher mymatcher --pairs configs/pairs.example.yaml
```

For matchers that rely on an external JAR, refer to [`aml.py`](src/diso_mappings/matchers/aml.py) or [`logmap.py`](src/diso_mappings/matchers/logmap.py) for the subprocess pattern, which includes per-pair temporary directories, `run_subprocess_with_timeout`, and post-run output validation. For Python matchers that are stateful or may leak memory across invocations, such as those loading large neural models, consult the BERTMap workers under [`_workers/`](src/diso_mappings/matchers/_workers) for the per-pair subprocess isolation approach.

## Project Roadmap

| **Feature**                                                       | **Status**                      | **Description**                                                                                                                                                                                                                                                                                                                                                                |
| ----------------------------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Download + extract DISO compact                                   | COMPLETE                        | Makes a cURL request to the official DISO repo (downloading all DISO ontologies), then  extracts the compact DISO variant ontologies.                                                                                                                                                                                                                                          |
| Local **ontology registry** + pair configs                        | COMPLETE                        | The local Ontology Registry walks a specified dir and collects any recognisable ontologies,  placing them (along with a path relative to their parent dir) in a project-wide directory.                                                                                                                                                                                        |
| Label enrichment + parseability fixes                             | COMPLETE                        | The term 'label enrichment' defined simply: given an OWL ontology whose classes do not  all have an `rdfs:label` annotation property, attach one by deriving it from an IRI-extracted local name (fixes parseability issues for rdflib).                                                                                                                                       |
| Matcher adapters (AML, LogMap, LogMapLt, BERTMap, BERTMapLt)      | COMPLETE                        | Each matcher adapter simply wraps existing ontology matching systems, in whatever form  they may be provisioned. For JARs/compiled binaries, these are invoked as subprocesses; we maintain this separation for modular python matchers by implementing python workers.                                                                                                        |
| OAEI RDF read/write                                               | COMPLETE                        | We provide an OAEI-compliant parser with multiple modes: (1) *lenient*: allowing for users to specify translation rules between OM system output, silently canonicalising any matchers output into OAEI-compliant form; (2) *strict*, similar to lenient, but raises a warning for each translation instance; and (3) *draconian*, raises an exception for ANY non-compliance. |
| **Consensus / silver-standard aggregation (reference alignment)** | COMPLETE                        | We combine output mappings from multiple ontology matching systems to obtain a silver standard reference alignment. The pipeline is exposed via `make consensus`; output layout and discovery semantics are documented under [Consensus](#consensus). The implementation lives in `src/diso_mappings/consensus/`.                                                    |
| LogMap-LLM & proprietary adapters                                 | _not shipped in this branch / release_   | LogMap-LLM is a heavy external dependency that we include as optional within a [separate branch](https://github.com/city-artificial-intelligence/DISO-mappings/tree/feature/logmap-llm); however, it is currently omitted from the main branch. Adapters for reported alignments obtained by proprietary software (i.e., Matcha) are not shipped due to licensing reasons.                                                                                                     |
| Ontology Matching System Registry                                 | _future project (out of scope)_ | Ideally, we would like users to easily download commonly used adapters and register their  own custom systems with adapters within a distributed registry, making benchmark results easy to obtain, automatic to register and *(optionally)* viewable on public leaderboards.                                                                                                  |

## Known issues and limitations

- BERTMap on CPU is unsupported in this version. The adapter raises `NotImplementedError`.
- `d3fend.ttl` is converted to RDF/XML during `imports-parseable` because AML cannot parse it as Turtle. The converted file is registered as `d3fend-rdf-xml`.
- Two ontologies (`mIOmerged`, `FacilityOntology`) have dead `owl:imports` that prevent the matchers from running. `imports-parseable` strips them and registers the cleaned files under the `*-noimports` names referenced in `pairs.paper.yaml`.
- LogMap’s translation module emits stack traces from a defunct Google Translate endpoint. This is a known upstream issue in the 2021 LogMap release; it will break the pipeline; you must recompile the source or use the patched version shipped in this repository under `matchers/logmap` _(replacing this with your own compiled LogMap JAR may still introduce issues)._
- `FacilityOntology` requires a raised recursion limit. `scripts/run_matcher.py` sets `sys.setrecursionlimit(48000)` for this reason.
- DeepOnto patches. The pinned DeepOnto fork patches minor upstream issues.

### Cite _(this work)_

Citation will be added if the accompanying resource paper is published. In the interim, please cite the [DISO collection](https://doi.org/10.5281/zenodo.20059507) as the canonical citable artefact in this ecosystem.

### Contributors

[Ernesto Jimenez-Ruiz](https://ernestojimenezruiz.github.io/), [Pedro Cotovio](https://pedrocotovio.github.io/), [Jon Dilworth](https://dilworth.io/) and [Dave Herron](https://djherron.github.io/).

## Acknowledgements

This research was supported by Turing Innovations Limited and The Alan Turing Institute’s Defence and Security Programme via the [GUARD project](https://ernestojimenezruiz.github.io/projects/guard/). We thank the maintainers of the matching systems and supporting libraries: [AgreementMakerLight](https://github.com/AgreementMakerLight/AML-Project), [LogMap](https://github.com/ernestojimenezruiz/logmap-matcher), [BERTMap](https://github.com/krr-oxford/bertmap), and [DeepOnto](https://github.com/KRR-Oxford/DeepOnto) _(that this pipeline depends on)_.

## References

_If you use this pipeline, please consider citing the underlying matchers and related materials._

* [1] AgreementMakerLight: [https://github.com/AgreementMakerLight/AML-Project](https://github.com/AgreementMakerLight/AML-Project) - Faria, D., Pesquita, C., Santos, E., Palmonari, M., Cruz, I. F., & Couto, F. M. (2013, September). The agreementmakerlight ontology matching system. In OTM Confederated International Conferences" On the Move to Meaningful Internet Systems" (pp. 527-541). Berlin, Heidelberg: Springer Berlin Heidelberg.
* [2] LogMap: [https://github.com/ernestojimenezruiz/logmap-matcher](https://github.com/ernestojimenezruiz/logmap-matcher) - Jiménez-Ruiz, E., & Cuenca Grau, B. (2011, October). Logmap: Logic-based and scalable ontology matching. In International Semantic Web Conference (pp. 273-288). Berlin, Heidelberg: Springer Berlin Heidelberg.
* [3] BERTMap (via DeepOnto): [https://github.com/KRR-Oxford/DeepOnto](https://github.com/KRR-Oxford/DeepOnto) - He, Y., Chen, J., Dong, H., Horrocks, I., Allocca, C., Kim, T., & Sapkota, B. (2024). DeepOnto: A Python package for ontology engineering with deep learning. Semantic Web, 15(5), 1991-2004.
* [4] BERTMap: [https://github.com/krr-oxford/bertmap](https://github.com/krr-oxford/bertmap) - He, Y., Chen, J., Antonyrajah, D., & Horrocks, I. (2022, June). BERTMap: a BERT-based ontology alignment system. In Proceedings of the AAAI Conference on Artificial Intelligence (Vol. 36, No. 5, pp. 5684-5691).
* [5] DISO repository: [https://github.com/city-artificial-intelligence/diso](https://github.com/city-artificial-intelligence/diso) - Dilworth, J., Cotovio, P., Herron, D., Pesquita, C., & Jiménez-Ruiz, E. (2026). DISO: Defence, Intelligence and Security Ontologies (Version 1.0.0). Zenodo. https://doi.org/10.5281/zenodo.20059507
* [6] OAEI Alignment Format: [https://moex.gitlabpages.inria.fr/alignapi/format.html](https://moex.gitlabpages.inria.fr/alignapi/format.html) - David, J., Euzenat, J., Scharffe, F., & Trojahn dos Santos, C. (2011). The alignment API 4.0. Semantic web, 2(1), 3-10.
