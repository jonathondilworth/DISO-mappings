"""
Shared helpers for DeepOnto-backed matchers (BERTMap, BERTMapLt)
Currently:
    * init_deeponto_jvm : JVM bring-up before any deeponto import
    * deeponto_tsv_to_mappings : parse DeepOntos mapping TSV output
    * deeponto_version : version string for the installed DeepOnto

DeepOnto's BERTMap pipeline has several known rough edges (small-dataset
Trainer bug, a typo in read_table_mappings, etc) that earlier versions of
this codebase patched at import time. However, we have since created a
vendor-fork of DeepOnto (see enviornment.yml).
"""
from __future__ import annotations

import csv
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from pathlib import Path

from diso_mappings.io.terminal import debug, info, warn

##
# _jvm_initialised
# global state used to manage/track whether the JVM 
# for DeepOnto has been started (see comments)
##

_jvm_initialised: bool = False


def deeponto_version() -> str:
    """
    Return the installed DeepOnto's version string via package metadata
    """
    try: return _pkg_version("deeponto")
    except PackageNotFoundError: return "unknown"


def init_deeponto_jvm(jvm_heap: str = "8g") -> None:
    """
    Start the JVM with max-heap; this must be called before any DeepOnto 
    code that touches the OWL API, because DeepOnto starts the JVM lazily 
    on first use and uses click.prompt to ask the user for heap size
    if not pre-configured —- this will hang silently in a subprocess
    """
    global _jvm_initialised
    if _jvm_initialised:
        debug(f"DeepOnto JVM already initialised (requested heap={jvm_heap})")
        return
    # deeponto.utils.init_jvm is the documented entry point; it internally calls JPype's 
    # startJVM with the given -Xmx and -ea flags, and sets up the OWL API classpath
    # see DeepOnto 0.9.3 source:
    # https://github.com/KRR-Oxford/DeepOnto/blob/main/src/deeponto/utils/__init__.py
    try:
        from deeponto import init_jvm as _deeponto_init_jvm
    except ImportError as import_exception:
        raise RuntimeError(
            f"DeepOnto is required for this matcher: {import_exception}"
        ) from import_exception
    # finally:
    info(f"Initialising DeepOnto JVM with heap={jvm_heap}")
    _deeponto_init_jvm(jvm_heap)
    _jvm_initialised = True



def deeponto_tsv_to_mappings(tsv_path: Path) -> list[tuple[str, str, float]]:
    """
    Parse a DeepOnto mapping TSV file into a list of (src_entity_iri,
    tgt_entity_iri, score) tuples. DeepOnto's TSV format is:
        SrcEntity\tTgtEntity\tScore
    Returns empty list if the file has a header row but no data rows.
    Raises FileNotFoundError if the file does not exist.
    """
    if not tsv_path.exists():
        raise FileNotFoundError(f"DeepOnto mapping TSV not found: {tsv_path}")

    parsed_mappings: list[tuple[str, str, float]] = []

    with open(tsv_path, newline="") as tsv_file:

        reader = csv.reader(tsv_file, delimiter="\t")
        header_row = next(reader, None)

        if header_row is None:
            warn(f"{tsv_path.name}: empty file (no header)")
            return parsed_mappings

        # locate columns case-insensitively; DeepOnto has used both 'Score' and 'score' across versions
        lower_header = [col.strip().lower() for col in header_row]
        try:
            src_idx   = lower_header.index("srcentity")
            tgt_idx   = lower_header.index("tgtentity")
            score_idx = lower_header.index("score")
        except ValueError as raised_exception:
            raise ValueError(
                f"{tsv_path.name}: expected columns 'SrcEntity', 'TgtEntity', 'Score'. got {header_row!r}"
            ) from raised_exception

        for row_idx, data_row in enumerate(reader, start=2):

            if not data_row or all(not cell.strip() for cell in data_row):
                continue  # blank line

            try:
                src_iri   = data_row[src_idx].strip()
                tgt_iri   = data_row[tgt_idx].strip()
                raw_score = data_row[score_idx].strip()
            except IndexError as raised_exception:
                raise ValueError(
                    f"{tsv_path.name}:{row_idx}: row has too few columns: {data_row!r}"
                ) from raised_exception

            try:
                parsed_score = float(raw_score)
            except ValueError as raised_exception:
                raise ValueError(
                    f"{tsv_path.name}:{row_idx}: score {raw_score!r} is not a float"
                ) from raised_exception

            parsed_mappings.append((src_iri, tgt_iri, parsed_score))

    return parsed_mappings