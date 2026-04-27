"""
Private worker helpers shared across matcher subprocess entry points.

A "worker" here is a standalone script invoked by a matcher adapter via
'subprocess.Popen', whose job is to provide an alignment for one pair and
and write the result as an OAEI RDF alignment file.

Conceptually, the driver (ie. the adapter) captures the workers output
(to std::out and std::err) into a per-pair log file via fd-level
redirection, so the work doesnt need to know where its logs live (it just
writes to console and the driver routes it).

Since some worker scripts share functions, such functionality can be placed
within this _common_workers.py file.

eg. add_common_args, configure_worker_logging, worker_main
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Callable

from diso_mappings.constants import VERBOSE_LOGGING_PKGS
from diso_mappings.io.terminal import configure, debug, error, info



def add_common_args(parser: argparse.ArgumentParser) -> None:
    """
    Every worker requires:
        --source         absolute path to source ontology file
        --target         absolute path to target ontology file
        --output-dir     directory to write the resulting .rdf into
        --pair-tag       stem used to name outputs (<pair-tag>.rdf etc.)
    Workers add their own matcher-specific arguments on top; we don't put
    those here because they may vary (BERTMapLt has --reasoner, AML doesn't, etc.)
    """
    parser.add_argument(
        "--source", required=True, type=Path,
        help="Absolute path to source ontology file",
    )
    parser.add_argument(
        "--target", required=True, type=Path,
        help="Absolute path to target ontology file",
    )
    parser.add_argument(
        "--output-dir", required=True, type=Path,
        help="Directory to write <pair-tag>.rdf (and any sidecars) into",
    )
    parser.add_argument(
        "--pair-tag", required=True,
        help="Stem used for output filenames (no extension, no slashes)",
    )



def configure_worker_logging() -> None:
    """
    Configure Python's stdlib logging (via diso_mappings.io.terminal) and
    loguru so the worker's log output is readable.
    """
    # call to diso_mappings.io.terminal.configure
    configure(level=logging.INFO, stream=sys.stdout)

    # some packages (eg. used by DeepOnto and BERTMap produce ALOT of outout)
    # so we can (optionally) silence some of them using VERBOSE_LOGGING_PKGS
    for this_noisy_logger in VERBOSE_LOGGING_PKGS:
        logging.getLogger(this_noisy_logger).setLevel(logging.WARNING)

    # loguru is DeepOnto-specific and is separate from stdlib logging
    # only required for use of BERTMap (so we dont 'hard require')
    try:
        from loguru import logger as _loguru_logger
    except ImportError:
        return

    _loguru_logger.remove()
    _loguru_logger.add(sys.stderr, level="WARNING")



def worker_main(body: Callable[[argparse.Namespace], int], parser: argparse.ArgumentParser) -> int:
    """
    Run a worker body with standardised exception handling. It should return
    an exit code, ie (0 = success, 1 = matcher failure raised, 2 = unknown error)
    """
    parsed_args = parser.parse_args()
    configure_worker_logging()
    try:
        return int(body(parsed_args))
    except KeyboardInterrupt:
        raise
    except (FileNotFoundError, RuntimeError, ValueError) as known_exception:
        error(f"{type(known_exception).__name__}: {known_exception}")
        return 1
    except Exception as unexpected_exception:
        # include the traceback via logging.exception since
        # we want the log file to contain the full stack trace
        logging.getLogger("diso_mappings").exception(
            f"Unexpected exception in worker: {type(unexpected_exception).__name__}"
        )
        return 2