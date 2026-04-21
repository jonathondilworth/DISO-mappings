"""
Largely imported from:

    https://github.com/jonathondilworth/logmap-llm/tree/jd-extended

with some tweaks. Usage (near entry point):

    import logging
    from diso_mappings.io.terminal import configure
    # ...
    configure(level=logging.DEBUG)  # NOTSET, DEBUG, INFO, WARNING, ERROR, and CRITICAL

Then, in all downstream modules, simply:

    from diso_mappings.io.terminal import debug, info, warn, error
    # ...
    debug("msg goes here") # etc.

Slight improvement over the prior implementation, where the python logger was mixed
in with coloured terminal output (while retaining success messages \w green and custom
messages \w magenta that do not pass through python logger). Also, note that, by default
we print to std::out (you may wish to modify this in configure to std::err).
"""
from __future__ import annotations

import logging
import re
import os
import sys

logger = logging.getLogger("diso_mappings")

# ANSI semantic colour codes

_BRIGHT_MAGENTA = "\033[0;95;49m"
_BRIGHT_RED     = "\033[91m"
_BOLD_RED       = "\033[1;31m"
_PASTEL_YELLOW  = "\033[0;38;5;186;49m"
_PASTEL_BLUE    = "\033[38;5;117m"
_GREEN          = "\033[38;5;114m"

_COLOURS = {
    logging.DEBUG:    _BRIGHT_MAGENTA, 
    logging.INFO:     _PASTEL_BLUE,
    logging.WARNING:  _PASTEL_YELLOW,
    logging.ERROR:    _BRIGHT_RED,
    logging.CRITICAL: _BOLD_RED,
}
_RESET = "\033[0m"
_ANSI_RE = re.compile(r"\033\[[0-9;]*m")


class _ColourFormatter(logging.Formatter):
    """
    A lightweight wrapper around the logging formatter.
    Allows for use of ANSI colour output @ terminal when using tooling.
    Can specify env var 'NO_COLOR' (see: https://no-color.org/)

    """
    def __init__(self, fmt: str, use_colour: bool):
        super().__init__(fmt)
        self._use_colour = use_colour

    def format(self, record: logging.LogRecord) -> str:
        if self._use_colour:
            colour = _COLOURS.get(record.levelno, "")
            record.levelname = f"{colour}{record.levelname}{_RESET}"
        return super().format(record)


def _should_colour(stream) -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    return hasattr(stream, "isatty") and stream.isatty()


def configure(level: int = logging.INFO, stream = sys.stdout) -> None:
    """
    Logger setup (idempotent); call once near program entry.
    """
    if logger.handlers:
        return
    handler = logging.StreamHandler(stream)
    handler.setFormatter(_ColourFormatter(
        fmt="%(levelname)s %(message)s",
        use_colour=_should_colour(stream),
    ))
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False



def debug(msg: str, *args, **kwargs) -> None:
    logger.debug(msg, *args, **kwargs)



def info(msg: str, *args, **kwargs)  -> None:
    logger.info(msg, *args, **kwargs)



def warn(msg: str, *args, **kwargs)  -> None:
    logger.warning(msg, *args, **kwargs)



def error(msg: str, *args, **kwargs) -> None:
    logger.error(msg, *args, **kwargs)



def _print_coloured(colour: str, msg: str, stream=sys.stdout) -> None:
    """
    Private helper to respect the 'NO_COLOR' issue (see above).
    Note that, by default, we're printing to std::out (adjust as neccesary).
    """
    print(f"{colour}{msg}{_RESET}" if _should_colour(stream) else msg, file=stream)



def success(msg: str) -> None:
    """
    Print a success / completion message in green.
    Note that this will not be logged as if it were called via logger.
    """
    _print_coloured(_GREEN, msg)



def highlight(msg: str) -> None:
    """
    Print a magenta / custom message in bright magenta!
    Note that this will not be logged as if it were called via logger.
    """
    _print_coloured(_BRIGHT_MAGENTA, msg)

