"""
Largely imported from:

    https://github.com/jonathondilworth/logmap-llm/tree/jd-extended

with some tweaks.
"""
from __future__ import annotations

import os
import re
import sys
from typing import NoReturn, TextIO

_RESET = "\033[0m"
_BRIGHT_MAGENTA = "\033[0;95;49m"
_BRIGHT_RED = "\033[91m"
_PASTEL_YELLOW = "\033[0;38;5;186;49m"
_PASTEL_BLUE = "\033[38;5;117m"
_GREEN = "\033[38;5;114m"
_ANSI_RE = re.compile(r"\033\[[0-9;]*m")


def strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


def _use_colour(stream: TextIO) -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    return hasattr(stream, "isatty") and stream.isatty()


def _emit(code: str, msg: str, stream: TextIO = sys.stderr) -> None:
    if _use_colour(stream):
        print(f"{code}{msg}{_RESET}", file=stream)
    else:
        print(msg, file=stream)


def init(msg: str) -> None:
    _emit(_BRIGHT_MAGENTA, msg)


def info(msg: str) -> None:
    _emit(_PASTEL_BLUE, msg)


def warn(msg: str) -> None:
    _emit(_PASTEL_YELLOW, msg)


def error(msg: str) -> None:
    _emit(_BRIGHT_RED, msg)


def success(msg: str) -> None:
    _emit(_GREEN, msg)


def fatal(msg: str, exception_cls: type[Exception] = RuntimeError) -> NoReturn:
    error(msg)
    raise exception_cls(msg)