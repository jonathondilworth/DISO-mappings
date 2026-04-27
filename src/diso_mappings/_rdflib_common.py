"""
Shared helpers for rdflib code (currently: preprocessing)
"""
from __future__ import annotations
import logging

##
# GLOBAL FILTER STATE (None by default)
##

_filter_singleton: _LiteralConversionFilter | None = None

##
# REPORT FILTERING: catch noisy logging records
# _(one should be cautious when exercising such suppression)_
##

_TARGET_MESSAGE_PREFIX = "Failed to convert Literal lexical form"

_RDFLIB_TERM_LOGGER_NAME = "rdflib.term"



class _LiteralConversionFilter(logging.Filter):
    """
    Drops log records whose message starts with _TARGET_MESSAGE_PREFIX 
    and keeps count of each suppression; all other records pass
    """
    def __init__(self) -> None:
        super().__init__()
        self._suppressed_count: int = 0

    def filter(self, record: logging.LogRecord) -> bool:
        rendered_message = record.getMessage()
        if rendered_message.startswith(_TARGET_MESSAGE_PREFIX):
            self._suppressed_count += 1
            return False  # drop
        return True  # pass

    @property
    def suppressed_count(self) -> int:
        return self._suppressed_count

    def reset(self) -> None:
        self._suppressed_count = 0



def install_rdflib_quiet_filter() -> None:
    """
    Attach a logging.Filter to the 'rdflib.term' logger; supresses:
    'Failed to convert Literal lexical form' - stops large amounts
    of noise being output to terminal during preprocessing (should
    only be used when is genuinely inconsequential to final results)
    """
    global _filter_singleton
    if _filter_singleton is not None:
        return # preserves an existing filter
    # else: create new filter
    _filter_singleton = _LiteralConversionFilter()
    logging.getLogger(_RDFLIB_TERM_LOGGER_NAME).addFilter(_filter_singleton)



def get_rdflib_suppressed_count() -> int:
    """
    Count the number of records dropped by the filter
    """
    if _filter_singleton is None:
        return 0
    return _filter_singleton.suppressed_count



def reset_rdflib_suppressed_count() -> None:
    """
    Reset the supression counter to 0
    """
    if _filter_singleton is None:
        return
    _filter_singleton.reset()