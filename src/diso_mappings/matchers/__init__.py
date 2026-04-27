"""
Matcher adapters: 
    importing this package triggers registration of every matcher stub
"""

from .base import Matcher, MatchResult, get_matcher, list_matchers, register

from . import aml
from . import logmap_lt
from . import logmap
from . import bertmap_lt
from . import bertmap

# UNCOMMENT IF USING LOGMAP_LLM || MATCHA

# from . import logmap_llm
# from . import matcha

# CODE FOR MATCHA IS NOT SHIPPED WITHIN THIS PROJECT.
# THIS VERSION OF THE CODEBASE DOES NOT CONTAIN THE INTERFACE FOR MATCHA OR LOGMAP-LLM.

__all__ = [
    "Matcher", 
    "MatchResult", 
    "get_matcher", 
    "list_matchers", 
    "register"
]
