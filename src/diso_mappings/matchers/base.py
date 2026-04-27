"""
Matcher ABC. Matchers can register themselves using the 'register' decorator.
Matchers are then identified by their name via 'get_matcher'
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Type


@dataclass
class MatchResult:
    alignment_path: Path
    duration_seconds: float


class Matcher(ABC):
    """
    ABS that all matcher adapters inherit from; subclasses must set a 
    unique 'name class attribute and implement 'run' and 'version'.
    Subclasses may [optionally] override 'family'; if left as None, 
    then family will resolve to the matcher's own name:

    The family (correlation) property is for use in consensus voting. 
    When None is specified, it means 'no declared family' for this 
    matcher. In which case it will fallback to the matcher name as the 
    family itself. For intersecting families, we could then apply some 
    voting strategy/weighting.
    
    Examples:
    ---------
    class LogMap(Matcher):    name = "logmap";     family = "LogMap";
    class LogMapLLM(Matcher): name = "logmap_llm"; family = "LogMap";
    class LogMapLt(Matcher):  name = "logmap_lt";  family = "LogMap";

    class AML(Matcher):       name = "aml"; # family resolves to 'aml'

    ...and so on.
    """
    name: str = "base" # matchers are ID'd by their (ideally) short UNIQUE name
    _family: str | None = None # family (correlation) tag for use in consensus voting
    show_timer: bool = False # whether to display a timer with this matcher

    @property
    def family(self) -> str:
        """Return the declared family, or the matcher's name if none declared"""
        return self._family if self._family is not None else self.name

    @property
    @abstractmethod
    def version(self) -> str:
        ... # return a human-readable string

    @abstractmethod
    def run(
        self,
        source: Path,                       # path to source ontology file
        target: Path,                       # path to target ontology file
        out_dir: Path,                      # path to dir (to write mappings to)
        config: dict | None = None,         # matcher configuration
        timeout: float | None = None,       # the number of seconds before 'give up'
    ) -> MatchResult:
        ... # implementations should output the mappings as an OAEI RDF Alignment
            # Format File within the 'out_dir' and return a 'MatchResult'.
            # Matchers should also Raise 'TimeoutError' when a timeout occurs,
            # and NotImplementedError for any stubs (w/ any other exceptions on fail).
            # These should be capture by the run manifest for reproducibility [TODO].


##
# MATCHERS REGISTRY
# -----------------
# Use the @register decorator to register a matcher for use from the CLI.
# See: register (below); and for examples, see any existing matcher in:
#
#   __PROJECTROOT__/src/diso_mappings/matchers/
#
##

_REGISTRY: dict[str, Type[Matcher]] = {}


def register(cls: Type[Matcher]) -> Type[Matcher]:
    """
    decorator which registers any matcher class by its name attribute
    """
    if cls.name in _REGISTRY and _REGISTRY[cls.name] is not cls:
        raise ValueError(f"Matcher name {cls.name!r} already registered")
    _REGISTRY[cls.name] = cls
    return cls


def get_matcher(name: str) -> Matcher:
    if name not in _REGISTRY:
        known = sorted(_REGISTRY)
        raise KeyError(f"Unknown matcher: {name!r}. Known: {known}")
    return _REGISTRY[name]()


def list_matchers() -> list[str]:
    return sorted(_REGISTRY)

