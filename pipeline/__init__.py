"""Personal Memory Pipeline - Minimal PoC.

Heavy imports (Neo4j, etc.) are lazy so ``import pipeline.babelfy_client`` works
in lightweight environments (e.g. unit tests without full ``requirements.txt``).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["MemoryPipeline", "ExtractedEntity", "ExtractedRelation", "ExtractionResult"]

if TYPE_CHECKING:
    from .extractor import ExtractedEntity, ExtractedRelation, ExtractionResult
    from .pipeline import MemoryPipeline


def __getattr__(name: str) -> Any:
    if name == "MemoryPipeline":
        from .pipeline import MemoryPipeline

        return MemoryPipeline
    if name == "ExtractedEntity":
        from .extractor import ExtractedEntity

        return ExtractedEntity
    if name == "ExtractedRelation":
        from .extractor import ExtractedRelation

        return ExtractedRelation
    if name == "ExtractionResult":
        from .extractor import ExtractionResult

        return ExtractionResult
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
