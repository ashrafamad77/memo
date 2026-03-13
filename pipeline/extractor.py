"""Lightweight entity types used by the pipeline.

Actual extraction is done by LLMExtractor (Azure) — this module only defines
the data structures shared with the graph and vector stores.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class ExtractedEntity:
    text: str
    label: str  # Person, Place, Date, etc.
    start_char: int
    end_char: int


@dataclass
class ExtractionResult:
    entities: List[ExtractedEntity] = field(default_factory=list)
    raw_text: str = ""
    timestamp: Optional[datetime] = None
