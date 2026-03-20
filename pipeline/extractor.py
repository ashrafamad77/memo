"""Lightweight entity types used by the pipeline.

Actual extraction is done by LLMExtractor (Azure) — this module only defines
the data structures shared with the graph and vector stores.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any


@dataclass
class ExtractedEntity:
    text: str
    label: str  # Person, Place, Date, etc.
    start_char: int
    end_char: int


@dataclass
class ExtractedRelation:
    """Triplet sujet-prédicat-objet pour le graphe event-centric."""
    subject: str
    predicate: str  # PARTICIPATED_IN, OCCURRED_AT, P67_refers_to, DISCUSSED, etc.
    obj: str
    sentiment: float = 0.5  # 0..1, défaut neutre


@dataclass
class ExtractionResult:
    entities: List[ExtractedEntity] = field(default_factory=list)
    relations: List[ExtractedRelation] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    raw_text: str = ""
    timestamp: Optional[datetime] = None
