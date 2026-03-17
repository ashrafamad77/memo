"""Shared embedding service (SentenceTransformers).

Used for:
- Weaviate document embeddings (already in VectorStore)
- Neo4j node embeddings (Person consolidator)
"""

from __future__ import annotations

from functools import lru_cache
from typing import List

from sentence_transformers import SentenceTransformer

from config import EMBEDDING_MODEL


@lru_cache(maxsize=1)
def _model() -> SentenceTransformer:
    return SentenceTransformer(EMBEDDING_MODEL)


def embed_text(text: str) -> List[float]:
    return _model().encode(text).tolist()


def embedding_dim() -> int:
    # SentenceTransformer.get_sentence_embedding_dimension exists on most models
    m = _model()
    if hasattr(m, "get_sentence_embedding_dimension"):
        return int(m.get_sentence_embedding_dimension())
    # Fallback: embed empty string once
    return len(embed_text("test"))

