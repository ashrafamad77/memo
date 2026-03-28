"""Shared embedding service via Weaviate transformers-inference sidecar (HTTP).

Used for:
- Weaviate document embeddings (VectorStore supplies vectors; class uses vectorizer=none)
- Neo4j node embeddings (Person consolidator)

Bare-metal app: set EMBEDDING_INFERENCE_URL to the host-mapped port (e.g. http://127.0.0.1:8082).
Weaviate in Docker uses TRANSFORMERS_INFERENCE_API=http://t2v-transformers:8080.

Default path POST /vectors with JSON {"text": "..."} matches Weaviate docs for semitechnologies/transformers-inference.
Override with EMBEDDING_INFERENCE_PATH if needed (e.g. /vectorize).
"""

from __future__ import annotations

from typing import Any, List

import httpx

from config import EMBEDDING_INFERENCE_PATH, EMBEDDING_INFERENCE_URL, EMBEDDING_VECTOR_DIM


def _parse_vector_response(data: Any) -> List[float]:
    if isinstance(data, list):
        return [float(x) for x in data]
    if isinstance(data, dict):
        for key in ("vector", "embedding", "vectors"):
            v = data.get(key)
            if isinstance(v, list):
                return [float(x) for x in v]
    raise ValueError(f"Unexpected embedding API response (expected vector list or object with 'vector'): {type(data)}")


def embed_text(text: str) -> List[float]:
    base = EMBEDDING_INFERENCE_URL.rstrip("/")
    path = EMBEDDING_INFERENCE_PATH if EMBEDDING_INFERENCE_PATH.startswith("/") else f"/{EMBEDDING_INFERENCE_PATH}"
    url = f"{base}{path}"
    with httpx.Client(timeout=120.0) as client:
        r = client.post(url, json={"text": text})
        r.raise_for_status()
        return _parse_vector_response(r.json())


def embedding_dim() -> int:
    return EMBEDDING_VECTOR_DIM
