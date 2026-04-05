"""Semantic coherence gate: reject Babelfy/BabelNet groundings that are out of context.

Problem: lexical surface matching can pick a synset whose *concept* is completely
unrelated to the journal entry (e.g. "Conversation" → "conversation tart", a French
pastry, when the journal is about a morning greeting).

Solution: embed both the journal context and the candidate concept text, then compute
cosine similarity.  If the similarity is below ``MEMO_SEMANTIC_GATE_THRESHOLD`` the
candidate is rejected and the grounding falls back to the next pipeline stage.

Graceful degradation: if the embedding service is unavailable the gate returns True
(accept), so the rest of the pipeline is unaffected.
"""
from __future__ import annotations

import logging
import math
import os
from typing import List, Optional

logger = logging.getLogger(__name__)

# Configurable minimum cosine similarity.  Values in [0, 1]; lower = more permissive.
# 0.30 rejects wrong-domain matches (food, music albums vs. social interaction) while
# accepting concepts whose description is semantically aligned with the journal context.
_DEFAULT_THRESHOLD = 0.30
_THRESHOLD: Optional[float] = None


def _threshold() -> float:
    global _THRESHOLD
    if _THRESHOLD is None:
        raw = os.getenv("MEMO_SEMANTIC_GATE_THRESHOLD", "").strip()
        try:
            _THRESHOLD = float(raw) if raw else _DEFAULT_THRESHOLD
        except ValueError:
            _THRESHOLD = _DEFAULT_THRESHOLD
    return _THRESHOLD


# ── embedding cache (in-process, keyed by text) ──────────────────────────────

_CACHE: dict[str, List[float]] = {}


def _embed(text: str) -> Optional[List[float]]:
    """Return the embedding for *text*, using a simple in-process cache.

    Uses a short 3-second timeout so a missing/slow embedding sidecar causes an
    immediate fail-open rather than blocking the pipeline for 120 seconds.
    """
    key = text.strip()
    if not key:
        return None
    if key in _CACHE:
        return _CACHE[key]
    try:
        import httpx
        from config import EMBEDDING_INFERENCE_PATH, EMBEDDING_INFERENCE_URL

        base = EMBEDDING_INFERENCE_URL.rstrip("/")
        path = EMBEDDING_INFERENCE_PATH if EMBEDDING_INFERENCE_PATH.startswith("/") else f"/{EMBEDDING_INFERENCE_PATH}"
        url = f"{base}{path}"
        with httpx.Client(timeout=3.0) as client:
            r = client.post(url, json={"text": key})
            r.raise_for_status()
            data = r.json()

        # parse vector — same logic as embedding_service._parse_vector_response
        vec: List[float]
        if isinstance(data, list):
            vec = [float(x) for x in data]
        elif isinstance(data, dict):
            for k in ("vector", "embedding", "vectors"):
                v = data.get(k)
                if isinstance(v, list):
                    vec = [float(x) for x in v]
                    break
            else:
                raise ValueError(f"Unexpected embedding response shape: {list(data.keys())}")
        else:
            raise ValueError(f"Unexpected embedding response type: {type(data)}")

        _CACHE[key] = vec
        return vec
    except Exception as exc:
        logger.debug("semantic_gate: embed failed (%s) — gate disabled for this call", exc)
        return None


# ── cosine similarity ─────────────────────────────────────────────────────────


def _cosine(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na < 1e-9 or nb < 1e-9:
        return 0.0
    return dot / (na * nb)


# ── public API ────────────────────────────────────────────────────────────────


def is_coherent(
    context_text: str,
    candidate_text: str,
    *,
    threshold: Optional[float] = None,
) -> bool:
    """Return True if *candidate_text* is semantically aligned with *context_text*.

    Args:
        context_text:   The full journal sentence / event description used as the
                        semantic reference zone.
        candidate_text: The proposed concept text to validate (label + gloss +
                        description, joined with spaces).
        threshold:      Override the default threshold for this call.

    Returns ``True`` (accept) when:
    - Either text is empty (nothing to compare — don't block).
    - The embedding service is unavailable (fail-open).
    - ``cosine_similarity >= threshold``.

    Returns ``False`` (reject) only when both embeddings succeed *and* the
    similarity is below the threshold.
    """
    ctx = (context_text or "").strip()
    cand = (candidate_text or "").strip()
    if not ctx or not cand:
        return True

    t = threshold if threshold is not None else _threshold()

    cv = _embed(ctx)
    kv = _embed(cand)
    if cv is None or kv is None:
        return True  # embedding service unavailable — fail open

    sim = _cosine(cv, kv)
    ok = sim >= t
    if not ok:
        logger.debug(
            "semantic_gate: REJECTED — sim=%.3f < %.3f | context=%r | candidate=%r",
            sim,
            t,
            ctx[:80],
            cand[:80],
        )
    return ok
