"""Wikidata Vector Search API client (GET /item/query/).

OpenAPI: https://wd-vectordb.wmcloud.org/docs — optional ``X-API-SECRET`` (omit when empty).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


def _normalize_item(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    qid = str(raw.get("QID") or raw.get("qid") or "").strip().upper()
    if not qid.startswith("Q"):
        return None

    def _f(key: str) -> Optional[float]:
        v = raw.get(key)
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    return {
        "qid": qid,
        "similarity_score": _f("similarity_score"),
        "rrf_score": _f("rrf_score"),
        "reranker_score": _f("reranker_score"),
        "source": str(raw.get("source") or "").strip(),
    }


def search_items(
    query: str,
    *,
    base_url: str,
    api_secret: str,
    k: int = 10,
    lang: str = "en",
    instance_of: Optional[str] = None,
    rerank: bool = True,
    timeout_sec: float = 45.0,
) -> List[Dict[str, Any]]:
    """Query Wikidata items; returns normalized dicts with ``qid`` and score fields."""
    q = (query or "").strip()
    secret = (api_secret or "").strip()
    if not q:
        return []

    url = f"{base_url.rstrip('/')}/item/query/"
    params: Dict[str, Any] = {
        "query": q,
        "lang": (lang or "en").strip() or "en",
        "K": max(1, min(int(k), 200)),
        "rerank": "true" if rerank else "false",
        "return_vectors": "false",
    }
    iof = (instance_of or "").strip()
    if iof:
        params["instanceof"] = iof

    headers: Dict[str, str] = {
        "User-Agent": "MemoPipeline/1.0 (Wikidata Vector)",
    }
    if secret:
        headers["X-API-SECRET"] = secret

    try:
        with httpx.Client(timeout=timeout_sec, follow_redirects=True) as client:
            r = client.get(url, params=params, headers=headers)
            r.raise_for_status()
            data = r.json()
    except Exception as exc:
        logger.warning("wikidata_vector_client: query failed: %s", exc)
        return []

    if not isinstance(data, list):
        return []

    out: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        norm = _normalize_item(row)
        if norm:
            out.append(norm)
    return out
