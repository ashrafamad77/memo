"""Ambiguity gate + optional LLM pick for Wikidata Vector hits."""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _primary_score(hit: Dict[str, Any]) -> Optional[float]:
    if hit.get("reranker_score") is not None:
        return float(hit["reranker_score"])
    if hit.get("similarity_score") is not None:
        return float(hit["similarity_score"])
    if hit.get("rrf_score") is not None:
        return float(hit["rrf_score"])
    return None


def _sort_hits_by_score(hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key(h: Dict[str, Any]) -> Tuple[int, float]:
        s = _primary_score(h)
        if s is None:
            return (1, 0.0)
        return (0, -s)

    return sorted(hits, key=key)


def is_clear_vector_winner(
    sorted_hits: List[Dict[str, Any]],
    *,
    margin: float,
    min_score: float,
) -> bool:
    """True when top hit beats the runner-up by ``margin`` and meets ``min_score``."""
    if len(sorted_hits) < 1:
        return False
    s0 = _primary_score(sorted_hits[0])
    if s0 is None:
        return False
    if s0 < min_score:
        return False
    if len(sorted_hits) == 1:
        return True
    s1 = _primary_score(sorted_hits[1])
    if s1 is None:
        return True
    return (s0 - s1) >= margin


def _get_openai_client():
    try:
        from config import (
            AZURE_OPENAI_API_KEY,
            AZURE_OPENAI_API_VERSION,
            AZURE_OPENAI_DEPLOYMENT,
            AZURE_OPENAI_ENDPOINT,
        )
        from openai import AzureOpenAI

        key = (AZURE_OPENAI_API_KEY or "").strip()
        endpoint = (AZURE_OPENAI_ENDPOINT or "").strip()
        if not key or not endpoint:
            return None, ""
        deployment = (AZURE_OPENAI_DEPLOYMENT or "gpt-4o-mini").strip() or "gpt-4o-mini"
        version = (AZURE_OPENAI_API_VERSION or "2024-12-01-preview").strip()
        return AzureOpenAI(api_key=key, azure_endpoint=endpoint, api_version=version), deployment
    except Exception as exc:
        logger.debug("wd_vector_verify: cannot build client: %s", exc)
        return None, ""


def llm_pick_qid(
    journal_text: str,
    mention_name: str,
    canonical_label: str,
    candidates: List[Dict[str, str]],
) -> Optional[str]:
    """Ask the LLM to pick one QID from labeled candidates, or null."""
    if not candidates:
        return None
    client, deployment = _get_openai_client()
    if client is None:
        return None

    cand_json = json.dumps(candidates, ensure_ascii=False)
    user = (
        f'Journal entry:\n"""\n{(journal_text or "").strip()}\n"""\n'
        f"Mention surface: {mention_name!r}\n"
        f"Canonical label (hint): {canonical_label!r}\n"
        f"Wikidata candidates (pick at most one QID, or null if none fit):\n{cand_json}\n"
        "Return ONLY JSON: {\"chosen_qid\": \"Q123\" or null}"
    )
    try:
        resp = client.chat.completions.create(
            model=deployment,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You choose which Wikidata item best matches a journal mention. "
                        "The mention is a real-world place, building, or library when the context says so — "
                        "reject catalog entries, software projects, abstract concepts, or unrelated homonyms. "
                        "Return JSON only: {\"chosen_qid\": \"Q...\" or null}. "
                        "If none of the candidates are correct, return null."
                    ),
                },
                {"role": "user", "content": user},
            ],
            temperature=0,
            max_tokens=200,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or ""
        parsed = json.loads(raw)
        q = parsed.get("chosen_qid")
        if q is None:
            return None
        qs = str(q).strip().upper()
        if not re.match(r"^Q\d+$", qs):
            return None
        allowed = {c.get("qid", "").strip().upper() for c in candidates}
        return qs if qs in allowed else None
    except Exception as exc:
        logger.warning("wd_vector_verify: LLM verify failed: %s", exc)
        return None


def pick_wikidata_qid_from_hits(
    hits: List[Dict[str, Any]],
    *,
    journal_text: str,
    mention_name: str,
    canonical_label: str,
    margin: float,
    min_score: float,
    llm_verify_top: int,
    verify_pool_top_n: int,
    label_fetcher: Callable[[List[str]], Dict[str, Tuple[str, str]]],
) -> Optional[str]:
    """Resolve vector hits to a single QID (clear winner, else LLM on top-N)."""
    if not hits:
        return None

    pool = _sort_hits_by_score(hits[: max(1, verify_pool_top_n)])
    if is_clear_vector_winner(pool, margin=margin, min_score=min_score):
        return str(pool[0].get("qid") or "").strip().upper() or None

    top_k = max(1, min(llm_verify_top, len(pool)))
    qids = [str(h["qid"]).strip().upper() for h in pool[:top_k] if h.get("qid")]
    if not qids:
        return None

    fetched = label_fetcher(qids)
    candidates: List[Dict[str, str]] = []
    for q in qids:
        lab, desc = fetched.get(q, ("", ""))
        candidates.append({
            "qid": q,
            "label": str(lab or "").strip(),
            "description": str(desc or "").strip(),
        })

    return llm_pick_qid(journal_text, mention_name, canonical_label, candidates)
