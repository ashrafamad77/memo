"""Tier-A E55 fallback: no Wikidata id + no candidates from primary path → LLM phrases + wbsearch.

Runs for any type name except those in ``MEMO_E55_FALLBACK_DENY_TYPES`` (default: Visit, Meeting),
so seed types **without** a seeded QID (e.g. Neighbourhood) can still get wbsearch + optional
``wikidata_related_*``. Types with a seed QID normally never reach Tier A. One pass per row key:
may set ``wikidata_id`` when embed+validation accept a hit, else optional related link.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from config import (
        MEMO_E55_FALLBACK,
        MEMO_E55_FALLBACK_DENY_TYPES,
        MEMO_E55_FALLBACK_MAX_QUERIES,
        MEMO_E55_FALLBACK_RELATED_MIN_EMBED,
    )
except ImportError:
    MEMO_E55_FALLBACK = os.getenv("MEMO_E55_FALLBACK", "1").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    MEMO_E55_FALLBACK_MAX_QUERIES = int(os.getenv("MEMO_E55_FALLBACK_MAX_QUERIES", "6"))
    MEMO_E55_FALLBACK_RELATED_MIN_EMBED = float(
        os.getenv("MEMO_E55_FALLBACK_RELATED_MIN_EMBED", "0.22")
    )
    MEMO_E55_FALLBACK_DENY_TYPES = os.getenv("MEMO_E55_FALLBACK_DENY_TYPES", "Visit,Meeting").strip()


def _e55_fallback_type_denied(type_name: str) -> bool:
    raw = (MEMO_E55_FALLBACK_DENY_TYPES or "").strip()
    if not raw:
        return False
    tl = re.sub(r"[\s_\-]+", "", (type_name or "").strip().lower())
    if not tl:
        return False
    for part in raw.split(","):
        p = re.sub(r"[\s_\-]+", "", part.strip().lower())
        if p and tl == p:
            return True
    return False


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _camel_to_words(name: str) -> str:
    s = re.sub(r"(?<!^)(?=[A-Z])", " ", (name or "").strip()).strip()
    return s if s else (name or "").strip()


def _default_search_queries(type_name: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for q in (type_name, _camel_to_words(type_name), type_name.replace("_", " ").strip()):
        t = (q or "").strip()
        if len(t) < 2 or t.lower() in seen:
            continue
        seen.add(t.lower())
        out.append(t)
    return out[:4]


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
        logger.debug("e55_fallback: no Azure client: %s", exc)
        return None, ""


def llm_expand_e55_queries(type_name: str, journal_text: str) -> Tuple[List[str], List[str]]:
    """Return (paraphrases, broader_concepts) for Wikidata search; max 2 + 3."""
    client, deployment = _get_openai_client()
    if client is None:
        return [], []
    user = (
        f'Journal entry:\n"""\n{(journal_text or "").strip()[:2500]}\n"""\n'
        f"Proposed abstract activity / concept type (CamelCase CRM label): {type_name!r}\n"
        "Return JSON only:\n"
        '{"paraphrases": ["...", "..."], "broader_concepts": ["...", "...", "..."]}\n'
        "- paraphrases: up to 2 short English noun phrases same specificity as the type.\n"
        "- broader_concepts: up to 3 more general concepts likely to exist in Wikidata "
        "(e.g. social interaction, courtship) — more general than paraphrases.\n"
        "Use [] if unsure. No markdown."
    )
    try:
        resp = client.chat.completions.create(
            model=deployment,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You help museum ontology grounding. Output compact JSON only. "
                        "Broader concepts must be generic Wikidata-friendly nouns, not slang."
                    ),
                },
                {"role": "user", "content": user},
            ],
            temperature=0,
            max_tokens=400,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or ""
        data = json.loads(raw)
        p = data.get("paraphrases") or []
        b = data.get("broader_concepts") or data.get("broader") or []
        par = [str(x).strip() for x in p if str(x).strip()][:2]
        br = [str(x).strip() for x in b if str(x).strip()][:3]
        return par, br
    except Exception as exc:
        logger.debug("e55_fallback: LLM expand failed: %s", exc)
        return [], []


def _wbsearch_merged(queries: List[str], *, per_query_lim: int = 6) -> List[Dict[str, str]]:
    from .type_grounding_embed import _wbsearchentities_one

    seen: set = set()
    out: List[Dict[str, str]] = []
    for q in queries:
        qq = (q or "").strip()
        if len(qq) < 2:
            continue
        chunk = _wbsearchentities_one(qq, fetch_lim=per_query_lim, instance_class=False)
        for row in chunk:
            qid = str(row.get("wikidata_id") or "").strip().upper()
            if not re.match(r"^Q\d+$", qid) or qid in seen:
                continue
            seen.add(qid)
            out.append({
                "qid": qid,
                "label": str(row.get("label") or "").strip(),
                "description": str(row.get("description") or "").strip(),
            })
    return out


def _pick_related_by_embed(
    type_name: str,
    journal_text: str,
    candidates: List[Dict[str, str]],
    *,
    min_sim: float,
) -> Optional[Tuple[str, str, float]]:
    if not candidates:
        return None
    try:
        from .embedding_service import embed_text
    except Exception:
        return None
    query_text = f"E55 activity type {type_name}. {(journal_text or '').strip()}"[:2000]
    try:
        q_vec = embed_text(query_text)
    except Exception as exc:
        logger.debug("e55_fallback: embed query failed: %s", exc)
        return None
    best: Tuple[str, str, float] = ("", "", -1.0)
    for c in candidates[:16]:
        qid = str(c.get("qid") or "").strip().upper()
        if not re.match(r"^Q\d+$", qid):
            continue
        lab = str(c.get("label") or "").strip()
        desc = str(c.get("description") or "").strip()
        doc = f"{lab}: {desc}"[:500]
        if not doc.strip():
            doc = qid
        try:
            d_vec = embed_text(doc)
            sim = _cosine(q_vec, d_vec)
        except Exception:
            sim = 0.0
        if sim > best[2]:
            best = (qid, desc or lab, sim)
    if best[2] >= min_sim and best[0]:
        return best
    return None


def apply_e55_tier_a_fallback(
    type_name: str,
    journal_text: str,
    row: Dict[str, Any],
) -> Dict[str, Any]:
    """If Tier A matches, enrich ``row`` in place (copy-safe return)."""
    out = dict(row) if isinstance(row, dict) else {}
    if not MEMO_E55_FALLBACK:
        return out
    if out.get("_e55_fallback_applied"):
        return out
    if _e55_fallback_type_denied(type_name):
        return out

    wid = str(out.get("wikidata_id") or "").strip()
    wc = out.get("wikidata_candidates")
    n_cand = len(wc) if isinstance(wc, list) else 0
    if wid or n_cand > 0:
        return out

    par, br = llm_expand_e55_queries(type_name, journal_text)
    queries: List[str] = []
    seen_q = set()
    for q in _default_search_queries(type_name) + par + br:
        t = (q or "").strip()
        if len(t) < 2 or t.lower() in seen_q:
            continue
        seen_q.add(t.lower())
        queries.append(t)
        if len(queries) >= max(1, MEMO_E55_FALLBACK_MAX_QUERIES):
            break

    merged = _wbsearch_merged(queries, per_query_lim=6)
    if not merged:
        out["_e55_fallback_applied"] = True
        return out

    out["wikidata_candidates"] = merged
    out["confidence"] = str(out.get("confidence") or "medium")

    primary: Optional[Dict[str, str]] = None
    try:
        from .type_grounding_embed import (
            embed_grounding_enabled,
            legacy_wbsearch_enabled,
            resolve_wikidata_from_batch_candidates,
        )

        if embed_grounding_enabled() and not legacy_wbsearch_enabled():
            primary = resolve_wikidata_from_batch_candidates(
                type_name,
                journal_text or "",
                "other",
                None,
                merged,
                "medium",
            )
    except Exception as exc:
        logger.debug("e55_fallback: resolve_wikidata_from_batch_candidates: %s", exc)

    if primary and primary.get("id"):
        qid = str(primary["id"]).strip().upper()
        out["wikidata_id"] = qid
        out["description"] = str(primary.get("description") or out.get("description") or "").strip()
        out["_e55_fallback_applied"] = True
        logger.info(
            "e55_fallback: primary WD %s for type %r (Tier A, %d candidates)",
            qid,
            type_name,
            len(merged),
        )
        return out

    rel = _pick_related_by_embed(
        type_name,
        journal_text or "",
        merged,
        min_sim=MEMO_E55_FALLBACK_RELATED_MIN_EMBED,
    )
    if rel:
        rqid, rdesc, sim = rel
        out["wikidata_related_id"] = rqid
        out["wikidata_related_description"] = (rdesc or "")[:500]
        out["_e55_fallback_applied"] = True
        logger.info(
            "e55_fallback: related WD %s (sim=%.3f) for type %r — approximate only",
            rqid,
            sim,
            type_name,
        )
    else:
        out["_e55_fallback_applied"] = True
        logger.debug(
            "e55_fallback: no primary/related accepted for type %r (%d wb candidates)",
            type_name,
            len(merged),
        )
    return out
