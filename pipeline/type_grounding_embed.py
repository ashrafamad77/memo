"""Stage 2–3 after batch LLM: embedding rerank + SPARQL validate. No per-type LLM here.

Default pipeline: batch TypeGroundingLLM (stage 1) → this module → optional AAT from batch only.

Circuit breaker: MEMO_WD_LEGACY_WBSEARCH=1 restores wbsearchentities + heuristics in type_resolver.get_wikidata_info.

LEGACY PATH — delete after 2026-05-15 once batch+embed is stable in production (see type_resolver legacy block).

Embedding cosine floors default for sentence-transformers / MiniLM-class models (~384d); calibrate from
accepted vs rejected pairs in logs. OpenAI text-embedding-3 family often needs ~0.55–0.65. Override via env.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from .embedding_service import embed_text

logger = logging.getLogger(__name__)

_WIKIDATA_API = "https://www.wikidata.org/w/api.php"
_WIKIDATA_UA = "MemoJournalApp/1.0"
_GETTY_SPARQL = "https://vocab.getty.edu/sparql"
_GETTY_AAT_GRAPH = "http://vocab.getty.edu/aat"
_GETTY_UA = "MemoJournalApp/1.0"

# Single switch: legacy string-search Wikidata path in type_resolver.
_MEMO_WD_LEGACY_WBSEARCH = os.getenv("MEMO_WD_LEGACY_WBSEARCH", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)

# HIGH < MEDIUM on purpose: high-confidence LLM rows need less embedding confirmation;
# medium-confidence rows require stronger cosine evidence before acceptance.
_MEMO_WD_EMBED_MIN_SIM_HIGH = float(os.getenv("MEMO_WD_EMBED_MIN_SIM_HIGH", "0.45"))
_MEMO_WD_EMBED_MIN_SIM_MEDIUM = float(os.getenv("MEMO_WD_EMBED_MIN_SIM_MEDIUM", "0.50"))


def legacy_wbsearch_enabled() -> bool:
    return _MEMO_WD_LEGACY_WBSEARCH


def embed_grounding_enabled() -> bool:
    """True when using batch+embed pipeline (not legacy wbsearch). Kept for call-site clarity."""
    return not legacy_wbsearch_enabled()


def wikidata_qid_exists(qid: str) -> bool:
    q = (qid or "").strip().upper()
    if not re.match(r"^Q\d+$", q):
        return False
    try:
        r = requests.get(
            _WIKIDATA_API,
            params={
                "action": "wbgetentities",
                "ids": q,
                "props": "labels",
                "languages": "en",
                "format": "json",
            },
            headers={"User-Agent": _WIKIDATA_UA},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        ent = (data.get("entities") or {}).get(q, {})
        return "missing" not in ent
    except Exception:
        return False


def wikidata_fetch_labels_descriptions(qids: List[str]) -> Dict[str, Tuple[str, str]]:
    ids = [q.strip().upper() for q in qids if re.match(r"^Q\d+$", (q or "").strip().upper())]
    if not ids:
        return {}
    out: Dict[str, Tuple[str, str]] = {}
    for i in range(0, len(ids), 40):
        batch = "|".join(ids[i : i + 40])
        try:
            r = requests.get(
                _WIKIDATA_API,
                params={
                    "action": "wbgetentities",
                    "ids": batch,
                    "props": "labels|descriptions",
                    "languages": "en",
                    "format": "json",
                },
                headers={"User-Agent": _WIKIDATA_UA},
                timeout=12,
            )
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue
        for q, ent in (data.get("entities") or {}).items():
            if not isinstance(ent, dict) or "missing" in ent:
                continue
            lab = ""
            desc = ""
            labels = ent.get("labels") or {}
            if isinstance(labels, dict) and "en" in labels:
                lab = str((labels["en"] or {}).get("value") or "")
            descs = ent.get("descriptions") or {}
            if isinstance(descs, dict) and "en" in descs:
                desc = str((descs["en"] or {}).get("value") or "")
            out[q] = (lab, desc)
    return out


def wikidata_entity_search_candidates(mention: str, *, limit: int = 12) -> List[Dict[str, Any]]:
    """Short-string Wikidata search for entity linking when the LLM yields no valid QIDs.

    Uses ``wbsearchentities`` (same API as legacy type_resolver). Results are Wikidata-backed
    labels/descriptions — not model-invented pairs.
    """
    q = (mention or "").strip()
    if len(q) < 2 or len(q) > 200:
        return []
    lim = max(1, min(int(limit), 50))
    try:
        r = requests.get(
            _WIKIDATA_API,
            params={
                "action": "wbsearchentities",
                "search": q,
                "language": "en",
                "uselang": "en",
                "format": "json",
                "limit": str(lim),
            },
            headers={"User-Agent": _WIKIDATA_UA},
            timeout=12,
        )
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for h in data.get("search") or []:
        if not isinstance(h, dict):
            continue
        qid = str(h.get("id") or "").strip().upper()
        if not re.match(r"^Q\d+$", qid) or qid in seen:
            continue
        seen.add(qid)
        lab = str(h.get("label") or "").strip()
        desc = str(h.get("description") or "").strip()
        out.append({
            "wikidata_id": qid,
            "label": lab,
            "description": desc,
            "confidence": "medium",
        })
        if len(out) >= lim:
            break
    return out


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def embed_rerank_candidates(
    type_name: str,
    journal_text: str,
    candidates: List[Dict[str, str]],
    wsd_keywords: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    jt = (journal_text or "").strip()[:900]
    tn = (type_name or "").strip()
    extra = ""
    if wsd_keywords:
        extra = " Related keywords: " + ", ".join(sorted(wsd_keywords)[:12]) + "."
    query_text = f"{tn} — in journal context: {jt}{extra}"
    try:
        q_vec = embed_text(query_text)
    except Exception as exc:
        logger.warning(
            "embedding_service unavailable for query embed (type=%r): %s — "
            "using LLM candidate order, embed_score=0.0 for all",
            tn,
            exc,
        )
        return [{**c, "embed_score": 0.0} for c in candidates]

    scored: List[Dict[str, Any]] = []
    for c in candidates:
        lab = str(c.get("label") or "")
        doc = f"{lab}: {c.get('description', '')}"
        qid = str(c.get("qid") or "").strip()
        try:
            d_vec = embed_text(doc[:500])
            sim = _cosine(q_vec, d_vec)
        except Exception as exc:
            logger.warning(
                "embedding_service unavailable for candidate (type=%r qid=%s): %s — embed_score=0.0",
                tn,
                qid or "?",
                exc,
            )
            sim = 0.0
        scored.append({**c, "embed_score": sim})
    scored.sort(key=lambda x: -float(x.get("embed_score") or 0.0))
    return scored


def validate_wikidata_candidate(
    qid: str,
    category: str,
    wsd_row: Optional[Dict[str, Any]],
) -> bool:
    from .type_resolver import (
        _WD_DEFAULT_PLACE_TAXONOMY_ROOT,
        _safe_wikidata_qid,
        _wsd_row_requires_spatial,
        wikidata_entity_forbidden_by_ontology,
        wikidata_entity_is_chart_or_screen_work,
        wikidata_entity_p31_reaches_root,
    )

    q = _safe_wikidata_qid(qid)
    if not q or not wikidata_qid_exists(q):
        return False
    if wikidata_entity_forbidden_by_ontology(q, category) is True:
        return False
    if wikidata_entity_is_chart_or_screen_work(q) is True:
        return False
    root = _safe_wikidata_qid(_WD_DEFAULT_PLACE_TAXONOMY_ROOT) or "Q2221906"
    if _normalize_cat(category) == "place" or _wsd_row_requires_spatial(wsd_row):
        reach = wikidata_entity_p31_reaches_root(q, root)
        if reach is False:
            return False
    return True


def _normalize_cat(category: str) -> str:
    from .type_resolver import _normalize_context_category

    return _normalize_context_category(category)


def _min_embed_for_confidence(confidence: str) -> float:
    c = (confidence or "medium").strip().lower()
    if c == "high":
        return _MEMO_WD_EMBED_MIN_SIM_HIGH
    if c == "medium":
        return _MEMO_WD_EMBED_MIN_SIM_MEDIUM
    return 1.0


def resolve_wikidata_from_batch_candidates(
    type_name: str,
    journal_text: str,
    category: str,
    wsd_profile: Optional[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
    confidence: str,
) -> Optional[Dict[str, str]]:
    """
    Stage 2–3 only: embed rerank + SPARQL/exists gates. No LLM.
    confidence 'low' → no Wikidata grounding from batch for this type.
    """
    if legacy_wbsearch_enabled():
        return None
    conf = (confidence or "medium").strip().lower()
    if conf == "low":
        return None
    if not candidates:
        return None

    from .type_resolver import _expert_keywords_from_wsd, _find_wsd_row_for_term

    cat = _normalize_cat(category)
    wsd_row = _find_wsd_row_for_term(type_name, cat, wsd_profile)
    wsd_kw: Set[str] = set()
    if wsd_row:
        wsd_kw = _expert_keywords_from_wsd(wsd_row)

    need_fetch: List[str] = []
    for c in candidates:
        qid = str(c.get("qid") or "").strip().upper()
        if not re.match(r"^Q\d+$", qid):
            continue
        lab = str(c.get("label") or "").strip()
        desc = str(c.get("description") or "").strip()
        if not lab or not desc:
            need_fetch.append(qid)
    fetched = wikidata_fetch_labels_descriptions(list(dict.fromkeys(need_fetch))) if need_fetch else {}

    enriched: List[Dict[str, str]] = []
    for c in candidates:
        qid = str(c.get("qid") or "").strip().upper()
        if not re.match(r"^Q\d+$", qid):
            continue
        lab = str(c.get("label") or "").strip() or fetched.get(qid, ("", ""))[0]
        desc = str(c.get("description") or "").strip() or fetched.get(qid, ("", ""))[1]
        enriched.append({"qid": qid, "label": lab, "description": desc})

    if not enriched:
        return None

    ranked = embed_rerank_candidates(type_name, journal_text, enriched, wsd_keywords=wsd_kw or None)
    min_sim = _min_embed_for_confidence(conf)

    for c in ranked:
        if float(c.get("embed_score") or 0.0) < min_sim:
            continue
        qid = c["qid"]
        if not validate_wikidata_candidate(qid, cat, wsd_row):
            continue
        return {
            "id": qid,
            "label": str(c.get("label") or "").strip(),
            "description": str(c.get("description") or "").strip(),
        }
    return None


def _aat_fetch_preflabel(aid: str) -> Optional[str]:
    if not re.match(r"^\d{5,10}$", (aid or "").strip()):
        return None
    esc = aid.strip().replace("\\", "\\\\")
    q = f"""PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?pref WHERE {{
  <http://vocab.getty.edu/aat/{esc}> skos:prefLabel ?pref .
  FILTER(LANG(?pref) = "en")
}} LIMIT 1"""
    try:
        r = requests.get(
            _GETTY_SPARQL,
            params={
                "query": q,
                "format": "json",
                "default-graph-uri": _GETTY_AAT_GRAPH,
            },
            headers={"User-Agent": _GETTY_UA, "Accept": "application/sparql-results+json"},
            timeout=12,
        )
        r.raise_for_status()
        data = r.json()
        bindings = (data.get("results") or {}).get("bindings") or []
        if not bindings:
            return None
        v = bindings[0].get("pref", {}).get("value")
        return str(v).strip() if v else None
    except Exception:
        return None


def validate_batch_aat(aat_id: str, aat_label: str, aat_confidence: str) -> Optional[Tuple[str, str]]:
    """Confirm batch-proposed AAT exists in Getty; reject low confidence."""
    aid = str(aat_id or "").strip()
    if not aid or not re.match(r"^\d{5,10}$", aid):
        return None
    if str(aat_confidence or "low").strip().lower() == "low":
        return None
    pref = _aat_fetch_preflabel(aid)
    if pref is None:
        return None
    label = (aat_label or "").strip() or pref
    return aid, label


def batch_candidates_cache_sig(candidates: List[Dict[str, Any]], confidence: str) -> str:
    try:
        blob = json.dumps({"c": candidates, "conf": confidence}, sort_keys=True)
    except (TypeError, ValueError):
        return ""
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]
