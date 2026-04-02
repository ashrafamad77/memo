"""Refresh Wikidata place-link candidates using a free-text user hint (chat / inbox follow-up)."""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Optional

from .type_grounding_embed import (
    wikidata_batch_p31_blocklist_filter,
    wikidata_entity_search_candidates,
    wikidata_fetch_labels_descriptions,
    wikidata_filter_qids_by_geo_anchor,
    wikidata_label_search_in_place,
)
from .entity_link_candidates import (
    canonicalize_entity_link_candidates,
    cap_entity_link_candidates,
    entity_link_max_candidates,
    wikidata_coheres_with_mention,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# LLM-based location extraction — handles any free-form hint text
# ---------------------------------------------------------------------------

_LOCATION_EXTRACT_SYSTEM = (
    "You are a geographic location extractor. "
    "Given a user's clarification hint about a place, extract the most specific "
    "geographic location mentioned (city, district, region, or country). "
    "Return ONLY the location name as a plain string — no explanation, no punctuation. "
    "If no geographic location is mentioned, return an empty string."
)


def _llm_extract_location(hint: str, journal_text: str) -> Optional[str]:
    """Ask the LLM to extract the geographic location from a free-form hint.

    Uses the same Azure/OpenAI client as the rest of the pipeline.
    Returns the location string (e.g. "London", "Paris 10th arrondissement") or None.
    Fails gracefully — any exception returns None so the caller falls back.
    """
    h = (hint or "").strip()
    if not h:
        return None
    user_msg = f"Hint: {h}"
    jt = (journal_text or "").strip()
    if jt:
        user_msg += f"\nJournal excerpt: {jt[:400]}"
    try:
        api_key = os.environ.get("AZURE_OPENAI_API_KEY", "")
        endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
        deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")
        api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
        if not api_key:
            return None
        if endpoint:
            from openai import AzureOpenAI
            client = AzureOpenAI(
                api_key=api_key,
                azure_endpoint=endpoint.rstrip("/"),
                api_version=api_version,
            )
        else:
            from openai import OpenAI
            client = OpenAI(api_key=api_key)
        res = client.chat.completions.create(
            model=deployment,
            messages=[
                {"role": "system", "content": _LOCATION_EXTRACT_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.0,
            max_tokens=64,
        )
        loc = (res.choices[0].message.content or "").strip().strip(".,;\"'")
        return loc if len(loc) >= 2 else None
    except Exception as exc:
        logger.debug("_llm_extract_location failed: %s", exc)
        return None


def _lookup_location_qid(location_str: str) -> Optional[str]:
    """Return the Wikidata QID for a location string via text search.

    Prefers results whose description contains geographic keywords; falls back
    to the top result.  Returns None if the search yields nothing.
    """
    candidates = wikidata_entity_search_candidates(
        location_str, limit=5, cidoc_label="E53_Place"
    )
    geo_keywords = {
        "city", "town", "country", "capital", "district", "region", "area",
        "borough", "county", "state", "province", "municipality", "village",
        "commune", "prefecture", "department", "territory", "nation",
    }
    for c in candidates:
        desc = (c.get("description") or "").lower()
        if any(kw in desc for kw in geo_keywords):
            return c["wikidata_id"]
    return candidates[0]["wikidata_id"] if candidates else None


def _location_prepend_searches(mention: str, location: str) -> List[str]:
    """Generic wbsearch strings that put the resolved location first.

    Covers the broad area sense (district/area) before the narrow transport
    sense (station) so both survive the geo-filter candidate pool.
    """
    m = mention.strip()
    loc = location.strip()
    return [
        f"{loc} {m}",
        f"{m} {loc}",
        f"{m} {loc} district",
        f"{m} {loc} area",
        f"{m} {loc} station",
    ]


def _sp_label_search_variants(mention: str, location: Optional[str]) -> List[str]:
    """Distinct wbsearchentities strings for P131*-filtered SPARQL (order preserved).

    Built only from the **mention** and the LLM-extracted **location** string — no hardcoded
    cities, QIDs, or countries. Extra strings (e.g. ``"{mention} {location} district"``) help
    surface district/neighbourhood items when bare EntitySearch ranks transit infrastructure first.
    """
    m = (mention or "").strip()
    if not m:
        return []
    out: List[str] = [m]
    loc = (location or "").strip()
    if loc:
        out.extend(
            [
                f"{m} {loc}",
                f"{loc} {m}",
                f"{m}, {loc}",
                f"{m} {loc} district",
                f"{m} {loc} area",
            ]
        )
    seen: set[str] = set()
    uniq: List[str] = []
    for s in out:
        s = s.strip()
        if not s or s in seen:
            continue
        seen.add(s)
        uniq.append(s)
    return uniq


def _geo_candidate_area_transit_bucket(label: str, description: str) -> int:
    """0 = settlement / district sense; 1 = neutral; 2 = station / rail infrastructure."""
    b = f"{label} {description}".lower()
    if re.search(
        r"\b(station|sidings|siding|railway station|tube station|metro station|"
        r"underground station|train station|tram stop|bus station|carriage|depot)\b",
        b,
    ):
        return 2
    if re.search(
        r"\b(district|quarter|neighbourhood|neighborhood|borough\b|ward\b|"
        r"suburb|human settlement|residential|locality)\b",
        b,
    ):
        return 0
    return 1


def _order_geo_candidates_area_before_transit(
    candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if len(candidates) < 2:
        return candidates
    indexed = list(enumerate(candidates))
    indexed.sort(
        key=lambda ic: (
            _geo_candidate_area_transit_bucket(
                str(ic[1].get("label") or ""),
                str(ic[1].get("description") or ""),
            ),
            ic[0],
        )
    )
    return [ic[1] for ic in indexed]


def _collect_wikidata_hits_in_place(
    mention: str,
    location: Optional[str],
    anchor_qid: str,
    *,
    per_search_limit: int = 24,
) -> List[Dict[str, Any]]:
    """Merge SPARQL+EntitySearch hits for several label strings; dedupe by QID (first wins)."""
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for s in _sp_label_search_variants(mention, location):
        for c in wikidata_label_search_in_place(s, anchor_qid, limit=per_search_limit):
            qid = str(c.get("wikidata_id") or "").strip().upper()
            if not qid or qid in seen:
                continue
            seen.add(qid)
            out.append(c)
    return out


# ---------------------------------------------------------------------------
# Public helpers (called from server/app.py and entity_enrichment)
# ---------------------------------------------------------------------------

def sibling_enrichment_anchor_qid_from_hint(hint: str, journal_text: str) -> Optional[str]:
    """Geo QID to refresh sibling place tasks before the user picks the primary mention.

    Uses the LLM to extract any geographic location from the hint (free-form text),
    then resolves it to a Wikidata QID — works for any city, region, or country.
    """
    location = _llm_extract_location(hint, journal_text)
    if not location:
        return None
    return _lookup_location_qid(location)


def _accept_geo_proven_candidates(
    mention: str,
    candidates: List[Dict[str, Any]],
    cidoc_label: str,
) -> List[Dict[str, Any]]:
    """Lighter gate for results already proven geographic by the P131* SPARQL anchor.

    Runs P31 blocklist + mention coherence only.  Skips entity_link_qid_plausible_instance
    because the items were already constrained to a specific Wikidata location — the SPARQL
    anchor is stronger evidence than the generic Q2221906-reachability proof.
    """
    if not candidates:
        return []
    qids = [c["wikidata_id"] for c in candidates if c.get("wikidata_id")]
    fetched = wikidata_fetch_labels_descriptions(qids)
    p31_rejected = wikidata_batch_p31_blocklist_filter(qids, cidoc_label)
    out: List[Dict[str, Any]] = []
    for c in candidates:
        qid = str(c.get("wikidata_id") or "").strip().upper()
        if qid in p31_rejected:
            continue
        pair = fetched.get(qid)
        lab = str(pair[0] if pair else c.get("label") or "").strip()
        desc = str(pair[1] if pair else c.get("description") or "").strip()
        if not lab:
            continue
        if not wikidata_coheres_with_mention(mention, lab, desc):
            continue
        out.append({"wikidata_id": qid, "label": lab, "description": desc, "confidence": "high"})
    return out


def refresh_place_candidates_with_user_hint(
    mention: str,
    entity_label: str,
    journal_text: str,
    hint: str,
    user_profile: Dict[str, Any] | None,
) -> List[Dict[str, Any]]:
    """Re-run contextual Wikidata search with journal + user clarification, then strict gates.

    When the hint provides a geographic anchor (any city/region/country):
      1. Extract location string from hint/journal text.
      2. Look up its Wikidata QID (geo-anchor).
      3. Run text search with location-context prepend queries to get raw candidates.
      4. Filter through wikidata_filter_qids_by_geo_anchor (single SPARQL batch) — items
         that pass are geographically proven, so only the lighter _accept_geo_proven_candidates
         gate runs (P31 blocklist + mention coherence, no Q2221906 proof).
      5. If the geo-filter returns results, return them.  Otherwise fall back to the full
         ``canonicalize_entity_link_candidates`` gate on all raw candidates.
    """
    m = (mention or "").strip()
    h = (hint or "").strip()
    if not m or not h:
        return []
    jt = (journal_text or "").strip()
    elab = (entity_label or "E53_Place").strip() or "E53_Place"
    prof = user_profile if isinstance(user_profile, dict) else {}

    location = _llm_extract_location(h, jt)
    anchor_qid = _lookup_location_qid(location) if location else None

    # --- Text search with location-context prepend queries ---
    augmented = (
        f"{jt}\n\n[User clarification for place \"{m}\"]: {h}"
        if jt
        else f"[Journal context unknown; user clarification for place \"{m}\"]: {h}"
    )
    lim = max(16, entity_link_max_candidates() * 5)

    prepend: List[str] = []
    if location:
        prepend = _location_prepend_searches(m, location)

    # When we have a location anchor, ignore profile city to avoid cross-city noise
    # (e.g. profile says "Paris" but hint says "London").
    prof_for_search: Dict[str, Any] = {} if location else prof

    raw = wikidata_entity_search_candidates(
        m,
        limit=lim,
        cidoc_label="E53_Place",
        journal_text=augmented,
        user_profile=prof_for_search,
        prepend_queries=prepend if prepend else None,
    )
    shaped = [
        {
            "wikidata_id": c.get("wikidata_id"),
            "label": str(c.get("label") or ""),
            "description": str(c.get("description") or ""),
            "confidence": str(c.get("confidence") or "medium"),
        }
        for c in raw
        if isinstance(c, dict) and c.get("wikidata_id")
    ]

    # --- Geo-filter path (when anchor QID available) ---
    if anchor_qid:
        # SPARQL + mwapi: multiple label strings from (mention, LLM location) so district-like
        # items are not displaced when bare EntitySearch ranks stations first.
        sparql_hits = _collect_wikidata_hits_in_place(m, location, anchor_qid)

        # wbsearch geo-filter: keep candidates that pass P131*/P17 batch SPARQL.
        raw_qids = [c["wikidata_id"] for c in shaped]
        passing_qids = wikidata_filter_qids_by_geo_anchor(raw_qids, anchor_qid) if shaped else set()
        wbsearch_geo = [c for c in shaped if c["wikidata_id"] in passing_qids]

        # Merge: SPARQL hits first (higher precision), wbsearch geo fills the rest.
        seen_merge: set = {c["wikidata_id"] for c in sparql_hits}
        merged = list(sparql_hits) + [c for c in wbsearch_geo if c["wikidata_id"] not in seen_merge]

        if merged:
            merged = _order_geo_candidates_area_before_transit(merged)

        if merged:
            accepted = _accept_geo_proven_candidates(m, merged, elab)
            if accepted:
                accepted = _order_geo_candidates_area_before_transit(accepted)
                return cap_entity_link_candidates(accepted)

    # --- Fallback: full strict gate on all raw candidates ---
    canon = canonicalize_entity_link_candidates(m, shaped, cidoc_label=elab)
    return cap_entity_link_candidates(canon)
