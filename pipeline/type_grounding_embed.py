"""Stage 2–3 after Babelfy/BabelNet E55 candidates: embedding rerank + SPARQL validate.

Default pipeline: Babelfy E55 grounding (candidates) → this module → optional AAT from batch only.

Circuit breaker: MEMO_WD_LEGACY_WBSEARCH=1 restores wbsearchentities + heuristics in type_resolver.get_wikidata_info.

LEGACY PATH — delete after 2026-05-15 once batch+embed is stable in production (see type_resolver legacy block).

Embedding cosine floors default for sentence-transformers / MiniLM-class models (~384d); calibrate from
accepted vs rejected pairs in logs. OpenAI text-embedding-3 family often needs ~0.55–0.65. Override via env.
"""

from __future__ import annotations

import difflib
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

# Entity linking: require a definitive WDQS **yes** for taxonomy; unknown/timeout ⇒ reject.
_MEMO_ENTITY_LINK_STRICT_WDQS = os.getenv("MEMO_ENTITY_LINK_STRICT_WDQS", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)
_MEMO_WD_ENTITY_LINK_SPARQL_TIMEOUT = int(os.getenv("MEMO_WD_ENTITY_LINK_SPARQL_TIMEOUT", "26"))

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


_MAX_E53_CONTEXT_QUERIES = int(os.getenv("MEMO_WD_E53_CONTEXT_QUERIES", "8"))


def _e53_profile_geo_search_strings(mention: str, user_profile: Optional[Dict[str, Any]]) -> List[str]:
    """Use author profile city/country as disambiguation context (not static QIDs)."""
    if not isinstance(user_profile, dict) or not user_profile:
        return []
    m = (mention or "").strip()
    if len(m) < 2:
        return []
    mf = m.casefold()
    out: List[str] = []
    for key in ("current_city", "home_country", "nationality"):
        raw = str(user_profile.get(key) or "").strip()
        if not raw:
            continue
        primary = raw.split(",")[0].strip()
        if len(primary) < 2:
            continue
        pf = primary.casefold()
        if pf == mf or pf in mf or mf in pf:
            continue
        for q in (f"{primary} {m}", f"{m} {primary}"):
            if q not in out:
                out.append(q)
    return out


def _e53_journal_geo_search_strings(mention: str, journal_text: str) -> List[str]:
    """No journal substring → search-string table: context comes from ``prepend_queries`` (e.g. chat
    hints), profile fields, and the bare mention only."""
    return []


def _wbsearchentities_one(
    search: str,
    *,
    fetch_lim: int,
    instance_class: bool,
) -> List[Dict[str, Any]]:
    search = (search or "").strip()
    if len(search) < 2:
        return []
    try:
        r = requests.get(
            _WIKIDATA_API,
            params={
                "action": "wbsearchentities",
                "search": search,
                "language": "en",
                "uselang": "en",
                "format": "json",
                "limit": str(max(1, min(int(fetch_lim), 50))),
            },
            headers={"User-Agent": _WIKIDATA_UA},
            timeout=12,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        logger.info("wbsearchentities failed for %r: %s", search[:80], exc)
        return []
    chunk: List[Dict[str, Any]] = []
    for h in data.get("search") or []:
        if not isinstance(h, dict):
            continue
        qid = str(h.get("id") or "").strip().upper()
        if not re.match(r"^Q\d+$", qid):
            continue
        lab = str(h.get("label") or "").strip()
        desc = str(h.get("description") or "").strip()
        if instance_class and _wikidata_description_is_abstract_concept(desc):
            continue
        chunk.append({
            "wikidata_id": qid,
            "label": lab,
            "description": desc,
            "confidence": "medium",
        })
    return chunk


def wikidata_entity_search_candidates(
    mention: str,
    *,
    limit: int = 12,
    cidoc_label: str = "",
    journal_text: str = "",
    user_profile: Optional[Dict[str, Any]] = None,
    prepend_queries: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Short-string Wikidata search for entity linking when the LLM yields no valid QIDs.

    Uses ``wbsearchentities`` (same API as legacy type_resolver). Results are Wikidata-backed
    labels/descriptions — not model-invented pairs.

    For **E53_Place**, merges ``prepend_queries`` (if any), **user profile** city/country strings
    with the mention, then the bare mention. No fixed list of city names inferred from journal text.

    When ``cidoc_label`` is an instance class (E53_Place, E21_Person, E74_Group), items whose
    descriptions mark them as abstract concepts ("type of …") are skipped immediately so that
    downstream SPARQL checks see a cleaner candidate list.

    ``prepend_queries`` (E53 only): extra wbsearchentities strings **first** (e.g. from chat hints
    or callers that derive context from the entry) so they are not displaced by profile or bare
    mention searches.
    """
    q = (mention or "").strip()
    if len(q) < 2 or len(q) > 200:
        return []
    instance_class = (cidoc_label or "").strip() in ("E53_Place", "E21_Person", "E74_Group")
    fetch_lim = max(1, min(int(limit) * 3 if instance_class else int(limit), 50))

    # E53: run prepend + profile-scoped queries **before** the bare mention so contextual strings
    # are not starved once the merge budget fills from the bare mention alone.
    search_order: List[str] = []
    if (cidoc_label or "").strip() == "E53_Place":
        for s in prepend_queries or []:
            ss = (s or "").strip()
            if len(ss) >= 2 and ss not in search_order:
                search_order.append(ss)
        for s in _e53_journal_geo_search_strings(q, journal_text):
            if s not in search_order:
                search_order.append(s)
        for s in _e53_profile_geo_search_strings(q, user_profile):
            if s not in search_order:
                search_order.append(s)
        if q not in search_order:
            search_order.append(q)
        _pre = len(prepend_queries or [])
        _cap = min(18, _pre + 1 + _MAX_E53_CONTEXT_QUERIES)
        search_order = search_order[:_cap]
    else:
        search_order = [q]

    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    e53_multi = (cidoc_label or "").strip() == "E53_Place" and len(search_order) > 1
    collect_target = max(40, int(limit) * 3) if e53_multi else int(limit)
    # Spread budget across queries so early strings cannot exhaust the whole list.
    per_query_cap = 8 if e53_multi else collect_target

    for search_str in search_order:
        chunk = _wbsearchentities_one(
            search_str,
            fetch_lim=fetch_lim,
            instance_class=instance_class,
        )
        added_here = 0
        for row in chunk:
            qid = row["wikidata_id"]
            if qid in seen:
                continue
            seen.add(qid)
            out.append(row)
            added_here += 1
            if added_here >= per_query_cap:
                break
            if len(out) >= collect_target:
                break
        if len(out) >= collect_target:
            break

    return out


_WDQS_URL = "https://query.wikidata.org/sparql"

# Default geo-search radius in km when using coordinate-based location enrichment.
_GEO_RADIUS_KM = int(os.getenv("MEMO_GEO_RADIUS_KM", "30"))

# P31 (instance of) values that immediately disqualify candidates per CIDOC class.
# Checked via fast wbgetentities API — no SPARQL, no timeout risk.
_P31_BLOCKLIST: Dict[str, Set[str]] = {
    "E53_Place": {
        "Q5",          # human
        "Q16521",      # taxon
        "Q737498",     # academic journal
        "Q5633421",    # scientific journal
        "Q41298",      # magazine
        "Q1002697",    # periodical literature
        "Q11424",      # film
        "Q5398426",    # television series
        "Q482994",     # album
        "Q7366",       # song
        "Q134556",     # single (music)
        "Q4167410",    # Wikimedia disambiguation page
        "Q4167836",    # Wikimedia category
        "Q17633526",   # Wikimedia article
        "Q7725634",    # literary work
        "Q571",        # book
        "Q17537576",   # creative work
        "Q3331189",    # version, edition, or translation
        "Q215380",     # musical group / band
        "Q178885",     # deity
        "Q4271324",    # mythological character
        "Q11688446",   # Roman deity
        "Q205985",     # mythological entity / figure (Wikidata uses for some gods)
        "Q36646373",   # publication identifier (e.g. LCCN)
        "Q36524",      # authority file
        "Q1982918",    # online public access catalog
        "Q35127",      # website
        "Q856638",     # library catalog
        "Q7397",       # software
        "Q166142",     # application
        # Sports / associations — never a geographic place
        "Q476028",     # association football club
        "Q4438121",    # sports club
        "Q847017",     # sports club (broader)
        "Q15944511",   # sports organization
        "Q18593544",   # sports association
        "Q2088357",    # athletic club
        "Q261716",     # athletic association
    },
    "E21_Person": {
        "Q16521",      # taxon
        "Q737498",     # academic journal
        "Q5633421",    # scientific journal
        "Q41298",      # magazine
        "Q4167410",    # Wikimedia disambiguation page
        "Q4167836",    # Wikimedia category
        "Q515",        # city
        "Q6256",       # country
        "Q3624078",    # sovereign state
        "Q35657",      # U.S. state
        "Q11424",      # film
        "Q7366",       # song
    },
    "E74_Group": {
        "Q5",          # individual human (not a group)
        "Q16521",      # taxon
        "Q4167410",    # Wikimedia disambiguation page
        "Q4167836",    # Wikimedia category
        "Q515",        # city
        "Q6256",       # country
        "Q11424",      # film
        "Q7366",       # song
    },
}


def wikidata_batch_p31_blocklist_filter(qids: List[str], cidoc_label: str) -> Set[str]:
    """Return the set of QIDs to reject based on their P31 (instance of) values.

    Uses wbgetentities API — fast and reliable, no SPARQL dependency.
    Returns empty set on API failure so callers remain unblocked.
    """
    blocklist = _P31_BLOCKLIST.get((cidoc_label or "").strip(), set())
    if not blocklist:
        return set()
    clean = [q.strip().upper() for q in qids if re.match(r"^Q\d+$", (q or "").strip().upper())]
    if not clean:
        return set()
    rejected: Set[str] = set()
    for i in range(0, len(clean), 50):
        batch = clean[i : i + 50]
        try:
            r = requests.get(
                _WIKIDATA_API,
                params={
                    "action": "wbgetentities",
                    "ids": "|".join(batch),
                    "props": "claims",
                    "format": "json",
                },
                headers={"User-Agent": _WIKIDATA_UA},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            logger.info("wikidata_batch_p31_blocklist_filter: API failed: %s", exc)
            continue
        for qid, ent in (data.get("entities") or {}).items():
            if not isinstance(ent, dict) or "missing" in ent:
                continue
            p31_values: Set[str] = set()
            for claim in (ent.get("claims") or {}).get("P31") or []:
                if not isinstance(claim, dict):
                    continue
                dv = ((claim.get("mainsnak") or {}).get("datavalue") or {}).get("value") or {}
                if isinstance(dv, dict):
                    v = str(dv.get("id") or "").strip().upper()
                    if re.match(r"^Q\d+$", v):
                        p31_values.add(v)
            hit = p31_values & blocklist
            if hit:
                rejected.add(qid.strip().upper())
                logger.info(
                    "wikidata_batch_p31_blocklist_filter: rejected %s for %s — P31 blocked: %s",
                    qid,
                    cidoc_label,
                    hit,
                )
    return rejected


def _wdqs_select(sparql: str, *, timeout: int = 18) -> Optional[List[dict]]:
    """Run a SPARQL SELECT against WDQS; return bindings list or None on error."""
    try:
        r = requests.get(
            _WDQS_URL,
            params={"query": sparql, "format": "json"},
            headers={"User-Agent": _WIKIDATA_UA, "Accept": "application/sparql-results+json"},
            timeout=timeout,
        )
        r.raise_for_status()
        return (r.json().get("results") or {}).get("bindings") or []
    except Exception as exc:
        logger.info("_wdqs_select failed: %s", exc)
        return None


def _parse_wkt_point(wkt: str) -> Optional[Tuple[float, float]]:
    """Extract (lon, lat) from a Wikidata WKT literal like 'Point(-123.36 48.42)'."""
    m = re.search(r"Point\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", wkt or "", re.IGNORECASE)
    if not m:
        return None
    try:
        return float(m.group(1)), float(m.group(2))
    except ValueError:
        return None


def _get_location_coords(location_qid: str) -> Optional[Tuple[float, float]]:
    """Return (lon, lat) for a Wikidata location QID via P625, or None."""
    lq = (location_qid or "").strip().upper()
    if not re.match(r"^Q\d+$", lq):
        return None
    bindings = _wdqs_select(
        f"SELECT ?coord WHERE {{ wd:{lq} wdt:P625 ?coord . }} LIMIT 1", timeout=10
    )
    if not bindings:
        return None
    wkt = str((bindings[0].get("coord") or {}).get("value") or "")
    return _parse_wkt_point(wkt)


def _get_location_ancestors(location_qid: str) -> List[str]:
    """Return QIDs of all P131* ancestors of location_qid (including itself, excluding Q2 Earth)."""
    lq = (location_qid or "").strip().upper()
    if not re.match(r"^Q\d+$", lq):
        return []
    bindings = _wdqs_select(
        f"SELECT ?anc WHERE {{ wd:{lq} wdt:P131* ?anc . FILTER(?anc != wd:Q2) }}", timeout=10
    )
    if bindings is None:
        return []
    out = []
    seen: Set[str] = set()
    for b in bindings:
        uri = str((b.get("anc") or {}).get("value") or "")
        qid = uri.rsplit("/", 1)[-1].upper() if "/" in uri else uri.upper()
        if re.match(r"^Q\d+$", qid) and qid not in seen:
            seen.add(qid)
            out.append(qid)
    return out


def _sparql_p31_matches_type_root(item_var: str, type_qid: str) -> str:
    """SPARQL lines: ``item_var`` has P31 to a class under ``type_qid`` (subclass or explicit roots).

    Q7075 (*library*) is expanded: real items are rarely ``wdt:P31 wd:Q7075``; they use public library,
    national library, library building, etc., which do not always have a P279* chain to Q7075 in WD.
    """
    tq = (type_qid or "").strip().upper()
    if not re.match(r"^Q\d+$", tq):
        return ""
    if tq == "Q7075":
        return f"""
  {item_var} wdt:P31 ?__enrich_c .
  FILTER(
    EXISTS {{ ?__enrich_c wdt:P279* wd:Q7075 . }}
    || ?__enrich_c IN (wd:Q7075, wd:Q22806, wd:Q1130459, wd:Q1438040, wd:Q856584)
  )"""
    return f"""
  {item_var} wdt:P31 ?__enrich_c .
  FILTER( EXISTS {{ ?__enrich_c wdt:P279* wd:{tq} . }} || ?__enrich_c = wd:{tq} )"""


def _bindings_to_candidates(bindings: List[dict]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for b in bindings:
        uri = str((b.get("item") or {}).get("value") or "")
        qid = uri.rsplit("/", 1)[-1].upper() if "/" in uri else uri.upper()
        if not re.match(r"^Q\d+$", qid) or qid in seen:
            continue
        seen.add(qid)
        lab = str((b.get("itemLabel") or {}).get("value") or "").strip()
        desc = str((b.get("itemDescription") or {}).get("value") or "").strip()
        if not lab or lab == qid:
            continue
        out.append({"wikidata_id": qid, "label": lab, "description": desc, "confidence": "medium"})
    return out


def wikidata_instances_in_place(
    type_qid: str, location_qid: str, *, limit: int = 5
) -> List[Dict[str, Any]]:
    """Find Wikidata instances of type_qid geographically near location_qid.

    Strategy (two attempts, returns on first success):
    1. Geo-nearby: fetch P625 coords of location_qid, then wikibase:around within
       MEMO_GEO_RADIUS_KM (default 30 km).  Robust against inconsistent P131 data.
    2. Ancestor-P131 fallback: get P131* ancestors of location_qid, query items with
       direct P131 matching any ancestor (most-local first, stops at country level).

    Returns candidates in {wikidata_id, label, description, confidence} format.
    """
    tq = (type_qid or "").strip().upper()
    lq = (location_qid or "").strip().upper()
    if not re.match(r"^Q\d+$", tq) or not re.match(r"^Q\d+$", lq):
        return []
    lim = max(1, min(int(limit), 20))

    # When the anchor has P17, prefer instances with the same country (or no P17).
    # Skips e.g. UK-anchor geo queries accidentally picking up cross-border junk; if the
    # anchor has no P17, OPTIONAL leaves filters vacuously true.
    p17_lines = f"""
  OPTIONAL {{ wd:{lq} wdt:P17 ?__memoAnchorP17 . }}
  OPTIONAL {{ ?item wdt:P17 ?__memoItemP17 . }}
  FILTER( !BOUND(?__memoAnchorP17) || !BOUND(?__memoItemP17) || ?__memoItemP17 = ?__memoAnchorP17 )
"""

    # --- Strategy 1: geo-nearby via P625 coordinates ---
    coords = _get_location_coords(lq)
    if coords:
        lon, lat = coords
        p31_block = _sparql_p31_matches_type_root("?item", tq)
        geo_sparql = f"""
SELECT DISTINCT ?item ?itemLabel ?itemDescription WHERE {{
  SERVICE wikibase:around {{
    ?item wdt:P625 ?coord .
    bd:serviceParam wikibase:center "Point({lon} {lat})"^^geo:wktLiteral .
    bd:serviceParam wikibase:radius "{_GEO_RADIUS_KM}" .
  }}
{p31_block}
{p17_lines}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }}
}} LIMIT {lim}
"""
        bindings = _wdqs_select(geo_sparql, timeout=18)
        if bindings:
            results = _bindings_to_candidates(bindings)
            if results:
                logger.info(
                    "wikidata_instances_in_place: geo strategy → %d results "
                    "(type=%s, loc=%s, r=%skm)", len(results), tq, lq, _GEO_RADIUS_KM
                )
                return results

    # --- Strategy 2: ancestor P131 fallback ---
    ancestors = _get_location_ancestors(lq)
    # Country-level QIDs to stop at (Q16=Canada, Q30=US, Q142=France, Q145=UK, Q183=Germany, etc.)
    # We stop before reaching country to avoid returning too-broad results.
    # Heuristic: skip if ancestor == location itself (already tried), skip if it's a continent/Earth.
    _SKIP_BROAD = {"Q2", "Q3624078", "Q6256"}  # Earth, sovereign state category, country class
    for anc in ancestors:
        if anc in _SKIP_BROAD:
            break
        p31_block = _sparql_p31_matches_type_root("?item", tq)
        anc_sparql = f"""
SELECT DISTINCT ?item ?itemLabel ?itemDescription WHERE {{
  ?item wdt:P131 wd:{anc} .
{p31_block}
{p17_lines}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }}
}} LIMIT {lim}
"""
        bindings = _wdqs_select(anc_sparql, timeout=12)
        if bindings:
            results = _bindings_to_candidates(bindings)
            if results:
                logger.info(
                    "wikidata_instances_in_place: ancestor strategy → %d results "
                    "(type=%s, ancestor=%s)", len(results), tq, anc
                )
                return results

    return []


def wikidata_label_search_in_place(
    label: str, location_qid: str, *, limit: int = 6
) -> List[Dict[str, Any]]:
    """Find Wikidata items whose English label matches `label` AND are located in/near location_qid.

    Uses the ``wikibase:mwapi`` EntitySearch service inside SPARQL so text matching and
    geographic filtering happen in one round-trip — no static country lists needed.

    Uses P131* (transitive administrative location) as the geographic filter.
    Returns empty list if WDQS returns nothing or times out.
    """
    lq = (location_qid or "").strip().upper()
    lbl = (label or "").strip()
    if not re.match(r"^Q\d+$", lq) or not lbl:
        return []
    lim = max(1, min(int(limit), 20))
    escaped = lbl.replace('"', '\\"')

    # Strategy 1: P131* — item is (transitively) located in the anchor area.
    sparql_p131 = f"""
SELECT DISTINCT ?item ?itemLabel ?itemDescription WHERE {{
  SERVICE wikibase:mwapi {{
    bd:serviceParam wikibase:endpoint "www.wikidata.org" ;
                    wikibase:api "EntitySearch" ;
                    mwapi:search "{escaped}" ;
                    mwapi:language "en" .
    ?item wikibase:apiOutputItem mwapi:item .
  }}
  ?item wdt:P131* wd:{lq} .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }}
}} LIMIT {lim}
"""
    bindings = _wdqs_select(sparql_p131, timeout=20)
    if bindings:
        results = _bindings_to_candidates(bindings)
        if results:
            logger.info(
                "wikidata_label_search_in_place: P131* → %d results (label=%r, loc=%s)",
                len(results), lbl, lq,
            )
            return results

    return []


def wikidata_filter_qids_by_geo_anchor(qids: List[str], anchor_qid: str) -> Set[str]:
    """Return the subset of `qids` that are geographically in/near `anchor_qid`.

    Two criteria (OR):
      (a) item is transitively administratively located in the anchor (P131*).
      (b) item shares the same P17 country as the anchor.

    A single batch SPARQL VALUES query is used — one round-trip regardless of list size.
    Returns an empty set on any SPARQL error or timeout so the caller can fall back gracefully.
    """
    aq = (anchor_qid or "").strip().upper()
    clean = [q.strip().upper() for q in (qids or []) if re.match(r"^Q\d+$", (q or "").strip().upper())]
    if not clean or not re.match(r"^Q\d+$", aq):
        return set()

    values_block = " ".join(f"wd:{q}" for q in clean)
    sparql = f"""
SELECT DISTINCT ?item WHERE {{
  VALUES ?item {{ {values_block} }}
  {{
    ?item wdt:P131* wd:{aq} .
  }}
  UNION
  {{
    wd:{aq} wdt:P17 ?anchorCountry .
    ?item wdt:P17 ?anchorCountry .
  }}
}}
"""
    try:
        bindings = _wdqs_select(sparql, timeout=15)
    except Exception as exc:
        logger.warning("wikidata_filter_qids_by_geo_anchor: SPARQL error: %s", exc)
        return set()

    if bindings is None:
        logger.warning("wikidata_filter_qids_by_geo_anchor: SPARQL timeout/error for anchor=%s", aq)
        return set()

    passing: Set[str] = set()
    for row in bindings:
        val = row.get("item", {}).get("value", "")
        m = re.search(r"Q\d+$", val)
        if m:
            passing.add(m.group(0).upper())

    logger.info(
        "wikidata_filter_qids_by_geo_anchor: anchor=%s, input=%d, passing=%d (%s)",
        aq, len(clean), len(passing), passing,
    )
    return passing


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


# Entity linking (E74): root for "organization-like" instances (not abstract org theory).
_WD_ORG_INSTANCE_ROOT = "Q43229"

# Common Wikidata description prefixes that signal abstract classes, not real-world instances.
# These are "starts-with" patterns matched against the lowercased description.
_CONCEPT_DESC_PREFIXES = (
    "type of ",
    "kind of ",
    "form of ",
    "genre of ",
    "class of ",
    "category of ",
    "style of ",
    "sort of ",
    "group of ",
    "variety of ",
    "species of ",
    "subtype of ",
    # "X of a Y" / "X of an Y" patterns — indefinite article signals a class, not a named instance:
    # e.g. "branch of a library organisation" (Q11396180), "part of a building" style descriptions.
    "branch of a ",
    "branch of an ",
    "part of a ",
    "part of an ",
    "subdivision of a ",
    "division of a ",
    "section of a ",
    "unit of a ",
    "element of a ",
    # Human names / linguistic items — never a place or real-world entity instance:
    "family name",
    "surname",
    "given name",
    "male given name",
    "female given name",
    "unisex given name",
    "human name",
    # Biological taxa — never a place:
    "genus of ",
    "species of ",
    "taxon ",
    # Other clearly non-instance types:
    "fictional character",
    "crater on ",
    "asteroid ",
    "mythological ",
)


def _wikidata_description_is_abstract_concept(description: str) -> bool:
    """Return True if the Wikidata description marks this item as an abstract class/type."""
    desc = (description or "").strip().lower()
    return any(desc.startswith(p) for p in _CONCEPT_DESC_PREFIXES)


def entity_link_qid_plausible_instance(
    qid: str, cidoc_label: str, *, description: str = ""
) -> Optional[bool]:
    """For E53/E21/E74 Wikidata linking: keep items that WDQS proves are valid instances.

    Uses Wikidata Query Service (SPARQL), not the Action API, for the final ontology gate.
    With default ``MEMO_ENTITY_LINK_STRICT_WDQS=1``, WDQS timeouts/errors count as **reject**
    (no fail-open). Override with ``MEMO_ENTITY_LINK_STRICT_WDQS=0`` to allow unknowns.

    Pass ``description`` for a fast pre-filter (abstract "type of …" phrases) before SPARQL.
    """
    lab = (cidoc_label or "").strip()
    if lab not in ("E53_Place", "E21_Person", "E74_Group"):
        return True
    # Fast text pre-filter: abstract-concept descriptions never represent instances.
    if description and _wikidata_description_is_abstract_concept(description):
        logger.info(
            "type_grounding_embed: rejected QID %s for %s — abstract-concept description: %r",
            qid,
            lab,
            description[:80],
        )
        return False
    from .type_resolver import (
        _WD_DEFAULT_PLACE_TAXONOMY_ROOT,
        _safe_wikidata_qid,
        wikidata_e53_must_not_reach_forbidden,
        wikidata_entity_p31_reaches_root,
    )

    q = _safe_wikidata_qid(qid)
    if not q:
        return False
    tmo = max(8, min(_MEMO_WD_ENTITY_LINK_SPARQL_TIMEOUT, 60))
    strict = _MEMO_ENTITY_LINK_STRICT_WDQS

    def _accept_reach(reach: Optional[bool]) -> bool:
        if reach is True:
            return True
        if reach is False:
            return False
        return not strict

    # Use instance_only=True: reject pure P279* subclass items (abstract concept classes).
    if lab == "E53_Place":
        root = _safe_wikidata_qid(_WD_DEFAULT_PLACE_TAXONOMY_ROOT) or "Q2221906"
        reach = wikidata_entity_p31_reaches_root(
            q, root, instance_only=True, timeout=tmo
        )
        if not _accept_reach(reach):
            logger.info(
                "type_grounding_embed: rejected QID %s for E53 — no WDQS proof under %s (reach=%s)",
                q,
                root,
                reach,
            )
            return False
        forbid = wikidata_e53_must_not_reach_forbidden(q, timeout=tmo)
        if forbid is True:
            logger.info(
                "type_grounding_embed: rejected QID %s for E53 — SPARQL forbidden superclass hit",
                q,
            )
            return False
        if forbid is None and strict:
            logger.info(
                "type_grounding_embed: rejected QID %s for E53 — forbidden-class SPARQL inconclusive",
                q,
            )
            return False
        return True
    if lab == "E21_Person":
        reach = wikidata_entity_p31_reaches_root(q, "Q5", instance_only=True, timeout=tmo)
        if not _accept_reach(reach):
            logger.info(
                "type_grounding_embed: rejected QID %s for E21 — no WDQS proof under Q5 (reach=%s)",
                q,
                reach,
            )
            return False
        return True
    if lab == "E74_Group":
        reach = wikidata_entity_p31_reaches_root(
            q, _WD_ORG_INSTANCE_ROOT, instance_only=True, timeout=tmo
        )
        if not _accept_reach(reach):
            logger.info(
                "type_grounding_embed: rejected QID %s for E74 — no WDQS proof under org root (reach=%s)",
                q,
                reach,
            )
            return False
        return True
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


def _aat_type_coheres_with_pref(type_name: str, pref: str) -> bool:
    """True if E55 type name plausibly matches Getty prefLabel (drops wrong numeric IDs)."""
    tn = (type_name or "").strip().lower()
    pr = (pref or "").strip().lower()
    if not tn or not pr:
        return True
    if tn in pr or pr in tn:
        return True
    tn_toks = [t for t in re.split(r"[^a-z0-9]+", tn) if len(t) >= 3]
    pr_toks = [t for t in re.split(r"[^a-z0-9]+", pr) if t]
    pr_flat = " " + pr + " "
    for t in tn_toks:
        if t in pr_flat:
            return True
    for t in pr_toks:
        if len(t) >= 4 and t in tn:
            return True
    return difflib.SequenceMatcher(None, tn, pr).ratio() >= 0.45


def validate_batch_aat(
    type_name: str,
    aat_id: str,
    aat_label: str,
    aat_confidence: str,
) -> Optional[Tuple[str, str]]:
    """Confirm batch-proposed AAT exists in Getty and matches the type name; reject low confidence.

    Always returns Getty ``prefLabel`` as the stored label — never the LLM's label alone (it can lie).
    """
    aid = str(aat_id or "").strip()
    if not aid or not re.match(r"^\d{5,10}$", aid):
        return None
    if str(aat_confidence or "low").strip().lower() == "low":
        return None
    pref = _aat_fetch_preflabel(aid)
    if pref is None:
        return None
    if not _aat_type_coheres_with_pref(type_name, pref):
        logger.info(
            "validate_batch_aat: rejected AAT %s (Getty pref=%r) for type %r",
            aid,
            pref,
            type_name,
        )
        return None
    return aid, pref


def batch_candidates_cache_sig(candidates: List[Dict[str, Any]], confidence: str) -> str:
    try:
        blob = json.dumps({"c": candidates, "conf": confidence}, sort_keys=True)
    except (TypeError, ValueError):
        return ""
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]
