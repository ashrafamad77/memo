"""BabelNet REST API v9 — synset details and linked lexicons (shared key with Babelfy)."""
from __future__ import annotations

import hashlib
import json
import logging
import os
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

BABELNET_GET_SYNSET = "https://babelnet.io/v9/getSynset"
BABELNET_GET_SENSES = "https://babelnet.io/v9/getSenses"
_DEFAULT_TIMEOUT = float(os.getenv("MEMO_BABELNET_TIMEOUT_SEC", "30"))
_CACHE_MAX = max(64, min(int(os.getenv("MEMO_BABELNET_CACHE_MAX", "512")), 8192))


class _LRU:
    def __init__(self, maxsize: int) -> None:
        self._max = maxsize
        self._d: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        if key not in self._d:
            return None
        self._d.move_to_end(key)
        return self._d[key]

    def set(self, key: str, value: Dict[str, Any]) -> None:
        self._d[key] = value
        self._d.move_to_end(key)
        while len(self._d) > self._max:
            self._d.popitem(last=False)


_synset_cache = _LRU(_CACHE_MAX)


def _cache_key(synset_id: str, target_lang: str) -> str:
    h = hashlib.sha256()
    h.update(synset_id.encode())
    h.update(b"|")
    h.update(target_lang.upper().encode())
    return h.hexdigest()


def fetch_synset(
    synset_id: str,
    *,
    api_key: str,
    target_lang: str = "EN",
    timeout: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    """GET getSynset; returns parsed JSON object or None."""
    sid = (synset_id or "").strip()
    key = (api_key or "").strip()
    if not sid or not key:
        return None
    tl = (target_lang or "EN").strip().upper() or "EN"
    ck = _cache_key(sid, tl)
    hit = _synset_cache.get(ck)
    if hit is not None:
        return hit

    params = {"id": sid, "key": key, "targetLang": tl}
    to = timeout if timeout is not None else _DEFAULT_TIMEOUT
    headers = {"User-Agent": "MemoPipeline/1.0 (BabelNet client)"}

    try:
        with httpx.Client(timeout=to, follow_redirects=True) as client:
            r = client.get(BABELNET_GET_SYNSET, params=params, headers=headers)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        logger.debug("babelnet_client: getSynset failed for %s: %s", sid, e)
        return None

    if not isinstance(data, dict):
        return None
    _synset_cache.set(ck, data)
    return data


def synset_to_resource_bundle(synset_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Extract linked KB pointers from a getSynset JSON body."""
    wikidata: List[str] = []
    wordnet: List[str] = []
    wikipedia_en: List[str] = []
    wiki_other: List[Tuple[str, str]] = []
    seen_wd: set = set()
    seen_wn: set = set()

    senses = synset_payload.get("senses")
    if not isinstance(senses, list):
        senses = []

    for s in senses:
        if not isinstance(s, dict):
            continue
        props = s.get("properties")
        if not isinstance(props, dict):
            continue
        src = str(props.get("source") or "").strip().upper()
        sk = str(props.get("senseKey") or "").strip()
        lang = str(props.get("language") or "").strip().upper()
        if not sk:
            continue
        if src in ("WIKIDATA", "WIKIDATA_ALIAS") and sk.startswith("Q") and sk[1:].isdigit():
            if sk not in seen_wd:
                seen_wd.add(sk)
                wikidata.append(sk)
        elif src == "WN" or sk.startswith("wn:"):
            if sk not in seen_wn:
                seen_wn.add(sk)
                wordnet.append(sk)
        elif src == "WIKI":
            if lang == "EN":
                wikipedia_en.append(sk)
            else:
                wiki_other.append((lang, sk))

    # Gloss: first English simpleLemma
    gloss = ""
    for s in senses:
        if not isinstance(s, dict):
            continue
        props = s.get("properties")
        if not isinstance(props, dict):
            continue
        if str(props.get("language") or "").upper() != "EN":
            continue
        gloss = str(props.get("simpleLemma") or props.get("fullLemma") or "").strip()
        if gloss:
            break

    return {
        "wikidata_qids": wikidata,
        "wordnet_ids": wordnet,
        "wikipedia_en_keys": wikipedia_en,
        "wiki_other": wiki_other,
        "gloss": gloss,
    }


def enrich_babel_synset(
    babel_synset_id: str,
    *,
    api_key: str,
    target_lang: str = "EN",
) -> Dict[str, Any]:
    """Return resource bundle for ``bn:…`` id; empty lists if API fails."""
    sid = (babel_synset_id or "").strip()
    if not sid.startswith("bn:"):
        return synset_to_resource_bundle({})
    raw = fetch_synset(sid, api_key=api_key, target_lang=target_lang)
    if not raw:
        return synset_to_resource_bundle({})
    return synset_to_resource_bundle(raw)


def get_senses(
    lemma: str,
    *,
    api_key: str,
    lang: str = "EN",
    pos: Optional[str] = None,
    timeout: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """GET getSenses for a lemma; returns list of sense objects (each has synsetID).

    Parameters
    ----------
    lemma:
        The canonical label to look up (e.g. "Computer programming", "Victoria, London").
    pos:
        Optional POS filter: "NOUN", "VERB", "ADJ", "ADV". When given, only senses
        with matching POS are returned.
    """
    lm = (lemma or "").strip()
    key = (api_key or "").strip()
    if not lm or not key:
        return []
    tl = (lang or "EN").strip().upper() or "EN"
    to = timeout if timeout is not None else _DEFAULT_TIMEOUT

    params: Dict[str, str] = {"lemma": lm, "searchLang": tl, "key": key}
    headers = {"User-Agent": "MemoPipeline/1.0 (BabelNet client)"}

    try:
        with httpx.Client(timeout=to, follow_redirects=True) as client:
            r = client.get(BABELNET_GET_SENSES, params=params, headers=headers)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        logger.debug("babelnet_client: getSenses failed for %r: %s", lm, e)
        return []

    if not isinstance(data, list):
        return []

    if not pos:
        return data

    pos_upper = pos.strip().upper()
    filtered = [
        s for s in data
        if isinstance(s, dict)
        and str(
            (s.get("synsetID") or {}).get("pos")
            or (s.get("properties") or {}).get("synsetID", {}).get("pos")
            or ""
        ).upper() == pos_upper
    ]
    return filtered if filtered else data  # fall back to all if filter yields nothing


def _synset_id_from_sense(sense: Dict[str, Any]) -> str:
    """Extract bn:XXXXXXXX from a getSenses sense object (handles both response shapes)."""
    # Shape 1: {"synsetID": {"id": "bn:00031883n", ...}, ...}
    sid = sense.get("synsetID")
    if isinstance(sid, dict):
        v = str(sid.get("id") or "").strip()
        if v.startswith("bn:"):
            return v
    # Shape 2: {"properties": {"synsetID": {"id": "bn:...", ...}, ...}}
    props = sense.get("properties")
    if isinstance(props, dict):
        sid2 = props.get("synsetID")
        if isinstance(sid2, dict):
            v = str(sid2.get("id") or "").strip()
            if v.startswith("bn:"):
                return v
        # Some versions store it flat
        v = str(props.get("synsetID") or "").strip()
        if v.startswith("bn:"):
            return v
    return ""


def _dbpedia_url_from_bundle(bundle: Dict[str, Any]) -> str:
    """Construct a DBpedia URL from the Wikipedia EN key in a resource bundle."""
    keys = bundle.get("wikipedia_en_keys") or []
    if not keys:
        return ""
    page = str(keys[0]).strip()
    if not page:
        return ""
    return f"https://dbpedia.org/resource/{page.replace(' ', '_')}"


def _babelnet_rdf_url_from_synset_id(synset_id: str) -> str:
    """Construct the BabelNet RDF page URL from a bn: synset ID."""
    sid = (synset_id or "").strip()
    if not sid.startswith("bn:"):
        return ""
    return f"https://babelnet.io/rdf/page/{sid}"


def lookup_by_label(
    canonical_label: str,
    *,
    api_key: str,
    lang: str = "EN",
    pos: str = "NOUN",
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """Resolve a canonical label to a BabelNet resource bundle.

    Steps:
    1. getSenses(canonical_label) → list of synsets
    2. Pick the first synset (BabelNet ranks by relevance; NOUN preference applied)
    3. enrich_babel_synset → Wikidata QIDs, WordNet IDs, Wikipedia keys, gloss

    Returns the resource bundle from ``synset_to_resource_bundle`` (empty lists on failure),
    extended with ``synset_id``, ``babelnet_rdf_url``, and ``dbpedia_url``.
    """
    empty: Dict[str, Any] = {
        "synset_id": "",
        "wikidata_qids": [],
        "wordnet_ids": [],
        "wikipedia_en_keys": [],
        "wiki_other": [],
        "gloss": "",
        "babelnet_rdf_url": "",
        "dbpedia_url": "",
    }
    lm = (canonical_label or "").strip()
    key = (api_key or "").strip()
    if not lm or not key:
        return empty

    senses = get_senses(lm, api_key=key, lang=lang, pos=pos, timeout=timeout)
    if not senses:
        return empty

    synset_id = ""
    for sense in senses:
        sid = _synset_id_from_sense(sense)
        if sid:
            synset_id = sid
            break

    if not synset_id:
        return empty

    bundle = enrich_babel_synset(synset_id, api_key=key, target_lang=lang)
    bundle["synset_id"] = synset_id
    bundle["babelnet_rdf_url"] = _babelnet_rdf_url_from_synset_id(synset_id)
    bundle["dbpedia_url"] = _dbpedia_url_from_bundle(bundle)
    return bundle


def babelfy_ann_sidecar(ann: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Subset of a Babelfy annotation object worth persisting (URLs, scores, source)."""
    if not isinstance(ann, dict):
        return {}
    out: Dict[str, Any] = {}
    for k in ("BabelNetURL", "DBpediaURL", "source"):
        v = ann.get(k)
        if v is not None and str(v).strip():
            out[k] = str(v).strip()
    for k in ("score", "coherenceScore", "globalScore"):
        v = ann.get(k)
        if isinstance(v, bool):
            out[k] = v
        elif isinstance(v, (int, float)):
            out[k] = float(v)
    return out


def bundle_to_sources_json(
    bundle: Dict[str, Any],
    *,
    babelfy_ann: Optional[Dict[str, Any]] = None,
) -> str:
    """Compact JSON for Neo4j: KB pointers, gloss, optional Babelfy annotation sidecar."""
    wiki_other = bundle.get("wiki_other") or []
    wo_json: List[Any] = []
    if isinstance(wiki_other, list):
        for item in wiki_other:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                wo_json.append([str(item[0]), str(item[1])])
            elif isinstance(item, dict):
                wo_json.append(
                    {
                        "lang": str(item.get("lang") or ""),
                        "key": str(item.get("key") or ""),
                    }
                )
    payload: Dict[str, Any] = {
        "wikidata": bundle.get("wikidata_qids") or [],
        "wordnet": bundle.get("wordnet_ids") or [],
        "wikipedia_en": bundle.get("wikipedia_en_keys") or [],
        "wiki_other": wo_json,
        "gloss": str(bundle.get("gloss") or "").strip(),
    }
    bf = babelfy_ann_sidecar(babelfy_ann)
    if bf:
        payload["babelfy"] = bf
    try:
        return json.dumps(payload, ensure_ascii=False)
    except (TypeError, ValueError):
        return "{}"
