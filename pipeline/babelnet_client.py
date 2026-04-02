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
