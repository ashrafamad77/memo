"""Babelfy **HTTP API 1.0** client (``GET /v1/disambiguate`` → JSON list).

Official contract (see Babelfy “HTTP API” docs): use **GET**, send
``Accept-Encoding: gzip``, and pass at least ``text``, ``lang``, and ``key``.
Optional query parameters—``annType``, ``annRes``, ``match``, ``th``, ``MCS``,
``dens``, ``cands``, ``posTag``, ``extAIDA``—are forwarded when set via
``MEMO_BABELFY_*`` env vars (see ``config``).

This app always sends ``annType`` (``NAMED_ENTITIES`` vs ``CONCEPTS``).
``annRes`` and other knobs are omitted when unset so Babelfy defaults apply.

Responses are cached in-process (LRU) keyed by all request parameters that
affect the response.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from collections import OrderedDict
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

BABELFY_DISAMBIGUATE_URL = "https://api.babelfy.org/v1/disambiguate"
_DEFAULT_TIMEOUT = float(os.getenv("MEMO_BABELFY_TIMEOUT_SEC", "60"))
_CACHE_MAX = max(32, min(int(os.getenv("MEMO_BABELFY_CACHE_MAX", "256")), 4096))

_OMIT = frozenset({"", "DEFAULT", "AUTO", "API", "OMIT", "NONE"})


class _LRU:
    def __init__(self, maxsize: int) -> None:
        self._max = maxsize
        self._d: "OrderedDict[str, List[Dict[str, Any]]]" = OrderedDict()

    def get(self, key: str) -> Optional[List[Dict[str, Any]]]:
        if key not in self._d:
            return None
        self._d.move_to_end(key)
        return self._d[key]

    def set(self, key: str, value: List[Dict[str, Any]]) -> None:
        self._d[key] = value
        self._d.move_to_end(key)
        while len(self._d) > self._max:
            self._d.popitem(last=False)


_cache = _LRU(_CACHE_MAX)


def _redact_query_key(url: str) -> str:
    return re.sub(r"([&?])key=[^&]+", r"\1key=***", str(url or ""), flags=re.I)


def _resolve_ann_res_param(explicit: str) -> Optional[str]:
    """Value for Babelfy ``annRes`` query param, or ``None`` to omit (API default)."""
    e = (explicit or "").strip().upper()
    if e and e not in _OMIT:
        return e
    try:
        from config import MEMO_BABELFY_ANN_RES

        raw = (MEMO_BABELFY_ANN_RES or "").strip()
    except ImportError:
        raw = os.getenv("MEMO_BABELFY_ANN_RES", "").strip()
    if not raw:
        return None
    u = raw.upper()
    if u in _OMIT:
        return None
    return u


def _resolve_match_param(explicit: str) -> Optional[str]:
    """``match`` query param or ``None`` to omit."""
    e = (explicit or "").strip().upper()
    if e and e not in _OMIT:
        return e
    try:
        from config import MEMO_BABELFY_MATCH

        raw = (MEMO_BABELFY_MATCH or "").strip()
    except ImportError:
        raw = os.getenv("MEMO_BABELFY_MATCH", "PARTIAL_MATCHING").strip()
    if not raw or raw.upper() in _OMIT:
        return None
    return raw.upper()


def _bool_query_value(raw: str) -> Optional[str]:
    """Normalize true/false env strings for Babelfy boolean-like params."""
    s = (raw or "").strip().lower()
    if not s or s in _OMIT:
        return None
    if s in ("1", "true", "yes", "on"):
        return "true"
    if s in ("0", "false", "no", "off"):
        return "false"
    return None


def _extra_babelfy_params() -> Dict[str, Any]:
    """Optional Babelfy parameters from config / env (omit keys with no value)."""
    try:
        from config import (
            MEMO_BABELFY_CANDS,
            MEMO_BABELFY_DENS,
            MEMO_BABELFY_EXT_AIDA,
            MEMO_BABELFY_MCS,
            MEMO_BABELFY_POS_TAG,
            MEMO_BABELFY_TH,
        )
    except ImportError:
        MEMO_BABELFY_TH = os.getenv("MEMO_BABELFY_TH", "")
        MEMO_BABELFY_MCS = os.getenv("MEMO_BABELFY_MCS", "")
        MEMO_BABELFY_DENS = os.getenv("MEMO_BABELFY_DENS", "")
        MEMO_BABELFY_CANDS = os.getenv("MEMO_BABELFY_CANDS", "")
        MEMO_BABELFY_POS_TAG = os.getenv("MEMO_BABELFY_POS_TAG", "")
        MEMO_BABELFY_EXT_AIDA = os.getenv("MEMO_BABELFY_EXT_AIDA", "")

    out: Dict[str, Any] = {}
    th = str(MEMO_BABELFY_TH or "").strip()
    if th:
        try:
            out["th"] = float(th)
        except ValueError:
            logger.debug("babelfy_client: ignoring invalid MEMO_BABELFY_TH=%r", th)

    mcs = str(MEMO_BABELFY_MCS or "").strip()
    if mcs:
        b = _bool_query_value(mcs)
        if b is not None:
            out["MCS"] = b
        else:
            out["MCS"] = mcs

    dens = _bool_query_value(str(MEMO_BABELFY_DENS or ""))
    if dens is not None:
        out["dens"] = dens

    cands = str(MEMO_BABELFY_CANDS or "").strip()
    if cands:
        out["cands"] = cands

    pos_tag = str(MEMO_BABELFY_POS_TAG or "").strip()
    if pos_tag:
        out["posTag"] = pos_tag

    ext = _bool_query_value(str(MEMO_BABELFY_EXT_AIDA or ""))
    if ext is not None:
        out["extAIDA"] = ext

    return out


def _cache_key(
    text: str,
    lang: str,
    ann_type: str,
    ann_res_token: str,
    match: str,
) -> str:
    """Stable cache key when no ``MEMO_BABELFY_*`` extras are set (tests / diagnostics)."""
    return _cache_signature(text, lang, ann_type, ann_res_token, match, {})


def _cache_signature(
    text: str,
    lang: str,
    ann_type: str,
    ann_res_token: str,
    match_token: str,
    extras: Dict[str, Any],
) -> str:
    h = hashlib.sha256()
    h.update(lang.upper().encode())
    h.update(b"|")
    h.update(ann_type.encode())
    h.update(b"|")
    h.update(ann_res_token.encode())
    h.update(b"|")
    h.update(match_token.encode())
    h.update(b"|")
    try:
        h.update(
            json.dumps(extras, sort_keys=True, default=str).encode("utf-8")
        )
    except (TypeError, ValueError):
        h.update(b"{}")
    h.update(b"|")
    h.update(text.encode("utf-8", errors="replace"))
    return h.hexdigest()


def disambiguate(
    text: str,
    *,
    api_key: str,
    lang: str = "EN",
    ann_type: str = "NAMED_ENTITIES",
    ann_res: str = "",
    match: str = "",
    timeout: Optional[float] = None,
    stats: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Call Babelfy disambiguate; returns a JSON array of annotation objects (``babelSynsetID``, etc.).

    ``match``: pass a Babelfy value (e.g. ``PARTIAL_MATCHING``) to override config for this call;
    empty string uses ``MEMO_BABELFY_MATCH`` (or omits if that is empty).
    Other optional API parameters come from ``MEMO_BABELFY_TH``, ``MEMO_BABELFY_MCS``, etc.
    """
    t = text or ""
    key = (api_key or "").strip()
    if not key:
        logger.debug("babelfy_client: no API key, skipping disambiguate")
        return []

    ar = _resolve_ann_res_param(ann_res)
    ar_cache = ar if ar is not None else "__omit__"
    match_v = _resolve_match_param(match)
    match_cache = match_v if match_v is not None else "__omit__"
    extras = _extra_babelfy_params()
    ck = _cache_signature(t, lang, ann_type, ar_cache, match_cache, extras)
    hit = _cache.get(ck)
    if hit is not None:
        if stats is not None:
            stats["babelfy_cache_hit"] = True
        return hit
    if stats is not None:
        stats["babelfy_cache_hit"] = False

    params: Dict[str, Any] = {
        "text": t,
        "lang": lang,
        "key": key,
        "annType": ann_type,
    }
    if ar is not None:
        params["annRes"] = ar
    if match_v is not None:
        params["match"] = match_v
    params.update(extras)

    to = timeout if timeout is not None else _DEFAULT_TIMEOUT
    headers = {"Accept-Encoding": "gzip", "User-Agent": "MemoPipeline/1.0 (Babelfy client)"}

    try:
        with httpx.Client(timeout=to, follow_redirects=True) as client:
            r = client.get(BABELFY_DISAMBIGUATE_URL, params=params, headers=headers)
            r.raise_for_status()
            data = r.json()
    except httpx.HTTPStatusError as e:
        logger.warning(
            "babelfy_client: disambiguate HTTP %s for %s (annType=%s annRes=%s)",
            e.response.status_code,
            _redact_query_key(str(e.request.url)),
            ann_type,
            ar if ar is not None else "(omitted)",
        )
        return []
    except Exception as e:
        msg = str(e)
        if "key=" in msg:
            msg = re.sub(r"key=[^&\s'\"<>]+", "key=***", msg)
        logger.warning("babelfy_client: disambiguate failed: %s", msg)
        return []

    if not isinstance(data, list):
        logger.warning("babelfy_client: unexpected response type %s", type(data))
        return []

    _cache.set(ck, data)
    return data
