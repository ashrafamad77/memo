"""Content-addressed cache for journal extraction (prep + WSD + draft graph spec, pre-resolver)."""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Dict, Optional


def journal_text_sha256(text: str) -> str:
    """SHA-256 of stripped UTF-8 text — key for exact-duplicate extraction reuse."""
    raw = (text or "").strip().encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def extraction_input_cache_enabled() -> bool:
    return os.getenv("MEMO_EXTRACTION_INPUT_CACHE", "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )


def unified_extraction_enabled() -> bool:
    return os.getenv("MEMO_UNIFIED_EXTRACTION", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _json_loads(s: str, default: Any) -> Any:
    if not s or not str(s).strip():
        return default
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return default


def pack_cache_payload(
    prep: Dict[str, Any],
    wsd_profile: Dict[str, Any],
    graph_spec: Dict[str, Any],
) -> Dict[str, str]:
    return {
        "prep_json": _json_dumps(prep if isinstance(prep, dict) else {}),
        "wsd_profile_json": _json_dumps(wsd_profile if isinstance(wsd_profile, dict) else {"entities": []}),
        "graph_spec_json": _json_dumps(
            graph_spec if isinstance(graph_spec, dict) else {"nodes": [], "edges": []}
        ),
    }


def unpack_cache_payload(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    prep = _json_loads(str(row.get("prep_json") or ""), {})
    wsd = _json_loads(str(row.get("wsd_profile_json") or ""), {"entities": []})
    spec = _json_loads(str(row.get("graph_spec_json") or ""), {"nodes": [], "edges": []})
    if not isinstance(prep, dict):
        prep = {}
    if not isinstance(wsd, dict):
        wsd = {"entities": []}
    if not isinstance(spec, dict):
        spec = {"nodes": [], "edges": []}
    return {"prep": prep, "wsd_profile": wsd, "graph_spec": spec}
