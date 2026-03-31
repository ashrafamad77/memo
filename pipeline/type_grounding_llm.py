"""Batch LLM grounding: one call per journal entry — Wikidata candidates + AAT for all E55 types."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

_DEFAULT_LOW_ROW: Dict[str, Any] = {
    "confidence": "low",
    "wikidata_candidates": [],
    "aat_id": "",
    "aat_label": "",
    "aat_confidence": "low",
    "description": "",
}

_MEMO_LLM_GROUND = os.getenv("MEMO_TYPE_LLM_GROUNDING", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)

_SYSTEM = """You ground journal taxonomy labels to public vocabulary for a CIDOC CRM graph.

You receive:
1) The full journal entry (English and/or other languages).
2) A JSON array "types" — each object has:
   - name: CamelCase label from the model (e.g. Stay, Libraryfacility, Deepwork)
   - context_category: one of place, activity, person, object, concept, organization, event, transfer, state, other
   - host_label: CIDOC class of the node carrying the type (e.g. E7_Activity, E53_Place, E55_Type)

3) Optional "wsd" — word-sense rows with mention, ner_type, disambiguation_sense, context_keywords, negative_keywords.

For EVERY item in "types", IN THE SAME ORDER, output one object with:

- name: exact same string as input
- confidence: "high" | "medium" | "low" — how sure you are about grounding this label in THIS journal
  - Use "low" if the label is a generic verb/word with no clear journal support, or you would be guessing.
- wikidata_candidates: array of 0–3 objects, each:
  {"qid": "Q…", "label": "English label", "description": "short Wikidata-style gloss (you may paraphrase the usual WD description)"}
  Prefer abstract types/classes useful as E55 vocabulary, NOT specific named instances (no particular cities, people, dated events).
  Use [] if no good Wikidata class fits or confidence is low.
  QIDs must be real items you believe exist; never invent QIDs.
- aat: either null or {"aat_id": "300…", "label": "English", "confidence": "high"|"medium"|"low"}
  Use AAT when Wikidata is weak or the concept is better as a Getty term. Use null if none fits.
  aat_id: digits only. Do not invent AAT IDs.

Disambiguation:
- Words like Stay, Run, Work collide with songs/films on Wikidata — prefer everyday/institutional senses matching the diary; use [] for wikidata_candidates if only pop-culture hits would apply.
- If the diary does not clearly use a generic verb (stayed, working, …), prefer confidence "low" and empty wikidata_candidates.

Output: a single JSON object {"types": [ ... ]} only — one entry per input type, same order, no extra top-level keys.
"""


def _normalize_qid(raw: str) -> str:
    q = str(raw or "").strip().upper()
    if q in ("", "NULL", "NONE"):
        return ""
    return q if re.match(r"^Q\d+$", q) else ""


def _normalize_grounding_row(row: Dict[str, Any]) -> Dict[str, Any]:
    conf = str(row.get("confidence") or "medium").strip().lower()
    if conf not in ("high", "medium", "low"):
        conf = "medium"

    candidates: List[Dict[str, str]] = []
    wc = row.get("wikidata_candidates")
    if isinstance(wc, list):
        for it in wc[:6]:
            if not isinstance(it, dict):
                continue
            qid = _normalize_qid(str(it.get("qid") or it.get("id") or ""))
            if not qid:
                continue
            candidates.append(
                {
                    "qid": qid,
                    "label": str(it.get("label") or "").strip(),
                    "description": str(it.get("description") or "").strip(),
                }
            )

    q_legacy = _normalize_qid(str(row.get("wikidata_id") or ""))
    legacy_single_wikidata_id = False
    if not candidates and q_legacy:
        legacy_single_wikidata_id = True
        candidates.append(
            {
                "qid": q_legacy,
                "label": str(row.get("wikidata_label") or "").strip(),
                "description": str(row.get("description") or "").strip(),
            }
        )

    aat_id = ""
    aat_label = ""
    aat_conf = "low"
    aat_obj = row.get("aat")
    if isinstance(aat_obj, dict):
        aat_id = str(aat_obj.get("aat_id") or "").strip()
        aat_label = str(aat_obj.get("label") or "").strip()
        ac = str(aat_obj.get("confidence") or "medium").strip().lower()
        if ac in ("high", "medium", "low"):
            aat_conf = ac
    if not aat_id:
        aat_id = str(row.get("aat_id") or "").strip()
        aat_label = aat_label or str(row.get("aat_label") or "").strip()
        ac = str(row.get("aat_confidence") or "medium").strip().lower()
        if ac in ("high", "medium", "low"):
            aat_conf = ac
    if aat_id and not re.match(r"^\d{5,10}$", aat_id):
        aat_id = ""
        aat_label = ""
        aat_conf = "low"

    desc = str(row.get("description") or "").strip()
    out: Dict[str, Any] = {
        "confidence": conf,
        "wikidata_candidates": candidates[:6],
        "aat_id": aat_id,
        "aat_label": aat_label,
        "aat_confidence": aat_conf,
        "description": desc,
    }
    if legacy_single_wikidata_id:
        # Pre-refactor rows / cached payloads: single wikidata_id without wikidata_candidates — do not
        # treat as medium-confidence modern batch (embedding floor would be misleading).
        out["confidence"] = "low"
        out["_legacy"] = True
    return out


def _parse_grounding_payload(data: Any) -> Dict[str, Dict[str, Any]]:
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return {}
    if not isinstance(data, dict):
        return {}
    rows = data.get("types")
    if not isinstance(rows, list):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        out[name] = _normalize_grounding_row(row)
    return out


def _fill_expected_types(parsed: Dict[str, Dict[str, Any]], expected: Set[str]) -> None:
    """Detect truncated or failed batch JSON; fill missing names and log."""
    if not expected:
        return
    if not parsed:
        logger.warning(
            "type_grounding_llm: unparseable or empty batch JSON (%d type(s) requested)",
            len(expected),
        )
        for nm in sorted(expected):
            parsed[nm] = dict(_DEFAULT_LOW_ROW)
        return
    missing = expected - set(parsed.keys())
    if missing:
        logger.warning(
            "type_grounding_llm: model omitted type(s) (possible max_tokens truncation): %s",
            sorted(missing),
        )
        for nm in missing:
            parsed[nm] = dict(_DEFAULT_LOW_ROW)


class TypeGroundingLLM:
    """One Azure chat call per entry: all types, full journal context."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gpt-4o-mini",
        azure_endpoint: Optional[str] = None,
        api_version: Optional[str] = None,
    ):
        self.api_key = api_key
        self.model = model
        self.azure_endpoint = azure_endpoint
        self.api_version = api_version
        self._client = None

    def _get_client(self):
        if self._client is None:
            import os as _os

            key = self.api_key or _os.environ.get("AZURE_OPENAI_API_KEY")
            if not key:
                raise ValueError("AZURE_OPENAI_API_KEY not configured.")
            if self.azure_endpoint:
                from openai import AzureOpenAI

                self._client = AzureOpenAI(
                    api_key=key,
                    azure_endpoint=self.azure_endpoint.rstrip("/"),
                    api_version=self.api_version or "2024-12-01-preview",
                )
            else:
                from openai import OpenAI

                self._client = OpenAI(api_key=key)
        return self._client

    def run(
        self,
        journal_text: str,
        type_requests: List[Dict[str, str]],
        wsd_profile: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        if not _MEMO_LLM_GROUND or not type_requests:
            return {}
        expected = {
            str(req.get("name") or "").strip()
            for req in type_requests[:40]
            if str(req.get("name") or "").strip()
        }
        t = (journal_text or "").strip()
        if len(t) < 4:
            out_short: Dict[str, Dict[str, Any]] = {}
            _fill_expected_types(out_short, expected)
            return out_short
        try:
            client = self._get_client()
            deployment = (self.model or "gpt-4o-mini").strip()
            payload: Dict[str, Any] = {"types": type_requests[:40]}
            if isinstance(wsd_profile, dict) and wsd_profile.get("entities"):
                payload["wsd"] = wsd_profile["entities"][:30]
            user = (
                f"Journal:\n\"\"\"\n{t[:7500]}\n\"\"\"\n\n"
                f"Ground these types (JSON):\n{json.dumps(payload, ensure_ascii=False)}\n\n"
                'Return only {"types":[...]} as specified.'
            )
            kwargs: Dict[str, Any] = {
                "model": deployment,
                "messages": [
                    {"role": "system", "content": _SYSTEM},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.1,
                "max_tokens": 8192,
            }
            try:
                kwargs["response_format"] = {"type": "json_object"}
            except Exception:
                pass
            res = client.chat.completions.create(**kwargs)
            content = (res.choices[0].message.content or "").strip()
            m = re.search(r"\{[\s\S]*\}", content)
            if m:
                content = m.group(0)
            parsed = _parse_grounding_payload(content)
            _fill_expected_types(parsed, expected)
            return parsed
        except Exception as exc:
            logger.warning("type_grounding_llm: batch LLM failed (%s)", exc)
            out_err: Dict[str, Dict[str, Any]] = {}
            _fill_expected_types(out_err, expected)
            return out_err
