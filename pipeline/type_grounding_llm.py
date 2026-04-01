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

# Max Wikidata options shown per entity in the human-review inbox (LLM + search fallback).
_ENTITY_LINK_CONF_ORDER = {"high": 0, "medium": 1, "low": 2}


def _entity_link_max_candidates() -> int:
    try:
        n = int(os.getenv("MEMO_ENTITY_LINK_MAX_CANDIDATES", "3"))
    except ValueError:
        n = 3
    return max(1, min(n, 10))


def _cap_entity_link_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep at most N options: higher confidence first, then original order (LLM / search rank)."""
    if not candidates:
        return []
    cap = _entity_link_max_candidates()
    indexed = list(enumerate(candidates))
    indexed.sort(
        key=lambda ic: (
            _ENTITY_LINK_CONF_ORDER.get(str(ic[1].get("confidence") or "medium").lower(), 9),
            ic[0],
        )
    )
    return [ic[1] for ic in indexed[:cap]]

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


def _wikidata_coheres_with_mention(mention: str, label: str, description: str) -> bool:
    """Reject LLM hallucinations like Q183 (Germany) paired with the mention "Victoria".

    Requires substantive tokens from the journal mention to appear in Wikidata's English
    label or description (substring for length ≥3; word-boundary match for length 2).
    """
    blob = f"{label} {description}".strip().lower()
    if not blob:
        return False
    m = (mention or "").strip().lower()
    if not m:
        return False
    if m in blob:
        return True
    tokens = [t for t in re.split(r"[^\w]+", m) if t]
    significant = [t for t in tokens if len(t) >= 3]
    if significant:
        return all(t in blob for t in significant)
    for t in tokens:
        if len(t) >= 2 and re.search(r"(?<!\w)" + re.escape(t) + r"(?!\w)", blob):
            return True
    return False


def _canonicalize_entity_link_candidates(
    entity_name: str,
    candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Replace LLM labels with Wikidata API truth; drop QIDs that do not match the mention."""
    if not candidates:
        return []
    try:
        from .type_grounding_embed import wikidata_fetch_labels_descriptions
    except ImportError:
        return candidates

    qids = list({c["wikidata_id"] for c in candidates})
    fetched = wikidata_fetch_labels_descriptions(qids)
    out: List[Dict[str, Any]] = []
    for c in candidates:
        qid = c["wikidata_id"]
        pair = fetched.get(qid)
        if not pair:
            logger.info(
                "type_grounding_llm: dropped QID %s for %r (not returned by Wikidata)",
                qid,
                entity_name,
            )
            continue
        lab, desc = pair[0] or "", pair[1] or ""
        if not (lab or desc):
            continue
        if not _wikidata_coheres_with_mention(entity_name, lab, desc):
            logger.info(
                "type_grounding_llm: dropped QID %s for mention %r (Wikidata label %r)",
                qid,
                entity_name,
                lab,
            )
            continue
        out.append({
            "wikidata_id": qid,
            "label": lab,
            "description": desc,
            "confidence": c["confidence"],
        })
    return out


def _resolve_entity_link_candidates(
    entity_name: str,
    llm_candidates: Any,
) -> List[Dict[str, Any]]:
    """Parse LLM candidates, reconcile with Wikidata; if none survive, use wbsearchentities."""
    raw: List[Any] = []
    if isinstance(llm_candidates, list):
        raw = llm_candidates
    valid_candidates: List[Dict[str, Any]] = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        qid = _normalize_qid(str(c.get("wikidata_id") or ""))
        if not qid:
            continue
        conf = str(c.get("confidence") or "medium").strip().lower()
        if conf not in ("high", "medium"):
            continue
        valid_candidates.append({
            "wikidata_id": qid,
            "label": str(c.get("label") or "").strip(),
            "description": str(c.get("description") or "").strip(),
            "confidence": conf,
        })
    if valid_candidates:
        canon = _canonicalize_entity_link_candidates(entity_name, valid_candidates)
        if canon:
            return _cap_entity_link_candidates(canon)
    try:
        from .type_grounding_embed import wikidata_entity_search_candidates
        # Fetch extra rows so coherence + cap still leaves up to N good hits
        search_raw = wikidata_entity_search_candidates(
            entity_name, limit=max(12, _entity_link_max_candidates() * 4)
        )
    except Exception:
        search_raw = []
    if not search_raw:
        return []
    resolved = _canonicalize_entity_link_candidates(entity_name, search_raw) or []
    return _cap_entity_link_candidates(resolved)


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


_ENTITY_SYSTEM = """You are an entity disambiguation system for a personal journal knowledge graph.

You receive:
1) A journal entry text.
2) The journal author's profile — IMPORTANT: use the author's current city and country as the primary geographic anchor for all place disambiguation.
3) A list of named entities (places, persons, organizations) to link to Wikidata.

For each entity return up to 3 ranked candidates (most likely first). The review UI only shows three options.

Rules:
- Places: find the SPECIFIC real-world location, NOT the abstract type.
  Weight the author's current city/country heavily — a place name likely refers to somewhere near the author.
  E.g. if author is in Paris: "Victoria" is more likely a Paris landmark or nearby than Victoria, Australia.
- Transit / stations: if the journal mentions trains, platforms, commuting, or "station", prefer a **railway station
  or transit stop** Wikidata item (e.g. London Victoria station) over a city, state, or country named Victoria.
  Do not substitute an unrelated famous QID (e.g. Q183 is Germany, not any place called Victoria).
- Persons: only assign QIDs for well-known public figures you are very confident about. Private individuals → empty candidates.
- Never invent QIDs. Every QID must be the Wikidata item you mean — wrong ID + plausible label will be rejected server-side.
- Confidence per candidate: "high" = very sure | "medium" = plausible | "low" = guessing (omit low).

Output:
{"entities": [
  {
    "name": "...",
    "candidates": [
      {"wikidata_id": "Q...", "label": "English label", "description": "short description", "confidence": "high"|"medium"}
    ]
  }
]}
Return only the JSON object, no markdown. Empty candidates list if nothing fits.
"""


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

    def run_entity_linking(
        self,
        journal_text: str,
        entity_requests: List[Dict[str, str]],
        user_profile: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Link E53_Place / E21_Person / E74_Group instances to Wikidata QIDs.

        Returns:
          {
            "confirmed": {entity_name: {wikidata_id, label, confidence, description}},
            "pending":   {entity_name: [{"wikidata_id": ..., "label": ..., "description": ..., "confidence": ...}, ...]},
          }

        "confirmed" = first high-confidence candidate (auto-accept).
        "pending"   = entities where only medium-confidence candidates exist (needs human review).
        QIDs are suggestions only — caller must validate with wikidata_qid_exists().
        Candidates are reconciled with Wikidata (English label/description) and filtered so the
        QID must match the mention text (stops wrong pairs like Q183 + "Victoria").
        """
        empty: Dict[str, Any] = {"confirmed": {}, "pending": {}}
        if not entity_requests:
            return empty
        t = (journal_text or "").strip()
        if len(t) < 4:
            return empty
        try:
            profile_lines = ""
            if isinstance(user_profile, dict) and user_profile:
                parts = [f"{k}: {v}" for k, v in user_profile.items() if v]
                profile_lines = "\n".join(parts)
            entities_json = json.dumps(
                [{"name": r["name"], "type": r.get("cidoc_label", "")} for r in entity_requests],
                ensure_ascii=False,
            )
            user_msg = (
                f"Journal entry:\n\"\"\"\n{t[:4000]}\n\"\"\"\n\n"
                f"Author profile:\n{profile_lines or '(unknown)'}\n\n"
                f"Entities to link:\n{entities_json}\n\n"
                'Return only {"entities": [...]} as specified.'
            )
            res = self._get_client().chat.completions.create(
                model=(self.model or "gpt-4o-mini").strip(),
                messages=[
                    {"role": "system", "content": _ENTITY_SYSTEM},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.0,
                max_tokens=1024,
                response_format={"type": "json_object"},
            )
            content = (res.choices[0].message.content or "").strip()
            m = re.search(r"\{[\s\S]*\}", content)
            if m:
                content = m.group(0)
            data = json.loads(content)
            confirmed: Dict[str, Dict[str, Any]] = {}
            pending: Dict[str, List[Dict[str, Any]]] = {}
            names_from_llm: Set[str] = set()
            for row in data.get("entities", []):
                if not isinstance(row, dict):
                    continue
                name = str(row.get("name") or "").strip()
                if not name:
                    continue
                names_from_llm.add(name)
                valid_candidates = _resolve_entity_link_candidates(name, row.get("candidates"))
                if not valid_candidates:
                    continue
                # Auto-accept first high-confidence candidate
                high = next((c for c in valid_candidates if c["confidence"] == "high"), None)
                if high:
                    confirmed[name] = high
                else:
                    # All medium (typical for wbsearch fallback) — queue for human review
                    pending[name] = valid_candidates
            # Entities the model omitted entirely: still offer search-backed candidates
            for req in entity_requests:
                nm = str(req.get("name") or "").strip()
                if not nm or nm in confirmed or nm in pending:
                    continue
                if nm in names_from_llm:
                    continue
                extra = _resolve_entity_link_candidates(nm, [])
                if extra:
                    pending[nm] = extra
            return {"confirmed": confirmed, "pending": pending}
        except Exception as exc:
            logger.warning("type_grounding_llm: entity linking failed (%s)", exc)
            return empty
