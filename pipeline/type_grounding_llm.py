"""Batch LLM grounding: map E55-style type names → Wikidata Q-id and/or Getty AAT id."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional

_MEMO_LLM_GROUND = os.getenv("MEMO_TYPE_LLM_GROUNDING", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)

_SYSTEM = """You ground journal taxonomy labels to public vocabulary IDs for a CIDOC CRM graph.

You receive:
1) The full journal entry (English and/or other languages).
2) A JSON array "types" — each object has:
   - name: CamelCase label from the model (e.g. Stay, Libraryfacility, Deepwork)
   - context_category: one of place, activity, person, object, concept, organization, event, transfer, state, other
   - host_label: CIDOC class of the node carrying the type (e.g. E7_Activity, E53_Place, E55_Type)

3) Optional "wsd" — prior word-sense rows with mention, ner_type, disambiguation_sense, context_keywords, negative_keywords.

Task: For EVERY item in "types", decide the meaning IN THIS JOURNAL ONLY and assign authority IDs:
- wikidata_id: a Wikidata item Q-id (e.g. Q12345) ONLY if it is the correct *concept* for that label in context. Use null if unsure or no good match.
- aat_id: Getty AAT numeric id (digits only, e.g. 300006824) ONLY if Wikidata is wrong or missing and AAT fits better; else null.
- description: one short English phrase naming what you linked (for the graph UI).

Critical disambiguation:
- Common English words (Stay, Run, Eat, Go, Work, …) often collide with songs, films, or celebrities on Wikidata. Prefer everyday / institutional / geographic senses matching the journal. If the best Wikidata hit would be a chart single, album track, or biographical article unrelated to the diary, use wikidata_id null (and optionally aat_id if a museum vocabulary term fits).
- Do NOT ground generic one-word verbs or ultra-common actions (Stay, Go, Eat, Run, Work, Play, Read, …) to Wikidata or AAT unless the journal text clearly uses that word or a normal inflection (e.g. stayed, eating) as what happened — not merely because an activity node name like MorningStayAtVictoria contains the substring in CamelCase. If the diary does not spell out that sense, return wikidata_id null and aat_id null.
- Do not invent Q-ids. Only use IDs you are confident exist and match the sense.

Output: a single JSON object with key "types" whose value is an array of objects, one per input type IN THE SAME ORDER as input, each shaped as:
{"name":"<exact same name>","wikidata_id":"Q…"|null,"aat_id":"<digits>"|null,"description":"…"}

No markdown, no extra keys at the top level besides "types".
"""


def _parse_grounding_payload(data: Any) -> Dict[str, Dict[str, str]]:
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
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        q = str(row.get("wikidata_id") or row.get("qid") or "").strip().upper()
        if q in ("", "NULL", "NONE"):
            q = ""
        if q and not re.match(r"^Q\d+$", q):
            q = ""
        aid = str(row.get("aat_id") or "").strip()
        if aid and not re.match(r"^\d{5,10}$", aid):
            aid = ""
        desc = str(row.get("description") or "").strip()
        entry: Dict[str, str] = {}
        if q:
            entry["wikidata_id"] = q
        if aid:
            entry["aat_id"] = aid
        if desc:
            entry["description"] = desc
        if entry:
            out[name] = entry
    return out


class TypeGroundingLLM:
    """One Azure chat call per entry to ground all type strings."""

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
    ) -> Dict[str, Dict[str, str]]:
        if not _MEMO_LLM_GROUND or not type_requests:
            return {}
        t = (journal_text or "").strip()
        if len(t) < 4:
            return {}
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
                "max_tokens": 2500,
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
            return _parse_grounding_payload(content)
        except Exception:
            return {}
