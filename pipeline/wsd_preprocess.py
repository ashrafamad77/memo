"""LLM NER + word-sense hints for Wikidata grounding (journal-wide, pre-modeling)."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional

_MEMO_WSD = os.getenv("MEMO_WSD_PREPROCESS", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)

_WSD_SYSTEM = """You are a journal NER + word-sense assistant for a CIDOC CRM knowledge graph.

Task: read the journal entry and list salient entity mentions with:
- mention: surface form (single word or short phrase as written or lemmatized in English when needed)
- ner_type: ONE CIDOC CRM class id such as E53_Place, E7_Activity, E21_Person, E22_Human_Made_Object, E74_Group, E28_Conceptual_Object, E13_Attribute_Assignment (use these exact prefixes).
- disambiguation_sense: one short English sentence committing to ONE meaning (not a list of possibilities).
- context_keywords: 3–8 English keywords that support that sense for overlap with knowledge bases.
- negative_keywords: optional 2–8 English keywords for senses to EXCLUDE (omit key or use [] if none).

Rules:
- Same word in different roles → separate objects (e.g. "library" as building vs unrelated uses).
- Prefer mentions that matter for typing places, activities, people, organizations.
- Output MUST be a single JSON object with key "entities" only. No markdown, no commentary.

Schema:
{"entities":[{"mention":"...","ner_type":"E53_Place","disambiguation_sense":"...","context_keywords":["..."],"negative_keywords":["..."]}]}
"""


def _normalize_entity(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    mention = str(raw.get("mention") or "").strip()
    if not mention:
        return None
    ner = str(raw.get("ner_type") or raw.get("cidoc_type") or "").strip()
    sense = str(raw.get("disambiguation_sense") or "").strip()
    ctx = raw.get("context_keywords")
    neg = raw.get("negative_keywords")
    if not isinstance(ctx, list):
        ctx = []
    if not isinstance(neg, list):
        neg = []
    ctx_s = [str(x).strip() for x in ctx if str(x).strip()]
    neg_s = [str(x).strip() for x in neg if str(x).strip()]
    return {
        "mention": mention,
        "ner_type": ner or "E28_Conceptual_Object",
        "disambiguation_sense": sense,
        "context_keywords": ctx_s[:12],
        "negative_keywords": neg_s[:12],
    }


def parse_wsd_payload(data: Any) -> Dict[str, Any]:
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return {"entities": []}
    if not isinstance(data, dict):
        return {"entities": []}
    raw_list = data.get("entities")
    if not isinstance(raw_list, list):
        return {"entities": []}
    out: List[Dict[str, Any]] = []
    for item in raw_list:
        row = _normalize_entity(item)
        if row:
            out.append(row)
    return {"entities": out}


class WsdPreprocessor:
    """Azure OpenAI JSON object mode — same credentials as the rest of the pipeline."""

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

    def run(self, text: str) -> Dict[str, Any]:
        if not _MEMO_WSD:
            return {"entities": []}
        t = (text or "").strip()
        if len(t) < 8:
            return {"entities": []}
        try:
            client = self._get_client()
            deployment = (self.model or "gpt-4o-mini").strip()
            user = f"Journal entry:\n\"\"\"\n{t[:8000]}\n\"\"\"\n\nReturn only the JSON object."
            kwargs: Dict[str, Any] = {
                "model": deployment,
                "messages": [
                    {"role": "system", "content": _WSD_SYSTEM},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.1,
                "max_tokens": 1200,
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
            return parse_wsd_payload(content)
        except Exception:
            return {"entities": []}
