"""LLM paraphrase: generic verb-like E55 labels → formal search terms for AAT / Wikidata."""

from __future__ import annotations

import json
import os
import re
from typing import List

_MEMO_VERB_CONCEPT_LLM = os.getenv("MEMO_VERB_CONCEPT_LLM", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)

_SYSTEM = """You suggest museum- and ontology-friendly search phrases for CIDOC CRM E55_Type labels.

The diary text may NOT literally use the verb (the label may be CamelCase only, e.g. MorningStayAtHotel).
Return English noun phrases that curators would use in Getty AAT or Wikidata: sojourn, occupancy, temporary residence, presence at a place, etc.

Rules:
- 4–10 short phrases (1–4 words each), most concrete first.
- No song titles, film titles, album names, or celebrity names.
- Do not echo ambiguous pop-culture homographs.

Output a single JSON object: {"search_terms": ["...", ...]} — no markdown, no other keys."""


def llm_paraphrase_verb_to_concepts(
    type_name: str,
    journal_text: str,
    lemma: str,
) -> List[str]:
    if not _MEMO_VERB_CONCEPT_LLM:
        return []
    tname = (type_name or "").strip()
    j = (journal_text or "").strip()
    if not tname:
        return []
    try:
        from config import (
            AZURE_OPENAI_API_KEY,
            AZURE_OPENAI_DEPLOYMENT,
            AZURE_OPENAI_ENDPOINT,
            AZURE_OPENAI_API_VERSION,
        )
    except ImportError:
        return []
    if not (AZURE_OPENAI_API_KEY or "").strip() or not (AZURE_OPENAI_ENDPOINT or "").strip():
        return []
    deployment = (AZURE_OPENAI_DEPLOYMENT or "gpt-4o-mini").strip()
    try:
        from openai import AzureOpenAI

        client = AzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT.rstrip("/"),
            api_version=AZURE_OPENAI_API_VERSION or "2024-12-01-preview",
        )
    except Exception:
        return []

    try:
        user = (
            f"Lemma (lowercase hint): {lemma}\n"
            f"Type label: {tname}\n\n"
            f'Journal excerpt:\n"""\n{j[:6000]}\n"""\n\n'
            'Return only {"search_terms":[...]} as specified.'
        )
        kwargs = {
            "model": deployment,
            "messages": [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": user},
            ],
            "temperature": 0.15,
            "max_tokens": 400,
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
        data = json.loads(content)
        raw_list = data.get("search_terms")
        if not isinstance(raw_list, list):
            return []
        out: List[str] = []
        seen = set()
        for x in raw_list:
            s = str(x).strip()
            if len(s) < 2 or len(s) > 80:
                continue
            k = s.casefold()
            if k in seen:
                continue
            seen.add(k)
            out.append(s)
        return out[:12]
    except Exception:
        return []


__all__ = ["llm_paraphrase_verb_to_concepts"]
