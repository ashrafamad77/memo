"""Single LLM call: prep decomposition + WSD-style entities + CIDOC graph spec (Phase 2)."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

from .modeling_agent import (
    CIDOC_VOCAB,
    MODELING_PROMPT,
    _prune_redundant_state_e7,
    _sanitize_lazy_e55_types,
)
from .prep_agent import PREP_PROMPT
from .type_vocab import SEED_VOCAB, TYPE_ALIAS_TO_CANONICAL, seed_type_names

logger = logging.getLogger(__name__)

_UNIFIED_WRAPPER = """
You are a unified journal extraction pipeline. Return ONE JSON object only (no markdown).

## Output schema (required top-level keys)
{
  "prep": { ... },
  "wsd_profile": { "entities": [ ... ] },
  "graph_spec": { "nodes": [ ... ], "edges": [ ... ] }
}

### prep
Exactly the structure described in the PREP RULES below (micro_events, event_links, mental_states,
expectations, habits, reflections, entities, normalized_text, confidence).

### wsd_profile.entities
Each item: mention, ner_type (e.g. E53_Place, E21_Person), disambiguation_sense (one short English sentence),
context_keywords (array of 3–8 strings), negative_keywords (array, optional).

### graph_spec
CIDOC nodes and edges as in the MODELING RULES — built **from your prep**. Do not invent facts absent from prep.

## Preferred type vocabulary (reuse exact CamelCase names when semantically appropriate)
Canonical E55 names: {preferred_types}

## Known synonyms → canonical type name (for WSD / typing)
{alias_lines}

## Execution order
1) Read the journal.
2) Fill prep.
3) Fill wsd_profile.entities for salient mentions that matter for Wikidata disambiguation.
4) Build graph_spec from prep following all modeling constraints.

Day bucket (for context only): {day_bucket}
Journal author name (exact): {user_name}
"""


class UnifiedExtractionAgent:
    """One structured JSON response replacing separate Prep + WSD + Modeling LLM calls."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gpt-4o-mini",
        base_url: Optional[str] = None,
        azure_endpoint: Optional[str] = None,
        api_version: Optional[str] = None,
    ):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.azure_endpoint = azure_endpoint
        self.api_version = api_version
        self._client = None

    def _get_client(self):
        if self._client is None:
            key = self.api_key or __import__("os").environ.get("AZURE_OPENAI_API_KEY")
            if not key:
                raise ValueError("AZURE_OPENAI_API_KEY non configuree.")
            if self.azure_endpoint:
                from openai import AzureOpenAI

                self._client = AzureOpenAI(
                    api_key=key,
                    azure_endpoint=self.azure_endpoint.rstrip("/"),
                    api_version=self.api_version or "2024-12-01-preview",
                )
            else:
                from openai import OpenAI

                kwargs: Dict[str, Any] = {"api_key": key}
                if self.base_url:
                    kwargs["base_url"] = self.base_url.rstrip("/") + "/"
                self._client = OpenAI(**kwargs)
        return self._client

    @staticmethod
    def _alias_prompt_block() -> str:
        lines = [
            f"  {alias!r} → {canon!r}"
            for alias, canon in sorted(TYPE_ALIAS_TO_CANONICAL.items())[:60]
        ]
        return "\n".join(lines) if lines else "  (none)"

    @staticmethod
    def _seed_with_qids_block() -> str:
        parts: List[str] = []
        for name in sorted(SEED_VOCAB.keys()):
            wid = SEED_VOCAB[name].get("wikidata_id")
            if wid:
                parts.append(f"{name}→{wid}")
        return ", ".join(parts[:80])

    def run(
        self,
        journal_text: str,
        user_name: str = "",
        existing_types: Optional[List[str]] = None,
        day_bucket: str = "",
    ) -> Optional[Dict[str, Any]]:
        t = (journal_text or "").strip()
        if len(t) < 2:
            return None
        types_str = ", ".join(existing_types) if existing_types else "(aucun)"
        seed_list = seed_type_names()
        cols = 4
        rows = [seed_list[i : i + cols] for i in range(0, len(seed_list), cols)]
        preferred_str = "\n".join("   " + ", ".join(row) for row in rows)
        modeling_body = MODELING_PROMPT.format(
            cidoc_vocab=CIDOC_VOCAB,
            existing_types=types_str,
            user_name=user_name or "utilisateur",
            preferred_types=preferred_str,
        )
        wrapper = _UNIFIED_WRAPPER.format(
            preferred_types=preferred_str + "\nSeeded Wikidata QIDs (when listed): " + self._seed_with_qids_block(),
            alias_lines=self._alias_prompt_block(),
            day_bucket=day_bucket or "(unknown)",
            user_name=user_name or "utilisateur",
        )
        system_content = (
            wrapper
            + "\n\n========== PREP RULES ==========\n"
            + PREP_PROMPT
            + "\n\n========== MODELING RULES ==========\n"
            + modeling_body
        )
        user_content = f"Journal entry:\n\"\"\"\n{t[:12000]}\n\"\"\"\n"

        try:
            client = self._get_client()
            deployment = (self.model or "gpt-4o-mini").strip()
            kwargs: Dict[str, Any] = {
                "model": deployment,
                "messages": [
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": user_content},
                ],
                "temperature": 0.1,
                "max_tokens": min(
                    int(os.getenv("MEMO_UNIFIED_EXTRACTION_MAX_TOKENS", "16384")),
                    32768,
                ),
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
        except Exception as exc:
            logger.warning("unified_extraction_agent: failed (%s)", exc)
            return None

        if not isinstance(data, dict):
            return None
        prep = data.get("prep")
        wsd_profile = data.get("wsd_profile")
        graph_spec = data.get("graph_spec")
        if not isinstance(prep, dict):
            prep = {}
        if not isinstance(wsd_profile, dict):
            wsd_profile = {"entities": []}
        if not isinstance(wsd_profile.get("entities"), list):
            wsd_profile["entities"] = []
        if not isinstance(graph_spec, dict):
            graph_spec = {"nodes": [], "edges": []}
        nodes = graph_spec.get("nodes")
        edges = graph_spec.get("edges")
        if not isinstance(nodes, list):
            nodes = []
        if not isinstance(edges, list):
            edges = []
        _sanitize_lazy_e55_types(nodes, edges)
        _prune_redundant_state_e7(nodes, edges)
        graph_spec = {"nodes": nodes, "edges": edges}
        return {"prep": prep, "wsd_profile": wsd_profile, "graph_spec": graph_spec}
