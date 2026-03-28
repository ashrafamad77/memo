"""LLM-backed semantic proposal reasoning (Azure OpenAI / OpenAI)."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional

from config import (
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_API_VERSION,
    AZURE_OPENAI_DEPLOYMENT,
    AZURE_OPENAI_ENDPOINT,
)

_log = logging.getLogger("uvicorn.error")

SEMANTIC_PROPOSER_SYSTEM = """You are a careful personal-memory assistant. You receive:
1) LIVE CONTEXT — local time, profile city, and current / short-range weather (JSON).
2) GRAPH FRAGMENTS — past journal-derived facts from the user's knowledge graph: activities (E7), feelings (E13), and E55_Type nodes that classify meaning (e.g. Joy, Hunger, SocialActivity).

Your job: compare LIVE CONTEXT to past patterns in the fragments. If current time, weather, or situation resembles a past sequence (e.g. arrival at office → hunger → cheese shop; sunny day → lunch with Zeena → Joy), propose ONE or more concrete, actionable suggestions grounded in those memories.

Rules:
- Only propose what the fragments plausibly support. If evidence is thin, return fewer proposals or an empty list.
- Never invent people, places, or shops not present in the fragments (paraphrasing names is OK).
- Prefer diverse kinds: see_person, visit_place, do_activity, buy_food, or other short snake_case kinds that fit.
- Each proposal MUST cite traceability: list graph_evidence entries that are copied from fragment ids you actually used (activity_key, assignment_ref, type_name, place_name, entry_id).
- anchor_date must be today's date from live_context.local_date (YYYY-MM-DD) unless you intentionally anchor to a specific forecast day; if using a forecast day, use that date string from weather.daily[].date.
- priority is a float from 0.35 to 0.95 (higher = stronger match to history + context).
- people: optional list of {name, tier} for social suggestions (tier: supportive|emerging_support|neutral).
- Output VALID JSON ONLY, no markdown fences."""

SEMANTIC_PROPOSER_USER_TEMPLATE = """LIVE_CONTEXT_JSON:
{live_context}

GRAPH_FRAGMENTS_JSON:
{fragments}

Fragment schema (use these field values verbatim in graph_evidence for traceability):
- activities[]: entry_id, activity_key, activity_node_id, activity_name, activity_meaning_types (E55 via P2_has_type), place_name, place_node_id, calendar_day, actors[].
- feelings[]: entry_id, assignment_ref, type_via_p141, type_via_p2, feeling_meaning, influenced_by_activity_key, place_name, place_node_id.

Return exactly this JSON shape:
{{
  "proposals": [
    {{
      "kind": "see_person|visit_place|do_activity|buy_food|other",
      "title": "short headline",
      "body": "1-3 sentences, warm and specific",
      "anchor_date": "YYYY-MM-DD",
      "priority": 0.72,
      "people": [{{"name": "...", "tier": "neutral"}}],
      "graph_evidence": [
        {{"ref_kind": "E7_Activity", "activity_key": "<from activities[].activity_key>"}},
        {{"ref_kind": "E55_Type", "type_name": "<from activity_meaning_types or feeling_meaning>"}},
        {{"ref_kind": "E13_Attribute_Assignment", "assignment_ref": "<from feelings[].assignment_ref>"}},
        {{"ref_kind": "E53_Place", "place_name": "<from place_name>", "place_node_id": "<optional>"}},
        {{"ref_kind": "E73_Information_Object", "entry_id": "<from entry_id>"}}
      ]
    }}
  ]
}}
Use empty people [] when not a social suggestion. graph_evidence must contain at least one entry per proposal."""


def _get_chat_client():
    key = (AZURE_OPENAI_API_KEY or "").strip()
    if not key:
        raise ValueError("AZURE_OPENAI_API_KEY is not set")
    endpoint = (AZURE_OPENAI_ENDPOINT or "").strip()
    if endpoint:
        from openai import AzureOpenAI

        return AzureOpenAI(
            api_key=key,
            azure_endpoint=endpoint.rstrip("/"),
            api_version=AZURE_OPENAI_API_VERSION or "2024-12-01-preview",
        ), (AZURE_OPENAI_DEPLOYMENT or "").strip() or "gpt-4o-mini"
    from openai import OpenAI

    return OpenAI(api_key=key), (AZURE_OPENAI_DEPLOYMENT or "").strip() or "gpt-4o-mini"


def _parse_json_object(content: str) -> Dict[str, Any]:
    content = (content or "").strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", content)
    if m:
        content = m.group(1).strip()
    return json.loads(content)


def run_semantic_proposer(
    live_context: Dict[str, Any],
    fragments: Dict[str, Any],
    *,
    user_name: str = "",
    max_proposals: int = 5,
) -> Dict[str, Any]:
    """
    Returns {"proposals": [...]} as parsed from the model (may be empty).
    Raises on missing API key, JSON errors, or API failures.
    """
    client, model = _get_chat_client()
    user_msg = SEMANTIC_PROPOSER_USER_TEMPLATE.format(
        live_context=json.dumps(live_context, ensure_ascii=False, indent=2),
        fragments=json.dumps(fragments, ensure_ascii=False, indent=2),
    )
    extra = ""
    if (user_name or "").strip():
        extra = f"\nThe journal author (the user) is named {user_name.strip()}. Do not suggest they meet themselves."

    kwargs: Dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": SEMANTIC_PROPOSER_SYSTEM + extra},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.25,
        "max_tokens": 2500,
        "response_format": {"type": "json_object"},
    }

    try:
        resp = client.chat.completions.create(**kwargs)
    except Exception as e:
        _log.warning("semantic proposer: json_object mode failed (%s), retrying without it", e)
        kwargs.pop("response_format", None)
        resp = client.chat.completions.create(**kwargs)
    raw = (resp.choices[0].message.content or "").strip()
    data = _parse_json_object(raw)
    proposals = data.get("proposals")
    if not isinstance(proposals, list):
        return {"proposals": []}
    out: List[Dict[str, Any]] = []
    for p in proposals[:max_proposals]:
        if isinstance(p, dict):
            out.append(p)
    return {"proposals": out}
