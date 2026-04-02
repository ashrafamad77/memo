"""LLM-driven semantic disambiguation: mention → canonical sense label.

Two separate jobs are handled here:

Entity disambiguation (E53_Place, E21_Person, E74_Group)
---------------------------------------------------------
The LLM determines the specific real-world referent of each mention.
When the context does not uniquely determine the referent, the LLM returns
up to 3 candidate labels so the user can pick.  Previously resolved mentions
are injected as context so downstream questions can often be auto-answered.

E55 concept grounding (E55_Type)
---------------------------------
This is a DIFFERENT task from entity linking.  An E55_Type is an abstract
activity/concept extracted from the text — it may appear as a verb ("coded"),
a noun ("coding session"), or even implicitly.  The job here is to:
  1. Understand what activity/concept is described (from any linguistic form).
  2. Return a canonical concept label (Wikipedia title style).
  3. Never ask the user — the LLM should always be able to infer the concept
     from the journal text; if truly unclear it falls back to the surface form.

The canonical label and optional ``wd_search_query`` are handed to Wikidata Vector
search (when configured) or to BabelNet getSenses for formal linking
(synset → Wikidata QID, WordNet ID, etc.).

Mention IDs
-----------
Every mention carries a stable ``id`` (e.g. "m0", "m1") independent of the
surface text.  Clarification answers are keyed by id, not surface text:

    clarification_answers = {"m0": "Victoria, London", "m2": "Victoria station"}

Context hints
-------------
After the user answers Q1, that answer is injected as a context hint for Q2+:

    context_hints = {"Victoria": "Victoria, London"}

This lets the LLM auto-resolve "Victoria Library" without asking the user.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_SYSTEM = """\
You are a semantic disambiguation assistant for a personal journal knowledge graph.

Given a journal entry and a list of mentions, identify the canonical sense for each.
Mentions have two roles:

══ ENTITY MENTIONS (E53_Place, E21_Person, E74_Group) ══════════════════════════════
These are specific real-world instances.  Your job:
- Return the canonical Wikipedia article title as canonical_label.
  Examples: "Victoria, London"  "Westminster Libraries"  "Ada Lovelace"
- When you cannot confidently determine the referent from the text (see rules below),
  set needs_clarification = true AND provide up to 3 candidate labels in candidates[].
  The user will pick one.  candidates[] MUST be non-empty whenever needs_clarification=true.
- When context_hints are provided (previously resolved mentions), use them as
  geographic/contextual anchors to resolve remaining items without asking.
  Example: if "Victoria" → "Victoria, London" is already resolved, then
  "Victoria Library" can be resolved as "Westminster Libraries" with confidence.

Rules for needs_clarification = true (entities only):
1. The name is a well-known toponym in multiple unrelated regions AND the text does
   not explicitly anchor it to one region (e.g. "Victoria" without country context).
2. The same surface form appears more than once with potentially different referents.
3. Ambiguous between different entity types (person vs place vs org).
4. Resolution requires information not present in the text.
5. Resolution depends on geographic inference (nearby landmarks, city co-occurrence)
   AND the country/city is not explicitly stated — implied geography is NOT anchoring.
6. The entity is prominent globally (capital city, famous landmark, famous person)
   but the text does not confirm that prominent entity is the one meant.
   Prominence is not disambiguation.

DO NOT set needs_clarification = false merely because a sense is globally more frequent,
because nearby entities suggest a city, or because a famous default exists.
Only explicit disambiguating information stated in the text counts as anchoring.

══ CONCEPT MENTIONS (E55_Type) ══════════════════════════════════════════════════════
These are abstract activity/concept labels for a CIDOC CRM E55_Type node.
Your job is DIFFERENT here:
- The surface form may be a verb ("coded"), a gerund ("coding"), a noun phrase
  ("coding session"), or even implicit from context.
- Return the canonical Wikipedia concept name as canonical_label.
  Examples: "coding" → "Computer programming"
            "went for a run" → "Running"
            "had a meeting" → "Meeting"
            "reading" → "Reading"  (already canonical)
- Also set wd_search_query: a rich phrase for Wikidata vector retrieval (include the
  activity, domain, and disambiguation hints), e.g. "Computer programming software development activity".
  For entities, include type and context, e.g. "Victoria London UK place".
- NEVER set needs_clarification = true for E55_Type — always infer from context.
  If the text is genuinely unclear, return the surface form as-is.
- Do not provide candidates[] for E55_Type items.

══ RESPONSE SCHEMA ══════════════════════════════════════════════════════════════════
Return ONLY valid JSON:
{
  "mentions": [
    {
      "id": "m0",
      "name": "...",
      "canonical_label": "...",
      "wd_search_query": "...",
      "candidates": [],            // up to 3 strings; required & non-empty when needs_clarification=true for entities
      "confidence": 0.95,
      "needs_clarification": false,
      "reason": "brief explanation"
    }
  ]
}
"""


def _get_openai_client():
    try:
        from config import (
            AZURE_OPENAI_API_KEY,
            AZURE_OPENAI_API_VERSION,
            AZURE_OPENAI_DEPLOYMENT,
            AZURE_OPENAI_ENDPOINT,
        )
        from openai import AzureOpenAI

        key = (AZURE_OPENAI_API_KEY or "").strip()
        endpoint = (AZURE_OPENAI_ENDPOINT or "").strip()
        if not key or not endpoint:
            return None, ""
        deployment = (AZURE_OPENAI_DEPLOYMENT or "gpt-4o-mini").strip() or "gpt-4o-mini"
        version = (AZURE_OPENAI_API_VERSION or "2024-12-01-preview").strip()
        return AzureOpenAI(api_key=key, azure_endpoint=endpoint, api_version=version), deployment
    except Exception as exc:
        logger.debug("llm_disambiguator: cannot build client: %s", exc)
        return None, ""


def assign_mention_ids(mentions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return a copy of ``mentions`` with a stable ``id`` field added to each item.

    IDs are sequential "m0", "m1", … independent of surface text so that duplicate
    names (e.g. two "Victoria" mentions) are unambiguous.
    """
    return [{**m, "id": f"m{i}"} for i, m in enumerate(mentions)]


def disambiguate_mentions(
    journal_text: str,
    mentions: List[Dict[str, Any]],
    *,
    clarification_answers: Optional[Dict[str, str]] = None,
    context_hints: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Disambiguate a list of mentions using the full journal text as context.

    Parameters
    ----------
    journal_text:
        Full journal entry text.
    mentions:
        List of dicts: ``{"id": "m0", "name": "...", "cidoc_label": "..."}``.
        Call ``assign_mention_ids`` first if ``id`` fields are absent.
    clarification_answers:
        Mapping of mention **id** → user-confirmed canonical label.
        These bypass the LLM entirely (confidence=1.0).
    context_hints:
        Mapping of surface name → already-resolved canonical label from prior
        answers in the same session.  Injected into the LLM prompt so it can
        auto-resolve downstream mentions using the established context.
        Example: ``{"Victoria": "Victoria, London"}``

    Returns
    -------
    List of dicts, one per input mention::

        {
            "id": "m0",
            "name": "Victoria",
            "cidoc_label": "E53_Place",
            "canonical_label": "Victoria, London",
            "candidates": ["Victoria, London", "Victoria, British Columbia", ...],
            "confidence": 0.6,
            "needs_clarification": True,
            "reason": "...",
            "wd_search_query": "...",
        }

    ``candidates`` is non-empty only for entity mentions where
    ``needs_clarification=True``.  E55_Type items never set
    ``needs_clarification=True`` and never carry candidates.

    ``wd_search_query`` falls back to ``canonical_label`` or surface ``name`` when absent.
    """
    if not mentions:
        return []

    stamped = [
        m if m.get("id") else {**m, "id": f"m{i}"}
        for i, m in enumerate(mentions)
    ]

    answers: Dict[str, str] = {
        k.strip(): v.strip()
        for k, v in (clarification_answers or {}).items()
        if k and v
    }
    hints: Dict[str, str] = {
        k.strip(): v.strip()
        for k, v in (context_hints or {}).items()
        if k and v
    }

    results: List[Dict[str, Any]] = []
    to_resolve: List[Dict[str, Any]] = []

    for m in stamped:
        mid = str(m.get("id") or "").strip()
        name = str(m.get("name") or "").strip()
        cidoc = str(m.get("cidoc_label") or "")
        if not name:
            continue
        if mid in answers:
            cl = answers[mid]
            results.append({
                "id": mid,
                "name": name,
                "cidoc_label": cidoc,
                "canonical_label": cl,
                "wd_search_query": f"{cl} Wikidata",
                "candidates": [],
                "confidence": 1.0,
                "needs_clarification": False,
                "reason": "user-provided clarification",
            })
        else:
            to_resolve.append(m)

    if not to_resolve:
        return results

    client, deployment = _get_openai_client()
    if client is None:
        for m in to_resolve:
            name = str(m.get("name") or "").strip()
            results.append({
                "id": str(m.get("id") or ""),
                "name": name,
                "cidoc_label": str(m.get("cidoc_label") or ""),
                "canonical_label": name,
                "wd_search_query": name,
                "candidates": [],
                "confidence": 0.0,
                "needs_clarification": False,
                "reason": "no LLM client available",
            })
        return results

    mentions_json = json.dumps(
        [
            {
                "id": str(m.get("id") or ""),
                "name": str(m.get("name") or ""),
                "cidoc_label": str(m.get("cidoc_label") or ""),
            }
            for m in to_resolve
        ],
        ensure_ascii=False,
    )

    context_block = ""
    if hints:
        lines = [f'- "{name}" → "{label}"' for name, label in hints.items()]
        context_block = (
            "\nPreviously resolved mentions in this entry (use as context anchor):\n"
            + "\n".join(lines)
            + "\n"
        )

    user_msg = (
        f'Journal entry:\n"""\n{(journal_text or "").strip()}\n"""\n'
        + context_block
        + f"\nMentions to disambiguate:\n{mentions_json}"
    )

    llm_items: List[Dict[str, Any]] = []
    try:
        resp = client.chat.completions.create(
            model=deployment,
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            temperature=0,
            max_tokens=1500,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or ""
        parsed = json.loads(raw)
        items = parsed.get("mentions") or []
        if isinstance(items, list):
            llm_items = items
    except Exception as exc:
        logger.warning("llm_disambiguator: LLM call failed: %s", exc)

    # Index by id (primary) and name (fallback)
    llm_by_id: Dict[str, Dict[str, Any]] = {}
    llm_by_name: Dict[str, Dict[str, Any]] = {}
    for item in llm_items:
        if not isinstance(item, dict):
            continue
        mid = str(item.get("id") or "").strip()
        nm = str(item.get("name") or "").strip()
        if mid:
            llm_by_id[mid] = item
        if nm and nm not in llm_by_name:
            llm_by_name[nm] = item

    for m in to_resolve:
        mid = str(m.get("id") or "").strip()
        name = str(m.get("name") or "").strip()
        cidoc = str(m.get("cidoc_label") or "")
        item = llm_by_id.get(mid) or llm_by_name.get(name)

        if item:
            try:
                conf = float(item.get("confidence") or 0.5)
            except (TypeError, ValueError):
                conf = 0.5
            conf = max(0.0, min(1.0, conf))
            canonical = str(item.get("canonical_label") or name).strip() or name
            wd_q = str(item.get("wd_search_query") or "").strip()
            if not wd_q:
                wd_q = canonical if canonical else name
            raw_cands = item.get("candidates") or []
            candidates = [str(c).strip() for c in raw_cands if str(c).strip()] if isinstance(raw_cands, list) else []
            needs = bool(item.get("needs_clarification", False))
            # E55_Type: never surface to user regardless of what LLM returned
            if cidoc == "E55_Type":
                needs = False
                candidates = []
            results.append({
                "id": mid,
                "name": name,
                "cidoc_label": cidoc,
                "canonical_label": canonical,
                "wd_search_query": wd_q,
                "candidates": candidates[:3],
                "confidence": conf,
                "needs_clarification": needs,
                "reason": str(item.get("reason") or "").strip(),
            })
        else:
            results.append({
                "id": mid,
                "name": name,
                "cidoc_label": cidoc,
                "canonical_label": name,
                "wd_search_query": name,
                "candidates": [],
                "confidence": 0.0,
                "needs_clarification": False,
                "reason": "not returned by LLM",
            })

    return results


def resolve_remaining_with_context(
    journal_text: str,
    remaining: List[Dict[str, Any]],
    context_hints: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Re-run disambiguation for items still pending, with the growing context.

    Returns the same list, some items may now have needs_clarification=False
    (auto-resolved via context).  Items that remain ambiguous keep needs_clarification=True.
    """
    if not remaining or not context_hints:
        return remaining
    return disambiguate_mentions(
        journal_text,
        remaining,
        context_hints=context_hints,
    )
