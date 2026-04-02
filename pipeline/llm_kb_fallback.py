"""LLM KB linker — fallback and combined Babelfy+LLM resolver.

Three entry points:

  llm_resolve_all_with_babelfy  — PRIMARY: resolve ALL types + entities in one LLM call,
      using ALL Babelfy concept/NE annotations as structured evidence. LLM reasons over
      the full Babelfy context even for types that don't appear literally in the text
      (e.g. "Visit" from "spent the morning at Victoria").

  llm_e55_grounding_fallback    — legacy: suggest QIDs for types Babelfy missed (no evidence)
  llm_entity_linking_fallback   — legacy: suggest QIDs for entities Babelfy missed (no evidence)

All functions validate each suggested QID via wikidata_fetch_labels_descriptions before
returning, so the caller never receives a hallucinated or non-existent QID.

Row shapes are compatible with:
  - grounding rows  → TypeResolver.resolve_graph_spec (llm_grounding arg)
  - entity rows     → apply_entity_linking (el_results arg)
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── helpers ───────────────────────────────────────────────────────────────────


def _get_openai_client():
    """Return an AzureOpenAI client built from config.py / env, or None."""
    try:
        from config import (
            AZURE_OPENAI_API_KEY,
            AZURE_OPENAI_API_VERSION,
            AZURE_OPENAI_ENDPOINT,
        )
        from openai import AzureOpenAI

        key = (AZURE_OPENAI_API_KEY or "").strip()
        endpoint = (AZURE_OPENAI_ENDPOINT or "").strip()
        version = (AZURE_OPENAI_API_VERSION or "2024-12-01-preview").strip()
        if not key or not endpoint:
            return None, ""
        deployment = _get_deployment()
        return AzureOpenAI(api_key=key, azure_endpoint=endpoint, api_version=version), deployment
    except Exception as exc:
        logger.debug("llm_kb_fallback: cannot build OpenAI client: %s", exc)
        return None, ""


def _get_deployment() -> str:
    try:
        from config import AZURE_OPENAI_DEPLOYMENT
        return (AZURE_OPENAI_DEPLOYMENT or "gpt-4o-mini").strip() or "gpt-4o-mini"
    except ImportError:
        import os
        return os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini").strip() or "gpt-4o-mini"


def _validate_qids(qids: List[str]) -> Dict[str, Tuple[str, str]]:
    """Return Wikidata (label, description) for each QID that actually exists."""
    from .type_grounding_embed import wikidata_fetch_labels_descriptions
    valid = [q for q in qids if re.match(r"^Q\d+$", (q or "").strip())]
    if not valid:
        return {}
    return wikidata_fetch_labels_descriptions(valid)


def _call_llm(client, deployment: str, messages: List[Dict[str, str]], label: str) -> Optional[str]:
    """Single LLM call; returns raw text or None on failure."""
    try:
        resp = client.chat.completions.create(
            model=deployment,
            messages=messages,
            temperature=0,
            max_tokens=512,
            response_format={"type": "json_object"},
        )
        return resp.choices[0].message.content or ""
    except Exception as exc:
        logger.warning("llm_kb_fallback (%s): LLM call failed: %s", label, exc)
        return None


def _parse_json_list(raw: str, key: str) -> List[Dict[str, Any]]:
    """Parse JSON from LLM response; accept either {key: [...]} or bare [...]."""
    try:
        obj = json.loads(raw or "{}")
    except json.JSONDecodeError:
        return []
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        # Accept {key: [...]} or any dict whose first list-valued key matches
        if isinstance(obj.get(key), list):
            return obj[key]
        for v in obj.values():
            if isinstance(v, list):
                return v
    return []


# ── E55 type grounding fallback ───────────────────────────────────────────────


def llm_e55_grounding_fallback(
    unmatched_names: List[str],
    journal_text: str,
) -> Dict[str, Dict[str, Any]]:
    """Return grounding rows for E55 type names that Babelfy CONCEPTS did not match.

    Each row is compatible with ``merged_ground`` in ``agentic.py``::

        {
            "confidence": "medium",
            "wikidata_candidates": [{"qid": "Q...", "label": "...", "description": "..."}],
            "aat_id": "", "aat_label": "", "aat_confidence": "low",
            "description": "...",
        }

    Skips any type whose LLM-suggested QID fails Wikidata validation.
    Returns an empty dict if no API credentials are configured.
    """
    if not unmatched_names:
        return {}
    client, deployment = _get_openai_client()
    if client is None:
        return {}

    names_block = "\n".join(f"- {n}" for n in unmatched_names)
    context_snippet = (journal_text or "")[:400].replace("\n", " ")

    system = (
        "You are a knowledge base assistant. "
        "Given activity/concept type names used in a personal CIDOC CRM journal graph, "
        "identify the best matching Wikidata entity for each. "
        "Types are abstract category labels like Visit, Programming, WorkSession, Reading, Library. "
        'Return ONLY valid JSON: {"types": [{"name": "...", "qid": "Q...", "label": "...", "description": "..."}]}'
    )
    user = (
        f"Journal snippet (context only): {context_snippet}\n\n"
        f"Type names to ground:\n{names_block}\n\n"
        "For each type name provide the single most appropriate Wikidata QID. "
        "If no good Wikidata entity exists set qid to empty string. "
        "description should be a short gloss (≤15 words)."
    )

    raw = _call_llm(client, deployment, [{"role": "system", "content": system}, {"role": "user", "content": user}], "e55")
    if not raw:
        return {}

    items = _parse_json_list(raw, "types")
    suggested: Dict[str, str] = {}  # name → qid
    for item in items:
        if not isinstance(item, dict):
            continue
        nm = str(item.get("name") or "").strip()
        qid = str(item.get("qid") or "").strip()
        if nm and re.match(r"^Q\d+$", qid):
            suggested[nm] = qid

    if not suggested:
        return {}

    # Validate all suggested QIDs in one batch call (returns uppercase keys)
    validated = _validate_qids(list(set(suggested.values())))

    out: Dict[str, Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        nm = str(item.get("name") or "").strip()
        if nm not in unmatched_names:
            continue
        qid = suggested.get(nm, "")
        if not qid or qid.upper() not in validated:
            continue
        qid_upper = qid.upper()
        wd_label, wd_desc = validated[qid_upper]
        desc = str(item.get("description") or wd_desc or wd_label or "").strip()
        out[nm] = {
            "confidence": "medium",
            "wikidata_candidates": [{"qid": qid_upper, "label": wd_label, "description": wd_desc}],
            "aat_id": "",
            "aat_label": "",
            "aat_confidence": "low",
            "description": desc,
        }
        logger.debug("llm_kb_fallback: E55 %r → %s (%s)", nm, qid_upper, wd_label)

    return out


# ── entity linking fallback ───────────────────────────────────────────────────


def llm_entity_linking_fallback(
    unlinked: List[Dict[str, str]],
    journal_text: str,
) -> Dict[str, Dict[str, Any]]:
    """Return entity linking rows for E53/E21/E74 nodes that Babelfy NAMED_ENTITIES missed.

    ``unlinked`` is a list of ``{"name": "...", "cidoc_label": "E53_Place"|"E21_Person"|"E74_Group"}``.

    Each returned row is compatible with ``el_results`` consumed by ``apply_entity_linking``::

        {
            "wikidata_id": "Q...",
            "description": "...",
            "babel_synset_id": "",   # empty — no synset from LLM path
            "wordnet_synset_id": "",
            "babel_gloss": "",
            "babelnet_rdf_url": "",
            "dbpedia_url": "",
            "babelnet_sources_json": "",
        }

    Skips any entity whose LLM-suggested QID fails Wikidata validation.
    """
    if not unlinked:
        return {}
    client, deployment = _get_openai_client()
    if client is None:
        return {}

    entities_block = "\n".join(
        f"- name: {e['name']}, type: {e.get('cidoc_label', '')}" for e in unlinked
    )
    context_snippet = (journal_text or "")[:600].replace("\n", " ")

    system = (
        "You are a knowledge base assistant. "
        "Given named entities from a personal journal, identify the best Wikidata entity for each. "
        "E53_Place = geographic place, E21_Person = individual person, E74_Group = organization/group. "
        'Return ONLY valid JSON: {"entities": [{"name": "...", "qid": "Q...", "label": "...", "description": "..."}]}'
    )
    user = (
        f"Journal (context): {context_snippet}\n\n"
        f"Entities to link:\n{entities_block}\n\n"
        "For each entity provide the single most appropriate Wikidata QID. "
        "If no good match exists set qid to empty string. "
        "description should be a short gloss (≤15 words)."
    )

    raw = _call_llm(client, deployment, [{"role": "system", "content": system}, {"role": "user", "content": user}], "el")
    if not raw:
        return {}

    items = _parse_json_list(raw, "entities")
    suggested: Dict[str, str] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        nm = str(item.get("name") or "").strip()
        qid = str(item.get("qid") or "").strip()
        if nm and re.match(r"^Q\d+$", qid):
            suggested[nm] = qid

    if not suggested:
        return {}

    validated = _validate_qids(list(set(suggested.values())))

    out: Dict[str, Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        nm = str(item.get("name") or "").strip()
        qid = suggested.get(nm, "")
        if not qid or qid.upper() not in validated:
            continue
        qid_upper = qid.upper()
        wd_label, wd_desc = validated[qid_upper]
        desc = str(item.get("description") or wd_desc or wd_label or "").strip()
        out[nm] = {
            "wikidata_id": qid_upper,
            "description": desc,
            "babel_synset_id": "",
            "wordnet_synset_id": "",
            "babel_gloss": "",
            "babelnet_rdf_url": "",
            "dbpedia_url": "",
            "babelnet_sources_json": "",
        }
        logger.debug("llm_kb_fallback: EL %r → %s (%s)", nm, qid_upper, wd_label)

    return out


# ── combined Babelfy + LLM resolver (primary path) ───────────────────────────


def _format_evidence_table(evidence: List[Dict[str, Any]]) -> str:
    """Compact text representation of Babelfy annotations for the LLM prompt."""
    if not evidence:
        return "  (none)"
    rows = []
    for e in evidence[:12]:  # cap prompt length
        wd = ", ".join(e.get("wikidata_candidates") or [])
        rows.append(
            f'  surface="{e["surface"]}" synset={e["synset_id"]} '
            f'score={e["score"]:.2f} gloss="{e["gloss"]}" wikidata=[{wd}]'
        )
    return "\n".join(rows)


def llm_resolve_all_with_babelfy(
    type_requests: List[Dict[str, Any]],
    entity_requests: List[Dict[str, Any]],
    journal_text: str,
    concept_evidence: List[Dict[str, Any]],
    ne_evidence: List[Dict[str, Any]],
    babelfy_type_hits: Optional[Dict[str, Dict[str, Any]]] = None,
    skip_type_names: Optional[set] = None,
) -> "Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]":
    """Combined Babelfy+LLM resolver: resolves ALL types AND entities in one LLM call.

    Parameters
    ----------
    type_requests:
        E55 type names to ground (from ``collect_e55_grounding_requests``).
    entity_requests:
        E53/E21/E74 nodes to link (from ``collect_entity_linking_requests``).
    journal_text:
        The full journal entry text.
    concept_evidence:
        ALL Babelfy CONCEPTS annotations enriched with BabelNet
        (from ``collect_babelfy_evidence(ann_type="CONCEPTS")``).
    ne_evidence:
        ALL Babelfy NAMED_ENTITIES annotations enriched with BabelNet
        (from ``collect_babelfy_evidence(ann_type="NAMED_ENTITIES")``).
    babelfy_type_hits:
        Per-type Babelfy hits from ``run_babelfy_e55_grounding`` — used to include
        the synset already found per type so LLM can confirm or override it.
    skip_type_names:
        Lowercase names already grounded (seed vocab + Neo4j cache); skip these.

    Returns
    -------
    (e55_grounding_rows, entity_linking_rows) — both compatible with existing consumers.
    Note: e55 rows do NOT include babel_synset_id (that comes from the Babelfy hit, merged
    by caller); entity rows may have empty babel fields (only wikidata_id populated here).
    """
    if not type_requests and not entity_requests:
        return {}, {}
    client, deployment = _get_openai_client()
    if client is None:
        return {}, {}

    skip = {n.lower() for n in (skip_type_names or set())}
    types_to_send = [r for r in type_requests if str(r.get("name") or "").strip().lower() not in skip]
    entities_to_send = list(entity_requests)

    if not types_to_send and not entities_to_send:
        return {}, {}

    # Build compact JSON representations for the prompt
    def _type_item(r: Dict[str, Any]) -> Dict[str, Any]:
        nm = str(r.get("name") or "").strip()
        hit = (babelfy_type_hits or {}).get(nm) or {}
        return {
            "name": nm,
            "seed_qid": r.get("seed_qid") or "",
            "babelfy_synset": str(hit.get("babel_synset_id") or ""),
            "babelfy_wikidata": [c["qid"] for c in (hit.get("wikidata_candidates") or []) if c.get("qid")],
            "babelfy_gloss": str(hit.get("description") or ""),
        }

    def _entity_item(r: Dict[str, Any]) -> Dict[str, Any]:
        return {"name": str(r.get("name") or ""), "cidoc_label": str(r.get("cidoc_label") or "")}

    context_snippet = (journal_text or "")[:500].replace("\n", " ")
    concept_table = _format_evidence_table(concept_evidence)
    ne_table = _format_evidence_table(ne_evidence)

    system = (
        "You are a knowledge base linking assistant for a personal CIDOC CRM journal graph.\n"
        "Use the Babelfy evidence (structured NLP output) as signal to identify the best Wikidata QID "
        "for each item. Babelfy provides lexical/semantic anchors even when the exact word is absent.\n"
        "Rules:\n"
        "- E55_Type: abstract activity/concept labels (Visit, Programming, Reading). These are taxonomic "
        "  labels, not literal words in the text. Use Babelfy CONCEPTS + journal context to infer meaning.\n"
        "- E53_Place: specific geographic locations. Match to a Wikidata geographic entity or leave empty.\n"
        "- E21_Person: individual persons. Match to Wikidata person or leave empty.\n"
        "- E74_Group: organizations/groups. Match to Wikidata organization or leave empty.\n"
        "- Return empty qid '' if no confident match (better than a wrong QID).\n"
        'Return ONLY valid JSON: {"types": [...], "entities": [...]}'
    )

    user_parts = [
        f'Journal: "{context_snippet}"\n',
        "Babelfy CONCEPTS found in text:\n" + concept_table,
        "\nBabelfy NAMED_ENTITIES found in text:\n" + ne_table,
    ]

    if types_to_send:
        types_json = json.dumps([_type_item(r) for r in types_to_send], ensure_ascii=False)
        user_parts.append(
            f"\nE55 types to ground to Wikidata:\n{types_json}\n"
            'Expected per type: {"name":"...", "qid":"Q...", "label":"...", "description":"..."}'
        )
    else:
        user_parts.append('\nE55 types: [] — return "types": []')

    if entities_to_send:
        ents_json = json.dumps([_entity_item(r) for r in entities_to_send], ensure_ascii=False)
        user_parts.append(
            f"\nEntities to link to Wikidata:\n{ents_json}\n"
            'Expected per entity: {"name":"...", "cidoc_label":"...", "qid":"Q...", "label":"...", "description":"..."}'
        )
    else:
        user_parts.append('\nEntities: [] — return "entities": []')

    raw = _call_llm(
        client, deployment,
        [{"role": "system", "content": system}, {"role": "user", "content": "\n".join(user_parts)}],
        "combined",
    )
    if not raw:
        return {}, {}

    parsed = {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}, {}

    # ── process type results ──────────────────────────────────────────────────
    type_items = parsed.get("types") or []
    if not isinstance(type_items, list):
        type_items = []

    type_suggested: Dict[str, str] = {}
    for item in type_items:
        if not isinstance(item, dict):
            continue
        nm = str(item.get("name") or "").strip()
        qid = str(item.get("qid") or "").strip()
        if nm and re.match(r"^Q\d+$", qid):
            type_suggested[nm] = qid

    # ── process entity results ────────────────────────────────────────────────
    ent_items = parsed.get("entities") or []
    if not isinstance(ent_items, list):
        ent_items = []

    ent_suggested: Dict[str, str] = {}
    for item in ent_items:
        if not isinstance(item, dict):
            continue
        nm = str(item.get("name") or "").strip()
        qid = str(item.get("qid") or "").strip()
        if nm and re.match(r"^Q\d+$", qid):
            ent_suggested[nm] = qid

    # ── batch-validate all suggested QIDs ────────────────────────────────────
    all_qids = list(set(list(type_suggested.values()) + list(ent_suggested.values())))
    validated = _validate_qids(all_qids) if all_qids else {}

    # ── build e55 grounding rows ──────────────────────────────────────────────
    e55_rows: Dict[str, Dict[str, Any]] = {}
    for item in type_items:
        if not isinstance(item, dict):
            continue
        nm = str(item.get("name") or "").strip()
        if not nm:
            continue
        qid = type_suggested.get(nm, "")
        if not qid or qid.upper() not in validated:
            continue
        qid_upper = qid.upper()
        wd_label, wd_desc = validated[qid_upper]
        desc = str(item.get("description") or wd_desc or wd_label or "").strip()
        e55_rows[nm] = {
            "confidence": "medium",
            # wikidata_id is the pre-validated top QID — used by TypeResolver as a direct
            # fallback when embedding reranking fails (see _resolve_one modern path).
            "wikidata_id": qid_upper,
            "wikidata_candidates": [{"qid": qid_upper, "label": wd_label, "description": wd_desc}],
            "aat_id": "",
            "aat_label": "",
            "aat_confidence": "low",
            "description": desc,
        }
        logger.debug("llm_kb_resolver: E55 %r → %s (%s)", nm, qid_upper, wd_label)

    # ── build entity linking rows ─────────────────────────────────────────────
    el_rows: Dict[str, Dict[str, Any]] = {}
    for item in ent_items:
        if not isinstance(item, dict):
            continue
        nm = str(item.get("name") or "").strip()
        if not nm:
            continue
        qid = ent_suggested.get(nm, "")
        if not qid or qid.upper() not in validated:
            continue
        qid_upper = qid.upper()
        wd_label, wd_desc = validated[qid_upper]
        desc = str(item.get("description") or wd_desc or wd_label or "").strip()
        el_rows[nm] = {
            "wikidata_id": qid_upper,
            "description": desc,
            "babel_synset_id": "",
            "wordnet_synset_id": "",
            "babel_gloss": "",
            "babelnet_rdf_url": "",
            "dbpedia_url": "",
            "babelnet_sources_json": "",
        }
        logger.debug("llm_kb_resolver: EL %r → %s (%s)", nm, qid_upper, wd_label)

    return e55_rows, el_rows
