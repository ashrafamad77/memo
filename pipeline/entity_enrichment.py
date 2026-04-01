"""Post-resolve context enrichment: re-enrich sibling disambiguation tasks.

When the user resolves an E53_Place task (picks a Wikidata QID for e.g. "Victoria"),
that QID becomes a **geographic anchor** for other open ``place_wikidata`` tasks in the
same journal entry (same ``entry_id``).

For each sibling mention that maps to a seed type (e.g. ``library`` → Q7075), we query
**Wikidata Query Service** (not the Action API):

1. **Geo** — ``wikibase:around`` using the anchor's **P625** (within ``MEMO_GEO_RADIUS_KM``).
2. **Admin** — fallback: ``P131`` of the anchor and its ancestors, matching items located
   under the same region.
3. **Country** — when the anchor has **P17**, keep instances with the same **P17** (or no **P17**);
   drops cross-country hits from geo noise.

Library-like items use an expanded ``P31`` pattern (public / national / research library,
library building, etc.); a bare ``wdt:P31 wd:Q7075`` almost never matches real libraries.

Results are then passed through the same **strict** gates as ingest
(``_canonicalize_entity_link_candidates``): P31 blocklist + WDQS proof under
``Q2221906`` / forbidden classes, so inbox options stay ontologically safe.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

logger = logging.getLogger(__name__)

# Only place resolution triggers enrichment for now.
_ENRICHMENT_SOURCE_LABELS = frozenset({"E53_Place"})
_ENRICHMENT_TARGET_LABELS = frozenset({"E53_Place", "E74_Group"})


def enrich_sibling_tasks(
    entry_id: str,
    resolved_task_id: str,
    resolved_qid: str,
    resolved_cidoc_label: str,
    repo: Any,
) -> int:
    """Re-enrich open sibling tasks in the same entry using a newly resolved place QID.

    For each open place/group task in ``entry_id`` (excluding the just-resolved one):
      1. Infer the Wikidata concept type from the mention (e.g. "Library" -> Q7075).
      2. SPARQL: find instances of that type located in (P131*) ``resolved_qid``.
      3. Update the task's candidates_json with the geographically relevant results.

    Returns the number of tasks updated.
    Failures are logged and silently swallowed — enrichment must never break the resolve flow.
    """
    if resolved_cidoc_label not in _ENRICHMENT_SOURCE_LABELS:
        return 0
    if not entry_id or not resolved_qid:
        return 0

    try:
        from .type_grounding_embed import wikidata_instances_in_place
        from .type_vocab import mention_to_type_qid
    except ImportError as exc:
        logger.warning("entity_enrichment: import error — %s", exc)
        return 0

    try:
        siblings = repo.get_open_tasks_for_entry(entry_id, exclude_task_id=resolved_task_id)
    except Exception as exc:
        logger.warning("entity_enrichment: get_open_tasks_for_entry failed — %s", exc)
        return 0

    updated = 0
    for sibling in siblings:
        if sibling.get("entity_label") not in _ENRICHMENT_TARGET_LABELS:
            continue
        mention = str(sibling.get("mention") or "").strip()
        if not mention:
            continue

        type_qid = mention_to_type_qid(mention)
        if not type_qid:
            logger.info(
                "entity_enrichment: no seed-vocab type QID for mention %r — cannot enrich task %s",
                mention,
                sibling.get("id"),
            )
            continue

        # Small cap: each row is re-validated with strict WDQS (2+ ASKs) — keep sibling refresh bounded.
        try:
            raw = wikidata_instances_in_place(type_qid, resolved_qid, limit=8)
        except Exception as exc:
            logger.warning(
                "entity_enrichment: SPARQL failed for type=%s location=%s: %s",
                type_qid, resolved_qid, exc,
            )
            continue

        if not raw:
            logger.info(
                "entity_enrichment: no WDQS results for type=%s near/under %s (mention=%r)",
                type_qid, resolved_qid, mention,
            )
            continue

        try:
            from pipeline.type_grounding_llm import (
                _canonicalize_entity_link_candidates,
                _cap_entity_link_candidates,
            )

            shaped = [
                {
                    "wikidata_id": c.get("wikidata_id"),
                    "label": str(c.get("label") or ""),
                    "description": str(c.get("description") or ""),
                    "confidence": str(c.get("confidence") or "medium"),
                }
                for c in raw
                if isinstance(c, dict) and c.get("wikidata_id")
            ]
            elab = str(sibling.get("entity_label") or "E53_Place").strip()
            candidates = _canonicalize_entity_link_candidates(
                mention, shaped, cidoc_label=elab
            )
            candidates = _cap_entity_link_candidates(candidates)
        except Exception as exc:
            logger.warning("entity_enrichment: canonicalize failed for %s: %s", mention, exc)
            continue

        if not candidates:
            logger.info(
                "entity_enrichment: WDQS returned hits for mention=%r but none passed strict "
                "entity-link gates (anchor=%s)",
                mention,
                resolved_qid,
            )
            continue

        try:
            repo.update_task_candidates(sibling["id"], candidates)
        except Exception as exc:
            logger.warning(
                "entity_enrichment: update_task_candidates failed for task %s: %s",
                sibling.get("id"), exc,
            )
            continue

        logger.info(
            "entity_enrichment: updated task %s (mention=%r) with %d candidates "
            "(type=%s in location=%s)",
            sibling["id"], mention, len(candidates), type_qid, resolved_qid,
        )
        updated += 1

    return updated
