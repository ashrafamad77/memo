"""Entity-link candidate helpers: cap, Wikidata fetch + P31 / coherence / ontology gates."""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)

_ENTITY_LINK_CONF_ORDER = {"high": 0, "medium": 1, "low": 2}


def entity_link_max_candidates() -> int:
    try:
        n = int(os.getenv("MEMO_ENTITY_LINK_MAX_CANDIDATES", "3"))
    except ValueError:
        n = 3
    return max(1, min(n, 10))


def cap_entity_link_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep at most N options: higher confidence first, then original order."""
    if not candidates:
        return []
    cap = entity_link_max_candidates()
    indexed = list(enumerate(candidates))
    indexed.sort(
        key=lambda ic: (
            _ENTITY_LINK_CONF_ORDER.get(str(ic[1].get("confidence") or "medium").lower(), 9),
            ic[0],
        )
    )
    return [ic[1] for ic in indexed[:cap]]


def wikidata_coheres_with_mention(mention: str, label: str, description: str) -> bool:
    """Reject QIDs whose English label/description does not relate to the journal mention."""
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


def canonicalize_entity_link_candidates(
    entity_name: str,
    candidates: List[Dict[str, Any]],
    *,
    cidoc_label: str = "",
) -> List[Dict[str, Any]]:
    """Replace labels with Wikidata API truth; drop QIDs that fail gates."""
    if not candidates:
        return []
    try:
        from .type_grounding_embed import (
            entity_link_qid_plausible_instance,
            wikidata_batch_p31_blocklist_filter,
            wikidata_fetch_labels_descriptions,
        )
    except ImportError:
        return candidates

    qids = list({c["wikidata_id"] for c in candidates})
    fetched = wikidata_fetch_labels_descriptions(qids)

    instance_class = (cidoc_label or "").strip() in ("E53_Place", "E21_Person", "E74_Group")
    p31_rejected: Set[str] = set()
    if instance_class:
        p31_rejected = wikidata_batch_p31_blocklist_filter(qids, cidoc_label)

    out: List[Dict[str, Any]] = []
    for c in candidates:
        qid = str(c["wikidata_id"] or "").strip().upper()

        if qid in p31_rejected:
            continue

        pair = fetched.get(qid)
        if not pair:
            logger.info(
                "dropped QID %s for %r (not returned by Wikidata)",
                qid,
                entity_name,
            )
            continue
        lab, desc = pair[0] or "", pair[1] or ""
        if not (lab or desc):
            continue

        if not wikidata_coheres_with_mention(entity_name, lab, desc):
            logger.info(
                "dropped QID %s for mention %r (Wikidata label %r)",
                qid,
                entity_name,
                lab,
            )
            continue

        plausible = entity_link_qid_plausible_instance(qid, cidoc_label, description=desc)
        if plausible is False:
            logger.info(
                "dropped QID %s for %r — not a plausible %s instance (ontology)",
                qid,
                entity_name,
                cidoc_label or "entity",
            )
            continue
        out.append({
            "wikidata_id": qid,
            "label": lab,
            "description": desc,
            "confidence": c["confidence"],
        })
    return out
