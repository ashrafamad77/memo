"""Ordered Wikidata class → E55_Type for E53 place typing via ``P31``/``P279*``.

More specific roots must appear **before** more general ones (e.g. ``NationalPark`` before ``Park``,
``MountainRange`` before ``Mountain``). Extend with ``MEMO_E53_WD_PLACE_TAXONOMY_EXTRA``.
"""
from __future__ import annotations

import os
import re
from typing import List, Tuple

# (Wikidata class QID, E55_Type name — must match a key in ``type_vocab.SEED_VOCAB`` for merge UX)
#
# Order: more specific roots before **Q486972 (human settlement)**. That class is a very broad
# ontology superclass — almost every place instance reaches it, so it must run *after*
# neighbourhood / district-like classes (e.g. London's Victoria is P31 Q2755753 ``area of London``,
# not ``city`` — without the rows below it would incorrectly be classified as HumanSettlement).
DEFAULT_E53_WD_PLACE_CHECKS: Tuple[Tuple[str, str], ...] = (
    ("Q5107", "Continent"),
    ("Q6256", "Country"),
    ("Q3624078", "Country"),
    ("Q515", "City"),
    ("Q3957", "Town"),
    ("Q532", "Village"),
    # Urban subdivisions (before Q486972 — see module docstring)
    ("Q2755753", "Neighbourhood"),  # area of London (en: "district of London")
    ("Q123705", "Neighbourhood"),  # neighbourhood
    ("Q486972", "HumanSettlement"),
    ("Q7075", "Library"),
    ("Q3918", "University"),
    ("Q33506", "Museum"),
    ("Q16917", "Hospital"),
    ("Q55488", "TrainStation"),
    ("Q1248784", "Airport"),
    ("Q11707", "Restaurant"),
    ("Q30022", "Cafe"),
    ("Q27686", "Hotel"),
    ("Q180846", "Supermarket"),
    ("Q46169", "NationalPark"),
    ("Q22698", "Park"),
    ("Q46831", "MountainRange"),
    ("Q8502", "Mountain"),
    ("Q54050", "Hill"),
    ("Q23397", "Lake"),
    ("Q4022", "River"),
    ("Q23442", "Island"),
    ("Q25243", "Archipelago"),
    ("Q9430", "Ocean"),
    ("Q165", "Sea"),
    ("Q8514", "Desert"),
    ("Q4421", "Forest"),
    ("Q39816", "Valley"),
    ("Q37901", "Strait"),
    ("Q34763", "Peninsula"),
)


def _parse_extra_taxonomy() -> List[Tuple[str, str]]:
    try:
        from config import MEMO_E53_WD_PLACE_TAXONOMY_EXTRA
    except ImportError:
        MEMO_E53_WD_PLACE_TAXONOMY_EXTRA = os.getenv("MEMO_E53_WD_PLACE_TAXONOMY_EXTRA", "")
    raw = (MEMO_E53_WD_PLACE_TAXONOMY_EXTRA or "").strip()
    if not raw:
        return []
    out: List[Tuple[str, str]] = []
    for part in raw.split(","):
        chunk = part.strip()
        if ":" not in chunk:
            continue
        qid, _, name = chunk.partition(":")
        qid = qid.strip().upper()
        name = re.sub(r"[^A-Za-z0-9_]", "", name.strip())
        if re.match(r"^Q\d+$", qid) and name:
            out.append((qid, name))
    return out


def merged_e53_wd_place_checks() -> Tuple[Tuple[str, str], ...]:
    """Default taxonomy plus ``QID:CamelCaseName`` pairs from env (appended)."""
    extra = _parse_extra_taxonomy()
    if not extra:
        return DEFAULT_E53_WD_PLACE_CHECKS
    return DEFAULT_E53_WD_PLACE_CHECKS + tuple(extra)
