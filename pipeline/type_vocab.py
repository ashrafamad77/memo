"""Seed vocabulary for E55_Type: canonical CamelCase name → Wikidata QID.

Two purposes:
1. Prompt guidance — the ModelingAgent is shown these names as preferred choices,
   which prevents it from inventing ad-hoc compounds like "Urbanvisit" or "Deepwork".
2. Zero-cost Wikidata — types with a wikidata_id in the seed get that QID directly in _resolve_one
   (no Babelfy search for WD). BabelNet synset metadata can still be filled via Wikidata→BabelNet
   when ``MEMO_E55_BABEL_FROM_WIKIDATA`` is on.

For types without a wikidata_id the name is still canonical (the prompt uses it), and
the type will be grounded once on first occurrence and cached in Neo4j forever.

Add entries freely; the vocabulary is intentionally open — it just bootstraps the cache
and guides naming conventions. QIDs are only included where confidence is high.
"""
from __future__ import annotations

from typing import Dict, List, Optional

# Each entry: wikidata_id (str or None), description (str), optional wikidata_label (English label
# of that WD item — may differ from our CamelCase ``name``).
# aat_id is not seeded here — too easy to hallucinate; AAT comes from the grounding pipeline.
_VocabEntry = Dict[str, Optional[str]]

SEED_VOCAB: Dict[str, _VocabEntry] = {
    # ── Activities (E7_Activity types) ────────────────────────────────────────
    # No seed QID: Wikidata Vector is noisy for one-word activity lemmas; legacy BabelNet uses
    # journal-aware sense ranking (see ``lookup_by_label_contextual``).
    "Visit":        {"wikidata_id": None,         "description": "act of going to see a person or place"},
    "Meeting":      {"wikidata_id": None,        "description": "encounter between people for a purpose"},
    "Meal":         {"wikidata_id": None,        "description": "eating occasion"},
    "Lunch":        {"wikidata_id": None,        "description": "midday meal"},
    "Dinner":       {"wikidata_id": None,        "description": "evening meal"},
    "Breakfast":    {"wikidata_id": None,        "description": "morning meal"},
    "Commute":      {"wikidata_id": None,        "description": "regular travel between home and work"},
    "Travel":       {"wikidata_id": None,        "description": "movement between places"},
    # Was Q1205510 — Wikidata returns entity as missing (removed/invalid); use canonical item.
    "Programming":  {"wikidata_id": "Q80006",   "description": "computer programming; designing and building programs"},
    "Reading":      {"wikidata_id": None,        "description": "act of reading"},
    "Writing":      {"wikidata_id": None,        "description": "act of writing"},
    "Teaching":     {"wikidata_id": None,        "description": "instruction of others"},
    "Exercise":     {"wikidata_id": None,        "description": "physical activity for health"},
    "Rest":         {"wikidata_id": None,        "description": "resting or relaxing"},
    "WakeUp":       {"wikidata_id": None,        "description": "waking from sleep"},
    "Sleep":        {"wikidata_id": "Q7369",     "description": "natural periodic state of rest"},
    "Conversation": {"wikidata_id": None,        "description": "interactive spoken communication between people"},
    "Shopping":     {"wikidata_id": None,        "description": "browsing and purchasing goods"},
    "Reflection":   {"wikidata_id": None,        "description": "introspective thought"},
    # Approx. "work session" / focused work block (Wikidata has no dedicated everyday lemma).
    "WorkSession":  {
        "wikidata_id": "Q31194416",
        "wikidata_label": "period of work",
        "description": "period of work; focused work session",
    },
    "Lecture":      {"wikidata_id": None,        "description": "oral presentation for teaching"},
    "Walk":         {"wikidata_id": None,        "description": "act of walking"},
    "PhoneCall":    {"wikidata_id": None,        "description": "telephone conversation"},
    "Cooking":      {"wikidata_id": None,        "description": "preparing food"},
    "Cleaning":     {"wikidata_id": None,        "description": "removing dirt or disorder"},
    "Errand":       {"wikidata_id": None,        "description": "short trip to perform a task"},
    # Used by graph_writer fallback when an activity is clearly conflict/war, not a visit.
    "ArmedConflict": {
        "wikidata_id": "Q350604",
        "wikidata_label": "armed conflict",
        "description": "conflict including violence where at least one of the acting groups is a state",
    },
    # Trip / intent planning (distinct from being physically at a place = Visit).
    "Planning": {
        "wikidata_id": "Q7201355",
        "wikidata_label": "cognitive planning",
        "description": "thought process",
    },

    # ── Place types (E53_Place types) ─────────────────────────────────────────
    "Library":       {"wikidata_id": "Q7075",    "description": "place for reading and borrowing books"},
    "Restaurant":    {"wikidata_id": "Q11707",   "description": "commercial food service establishment"},
    "TrainStation":  {"wikidata_id": "Q55488",   "description": "railway station"},
    "University":    {"wikidata_id": "Q3918",    "description": "higher education institution"},
    "Park":          {"wikidata_id": "Q22698",   "description": "public outdoor green space"},
    "Cafe":          {"wikidata_id": "Q30022",   "description": "coffeehouse or small eatery"},
    "Neighbourhood": {"wikidata_id": None,       "description": "district within a city"},
    "Country":       {"wikidata_id": "Q6256",    "description": "sovereign state or nation"},
    "City":          {"wikidata_id": "Q515",     "description": "city"},
    "Town":          {"wikidata_id": "Q3957",    "description": "town"},
    "Village":       {"wikidata_id": "Q532",     "description": "village"},
    "Continent":     {"wikidata_id": "Q5107",    "description": "continent"},
    "HumanSettlement": {"wikidata_id": "Q486972", "description": "human settlement (generic)"},
    "NationalPark":  {"wikidata_id": "Q46169",   "description": "national park or protected area"},
    "MountainRange": {"wikidata_id": "Q46831",   "description": "mountain range"},
    "Mountain":      {"wikidata_id": "Q8502",    "description": "mountain"},
    "Hill":          {"wikidata_id": "Q54050",   "description": "hill"},
    "Lake":          {"wikidata_id": "Q23397",   "description": "lake"},
    "River":         {"wikidata_id": "Q4022",    "description": "river"},
    "Island":        {"wikidata_id": "Q23442",   "description": "island"},
    "Archipelago":   {"wikidata_id": "Q25243",   "description": "archipelago"},
    "Ocean":         {"wikidata_id": "Q9430",    "description": "ocean"},
    "Sea":           {"wikidata_id": "Q165",     "description": "sea"},
    "Desert":        {"wikidata_id": "Q8514",    "description": "desert"},
    "Forest":        {"wikidata_id": "Q4421",    "description": "forest"},
    "Valley":        {"wikidata_id": "Q39816",   "description": "valley"},
    "Strait":        {"wikidata_id": "Q37901",   "description": "strait"},
    "Peninsula":     {"wikidata_id": "Q34763",   "description": "peninsula"},
    "Office":        {"wikidata_id": None,       "description": "room or building for professional work"},
    "Home":          {"wikidata_id": None,       "description": "one's place of residence"},
    "Airport":       {"wikidata_id": "Q1248784", "description": "aviation terminal facility"},
    "Hospital":      {"wikidata_id": "Q16917",   "description": "medical care facility"},
    "Supermarket":   {"wikidata_id": "Q180846",  "description": "large self-service grocery store"},
    "Museum":        {"wikidata_id": "Q33506",   "description": "institution for preserving artefacts"},
    "Gym":           {"wikidata_id": None,       "description": "fitness facility"},
    "Hotel":         {"wikidata_id": "Q27686",   "description": "establishment providing lodging"},

    # ── Mental states / attributes (E13 / E55) ────────────────────────────────
    "Hunger":        {"wikidata_id": "Q485513",  "description": "sensation of needing food"},
    "Fatigue":       {"wikidata_id": "Q178036",  "description": "state of tiredness"},
    "Stress":        {"wikidata_id": "Q183169",  "description": "mental or emotional strain"},
    "Joy":           {"wikidata_id": "Q132537",  "description": "feeling of happiness"},
    "Sadness":       {"wikidata_id": None,       "description": "emotional state of sorrow"},
    "Anxiety":       {"wikidata_id": None,       "description": "unpleasant mental unease"},
    "Satisfaction":  {"wikidata_id": None,       "description": "fulfillment of a need or desire"},
    "Boredom":       {"wikidata_id": "Q188522",  "description": "state of being bored"},
    "Nostalgia":     {"wikidata_id": None,       "description": "sentimental longing for the past"},
    "Gratitude":     {"wikidata_id": None,       "description": "feeling of thankfulness"},
    "Expectation":   {"wikidata_id": None,       "description": "belief that something will happen"},
    "Motivation":    {"wikidata_id": None,       "description": "reason for action"},
    "Frustration":   {"wikidata_id": None,       "description": "feeling of annoyance at obstacles"},

    # ── Concepts / habits (E28 / E89) ─────────────────────────────────────────
    "Habit":         {"wikidata_id": "Q169930",  "description": "settled or regular tendency or behaviour"},
    "Routine":       {"wikidata_id": None,       "description": "sequence of actions regularly followed"},
    "Goal":          {"wikidata_id": None,       "description": "desired outcome"},
    "Problem":       {"wikidata_id": None,       "description": "matter regarded as unwelcome or needing resolution"},
    "Plan":          {"wikidata_id": None,       "description": "intended future course of action"},
    "Decision":      {"wikidata_id": None,       "description": "conclusion or resolution reached after consideration"},
    "Idea":          {"wikidata_id": None,       "description": "mental representation or concept"},

    # ── Special ───────────────────────────────────────────────────────────────
    "User":          {"wikidata_id": None,       "description": "the journal author"},
}


def infer_place_type_name_from_mention(pname: str) -> str:
    """Map a free-text place name to a canonical E55_Type seed key for P7 venues.

    Used when the model did not supply a place type: avoids defaulting every unknown
    venue to ``Neighbourhood`` (e.g. names containing "library" → ``Library``).
    """
    p = (pname or "").strip()
    if not p:
        return "Neighbourhood"
    if get_seed_entry(p):
        return p
    pl = p.lower()
    if "library" in pl or "bibliothèque" in pl:
        return "Library"
    if "museum" in pl:
        return "Museum"
    if "restaurant" in pl:
        return "Restaurant"
    if "café" in pl or "cafe" in pl:
        return "Cafe"
    if "station" in pl:
        return "TrainStation"
    if "airport" in pl:
        return "Airport"
    if "hospital" in pl or "clinic" in pl:
        return "Hospital"
    if "park" in pl and "parking" not in pl:
        return "Park"
    if "hotel" in pl:
        return "Hotel"
    if "supermarket" in pl or "grocery" in pl:
        return "Supermarket"
    if "university" in pl or "college" in pl:
        return "University"
    if "office" in pl:
        return "Office"
    if "gym" in pl or "fitness" in pl:
        return "Gym"
    return "Neighbourhood"


def canonical_seed_name_for_qid(qid: str) -> Optional[str]:
    """Return the SEED_VOCAB key for *qid*, if any (stable canonical E55 name).

    Used so Wikidata-backed types always merge under the vocabulary key (e.g. ``HumanSettlement``
    for Q486972), not a legacy Neo4j typo such as ``Humansettlement`` from single-token labels.

    Also consults ``e53_wd_place_taxonomy`` so roots whose label maps to a seed key (e.g.
    Q2755753 → ``Neighbourhood``) resolve even when ``Neighbourhood`` has no ``wikidata_id`` row.
    """
    q = (qid or "").strip()
    if not q:
        return None
    for name, entry in SEED_VOCAB.items():
        wid = str(entry.get("wikidata_id") or "").strip()
        if wid == q:
            return name
    try:
        from .e53_wd_place_taxonomy import merged_e53_wd_place_checks

        for root, label in merged_e53_wd_place_checks():
            if root != q:
                continue
            if label in SEED_VOCAB:
                return label
    except ImportError:
        pass
    return None


def get_seed_entry(type_name: str) -> Optional[_VocabEntry]:
    """Return the seed entry for *type_name*, or None if not found.

    Tries exact match first, then case-insensitive.
    """
    if not type_name:
        return None
    if type_name in SEED_VOCAB:
        return SEED_VOCAB[type_name]
    lower = type_name.lower()
    for k, v in SEED_VOCAB.items():
        if k.lower() == lower:
            return v
    return None


def mention_to_type_qid(mention: str) -> Optional[str]:
    """Return the Wikidata concept QID for a place/group mention, via seed vocab lookup.

    Used by entity enrichment: "Library" -> Q7075 so SPARQL can find library instances
    in a resolved location. Tries CamelCase and compound-word normalizations.
    Returns None if no seeded QID maps to this mention.
    """
    m = (mention or "").strip()
    if not m:
        return None
    candidates = [
        m,                                              # "Library"
        m.capitalize(),                                 # "library" -> "Library"
        m.title(),                                      # "train station" -> "Train Station" (no match, but harmless)
        "".join(w.capitalize() for w in m.split()),    # "train station" -> "TrainStation"
    ]
    for key in candidates:
        entry = SEED_VOCAB.get(key)
        if entry and entry.get("wikidata_id"):
            return str(entry["wikidata_id"])
    # Case-insensitive fallback
    m_lower = m.lower()
    for k, v in SEED_VOCAB.items():
        if k.lower() == m_lower and v.get("wikidata_id"):
            return str(v["wikidata_id"])
    return None


def seed_type_names() -> List[str]:
    """Sorted list of canonical type names, for injection into the modeling prompt."""
    return sorted(SEED_VOCAB.keys())


def grounded_seed_names() -> List[str]:
    """Names that have a seeded wikidata_id (applied directly; no Babelfy WD search for them)."""
    return [name for name, entry in SEED_VOCAB.items() if entry.get("wikidata_id")]
