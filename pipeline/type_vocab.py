"""Seed vocabulary for E55_Type: canonical CamelCase name → Wikidata QID.

Two purposes:
1. Prompt guidance — the ModelingAgent is shown these names as preferred choices,
   which prevents it from inventing ad-hoc compounds like "Urbanvisit" or "Deepwork".
2. Zero-cost grounding — types with a wikidata_id skip Babelfy E55 grounding entirely;
   the mapping is applied directly in _resolve_one (TypeResolver).

For types without a wikidata_id the name is still canonical (the prompt uses it), and
the type will be grounded once on first occurrence and cached in Neo4j forever.

Add entries freely; the vocabulary is intentionally open — it just bootstraps the cache
and guides naming conventions. QIDs are only included where confidence is high.
"""
from __future__ import annotations

from typing import Dict, List, Optional

# Each entry: wikidata_id (str or None), description (str).
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
    "Programming":  {"wikidata_id": "Q1205510",  "description": "writing computer code"},
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
    "WorkSession":  {"wikidata_id": None,        "description": "focused work period"},
    "Lecture":      {"wikidata_id": None,        "description": "oral presentation for teaching"},
    "Walk":         {"wikidata_id": None,        "description": "act of walking"},
    "PhoneCall":    {"wikidata_id": None,        "description": "telephone conversation"},
    "Cooking":      {"wikidata_id": None,        "description": "preparing food"},
    "Cleaning":     {"wikidata_id": None,        "description": "removing dirt or disorder"},
    "Errand":       {"wikidata_id": None,        "description": "short trip to perform a task"},

    # ── Place types (E53_Place types) ─────────────────────────────────────────
    "Library":       {"wikidata_id": "Q7075",    "description": "place for reading and borrowing books"},
    "Restaurant":    {"wikidata_id": "Q11707",   "description": "commercial food service establishment"},
    "TrainStation":  {"wikidata_id": "Q55488",   "description": "railway station"},
    "University":    {"wikidata_id": "Q3918",    "description": "higher education institution"},
    "Park":          {"wikidata_id": "Q22698",   "description": "public outdoor green space"},
    "Cafe":          {"wikidata_id": "Q30022",   "description": "coffeehouse or small eatery"},
    "Neighbourhood": {"wikidata_id": None,       "description": "district within a city"},
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
    """Names that have a wikidata_id — these skip Babelfy E55 grounding entirely."""
    return [name for name, entry in SEED_VOCAB.items() if entry.get("wikidata_id")]
