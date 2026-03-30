"""Type Resolver: E55_Type reuse, name normalization, and Wikidata authority control."""
import difflib
import hashlib
import json
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import requests


# Shared across TypeResolver instances in one process (e.g. agentic resolve + writer post-pass).
_WIKIDATA_TERM_CACHE: Dict[str, Optional[Dict[str, str]]] = {}
_SPARQL_FORBIDDEN_CACHE: Dict[str, Optional[bool]] = {}
_SPARQL_CLASS_LABELS_CACHE: Dict[str, List[str]] = {}
_SPARQL_P31_ROOT_CACHE: Dict[str, Optional[bool]] = {}

# E53_Place guard: P31/P279* must reach this class. Default Q2221906 = geographic location.
_WD_DEFAULT_PLACE_TAXONOMY_ROOT = (
    (os.getenv("MEMO_WD_E53_PLACE_ROOT") or "Q2221906").strip() or "Q2221906"
)

_WIKIDATA_API = "https://www.wikidata.org/w/api.php"
_WDQS_URL = "https://query.wikidata.org/sparql"
_WIKIDATA_UA = "MemoJournalApp/1.0"
_WD_MIN_QUERY_LEN = 3
_WD_SEARCH_LIMIT = 12
# Lexical pre-filter (low): ontology + overlap decide acceptance, not string score alone.
_WD_SOFT_LEXICAL = 0.38
# Final adjusted score after overlap / bonuses (strict enough to avoid random matches).
_WD_FINAL_ACCEPT = 0.50
# Accept slightly lower lexical if journal ↔ ontology overlap is strong.
_WD_FINAL_LOOSE = 0.42
_WD_LOOSE_MIN_OVERLAP = 0.30
_WD_MAX_SEARCH_LEN = 200
_MEMO_WD_LLM_RERANK = os.getenv("MEMO_WIKIDATA_LLM_RERANK", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)
_MEMO_AAT_LOOKUP = os.getenv("MEMO_AAT_LOOKUP", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)
_GETTY_SPARQL = "https://vocab.getty.edu/sparql"
_GETTY_AAT_GRAPH = "http://vocab.getty.edu/aat"
_GETTY_UA = "MemoJournalApp/1.0"
_AAT_TERM_CACHE: Dict[str, Optional[Dict[str, str]]] = {}
# Ontological rejection: instance-of / subclass-of (P31 / P279*) must not reach these roots.
# Q12136 = disease, Q16521 = taxon, Q5 = human (wrong for non-person journal tags).
_WD_ROOT_DISEASE = "Q12136"
_WD_ROOT_TAXON = "Q16521"
_WD_ROOT_HUMAN = "Q5"

_JOURNAL_STOPWORDS = frozenset(
    """
    the and for are but not you all can had her was one our out day get has him his how its may new now old see two way who boy did get got let put say she too use her any few per own such than that this with have from they been call into like long make over such time very when come here just know take than them well were what will your about after again below each more most much some such than their there these those under where which while whose would could should
    """.split()
)


def _forbidden_roots_for_category(category: str) -> Tuple[str, ...]:
    """Wikidata Q-ids that must not appear in P31/P279* closure for this journal-tag category."""
    cat = _normalize_context_category(category)
    base = (_WD_ROOT_DISEASE, _WD_ROOT_TAXON)
    if cat == "person":
        return base
    if cat in (
        "place",
        "activity",
        "object",
        "concept",
        "organization",
        "transfer",
        "event",
        "state",
        "other",
    ):
        if cat == "state":
            return base
        return base + (_WD_ROOT_HUMAN,)
    return base + (_WD_ROOT_HUMAN,)


def _safe_wikidata_qid(raw: str) -> Optional[str]:
    s = (raw or "").strip().upper()
    return s if re.match(r"^Q\d+$", s) else None


def _sparql_wdqs(query: str) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(
            _WDQS_URL,
            params={"query": query, "format": "json"},
            headers={
                "User-Agent": _WIKIDATA_UA,
                "Accept": "application/sparql-results+json",
            },
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def wikidata_entity_forbidden_by_ontology(qid: str, category: str) -> Optional[bool]:
    """
    True  => P31/P279* reaches a forbidden root for this category (reject candidate).
    False => does not reach those roots.
    None  => WDQS error / timeout (caller should not treat as forbidden).
    """
    q = _safe_wikidata_qid(qid)
    if not q:
        return None
    roots = _forbidden_roots_for_category(category)
    if not roots:
        return False
    cache_key = f"{q}|{_normalize_context_category(category)}|{','.join(roots)}"
    if cache_key in _SPARQL_FORBIDDEN_CACHE:
        return _SPARQL_FORBIDDEN_CACHE[cache_key]
    vals = " ".join(f"wd:{x}" for x in roots)
    ask = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
ASK {{
  VALUES ?bad {{ {vals} }}
  wd:{q} wdt:P31/wdt:P279* ?bad .
}}
"""
    data = _sparql_wdqs(ask)
    if not data or "boolean" not in data:
        _SPARQL_FORBIDDEN_CACHE[cache_key] = None
        return None
    out = bool(data["boolean"])
    _SPARQL_FORBIDDEN_CACHE[cache_key] = out
    return out


def wikidata_entity_p31_reaches_root(qid: str, root_qid: str) -> Optional[bool]:
    """
    True  => wd:qid is compatible with the place taxonomy root:
    - instance chain: wdt:P31/wdt:P279* root (specific instances), or
    - class chain: wdt:P279* root (class items with no P31).
    False => SPARQL proved neither path exists.
    None  => WDQS error (caller should not treat as disproof).
    """
    q = _safe_wikidata_qid(qid)
    root = _safe_wikidata_qid(root_qid)
    if not q or not root:
        return None
    cache_key = f"pctax|{q}|{root}"
    if cache_key in _SPARQL_P31_ROOT_CACHE:
        return _SPARQL_P31_ROOT_CACHE[cache_key]
    ask = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
ASK {{
  {{
    wd:{q} wdt:P31/wdt:P279* wd:{root} .
  }} UNION {{
    wd:{q} wdt:P279* wd:{root} .
  }}
}}
"""
    data = _sparql_wdqs(ask)
    if not data or "boolean" not in data:
        _SPARQL_P31_ROOT_CACHE[cache_key] = None
        return None
    out = bool(data["boolean"])
    _SPARQL_P31_ROOT_CACHE[cache_key] = out
    return out


def wikidata_entity_class_labels_en(qid: str) -> List[str]:
    """English labels along P31/P279* (semantic neighborhood for overlap with journal text)."""
    q = _safe_wikidata_qid(qid)
    if not q:
        return []
    if q in _SPARQL_CLASS_LABELS_CACHE:
        return _SPARQL_CLASS_LABELS_CACHE[q]
    sel = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT ?lab WHERE {{
  wd:{q} wdt:P31/wdt:P279* ?c .
  ?c rdfs:label ?lab .
  FILTER(LANG(?lab) = "en")
}} LIMIT 40
"""
    data = _sparql_wdqs(sel)
    labs: List[str] = []
    if data and "results" in data:
        for b in data["results"].get("bindings", []):
            lab = b.get("lab", {}).get("value")
            if lab:
                labs.append(str(lab))
        _SPARQL_CLASS_LABELS_CACHE[q] = labs
    return labs


def _journal_keywords(text: str) -> Set[str]:
    words = re.findall(r"[a-z]{3,}", _casefold_type(text))
    return {w for w in words if w not in _JOURNAL_STOPWORDS}


def _ontology_journal_overlap(class_labels: List[str], extra_phrases: List[str], keywords: Set[str]) -> float:
    """0–1: how much Wikidata class / label text aligns with journal keywords (lightweight semantic hint)."""
    if not keywords:
        return 0.0
    matched: Set[str] = set()
    blob = class_labels + extra_phrases
    for phrase in blob:
        cf = _casefold_type(phrase)
        for w in re.findall(r"[a-z]{3,}", cf):
            if w in keywords:
                matched.add(w)
        for kw in keywords:
            if len(kw) >= 4 and kw in cf:
                matched.add(kw)
    return min(1.0, len(matched) / 5.0)


def _label_token_bonus(scoring_term: str, label: str, description: str) -> float:
    """Small boost when CamelCase tokens from the tag appear in Wikidata label/description."""
    blob = f"{label} {description}"
    b = _casefold_type(blob)
    bonus = 0.0
    for tok in _split_camel_tokens(scoring_term):
        tl = tok.lower()
        if len(tl) >= 4 and tl in b:
            bonus += 0.035
    return min(0.12, bonus)


def _truncate_wbsearch(q: str) -> str:
    q = (q or "").strip()
    if len(q) <= _WD_MAX_SEARCH_LEN:
        return q
    cut = q[:_WD_MAX_SEARCH_LEN].rsplit(" ", 1)[0].strip()
    return cut or q[:_WD_MAX_SEARCH_LEN]


# Glued CamelCase tails (domain-agnostic): "Xfacility", "Xsession", "Xarea", …
_E55_GLUED_SUFFIX = re.compile(
    r"^([A-Z][a-z\d]{2,})(facility|facilities|building|buildings|session|sessions|"
    r"area|areas|location|locations|center|centre|centers|centres|station|stations|"
    r"zone|zones|work|works|type|types)$",
    re.I,
)


def _split_glued_suffix_head_tail(st: str) -> Optional[Tuple[str, str]]:
    m = _E55_GLUED_SUFFIX.match((st or "").strip())
    if not m:
        return None
    return m.group(1), m.group(2).lower()


def _e55_wbsearch_phrase_variants(scoring_term: str) -> List[str]:
    """Split glued suffixes (Libraryfacility) and CamelCase; prefer short base queries for wbsearch."""
    st = (scoring_term or "").strip()
    seen: Set[str] = set()
    out: List[str] = []

    def add(s: str) -> None:
        s = (s or "").strip()
        if len(s) < _WD_MIN_QUERY_LEN:
            return
        k = s.casefold()
        if k in seen:
            return
        seen.add(k)
        out.append(_truncate_wbsearch(s))

    glued = _split_glued_suffix_head_tail(st)
    tokens = _split_camel_tokens(st)
    if glued:
        head, suf = glued
        add(" ".join([head, suf]))
        add(head)
    spaced = " ".join(tokens) if len(tokens) >= 2 else ""
    if spaced:
        add(spaced)
        if len(tokens) >= 2 and tokens[-1].lower() in ("facility", "facilities"):
            add(" ".join(tokens[:-1]))
    add(st)
    if "_" in st:
        add(st.replace("_", " "))
    return out


def _wikidata_wbsearch_variants(
    scoring_term: str,
    *,
    search_phrase: Optional[str] = None,
) -> List[str]:
    """
    wbsearchentities almost always returns no rows for long sentence-like strings
    (journal window + CamelCase + extra hints). Use short, entity-style queries and
    a spaced-CamelCase fallback (e.g. SocialActivity -> social activity).
    """
    seen: Set[str] = set()
    out: List[str] = []
    for cand in ((search_phrase or "").strip(), *_e55_wbsearch_phrase_variants(scoring_term)):
        if len(cand) < _WD_MIN_QUERY_LEN:
            continue
        k = cand.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(_truncate_wbsearch(cand))
    return out


def _llm_disambiguate_wikidata(
    journal_text: str,
    scoring_term: str,
    choices: List[Dict[str, str]],
) -> Optional[str]:
    """
    Optional: pick best Q-id when ontology-safe candidates stay ambiguous.
    Uses same Azure/OpenAI config as the rest of the pipeline. Disable with MEMO_WIKIDATA_LLM_RERANK=0.
    """
    if not _MEMO_WD_LLM_RERANK or len(choices) < 2:
        return None
    try:
        from config import (
            AZURE_OPENAI_API_KEY,
            AZURE_OPENAI_DEPLOYMENT,
            AZURE_OPENAI_ENDPOINT,
            AZURE_OPENAI_API_VERSION,
        )
    except ImportError:
        return None
    if not (AZURE_OPENAI_API_KEY or "").strip() or not (AZURE_OPENAI_ENDPOINT or "").strip():
        return None
    deployment = (AZURE_OPENAI_DEPLOYMENT or "gpt-4o-mini").strip()
    try:
        from openai import AzureOpenAI

        client = AzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT.rstrip("/"),
            api_version=AZURE_OPENAI_API_VERSION or "2024-12-01-preview",
        )
    except Exception:
        return None
    jt = (journal_text or "").strip()[:1200]
    lines = "\n".join(
        f"- {c.get('id')}: {c.get('label')} — {c.get('description', '')[:160]}"
        for c in choices[:6]
    )
    prompt = (
        f'Journal excerpt:\n"""{jt}"""\n\n'
        f'Journal tag / type to ground in Wikidata: "{scoring_term}"\n\n'
        f"Candidates (pick at most one Wikidata ID that fits the tag IN THIS JOURNAL CONTEXT, or null):\n{lines}\n\n"
        'Reply with ONLY JSON: {"id": "Q12345"} or {"id": null} . No markdown.'
    )
    try:
        res = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=80,
        )
        raw = (res.choices[0].message.content or "").strip()
        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            raw = m.group(0)
        data = json.loads(raw)
        qid = str(data.get("id") or "").strip().upper()
        if qid in ("NULL", "NONE", ""):
            return None
        return qid if re.match(r"^Q\d+$", qid) else None
    except Exception:
        return None


def _journal_text_cache_sig(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    return hashlib.sha256(t.encode("utf-8")).hexdigest()[:14]


# Wikidata descriptions for chart singles, tracks, etc. — high-precision phrases (not artist lists).
# Down-rank when a mundane type token (Stay, Run, Happy, …) matches a song/single item.
_WORK_OF_ART_DESC_FRAGMENTS: Tuple[str, ...] = (
    "single by",
    "song by",
    " ep by",
    "album by",
    "music video",
    "sound recording",
    "theme song",
    "soundtrack",
)

# Substrings in Wikidata label+description that clash with how we use journal "types".
_CATEGORY_DESC_PENALTY_TERMS: Dict[str, Tuple[str, ...]] = {
    "place": (
        "disease",
        "bacterium",
        "bacterial",
        "infectious disease",
        "pathogen",
        "tuberculosis",
        "album",
        "film",
        "television series",
        "disambiguation",
        "surname",
        "family name",
        "given name",
        "football player",
        "basketball player",
        "baseball player",
        "politician",
        "actor",
        "actress",
        "singer",
        "genus ",
        "species of",
        "wikimedia list",
        "video game",
        "computer game",
        "mobile game",
    ),
    "activity": (
        "disease",
        "bacterium",
        "bacterial",
        "infectious",
        "tuberculosis",
        "album",
        "film",
        "genus ",
        "species of",
        "medical condition",
        "pathogen",
        "video game",
        "computer game",
        "early access video game",
        "book (work)",
    ),
    "person": (
        "disease",
        "bacterium",
        "railway station",
        "metro station",
        "album",
        "film",
        "genus ",
    ),
    "organization": ("disease", "bacterium", "album", "genus ", "species of"),
    "state": (
        "album",
        "film",
        "bacterium",
        "railway station",
        "genus ",
        "species of",
        "infectious disease",
        "family name",
        "surname",
        "given name",
        "video game",
        "book (work)",
    ),
    "object": ("disease", "bacterium", "album", "genus ", "species of", "infectious"),
    "concept": ("disease", "bacterium", "album", "species ", "bacterium"),
    "transfer": ("disease", "bacterium", "album", "infectious"),
    "event": ("disease", "bacterium", "album", "genus ", "infectious disease"),
    "other": ("bacterium", "infectious disease", "pathogen", "genus ", "species of", "tuberculosis"),
}


def _casefold_type(s: str) -> str:
    return s.lower().replace("_", " ").replace("-", " ").strip()


def _split_camel_tokens(s: str) -> List[str]:
    spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
    spaced = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", spaced)
    return re.findall(r"[A-Za-z]{2,}|\d{3,}", spaced)


# Single-token E55 names that are everyday verbs/nouns — Wikidata is usually a song/film/person.
# We only allow wikidata_id / aat_id if the journal text shows that lemma (or common inflection).
_AMBIGUOUS_LEMMA_JOURNAL_PATTERNS: Dict[str, str] = {
    "stay": r"\b(stay|stays|stayed|staying|séjour|séjours|rester|resté|restée|restés|restées)\b",
    "go": r"\b(go|goes|going|went|gone)\b",
    "eat": r"\b(eat|eats|ate|eating|eaten)\b",
    "run": r"\b(run|runs|running|ran)\b",
    "walk": r"\b(walk|walks|walked|walking)\b",
    "sleep": r"\b(sleep|sleeps|slept|sleeping)\b",
    "wake": r"\b(wake|wakes|woke|woken|waking)\b",
    "play": r"\b(play|plays|played|playing)\b",
    "read": r"\b(read|reads|reading)\b",
    "write": r"\b(write|writes|wrote|written|writing)\b",
    "talk": r"\b(talk|talks|talked|talking)\b",
    "sit": r"\b(sit|sits|sat|sitting)\b",
    "stand": r"\b(stand|stands|stood|standing)\b",
    "leave": r"\b(leave|leaves|left|leaving)\b",
    "come": r"\b(come|comes|came|coming)\b",
    "get": r"\b(get|gets|got|getting|gotten)\b",
    "take": r"\b(take|takes|took|taken|taking)\b",
    "give": r"\b(give|gives|gave|given|giving)\b",
    "make": r"\b(make|makes|made|making)\b",
    "see": r"\b(see|sees|saw|seen|seeing)\b",
    "work": r"\b(work|works|worked|working)\b",
    "rest": r"\b(rest|rests|rested|resting)\b",
    "wait": r"\b(wait|waits|waited|waiting)\b",
    "stop": r"\b(stop|stops|stopped|stopping)\b",
    "start": r"\b(start|starts|started|starting)\b",
    "live": r"\b(live|lives|lived|living)\b",
    "love": r"\b(love|loves|loved|loving)\b",
    "hope": r"\b(hope|hopes|hoped|hoping)\b",
    "fear": r"\b(fear|fears|feared|fearing)\b",
    "think": r"\b(think|thinks|thought|thinking)\b",
    "feel": r"\b(feel|feels|felt|feeling)\b",
}


def _ambiguous_type_lemma(raw: str) -> Optional[str]:
    toks = _split_camel_tokens((raw or "").strip())
    if len(toks) != 1:
        return None
    lemma = _casefold_type(toks[0]).replace(" ", "")
    if lemma in _AMBIGUOUS_LEMMA_JOURNAL_PATTERNS:
        return lemma
    return None


def _journal_supports_ambiguous_lemma(lemma: str, journal_text: str) -> bool:
    if not (journal_text or "").strip():
        return False
    j = _casefold_type(journal_text)
    pat = _AMBIGUOUS_LEMMA_JOURNAL_PATTERNS.get(lemma)
    if not pat:
        return False
    return bool(re.search(pat, j))


def _ambiguous_type_blocks_authority(raw: str, journal_text: str) -> bool:
    """If True, skip Wikidata/AAT/LLM ids — type is a generic lemma absent from journal wording."""
    lem = _ambiguous_type_lemma(raw)
    if not lem:
        return False
    return not _journal_supports_ambiguous_lemma(lem, journal_text)


def _normalize_context_category(raw: str) -> str:
    c = (raw or "").strip().lower()
    aliases = {
        "activities": "activity",
        "action": "activity",
        "location": "place",
        "locations": "place",
        "people": "person",
        "org": "organization",
        "organisation": "organization",
        "orgs": "organization",
        "emotion": "state",
        "mental": "state",
        "feeling": "state",
        "feelings": "state",
    }
    return aliases.get(c, c) if c else "other"


def _category_penalty_multiplier(category: str, label: str, desc: str) -> float:
    cat = _normalize_context_category(category)
    if cat not in _CATEGORY_DESC_PENALTY_TERMS:
        cat = "other"
    blob = f"{_casefold_type(label)} {_casefold_type(desc)}"
    mult = 1.0
    for needle in _CATEGORY_DESC_PENALTY_TERMS[cat]:
        if needle in blob:
            mult *= 0.32
    for needle in _WORK_OF_ART_DESC_FRAGMENTS:
        if needle in blob:
            mult *= 0.18
    return max(mult, 0.08)


def _wikidata_hit_score(
    scoring_term: str,
    hit: Dict[str, Any],
    *,
    context_category: str = "",
) -> float:
    label = str(hit.get("label") or "")
    desc = str(hit.get("description") or "")
    match_obj = hit.get("match")
    match_text = ""
    if isinstance(match_obj, dict):
        match_text = str(match_obj.get("text") or "")

    q = _casefold_type(scoring_term)
    if not q:
        return 0.0
    l = _casefold_type(label)
    mt = _casefold_type(match_text)
    d = _casefold_type((desc or "")[:320])

    scores: List[float] = []
    for cand in (l, mt):
        if not cand:
            continue
        if q == cand:
            scores.append(1.0)
            continue
        if len(q) >= 4 and (q in cand or cand in q):
            scores.append(0.92)
            continue
        scores.append(difflib.SequenceMatcher(None, q, cand).ratio())

    q_tokens = {t.lower() for t in _split_camel_tokens(scoring_term) if len(t) >= 4}
    if not q_tokens and len(q) >= 4:
        q_tokens = {q}
    blob = f"{l} {d}"
    for t in q_tokens:
        if len(t) >= 4 and t in blob:
            scores.append(0.88)

    base = max(scores) if scores else 0.0
    if base <= 0:
        return 0.0
    mult = _category_penalty_multiplier(context_category, label, desc)
    return base * mult


def _infer_host_category(label: str) -> str:
    return {
        "E7_Activity": "activity",
        "E10_Transfer_of_Custody": "transfer",
        "E5_Event": "event",
        "E13_Attribute_Assignment": "state",
        "E53_Place": "place",
        "E21_Person": "person",
        "E74_Group": "organization",
        "E22_Human_Made_Object": "object",
        "E28_Conceptual_Object": "concept",
        "E89_Propositional_Object": "concept",
    }.get(label, "other")


_WSD_NER_FOR_CATEGORY: Dict[str, Tuple[str, ...]] = {
    "place": ("E53_Place",),
    "activity": ("E7_Activity",),
    "person": ("E21_Person",),
    "object": ("E22_Human_Made_Object",),
    "concept": ("E28_Conceptual_Object", "E89_Propositional_Object"),
    "organization": ("E74_Group",),
    "event": ("E5_Event",),
    "transfer": ("E10_Transfer_of_Custody",),
    "state": ("E13_Attribute_Assignment", "E28_Conceptual_Object"),
    "other": (),
}


def _normalize_wsd_ner_type(raw: str) -> str:
    s = (raw or "").strip().upper().replace(" ", "_")
    if s.startswith("E") and "_" in s:
        return s
    return (raw or "").strip()


def _wsd_row_requires_spatial(row: Optional[Dict[str, Any]]) -> bool:
    if not row or not isinstance(row, dict):
        return False
    return _normalize_wsd_ner_type(str(row.get("ner_type") or "")) == "E53_PLACE"


def _find_wsd_row_for_term(
    scoring_term: str,
    context_category: str,
    wsd_profile: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not wsd_profile or not isinstance(wsd_profile, dict):
        return None
    ents = wsd_profile.get("entities")
    if not isinstance(ents, list) or not ents:
        return None
    cat = _normalize_context_category(context_category)
    expected = _WSD_NER_FOR_CATEGORY.get(cat, ())
    st = _casefold_type(scoring_term).replace(" ", "")
    tokens = {
        _casefold_type(t).replace(" ", "")
        for t in _split_camel_tokens(scoring_term)
        if len(t) >= 3
    }
    best: Optional[Dict[str, Any]] = None
    best_score = 0
    for raw in ents:
        if not isinstance(raw, dict):
            continue
        mention = str(raw.get("mention") or "").strip()
        if not mention:
            continue
        m_cf = _casefold_type(mention).replace(" ", "")
        ner = str(raw.get("ner_type") or "").strip()
        score = 0
        if m_cf and (m_cf in st or st in m_cf):
            score += 100
        elif tokens:
            mtoks = set(re.findall(r"[a-z0-9]{3,}", m_cf))
            overlap = len(tokens & mtoks)
            if overlap:
                score += 35 * overlap
        if expected and ner in expected:
            score += 18
        elif expected and ner and ner not in expected:
            score -= 25
        if score > best_score:
            best_score = score
            best = raw
    if best_score < 28:
        return None
    return best


def _wsd_row_cache_sig(row: Optional[Dict[str, Any]]) -> str:
    if not row:
        return ""
    try:
        payload = json.dumps(
            {
                "m": row.get("mention"),
                "n": row.get("ner_type"),
                "s": row.get("disambiguation_sense"),
                "c": row.get("context_keywords"),
                "g": row.get("negative_keywords"),
            },
            sort_keys=True,
        )
    except (TypeError, ValueError):
        return "x"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def _expert_keywords_from_wsd(row: Dict[str, Any]) -> Set[str]:
    out: Set[str] = set()
    ctx = row.get("context_keywords")
    if isinstance(ctx, list):
        for k in ctx:
            for w in re.findall(r"[a-z]{3,}", _casefold_type(str(k))):
                if w not in _JOURNAL_STOPWORDS:
                    out.add(w)
    sense = str(row.get("disambiguation_sense") or "")
    for w in re.findall(r"[a-z]{3,}", _casefold_type(sense)):
        if w not in _JOURNAL_STOPWORDS:
            out.add(w)
    return out


def _wsd_negative_multiplier(label: str, desc: str, negatives: List[str]) -> float:
    if not negatives:
        return 1.0
    blob = f" {_casefold_type(label)} {_casefold_type(desc)} "
    m = 1.0
    for n in negatives:
        ncf = _casefold_type(str(n)).strip()
        if len(ncf) < 3:
            continue
        if re.search(rf"(?<![a-z0-9]){re.escape(ncf)}(?![a-z0-9])", blob):
            m *= 0.2
        elif ncf in blob:
            m *= 0.45
    return max(m, 0.05)


def _e55_target_ids_from_p141(edges: List[Dict[str, Any]]) -> Set[str]:
    out: Set[str] = set()
    for e in edges:
        if not isinstance(e, dict):
            continue
        if str(e.get("property", "")) != "P141_assigned":
            continue
        tid = str(e.get("to", "") or "").strip()
        if tid:
            out.add(tid)
    return out


def _type_item_name(entry: Any) -> str:
    if isinstance(entry, dict):
        return str(entry.get("name") or "").strip()
    return str(entry or "").strip()


def _type_item_category(entry: Any, host_label: str) -> str:
    if isinstance(entry, dict):
        c = str(entry.get("context_category") or "").strip()
        if c:
            return c
    return _infer_host_category(host_label)


def collect_e55_grounding_requests(spec: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Unique taxonomy labels from the graph spec for LLM authority grounding.
    Each item: name, context_category, host_label (CIDOC class hosting the type).
    """
    nodes = spec.get("nodes", [])
    edges = spec.get("edges", [])
    if not isinstance(nodes, list):
        return []
    if not isinstance(edges, list):
        edges = []
    p141_e55_ids = _e55_target_ids_from_p141(edges)
    cat_by_type: Dict[str, str] = {}
    host_by_type: Dict[str, str] = {}

    for node in nodes:
        if not isinstance(node, dict):
            continue
        lbl = str(node.get("label", ""))
        props = node.get("properties", {})
        if not isinstance(props, dict):
            props = {}

        if lbl == "E55_Type":
            nm = str(node.get("name") or "").strip()
            if not nm:
                continue
            cc = str(node.get("context_category") or props.get("context_category") or "").strip()
            if not cc and str(node.get("id") or "") in p141_e55_ids:
                cc = "state"
            if not cc:
                cc = "other"
            cat_by_type[nm] = _normalize_context_category(cc)
            host_by_type.setdefault(nm, "E55_Type")

        for t in node.get("types") or []:
            nm = _type_item_name(t)
            if not nm:
                continue
            cc = _type_item_category(t, lbl)
            cn = _normalize_context_category(cc)
            cat_by_type.setdefault(nm, cn)
            host_by_type.setdefault(nm, lbl)

    strings: Set[str] = set()
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("label") == "E55_Type":
            nm = str(node.get("name") or "").strip()
            if nm:
                strings.add(nm)
        for t in node.get("types") or []:
            nm = _type_item_name(t)
            if nm:
                strings.add(nm)

    return [
        {
            "name": nm,
            "context_category": cat_by_type.get(nm, "other"),
            "host_label": host_by_type.get(nm, "E55_Type"),
        }
        for nm in sorted(strings)
    ]


def _strip_resolver_fields_from_spec(spec: Dict[str, Any]) -> None:
    nodes = spec.get("nodes", [])
    if not isinstance(nodes, list):
        return
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node.pop("context_category", None)
        props = node.get("properties")
        if isinstance(props, dict):
            props.pop("context_category", None)
        types = node.get("types")
        if not isinstance(types, list):
            continue
        flat: List[str] = []
        for t in types:
            nm = _type_item_name(t)
            if nm:
                flat.append(nm)
        node["types"] = flat


def _wikidata_named_entity_penalty_mult(
    label: str,
    desc: str,
    scoring_term: str,
    keywords: Set[str],
) -> float:
    """
    Down-rank Wikidata hits that look like *named instances* (IDs, long proper names)
    when the journal tag is a short abstract type — without domain lists (hotel, library, …).
    """
    cf_l = _casefold_type(label)
    cf_d = _casefold_type((desc or "")[:400])
    blob = f"{cf_l} {cf_d}"
    st_cf = _casefold_type(scoring_term)
    st_alnum = re.sub(r"[^a-z0-9]+", "", st_cf)
    st_tokens = {w for w in re.findall(r"[a-z]{3,}", st_cf) if w not in _JOURNAL_STOPWORDS}
    blob_tokens = set(re.findall(r"[a-z]{3,}", blob))
    m = 1.0
    type_has_digit = bool(re.search(r"\d", st_alnum))
    label_has_long_id = bool(re.search(r"\d{4,}", cf_l))
    if label_has_long_id and not type_has_digit:
        m *= 0.5
    if st_tokens:
        tok_overlap = len(st_tokens & blob_tokens) / len(st_tokens)
        if tok_overlap < 0.35 and len(blob_tokens) > 14:
            m *= 0.62
    if len(st_alnum) <= 22 and not type_has_digit and len(cf_l) > max(28, 3 * max(8, len(st_alnum))):
        m *= 0.68
    kw_olap = _ontology_journal_overlap([], [label, desc], keywords)
    if kw_olap < 0.1 and label_has_long_id and not type_has_digit:
        m *= 0.55
    return max(m, 0.15)


def _aat_sanitize_phrase(phrase: str) -> str:
    return re.sub(r"[^a-z0-9 \-']+", " ", (phrase or "").lower()).strip()[:48]


def _aat_token_related(a: str, b: str) -> bool:
    if not a or not b:
        return False
    if a in b or b in a:
        return True
    n = min(len(a), len(b), 5)
    if n >= 4 and a[:n] == b[:n]:
        return True
    return False


def lookup_getty_aat_concept(
    phrase: str,
    context_category: str,
    journal_keywords: Optional[Set[str]] = None,
) -> Optional[Dict[str, str]]:
    """SPARQL lookup on Getty AAT (default graph). Returns aat_id + prefLabel."""
    if not _MEMO_AAT_LOOKUP:
        return None
    safe = _aat_sanitize_phrase(phrase)
    if len(safe) < 3:
        return None
    cat = _normalize_context_category(context_category)
    kw = journal_keywords or set()
    kw_sig = hashlib.sha256(" ".join(sorted(kw)).encode("utf-8")).hexdigest()[:12]
    cache_key = f"{safe}|{cat}|{kw_sig}"
    if cache_key in _AAT_TERM_CACHE:
        return _AAT_TERM_CACHE[cache_key]
    needles: Set[str] = {safe}
    if len(safe) >= 4 and not safe.endswith("s"):
        needles.add(safe + "s")
    if safe.endswith("y") and len(safe) >= 4:
        needles.add(safe[:-1] + "ies")
    parts: List[str] = []
    for n in sorted(needles):
        if len(n) < 3:
            continue
        escn = n.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'CONTAINS(LCASE(?pref), "{escn}")')
    if not parts:
        _AAT_TERM_CACHE[cache_key] = None
        return None
    filter_or = " || ".join(parts)
    q = f"""PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?c ?pref WHERE {{
  ?c skos:inScheme <http://vocab.getty.edu/aat/> ; skos:prefLabel ?pref .
  FILTER(LANG(?pref) = "en")
  FILTER({filter_or})
  FILTER(STRLEN(?pref) < 120)
}} LIMIT 50"""
    try:
        r = requests.get(
            _GETTY_SPARQL,
            params={
                "query": q,
                "format": "json",
                "default-graph-uri": _GETTY_AAT_GRAPH,
            },
            headers={"User-Agent": _GETTY_UA},
            timeout=14,
        )
        r.raise_for_status()
        data = r.json()
    except Exception:
        _AAT_TERM_CACHE[cache_key] = None
        return None
    bindings = (data.get("results") or {}).get("bindings") or []
    best_id: Optional[str] = None
    best_label: Optional[str] = None
    best_s = -999.0
    for b in bindings:
        cu = (b.get("c") or {}).get("value") or ""
        pref = (b.get("pref") or {}).get("value") or ""
        if not cu or not pref:
            continue
        pl = pref.lower()
        sid = cu.rsplit("/", 1)[-1]
        tokens_alpha = re.findall(r"[a-z]{3,}", pl)
        if not tokens_alpha:
            continue
        first_w = tokens_alpha[0]
        qtoks = {w for w in re.findall(r"[a-z]{3,}", safe) if w not in _JOURNAL_STOPWORDS}
        ref = qtoks | kw
        ratio = difflib.SequenceMatcher(None, safe, first_w).ratio()
        left_of_paren = (pref.split("(", 1)[0] if "(" in pref else pref).lower()
        left_tokens = re.findall(r"[a-z]{3,}", left_of_paren)
        if len(left_tokens) >= 2:
            t2 = left_tokens[1]
            if (
                t2 not in ref
                and not any(_aat_token_related(t2, r) for r in ref)
                and not any(_aat_token_related(t2, q) for q in qtoks)
            ):
                ratio *= 0.62
        s = 26.0 * ratio
        s += 16.0 * _ontology_journal_overlap([], [pref], kw)
        s -= min(7.0, max(0.0, (len(pl) - 36) * 0.1))
        tail_words = set(tokens_alpha)
        unrelated_tail = [
            t
            for t in tail_words
            if t not in ref
            and not any(_aat_token_related(t, r) for r in ref)
            and not any(_aat_token_related(t, q) for q in qtoks)
        ]
        if unrelated_tail:
            s -= min(18.0, 4.5 * len(unrelated_tail))
        for inner in re.findall(r"\(([^)]*)\)", pref):
            s += 7.0 * _ontology_journal_overlap([], [inner], kw)
        if s > best_s:
            best_s = s
            best_id = sid
            best_label = pref
    if best_id is None or best_s < 8.0:
        _AAT_TERM_CACHE[cache_key] = None
        return None
    out = {"aat_id": best_id, "label": best_label or "", "description": "Getty AAT"}
    _AAT_TERM_CACHE[cache_key] = out
    return out


def _e55_aat_query_phrases(raw_type_name: str) -> List[str]:
    st = (raw_type_name or "").strip()
    out: List[str] = []
    glued = _split_glued_suffix_head_tail(st)
    if glued:
        out.append(glued[0].lower())
        out.append(f"{glued[0]} {glued[1]}".lower())
    for t in _split_camel_tokens(st):
        tl = t.lower()
        if len(tl) >= 3:
            out.append(tl)
    if "_" in st:
        for p in st.replace("_", " ").split():
            pl = p.lower()
            if len(pl) >= 3:
                out.append(pl)
    seen: Set[str] = set()
    uniq: List[str] = []
    for p in out:
        k = p.strip()
        if len(k) < 3 or k in seen:
            continue
        seen.add(k)
        uniq.append(k)
    return uniq[:10]


class TypeResolver:
    """Queries existing E55_Type nodes, normalizes names, and grounds types in Wikidata."""

    def __init__(self, driver):
        self.driver = driver
        self._wikidata_cache: Dict[str, Optional[Dict[str, str]]] = {}

    def get_existing_types(self) -> List[str]:
        with self.driver.session() as s:
            result = s.run("MATCH (t:E55_Type) RETURN t.name AS name ORDER BY name")
            return [r["name"] for r in result if r["name"]]

    def find_e55_name_by_wikidata_id(self, wikidata_id: str) -> Optional[str]:
        qid = (wikidata_id or "").strip()
        if not qid:
            return None
        with self.driver.session() as s:
            row = s.run(
                """
                MATCH (t:E55_Type)
                WHERE t.wikidata_id = $qid
                RETURN t.name AS name
                LIMIT 1
                """,
                qid=qid,
            ).single()
            if row and row.get("name"):
                return str(row["name"])
            return None

    def get_wikidata_info(
        self,
        scoring_term: str,
        *,
        search_phrase: Optional[str] = None,
        context_category: str = "",
        journal_text: str = "",
        wsd_profile: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, str]]:
        """
        Search Wikidata (short query variants + merge), lexical + description penalties, then:
        - SPARQL ASK: reject if P31/P279* hits forbidden roots for context_category.
        - SPARQL SELECT: class labels + overlap boost vs journal keywords.
        Optional WSD profile: expert keywords + negatives; E53_Place rows require place taxonomy root.
        Cached per (term, category, journal hash, WSD row sig).
        """
        st = (scoring_term or "").strip()
        if not st or len(st) < _WD_MIN_QUERY_LEN:
            return None
        cat = _normalize_context_category(context_category)
        j_sig = _journal_text_cache_sig(journal_text)
        wsd_row = _find_wsd_row_for_term(st, cat, wsd_profile)
        wsd_sig = _wsd_row_cache_sig(wsd_row)
        cache_key = f"{st}\n{cat}\n{j_sig}\n{wsd_sig}"
        if cache_key in self._wikidata_cache:
            return self._wikidata_cache[cache_key]
        if cache_key in _WIKIDATA_TERM_CACHE:
            self._wikidata_cache[cache_key] = _WIKIDATA_TERM_CACHE[cache_key]
            return self._wikidata_cache[cache_key]
        variants = _wikidata_wbsearch_variants(st, search_phrase=search_phrase)
        if not variants:
            self._wikidata_cache[cache_key] = None
            _WIKIDATA_TERM_CACHE[cache_key] = None
            return None
        try:
            hits_by_id: Dict[str, Dict[str, Any]] = {}
            hit_order: List[str] = []
            for qv in variants:
                r = requests.get(
                    _WIKIDATA_API,
                    params={
                        "action": "wbsearchentities",
                        "search": qv,
                        "language": "en",
                        "format": "json",
                        "limit": _WD_SEARCH_LIMIT,
                    },
                    headers={"User-Agent": _WIKIDATA_UA},
                    timeout=12,
                )
                r.raise_for_status()
                data = r.json()
                for h in data.get("search") or []:
                    if not isinstance(h, dict):
                        continue
                    qid = str(h.get("id") or "")
                    if not qid or qid in hits_by_id:
                        continue
                    hits_by_id[qid] = h
                    hit_order.append(qid)
            hits = [hits_by_id[i] for i in hit_order]
            keywords = set(_journal_keywords(journal_text))
            if wsd_row:
                keywords |= _expert_keywords_from_wsd(wsd_row)
            soft_pool: List[Tuple[float, Dict[str, Any]]] = []
            for h in hits:
                if not isinstance(h, dict):
                    continue
                qid = str(h.get("id") or "")
                if not qid:
                    continue
                base = _wikidata_hit_score(st, h, context_category=cat)
                if base < _WD_SOFT_LEXICAL:
                    continue
                soft_pool.append((base, h))
            soft_pool.sort(key=lambda x: -x[0])

            survivors: List[Dict[str, Any]] = []
            for base, h in soft_pool:
                qid = _safe_wikidata_qid(str(h.get("id") or ""))
                if not qid:
                    continue
                if wikidata_entity_forbidden_by_ontology(qid, cat) is True:
                    continue
                label = str(h.get("label") or "")
                desc = str(h.get("description") or "")
                cls_labs = wikidata_entity_class_labels_en(qid)
                olap = _ontology_journal_overlap(cls_labs, [label, desc], keywords)
                adj = min(
                    1.0,
                    base * (1.0 + 0.28 * olap) + _label_token_bonus(st, label, desc),
                )
                if wsd_row:
                    negs = wsd_row.get("negative_keywords")
                    if isinstance(negs, list) and negs:
                        adj *= _wsd_negative_multiplier(
                            label, desc, [str(x) for x in negs if str(x).strip()]
                        )
                adj *= _wikidata_named_entity_penalty_mult(label, desc, st, keywords)
                ok = adj >= _WD_FINAL_ACCEPT or (
                    adj >= _WD_FINAL_LOOSE and olap >= _WD_LOOSE_MIN_OVERLAP
                )
                if not ok:
                    continue
                survivors.append(
                    {
                        "adj": adj,
                        "olap": olap,
                        "base": base,
                        "id": qid,
                        "label": label,
                        "description": desc,
                    }
                )
            survivors.sort(key=lambda x: -x["adj"])

            if wsd_row and _wsd_row_requires_spatial(wsd_row) and survivors:
                root = _safe_wikidata_qid(_WD_DEFAULT_PLACE_TAXONOMY_ROOT) or "Q2221906"
                kept: List[Dict[str, Any]] = []
                for s in survivors:
                    reach = wikidata_entity_p31_reaches_root(s["id"], root)
                    if reach is False:
                        continue
                    kept.append(s)
                survivors = kept

            out: Optional[Dict[str, str]] = None
            if survivors:
                use_llm = (
                    len(survivors) >= 2
                    and (survivors[0]["adj"] - survivors[1]["adj"]) < 0.09
                ) or (survivors[0]["adj"] < 0.56)
                picked_id: Optional[str] = None
                if use_llm:
                    picked_id = _llm_disambiguate_wikidata(
                        journal_text,
                        st,
                        [
                            {
                                "id": s["id"],
                                "label": s["label"],
                                "description": s["description"],
                            }
                            for s in survivors[:6]
                        ],
                    )
                if picked_id:
                    for s in survivors:
                        if s["id"] == picked_id:
                            out = {
                                "id": s["id"],
                                "label": s["label"],
                                "description": s["description"],
                            }
                            break
                if out is None:
                    top = survivors[0]
                    out = {
                        "id": top["id"],
                        "label": top["label"],
                        "description": top["description"],
                    }

            self._wikidata_cache[cache_key] = out
            _WIKIDATA_TERM_CACHE[cache_key] = out
            return out
        except Exception:
            self._wikidata_cache[cache_key] = None
            _WIKIDATA_TERM_CACHE[cache_key] = None
            return None

    @staticmethod
    def _label_to_camel(label: str) -> str:
        parts = re.findall(r"[A-Za-z0-9]+", label)
        if not parts:
            return label.strip() or "Type"
        out = []
        for p in parts:
            if len(p) == 1:
                out.append(p.upper())
            else:
                out.append(p[0].upper() + p[1:].lower())
        return "".join(out)

    def _closest_existing(self, norm: str, working: List[str]) -> Optional[str]:
        if not norm or not working:
            return None
        cf_norm = _casefold_type(norm)
        cf_keys = [_casefold_type(w) for w in working]
        hits = difflib.get_close_matches(cf_norm, cf_keys, n=1, cutoff=0.86)
        if not hits:
            return None
        for w, cf in zip(working, cf_keys):
            if cf == hits[0]:
                return w
        return None

    def _term_variants(self, norm: str, raw: str) -> List[str]:
        seen = set()
        out: List[str] = []
        for t in (norm, raw.strip(), norm.replace(" ", ""), raw.strip().replace(" ", "")):
            t = (t or "").strip()
            if t and t not in seen:
                seen.add(t)
                out.append(t)
        return out

    def _resolve_one(
        self,
        raw: str,
        working: List[str],
        *,
        journal_text: str,
        context_category: str,
        wsd_profile: Optional[Dict[str, Any]] = None,
        llm_grounding: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> Tuple[str, Optional[str], str, Optional[str]]:
        """Returns (canonical_name, wikidata_id, description, aat_id)."""
        raw = (raw or "").strip()
        norm = self.normalize_type_name(raw, working)
        block_ambiguous = _ambiguous_type_blocks_authority(raw, journal_text)
        if llm_grounding and not block_ambiguous:
            row = llm_grounding.get(raw)
            if isinstance(row, dict):
                wid = str(row.get("wikidata_id") or "").strip()
                aid = str(row.get("aat_id") or "").strip()
                desc = str(row.get("description") or "").strip()
                if wid and _safe_wikidata_qid(wid):
                    hit_name = self.find_e55_name_by_wikidata_id(wid)
                    if hit_name:
                        return hit_name, wid, desc, None
                    return norm, wid, desc, None
                if aid and not wid:
                    return norm, None, desc or "Getty AAT", aid
        if block_ambiguous:
            close = self._closest_existing(norm, working)
            if close:
                return close, None, "", None
            return norm, None, "", None
        info: Optional[Dict[str, str]] = None
        for tv in self._term_variants(norm, raw):
            info = self.get_wikidata_info(
                tv,
                context_category=context_category,
                journal_text=journal_text,
                wsd_profile=wsd_profile,
            )
            if info and info.get("id"):
                break
        if info and info.get("id"):
            qid = info["id"]
            desc = (info.get("description") or "").strip()
            hit_name = self.find_e55_name_by_wikidata_id(qid)
            if hit_name:
                return hit_name, qid, desc, None
            lab = (info.get("label") or norm).strip()
            canon = self.normalize_type_name(self._label_to_camel(lab), working)
            return canon, qid, desc, None
        aat_kw: Set[str] = set(_journal_keywords(journal_text))
        wsd_row = _find_wsd_row_for_term(raw, context_category, wsd_profile)
        if wsd_row:
            aat_kw |= _expert_keywords_from_wsd(wsd_row)
        aat: Optional[Dict[str, str]] = None
        for phrase in _e55_aat_query_phrases(raw):
            aat = lookup_getty_aat_concept(phrase, context_category, aat_kw)
            if aat and aat.get("aat_id"):
                break
        if aat and aat.get("aat_id"):
            alab = (aat.get("label") or "").strip()
            return norm, None, alab or (aat.get("description") or "Getty AAT"), str(aat["aat_id"])
        close = self._closest_existing(norm, working)
        if close:
            return close, None, "", None
        return norm, None, "", None

    def normalize_type_name(self, name: str, existing: List[str]) -> str:
        """CamelCase normalize and match against existing types."""
        if not name or not name.strip():
            return name
        clean = name.strip()
        lower = clean.lower().replace("_", " ").replace("-", " ")
        for ex in existing:
            if ex.lower().replace("_", " ").replace("-", " ") == lower:
                return ex
        return "".join(w.capitalize() for w in clean.split())

    def resolve_graph_spec(
        self,
        spec: Dict[str, Any],
        existing: Optional[List[str]] = None,
        *,
        journal_text: str = "",
        wsd_profile: Optional[Dict[str, Any]] = None,
        llm_grounding: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Normalize type strings, merge by Wikidata id, attach _e55_authority_meta; strip LLM helper fields."""
        if existing is None:
            existing = self.get_existing_types()

        if llm_grounding is not None:
            spec["_type_llm_grounding"] = llm_grounding
        eff_llm = spec.get("_type_llm_grounding")
        if not isinstance(eff_llm, dict):
            eff_llm = {}

        nodes = spec.get("nodes", [])
        edges = spec.get("edges", [])
        if not isinstance(nodes, list):
            nodes = []
        if not isinstance(edges, list):
            edges = []

        p141_e55_ids = _e55_target_ids_from_p141(edges)
        cat_by_type: Dict[str, str] = {}

        for node in nodes:
            if not isinstance(node, dict):
                continue
            lbl = str(node.get("label", ""))
            props = node.get("properties", {})
            if not isinstance(props, dict):
                props = {}

            if lbl == "E55_Type":
                nm = str(node.get("name") or "").strip()
                if not nm:
                    continue
                cc = str(node.get("context_category") or props.get("context_category") or "").strip()
                if not cc and str(node.get("id") or "") in p141_e55_ids:
                    cc = "state"
                if not cc:
                    cc = "other"
                cat_by_type[nm] = _normalize_context_category(cc)

            for t in node.get("types") or []:
                nm = _type_item_name(t)
                if not nm:
                    continue
                cc = _type_item_category(t, lbl)
                cat_by_type.setdefault(nm, _normalize_context_category(cc))

        strings: set = set()
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if node.get("label") == "E55_Type":
                nm = str(node.get("name") or "").strip()
                if nm:
                    strings.add(nm)
            for t in node.get("types") or []:
                nm = _type_item_name(t)
                if nm:
                    strings.add(nm)

        jt = journal_text or ""
        working = list(dict.fromkeys(existing))
        resolved_map: Dict[str, Tuple[str, Optional[str], str, Optional[str]]] = {}
        for s in sorted(strings):
            cat = cat_by_type.get(s, "other")
            resolved_map[s] = self._resolve_one(
                s,
                working,
                journal_text=jt,
                context_category=cat,
                wsd_profile=wsd_profile,
                llm_grounding=eff_llm if eff_llm else None,
            )
            canon = resolved_map[s][0]
            if canon not in working:
                working.append(canon)

        prior_auth = spec.get("_e55_authority_meta")
        authority: Dict[str, Dict[str, str]] = {}
        if isinstance(prior_auth, dict):
            for k, v in prior_auth.items():
                if not isinstance(v, dict):
                    continue
                q = str(v.get("wikidata_id") or "").strip()
                aid = str(v.get("aat_id") or "").strip()
                desc_p = str(v.get("description") or "").strip()
                if not q and not aid:
                    continue
                row: Dict[str, str] = {}
                if q:
                    row["wikidata_id"] = q
                if aid:
                    row["aat_id"] = aid
                if desc_p:
                    row["description"] = desc_p
                authority[str(k)] = row
        for canon, wid, desc, aid in resolved_map.values():
            if not wid and not aid and not desc:
                continue
            am = authority.setdefault(canon, {})
            if wid:
                am["wikidata_id"] = wid
                am.pop("aat_id", None)
            elif aid:
                am["aat_id"] = aid
            if desc:
                am["description"] = desc

        for node in nodes:
            if not isinstance(node, dict):
                continue
            if node.get("label") == "E55_Type":
                old = str(node.get("name") or "").strip()
                if old in resolved_map:
                    canon, wid, desc, aid = resolved_map[old]
                    node["name"] = canon
                    props = node.setdefault("properties", {})
                    if not isinstance(props, dict):
                        props = {}
                        node["properties"] = props
                    if wid:
                        props["wikidata_id"] = wid
                    elif not str(props.get("wikidata_id") or "").strip():
                        props.pop("wikidata_id", None)
                    if aid and not wid:
                        props["aat_id"] = aid
                    else:
                        props.pop("aat_id", None)
                    if desc:
                        props["description"] = desc
                    elif not str(props.get("description") or "").strip():
                        props.pop("description", None)
            types = node.get("types")
            if isinstance(types, list):
                new_types: List[str] = []
                for t in types:
                    ts = _type_item_name(t)
                    if not ts:
                        continue
                    if ts in resolved_map:
                        new_types.append(resolved_map[ts][0])
                    else:
                        new_types.append(self.normalize_type_name(ts, working))
                node["types"] = new_types

        for node in nodes:
            if not isinstance(node, dict) or node.get("label") != "E55_Type":
                continue
            nm = str(node.get("name") or "").strip()
            if not nm:
                continue
            props = node.get("properties")
            if not isinstance(props, dict):
                continue
            qw = str(props.get("wikidata_id") or "").strip()
            qa = str(props.get("aat_id") or "").strip()
            if qw and (
                nm not in authority
                or not str(authority[nm].get("wikidata_id") or "").strip()
            ):
                authority[nm] = {
                    "wikidata_id": qw,
                    "description": str(props.get("description") or ""),
                }
            elif qa and (
                nm not in authority
                or not str(authority[nm].get("aat_id") or "").strip()
            ) and not str(authority.get(nm, {}).get("wikidata_id") or "").strip():
                authority[nm] = {
                    "aat_id": qa,
                    "description": str(props.get("description") or ""),
                }

        spec["_e55_authority_meta"] = authority
        _strip_resolver_fields_from_spec(spec)
        return spec
