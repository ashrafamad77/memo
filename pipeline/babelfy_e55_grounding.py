"""E55 taxonomy hints: Babelfy CONCEPTS → BabelNet synsets → Wikidata candidates + synset metadata."""
from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from .babelnet_client import bundle_to_sources_json, enrich_babel_synset
from .babelfy_client import disambiguate
from .babelfy_entity_link import _annotation_score, _char_fragment_span, _find_mention_span, _spans_overlap
from .semantic_gate import is_coherent
from .type_grounding_embed import wikidata_fetch_labels_descriptions

logger = logging.getLogger(__name__)


def collect_babelfy_evidence(
    text: str,
    *,
    api_key: str,
    lang: str = "",
    ann_type: str = "CONCEPTS",
) -> List[Dict[str, Any]]:
    """Return ALL Babelfy annotations for ``text``, each enriched with BabelNet gloss + Wikidata candidates.

    Unlike ``run_babelfy_e55_grounding`` (which only returns span-matched types),
    this returns everything Babelfy found — used as context for the combined LLM resolver.

    Each item in the returned list::

        {
            "surface": "library",        # text slice at annotation span
            "synset_id": "bn:00050967n",
            "score": 0.0,
            "wikidata_candidates": [],   # from BabelNet getSynset
            "wordnet_id": "library%1:06:01::",
            "gloss": "library",
            "babelnet_url": "...",
            "dbpedia_url": "...",
        }

    Calls are cached (``babelfy_client`` LRU + ``babelnet_client`` LRU) so calling this
    alongside ``run_babelfy_e55_grounding`` for the same text adds no extra API cost.
    """
    key = (api_key or "").strip()
    if not key or not (text or "").strip():
        return []
    try:
        from config import MEMO_BABELFY_LANG
        bab_lang = (lang or MEMO_BABELFY_LANG or "EN").strip().upper() or "EN"
    except ImportError:
        bab_lang = (lang or os.getenv("MEMO_BABELFY_LANG", "EN")).strip().upper() or "EN"

    try:
        anns = disambiguate(text, api_key=key, lang=bab_lang, ann_type=ann_type, ann_res="")
    except Exception as exc:
        logger.debug("collect_babelfy_evidence(%s): %s", ann_type, exc)
        return []

    # Fetch BabelNet bundles for unique synsets (cached)
    unique_sids = list({
        str(a.get("babelSynsetID") or "")
        for a in anns
        if isinstance(a, dict) and str(a.get("babelSynsetID") or "").startswith("bn:")
    })
    bundles: Dict[str, Dict[str, Any]] = {}
    for sid in unique_sids:
        try:
            bundles[sid] = enrich_babel_synset(sid, api_key=key, target_lang=bab_lang)
        except Exception:
            bundles[sid] = {}

    out: List[Dict[str, Any]] = []
    for ann in anns:
        if not isinstance(ann, dict):
            continue
        sid = str(ann.get("babelSynsetID") or "").strip()
        if not sid.startswith("bn:"):
            continue
        cf = ann.get("charFragment") or {}
        sp = _char_fragment_span(cf if isinstance(cf, dict) else {})
        surface = text[sp[0]:sp[1]].strip() if sp and text else ""
        bundle = bundles.get(sid, {})
        wd_list = list(bundle.get("wikidata_qids") or [])[:4]
        wn_ids = list(bundle.get("wordnet_ids") or [])
        gloss = str(bundle.get("gloss") or "").strip()
        out.append({
            "surface": surface,
            "synset_id": sid,
            "score": _annotation_score(ann),
            "wikidata_candidates": wd_list,
            "wordnet_id": wn_ids[0] if wn_ids else "",
            "gloss": gloss,
            "babelnet_url": str(ann.get("BabelNetURL") or "").strip(),
            "dbpedia_url": str(ann.get("DBpediaURL") or "").strip(),
        })
    return out


# Fallback row when Babelfy/BabelNet yields no synset or Wikidata candidates (TypeResolver).
DEFAULT_E55_LOW_ROW: Dict[str, Any] = {
    "confidence": "low",
    "wikidata_candidates": [],
    "aat_id": "",
    "aat_label": "",
    "aat_confidence": "low",
    "description": "",
}


def _type_surface_variants(name: str) -> List[str]:
    n = (name or "").strip()
    if not n:
        return []
    out: List[str] = []
    seen = set()

    def add(s: str) -> None:
        t = s.strip()
        if t and t not in seen:
            seen.add(t)
            out.append(t)

    add(n)
    add(re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", n))
    add(n.replace("_", " "))
    add(re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", n.replace("_", " ")))
    return out


def run_babelfy_e55_grounding(
    journal_text: str,
    type_requests: List[Dict[str, str]],
    *,
    api_key: str,
    lang: str = "",
    wsd_profile: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Grounding rows for ``resolve_graph_spec``: ``babel_synset_id`` + ``wikidata_candidates`` from synset."""
    del wsd_profile  # reserved for future WSD-aware filtering
    stats: Dict[str, Any] = {
        "ran": False,
        "annotations": 0,
        "with_synset": 0,
        "types_matched": 0,
        "babelfy_cache_hit": None,
    }
    out: Dict[str, Dict[str, Any]] = {}

    if not type_requests:
        return out, stats

    key = (api_key or "").strip()
    if not key:
        return out, stats

    try:
        from config import MEMO_BABELFY_E55, MEMO_BABELFY_LANG

        if str(MEMO_BABELFY_E55 or "1").strip().lower() in ("0", "false", "no"):
            return out, stats
        bab_lang = (lang or MEMO_BABELFY_LANG or "EN").strip().upper() or "EN"
    except ImportError:
        if os.getenv("MEMO_BABELFY_E55", "1").strip().lower() in ("0", "false", "no"):
            return out, stats
        bab_lang = (lang or os.getenv("MEMO_BABELFY_LANG", "EN")).strip().upper() or "EN"

    text = journal_text or ""
    anns = disambiguate(
        text,
        api_key=key,
        lang=bab_lang,
        ann_type="CONCEPTS",
        ann_res="",
        stats=stats,
    )
    stats["ran"] = True
    stats["annotations"] = len(anns)

    concept_hits: List[Tuple[Tuple[int, int], str, float, Dict[str, Any]]] = []
    for ann in anns:
        if not isinstance(ann, dict):
            continue
        sid = str(ann.get("babelSynsetID") or "").strip()
        if not sid.startswith("bn:"):
            continue
        stats["with_synset"] += 1
        cf = ann.get("charFragment") or {}
        sp = _char_fragment_span(cf if isinstance(cf, dict) else {})
        if not sp:
            continue
        concept_hits.append((sp, sid, _annotation_score(ann), ann))

    if not concept_hits:
        return out, stats

    # Category → short phrase describing what kind of concept is expected.
    # Used as the gate context instead of the full journal text so multi-topic
    # journals don't bleed irrelevant domain signal into the comparison.
    _CAT_HINTS: Dict[str, str] = {
        "activity":     "human activity action behavior social interaction",
        "event":        "event occurrence experience happening",
        "concept":      "abstract concept category idea type",
        "object":       "physical object artifact material thing",
        "state":        "mental state condition emotion feeling",
        "place":        "geographic location place spatial",
        "person":       "person human individual",
        "organization": "organization group institution",
        "transfer":     "transfer exchange give receive",
        "other":        "concept category type",
    }

    for req in type_requests:
        name = str(req.get("name") or "").strip()
        if not name:
            continue
        category = str(req.get("context_category") or "other").strip().lower() or "other"
        gate_context = _CAT_HINTS.get(category, _CAT_HINTS["other"])

        # Collect all surface-matched candidates for this type, sorted best-score first.
        # The semantic gate is applied in order so that if the top Babelfy pick is
        # wrong-domain (e.g. "Conversation tart") a lower-ranked but correct synset
        # (e.g. bn:00022349n "conversation, verbal exchange") can still be accepted.
        ranked: List[Tuple[float, str, Dict[str, Any]]] = []
        for sp, sid, sc, ann in concept_hits:
            s0, s1 = sp
            surface = text[s0:s1].strip() if text else ""
            ok = False
            for variant in _type_surface_variants(name):
                span = _find_mention_span(text, variant)
                if span and _spans_overlap(span[0], span[1], s0, s1):
                    ok = True
                    break
                if surface and (
                    surface.casefold() == variant.casefold()
                    or variant.casefold() in surface.casefold()
                    or surface.casefold() in variant.casefold()
                ):
                    ok = True
                    break
            if ok:
                ranked.append((sc, sid, ann))

        if not ranked:
            continue
        ranked.sort(key=lambda t: t[0], reverse=True)

        accepted_sid: Optional[str] = None
        accepted_sc: float = -1.0
        accepted_ann: Optional[Dict[str, Any]] = None
        accepted_bundle: Dict[str, Any] = {}
        accepted_candidates: List[Dict[str, str]] = []
        accepted_gloss: str = ""

        for best_sc, best_sid, best_ann in ranked:
            bundle = enrich_babel_synset(best_sid, api_key=key, target_lang=bab_lang)
            wd_list_raw = list(bundle.get("wikidata_qids") or [])[:6]
            wn_ids_raw = list(bundle.get("wordnet_ids") or [])
            gloss_raw = str(bundle.get("gloss") or "").strip()

            cands_raw: List[Dict[str, str]] = []
            if wd_list_raw:
                fetched = wikidata_fetch_labels_descriptions(wd_list_raw)
                for q in wd_list_raw:
                    lab, dsc = fetched.get(q, ("", ""))
                    cands_raw.append(
                        {
                            "qid": q,
                            "label": str(lab or "").strip(),
                            "description": str(dsc or "").strip(),
                        }
                    )

            # Semantic coherence gate: compare the concept description against the
            # category hint (not the full journal) so multi-topic journals don't bleed
            # unrelated domain signal (e.g. "music" boosting "album by Twinz").
            # Use only Wikidata descriptions — the BabelNet gloss is usually just the
            # headword repeated and adds no discriminating power.
            cand_parts = [c.get("description", "") for c in cands_raw[:2]]
            # Fall back to gloss only when Wikidata returned no descriptions.
            if not any(cand_parts):
                cand_parts = [gloss_raw]
            candidate_text = " ".join(p for p in cand_parts if p)
            if not is_coherent(gate_context, candidate_text):
                logger.debug(
                    "E55 semantic gate: rejected type=%r synset=%s gloss=%r — trying next candidate",
                    name, best_sid, gloss_raw,
                )
                continue

            accepted_sid = best_sid
            accepted_sc = best_sc
            accepted_ann = best_ann
            accepted_bundle = bundle
            accepted_candidates = cands_raw
            accepted_gloss = gloss_raw
            break

        if not accepted_sid:
            continue

        best_sid = accepted_sid
        best_sc = accepted_sc
        best_ann = accepted_ann
        bundle = accepted_bundle
        candidates = accepted_candidates
        gloss = accepted_gloss
        wd_list = list(bundle.get("wikidata_qids") or [])[:6]
        wn_ids = list(bundle.get("wordnet_ids") or [])

        conf = "high" if best_sc >= 0.2 else "medium"
        bru = ""
        dpu = ""
        if isinstance(best_ann, dict):
            bru = str(best_ann.get("BabelNetURL") or "").strip()
            dpu = str(best_ann.get("DBpediaURL") or "").strip()

        out[name] = {
            "confidence": conf,
            "wikidata_candidates": candidates,
            "aat_id": "",
            "aat_label": "",
            "aat_confidence": "low",
            "description": gloss,
            "babel_synset_id": best_sid,
            "wordnet_synset_id": wn_ids[0] if wn_ids else "",
            "babel_gloss": gloss,
            "babelnet_rdf_url": bru,
            "dbpedia_url": dpu,
            "babelnet_sources_json": bundle_to_sources_json(
                bundle, babelfy_ann=best_ann
            ),
        }
        stats["types_matched"] += 1

    return out, stats
