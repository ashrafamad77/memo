"""E55 taxonomy hints: Babelfy CONCEPTS → BabelNet synsets → Wikidata candidates + synset metadata."""
from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from .babelnet_client import bundle_to_sources_json, enrich_babel_synset
from .babelfy_client import disambiguate
from .babelfy_entity_link import _annotation_score, _char_fragment_span, _find_mention_span, _spans_overlap
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

    for req in type_requests:
        name = str(req.get("name") or "").strip()
        if not name:
            continue

        best_sid: Optional[str] = None
        best_sc = -1.0
        best_ann: Optional[Dict[str, Any]] = None

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
            if not ok:
                continue
            if sc > best_sc:
                best_sc = sc
                best_sid = sid
                best_ann = ann

        if not best_sid:
            continue

        bundle = enrich_babel_synset(best_sid, api_key=key, target_lang=bab_lang)
        wd_list = list(bundle.get("wikidata_qids") or [])[:6]
        wn_ids = list(bundle.get("wordnet_ids") or [])
        gloss = str(bundle.get("gloss") or "").strip()

        candidates: List[Dict[str, str]] = []
        if wd_list:
            fetched = wikidata_fetch_labels_descriptions(wd_list)
            for q in wd_list:
                lab, dsc = fetched.get(q, ("", ""))
                candidates.append(
                    {
                        "qid": q,
                        "label": str(lab or "").strip(),
                        "description": str(dsc or "").strip(),
                    }
                )

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
