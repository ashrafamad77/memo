"""Instance linking via Babelfy → BabelNet synsets → Wikidata / WordNet / other mapped resources."""
from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from .babelnet_client import bundle_to_sources_json, enrich_babel_synset
from .babelfy_client import disambiguate
from .type_grounding_embed import wikidata_fetch_labels_descriptions
from .type_resolver import apply_entity_linking, collect_entity_linking_requests

logger = logging.getLogger(__name__)


def _resolve_babelfy_key(api_key: str) -> str:
    k = (api_key or "").strip()
    if k:
        return k
    try:
        from config import BABELFY_API_KEY

        return (BABELFY_API_KEY or "").strip()
    except ImportError:
        return os.getenv("BABELFY_API_KEY", "").strip()


def _resolve_babelfy_lang(lang_override: str) -> str:
    if (lang_override or "").strip():
        return (lang_override or "").strip().upper() or "EN"
    try:
        from config import MEMO_BABELFY_LANG

        return (MEMO_BABELFY_LANG or "EN").strip().upper() or "EN"
    except ImportError:
        return os.getenv("MEMO_BABELFY_LANG", "EN").strip().upper() or "EN"


def _char_fragment_span(cf: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    if not isinstance(cf, dict):
        return None
    try:
        start = int(cf.get("start"))
        end = int(cf.get("end"))
    except (TypeError, ValueError):
        return None
    if start < 0 or end < start:
        return None
    return start, end + 1


def _spans_overlap(a0: int, a1: int, b0: int, b1: int) -> bool:
    return max(a0, b0) < min(a1, b1)


def _find_mention_span(text: str, mention: str) -> Optional[Tuple[int, int]]:
    m = (mention or "").strip()
    if not m or not text:
        return None
    lo, ml = text.lower(), m.lower()
    idx = lo.find(ml)
    if idx >= 0:
        return idx, idx + len(m)
    parts = [p for p in re.split(r"\s+", m) if p]
    if len(parts) < 2:
        return None
    pattern = r"\s+".join(re.escape(p) for p in parts)
    mo = re.search(pattern, text, flags=re.IGNORECASE)
    if mo:
        return mo.start(), mo.end()
    return None


def _annotation_score(ann: Dict[str, Any]) -> float:
    for k in ("globalScore", "coherenceScore", "score"):
        v = ann.get(k)
        if isinstance(v, (int, float)):
            return float(v)
    return 0.0


def run_babelfy_entity_linking(
    journal_text: str,
    spec: Dict[str, Any],
    *,
    user_name: str = "",
    api_key: str = "",
    lang: str = "",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Match Babelfy annotations to E53/E21/E74 nodes; enrich via BabelNet ``getSynset``."""
    stats: Dict[str, Any] = {
        "ran": False,
        "annotations": 0,
        "with_synset": 0,
        "matched_nodes": 0,
        "babelfy_cache_hit": None,
    }

    key = _resolve_babelfy_key(api_key)
    if not key:
        logger.info("babelfy_entity_link: BABELFY_API_KEY unset, skipping")
        return spec, stats

    reqs = collect_entity_linking_requests(spec, user_name=user_name)
    if not reqs:
        return spec, stats

    bab_lang = _resolve_babelfy_lang(lang)
    text = journal_text or ""

    anns = disambiguate(
        text,
        api_key=key,
        lang=bab_lang,
        ann_type="NAMED_ENTITIES",
        ann_res="",
        stats=stats,
    )
    stats["ran"] = True
    stats["annotations"] = len(anns)

    # (span, synset_id, score, raw Babelfy annotation dict)
    candidates: List[Tuple[Tuple[int, int], str, float, Dict[str, Any]]] = []
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
        sc = _annotation_score(ann)
        candidates.append((sp, sid, sc, ann))

    el_results: Dict[str, Dict[str, Any]] = {}

    for req in reqs:
        name = str(req.get("name") or "").strip()
        if not name:
            continue

        span = _find_mention_span(text, name)
        best_sid: Optional[str] = None
        best_sc = -1.0
        best_ann: Optional[Dict[str, Any]] = None

        for sp, sid, sc, ann in candidates:
            s0, s1 = sp
            surface = text[s0:s1].strip() if text else ""
            ok = False
            if span and _spans_overlap(span[0], span[1], s0, s1):
                ok = True
            elif surface and (
                surface.casefold() == name.casefold()
                or name.casefold() in surface.casefold()
                or surface.casefold() in name.casefold()
            ):
                ok = True
            if not ok:
                continue
            if sc > best_sc:
                best_sc = sc
                best_sid = sid
                best_ann = ann

        if not best_sid:
            continue

        bundle = enrich_babel_synset(best_sid, api_key=key, target_lang=bab_lang)
        wd_list = list(bundle.get("wikidata_qids") or [])
        qid = wd_list[0] if wd_list else ""
        desc = str(bundle.get("gloss") or "").strip()
        if qid:
            fetched = wikidata_fetch_labels_descriptions([qid])
            pair = fetched.get(qid)
            if pair:
                lab, d2 = pair[0] or "", pair[1] or ""
                if d2:
                    desc = d2
                elif lab and not desc:
                    desc = lab
        wn_ids = list(bundle.get("wordnet_ids") or [])
        wn0 = wn_ids[0] if wn_ids else ""
        gloss_bn = str(bundle.get("gloss") or "").strip()
        bru = ""
        dpu = ""
        if isinstance(best_ann, dict):
            bru = str(best_ann.get("BabelNetURL") or "").strip()
            dpu = str(best_ann.get("DBpediaURL") or "").strip()

        el_results[name] = {
            "wikidata_id": qid,
            "description": desc,
            "babel_synset_id": best_sid,
            "wordnet_synset_id": wn0,
            "babel_gloss": gloss_bn,
            "babelnet_rdf_url": bru,
            "dbpedia_url": dpu,
            "babelnet_sources_json": bundle_to_sources_json(
                bundle, babelfy_ann=best_ann
            ),
        }
        stats["matched_nodes"] += 1

    if not el_results:
        return spec, stats

    spec = apply_entity_linking(spec, el_results, user_name=user_name)
    return spec, stats
