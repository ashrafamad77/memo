"""Generic CIDOC CRM Graph Writer.

Executes a graph spec (nodes + edges) produced by the Modeling Agent.
Minimal structural completeness only; E13 P141 may be inferred from text when the
agent omits a concrete E55_Type (see _infer_e13_p141_type).
"""
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _merge_babelfy_e55_into_spec_grounding(
    spec: Dict[str, Any],
    journal_text: str,
    wsd_profile: Optional[Dict[str, Any]],
) -> None:
    """Ground E55 names missing from ``_type_llm_grounding`` using Babelfy + combined LLM resolver.

    Called inside ``_write_tx`` after normalization adds auto types (Visit, Neighbourhood, etc.)
    that were not present when the agentic resolver ran. Uses the combined Babelfy+LLM approach
    so abstract types like "Visit" get proper Wikidata QIDs even when they don't appear in text.
    """
    try:
        from config import BABELFY_API_KEY
    except ImportError:
        BABELFY_API_KEY = ""
    key = (BABELFY_API_KEY or "").strip()
    if not key:
        return
    try:
        from .babelfy_e55_grounding import (
            collect_babelfy_evidence,
            run_babelfy_e55_grounding,
        )
    except ImportError:
        return
    nodes = spec.get("nodes")
    if not isinstance(nodes, list):
        return
    eff = spec.get("_type_llm_grounding")
    if isinstance(eff, dict):
        eff = dict(eff)
    else:
        eff = {}
    missing: List[str] = []
    seen = set(eff.keys())
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("label", "")) != "E55_Type":
            continue
        nm = str(node.get("name") or "").strip()
        if nm and nm not in seen:
            missing.append(nm)
            seen.add(nm)
    if not missing:
        return
    extra_reqs = [
        {"name": nm, "context_category": "other", "host_label": "E55_Type"}
        for nm in sorted(missing)
    ]
    try:
        bf_out, _st = run_babelfy_e55_grounding(
            journal_text or "", extra_reqs, api_key=key, wsd_profile=wsd_profile,
        )
    except Exception as exc:
        logger.debug("graph_writer: Babelfy E55 supplement skipped: %s", exc)
        bf_out = {}

    # Collect all Babelfy concept evidence (cached — same text already called upstream)
    concept_evidence: List[Any] = []
    try:
        concept_evidence = collect_babelfy_evidence(
            journal_text or "", api_key=key, ann_type="CONCEPTS"
        )
    except Exception:
        pass

    # Combined LLM resolver for auto-added types using full Babelfy context
    combined_e55_rows: Dict[str, Any] = {}
    try:
        from .llm_kb_fallback import llm_resolve_all_with_babelfy
        combined_e55_rows, _ = llm_resolve_all_with_babelfy(
            type_requests=extra_reqs,
            entity_requests=[],
            journal_text=journal_text or "",
            concept_evidence=concept_evidence,
            ne_evidence=[],
            babelfy_type_hits=bf_out,
            skip_type_names=set(),
        )
    except Exception as exc:
        logger.debug("graph_writer: combined LLM resolver for auto-types skipped: %s", exc)

    changed = False
    for nm in missing:
        bf_row = bf_out.get(nm)
        llm_row = combined_e55_rows.get(nm)
        has_bf_synset = isinstance(bf_row, dict) and str(
            bf_row.get("babel_synset_id") or ""
        ).startswith("bn:")
        if has_bf_synset and llm_row:
            merged = dict(bf_row)
            if llm_row.get("wikidata_candidates"):
                merged["wikidata_candidates"] = llm_row["wikidata_candidates"]
                merged["description"] = llm_row.get("description") or merged.get("description", "")
            eff[nm] = merged
            changed = True
        elif has_bf_synset:
            eff[nm] = bf_row
            changed = True
        elif llm_row and llm_row.get("wikidata_candidates"):
            eff[nm] = llm_row
            changed = True
    if changed:
        spec["_type_llm_grounding"] = eff


VALID_LABELS = {
    "E5_Event",
    "E7_Activity",
    "E10_Transfer_of_Custody",
    "E13_Attribute_Assignment",
    "E21_Person",
    "E22_Human_Made_Object",
    "E28_Conceptual_Object",
    "E39_Actor",
    "E52_Time_Span",
    "E53_Place",
    "E55_Type",
    "E73_Information_Object",
    "E74_Group",
    "E89_Propositional_Object",
}

VALID_PROPERTIES = {
    "P2_has_type",
    "P4_has_time_span",
    "P7_took_place_at",
    "P14_carried_out_by",
    "P14i_performed",
    "P15_was_influenced_by",
    "P17_was_motivated_by",
    "P28_custody_surrendered_by",
    "P29_custody_received_by",
    "P30_transferred_custody_of",
    "P67_refers_to",
    "P120_occurs_before",
    "P140_assigned_attribute_to",
    "P141_assigned",
    "P129_is_about",
    "P129i_is_subject_of",
}

MULTI_LABEL_MAP = {
    "E7_Activity": "E7_Activity:E5_Event",
    "E10_Transfer_of_Custody": "E10_Transfer_of_Custody:E7_Activity:E5_Event",
    "E21_Person": "E21_Person:E39_Actor",
}


def _spec_type_item_str(t: Any) -> str:
    if isinstance(t, dict):
        return str(t.get("name") or "").strip()
    return str(t or "").strip()


class GraphWriter:
    """Writes a CIDOC CRM graph spec to Neo4j."""

    def __init__(self, driver):
        self.driver = driver

    @staticmethod
    def _infer_e13_p141_type(assignment_name: str, raw_text: str) -> Optional[str]:
        """
        When the modeling agent omits P141 / types on E13, derive a concrete E55 label
        from the assignment wording and entry text (avoids meaningless 'AssignedState').
        """
        blob = f" {assignment_name} {raw_text} ".lower()
        # (needles, label) — keep labels stable for UI / taxonomy
        rules: List[Tuple[Tuple[str, ...], str]] = [
            (("faim", "affam", "hungry", "hunger", "feeling hungry", "had hunger"), "Hunger"),
            (("fatigue", "tired", "épuis", "epuis", "exhausted", "somnol", "sleepy"), "Fatigue"),
            (("stress", "stressed", "anxious", "anxiety", "angoiss", "worried"), "Stress"),
            (("joie", "heureux", "heureuse", "happy", "glad", "pleased", "content "), "Joy"),
            (("triste", "sad", "melanc", "down ", "depressed"), "Sadness"),
            (("peur", "fear", "afraid", "scared", "fright"), "Fear"),
            (("colère", "colere", "angry", "rage", "furious"), "Anger"),
            (("déception", "deception", "disappointed", "let down"), "Disappointment"),
            (("expect", "attente", "waiting for", "hope ", "hoping"), "Expectation"),
            (("douleur", "pain", "hurt", "suffering"), "EmotionalPain"),
            (("ennui", "bored", "boredom"), "Boredom"),
            (("surprise", "surpris", "shocked", "astonish"), "Surprise"),
            (("calm", "calme", "relaxed", "detendu", "détendu"), "Calm"),
            (("nostalg", "nostalgia"), "Nostalgia"),
            (("gratitude", "grateful", "reconnaissant"), "Gratitude"),
        ]
        for needles, label in rules:
            if any(n in blob for n in needles):
                return label
        return None

    def write(
        self,
        spec: Dict[str, Any],
        entry_id: str,
        raw_text: str,
        user_name: str = "",
        day_bucket: str = "",
        input_ts: Optional[str] = None,
        wsd_profile: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not spec or (not spec.get("nodes") and not spec.get("edges")):
            return {"status": "skipped", "reason": "empty_spec"}
        ts = input_ts or datetime.now().astimezone().isoformat()

        with self.driver.session() as session:
            return session.execute_write(
                self._write_tx,
                spec=spec,
                entry_id=entry_id,
                raw_text=raw_text,
                user_name=user_name,
                day_bucket=day_bucket,
                ts=ts,
                driver=self.driver,
                wsd_profile=wsd_profile,
            )

    @staticmethod
    def _write_tx(
        tx,
        spec: Dict[str, Any],
        entry_id: str,
        raw_text: str,
        user_name: str,
        day_bucket: str,
        ts: str,
        driver=None,
        wsd_profile: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        nodes: List[Dict] = spec.get("nodes", [])
        edges: List[Dict] = spec.get("edges", [])

        # Build node label lookup from the spec for edge validation/repair.
        # We validate at write-time so malformed LLM output cannot corrupt CIDOC directionality.
        id_to_label: Dict[str, str] = {}
        for node in nodes:
            if not isinstance(node, dict):
                continue
            nid = str(node.get("id", ""))
            label = str(node.get("label", ""))
            if nid and label:
                id_to_label[nid] = label

        def _is_activity(label: str) -> bool:
            return label in {"E5_Event", "E7_Activity", "E10_Transfer_of_Custody", "E13_Attribute_Assignment"}

        def _is_actor(label: str) -> bool:
            return label in {"E39_Actor", "E21_Person"}

        def _is_transfer_intent(text: str) -> bool:
            t = (text or "").lower()
            # Strict pattern to avoid hallucinated custody events for unrelated verbs.
            transfer_verbs = [
                "donne", "donné", "donner", "gave", "give", "lending", "lend",
                "prête", "prêt", "preter", "prêter", "borrow", "emprunt",
                "retourne le", "returned the", "return the",
            ]
            transfer_objects = ["livre", "book", "objet", "item", "cadeau", "gift"]
            return any(v in t for v in transfer_verbs) and any(o in t for o in transfer_objects)

        def _is_state_intent(text: str) -> bool:
            t = (text or "").lower()
            state_markers = [
                "fait mal", "mal", "douleur", "pain", "hurt", "triste", "decu",
                "déçu", "deception", "disappointment", "attente", "expectation",
            ]
            return any(m in t for m in state_markers)

        def _get_node_name(nid: str) -> str:
            for n in nodes:
                if isinstance(n, dict) and str(n.get("id", "")) == nid:
                    return str(n.get("name", ""))
            return ""

        def _add_node_once(nid: str, label: str, name: str, types: Optional[List[str]] = None) -> None:
            if nid in id_to_label:
                return
            nodes.append(
                {
                    "id": nid,
                    "label": label,
                    "name": name,
                    "types": types or [],
                    "properties": {},
                }
            )
            id_to_label[nid] = label

        def _edge_allowed(prop: str, from_label: str, to_label: str) -> bool:
            if prop == "P2_has_type":
                return to_label == "E55_Type"
            if prop == "P4_has_time_span":
                return _is_activity(from_label) and to_label == "E52_Time_Span"
            if prop == "P7_took_place_at":
                return _is_activity(from_label) and to_label == "E53_Place"
            if prop == "P14_carried_out_by":
                return _is_activity(from_label) and _is_actor(to_label)
            if prop == "P14i_performed":
                return _is_actor(from_label) and _is_activity(to_label)
            if prop == "P15_was_influenced_by":
                return _is_activity(from_label)
            if prop == "P17_was_motivated_by":
                return _is_activity(from_label)
            if prop in {"P28_custody_surrendered_by", "P29_custody_received_by"}:
                return from_label == "E10_Transfer_of_Custody" and _is_actor(to_label)
            if prop == "P30_transferred_custody_of":
                return from_label == "E10_Transfer_of_Custody" and to_label == "E22_Human_Made_Object"
            if prop == "P67_refers_to":
                return from_label in {"E73_Information_Object", "E89_Propositional_Object", "E7_Activity", "E13_Attribute_Assignment"}
            if prop == "P120_occurs_before":
                return _is_activity(from_label) and _is_activity(to_label)
            if prop == "P140_assigned_attribute_to":
                return from_label == "E13_Attribute_Assignment"
            if prop == "P141_assigned":
                return from_label == "E13_Attribute_Assignment" and to_label in {"E55_Type", "E28_Conceptual_Object", "E89_Propositional_Object"}
            if prop == "P129_is_about":
                return _is_activity(from_label)
            if prop == "P129i_is_subject_of":
                return _is_activity(to_label)
            return False

        def _normalize_edge(edge: Dict[str, Any]) -> Optional[Tuple[str, str, str, Dict[str, Any]]]:
            from_id = str(edge.get("from", ""))
            to_id = str(edge.get("to", ""))
            prop = str(edge.get("property", ""))
            eprops = edge.get("properties", {})
            if not isinstance(eprops, dict):
                eprops = {}
            if not from_id or not to_id or not prop:
                return None
            if prop not in VALID_PROPERTIES:
                logger.warning("Unknown property %s, skipping edge", prop)
                return None
            fl = id_to_label.get(from_id, "")
            tl = id_to_label.get(to_id, "")
            if not fl or not tl:
                return None

            # Auto-repair known inverse mistakes from LLM output.
            if prop == "P14i_performed" and _is_activity(fl) and _is_actor(tl):
                from_id, to_id = to_id, from_id
                fl, tl = tl, fl
            elif prop == "P140_assigned_attribute_to" and fl != "E13_Attribute_Assignment" and tl == "E13_Attribute_Assignment":
                from_id, to_id = to_id, from_id
                fl, tl = tl, fl
            elif prop == "P141_assigned" and fl != "E13_Attribute_Assignment" and tl == "E13_Attribute_Assignment":
                from_id, to_id = to_id, from_id
                fl, tl = tl, fl
            elif prop == "P15_was_influenced_by" and _is_activity(fl) and tl == "E22_Human_Made_Object":
                # For "non-return of X", the object is the subject of the event, not its influencer.
                prop = "P129_is_about"
            elif prop == "P129i_is_subject_of" and _is_activity(fl):
                # If modeled in the wrong inverse direction, normalize to event -> object.
                prop = "P129_is_about"
            elif prop == "P129i_is_subject_of" and _is_activity(tl):
                from_id, to_id = to_id, from_id
                fl, tl = tl, fl
                prop = "P129_is_about"

            if not _edge_allowed(prop, fl, tl):
                logger.warning(
                    "CIDOC validation rejected edge: %s (%s -> %s), skipping",
                    prop,
                    fl,
                    tl,
                )
                return None
            return from_id, to_id, prop, eprops

        normalized_edges: List[Tuple[str, str, str, Dict[str, Any]]] = []
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            norm = _normalize_edge(edge)
            if norm:
                normalized_edges.append(norm)

        transfer_intent = _is_transfer_intent(raw_text)
        state_intent = _is_state_intent(raw_text)

        actor_ids = [nid for nid, lab in id_to_label.items() if _is_actor(lab)]
        user_actor_id = ""
        for aid in actor_ids:
            if _get_node_name(aid).strip().lower() == (user_name or "").strip().lower() and user_name.strip():
                user_actor_id = aid
                break
        if not user_actor_id and user_name.strip():
            user_actor_id = "auto_user_actor"
            _add_node_once(user_actor_id, "E21_Person", user_name.strip(), ["User"])
            actor_ids.append(user_actor_id)

        other_actor_id = ""
        for aid in actor_ids:
            if aid != user_actor_id:
                other_actor_id = aid
                break

        transfer_ids = [nid for nid, lab in id_to_label.items() if lab == "E10_Transfer_of_Custody"]
        if transfer_intent and not transfer_ids:
            transfer_name = "transfer of custody"
            txt = (raw_text or "").lower()
            if "livre" in txt or "book" in txt:
                transfer_name = "don de livre"
            tid = "auto_transfer_1"
            _add_node_once(tid, "E10_Transfer_of_Custody", transfer_name, ["BookLending"])
            transfer_ids.append(tid)

        # Completeness rule: every transfer should point to an object via P30.
        transfer_with_p30 = {f for (f, _t, p, _ep) in normalized_edges if p == "P30_transferred_custody_of"}
        book_hint = "book"
        text_l = (raw_text or "").lower()
        if "livre" in text_l:
            book_hint = "livre"
        for tid in transfer_ids:
            if tid in transfer_with_p30:
                continue
            oid = f"auto_obj_{tid}"
            if oid not in id_to_label:
                nodes.append(
                    {
                        "id": oid,
                        "label": "E22_Human_Made_Object",
                        "name": book_hint,
                        "types": [],
                        "properties": {},
                    }
                )
                id_to_label[oid] = "E22_Human_Made_Object"
            normalized_edges.append((tid, oid, "P30_transferred_custody_of", {}))

        # Completeness rule: attach transfer actors when available.
        p28_from = {f for (f, _t, p, _ep) in normalized_edges if p == "P28_custody_surrendered_by"}
        p29_from = {f for (f, _t, p, _ep) in normalized_edges if p == "P29_custody_received_by"}
        for tid in transfer_ids:
            if user_actor_id and tid not in p28_from:
                normalized_edges.append((tid, user_actor_id, "P28_custody_surrendered_by", {}))
            if other_actor_id and tid not in p29_from:
                normalized_edges.append((tid, other_actor_id, "P29_custody_received_by", {}))

        # Completeness rule: when emotional/expectation intent exists, ensure E13 assignment exists.
        assignment_ids = [nid for nid, lab in id_to_label.items() if lab == "E13_Attribute_Assignment"]
        if state_intent and not assignment_ids:
            sid = "auto_state_1"
            state_type = "EmotionalPain"
            if any(k in text_l for k in ["expect", "attente", "retour"]):
                state_type = "Expectation"
            _add_node_once(sid, "E13_Attribute_Assignment", "state assignment")
            stid = "auto_state_type_1"
            _add_node_once(stid, "E55_Type", state_type)
            target_id = user_actor_id or (actor_ids[0] if actor_ids else "")
            if target_id:
                normalized_edges.append((sid, target_id, "P140_assigned_attribute_to", {}))
            normalized_edges.append((sid, stid, "P141_assigned", {}))
            assignment_ids.append(sid)

        # Completeness rule: each E13 must have P140 + P141.
        for aid in assignment_ids:
            has_p140 = any(f == aid and p == "P140_assigned_attribute_to" for (f, _t, p, _ep) in normalized_edges)
            has_p141 = any(f == aid and p == "P141_assigned" for (f, _t, p, _ep) in normalized_edges)
            e13_name = _get_node_name(aid)
            inferred_p141 = GraphWriter._infer_e13_p141_type(e13_name, raw_text or "")

            if not has_p140:
                target_id = user_actor_id or (actor_ids[0] if actor_ids else "")
                if target_id:
                    normalized_edges.append((aid, target_id, "P140_assigned_attribute_to", {}))

            if not has_p141:
                assigned_type_name = "AssignedState"
                for n in nodes:
                    if not isinstance(n, dict) or str(n.get("id", "")) != aid:
                        continue
                    ntypes = n.get("types", [])
                    if isinstance(ntypes, list) and ntypes:
                        assigned_type_name = _spec_type_item_str(ntypes[0]) or assigned_type_name
                    break
                if inferred_p141 and (
                    not assigned_type_name.strip() or assigned_type_name in ("AssignedState", "State")
                ):
                    assigned_type_name = inferred_p141
                p141_tid = f"auto_p141_type_{aid}"
                _add_node_once(p141_tid, "E55_Type", assigned_type_name)
                normalized_edges.append((aid, p141_tid, "P141_assigned", {}))
            elif inferred_p141:
                p141_tids = [t for (f, t, p, _ep) in normalized_edges if f == aid and p == "P141_assigned"]
                if p141_tids:
                    tid = p141_tids[0]
                    for n in nodes:
                        if not isinstance(n, dict) or str(n.get("id", "")) != tid:
                            continue
                        if id_to_label.get(tid) != "E55_Type":
                            break
                        nm = str(n.get("name", "") or "").strip()
                        if nm and nm not in ("AssignedState", "State"):
                            break
                        n["name"] = inferred_p141
                        break

        # Precision rule: emotional/expectation assignments should point to a triggering activity.
        activity_ids = [nid for nid, lab in id_to_label.items() if _is_activity(lab) and lab != "E13_Attribute_Assignment"]
        trigger_candidates = []
        for eid in activity_ids:
            n = _get_node_name(eid).lower()
            score = 0
            if "non-retour" in n or "non return" in n or "failure" in n:
                score = 2
            elif "retour" in n or "return" in n:
                score = 1
            trigger_candidates.append((score, eid))
        trigger_candidates.sort(reverse=True)
        default_trigger = trigger_candidates[0][1] if trigger_candidates else (activity_ids[-1] if activity_ids else "")
        for aid in assignment_ids:
            has_causal = any(
                f == aid and p in {"P15_was_influenced_by", "P17_was_motivated_by"} and _is_activity(id_to_label.get(t, ""))
                for (f, t, p, _ep) in normalized_edges
            )
            if not has_causal and default_trigger:
                normalized_edges.append((aid, default_trigger, "P15_was_influenced_by", {}))

        # Completeness rule: every E7_Activity should have at least one E55_Type.
        # Use context clues from the edges rather than parsing the node name.
        # Completeness rule: E53_Place nodes without a type get a default based on context.
        places_with_p2 = {f for (f, _t, p, _ep) in normalized_edges if p == "P2_has_type"
                          if id_to_label.get(f) == "E53_Place"}
        places_as_p7_target = {t for (_f, t, p, _ep) in normalized_edges if p == "P7_took_place_at"}
        for nid, lab in list(id_to_label.items()):
            if lab != "E53_Place":
                continue
            if nid in places_with_p2:
                continue
            # Only type places that are actual activity venues (P7 target); leave remote refs untyped
            if nid not in places_as_p7_target:
                continue
            # If the place name matches a seed vocab entry, use that canonical type.
            # Otherwise fall back to Neighbourhood (generic urban venue).
            from .type_vocab import infer_place_type_name_from_mention

            pname = _get_node_name(nid)
            place_type = infer_place_type_name_from_mention(pname)
            tid = f"auto_place_type_{nid}"
            _add_node_once(tid, "E55_Type", place_type)
            normalized_edges.append((nid, tid, "P2_has_type", {}))

        activities_with_p2 = {f for (f, _t, p, _ep) in normalized_edges if p == "P2_has_type"}
        activities_with_place = {f for (f, _t, p, _ep) in normalized_edges if p == "P7_took_place_at"}
        for nid, lab in list(id_to_label.items()):
            if lab not in {"E7_Activity", "E10_Transfer_of_Custody"}:
                continue
            if nid in activities_with_p2:
                continue
            # Activity at a place → Visit; everything else → WorkSession
            fallback_type = "Visit" if nid in activities_with_place else "WorkSession"
            tid = f"auto_type_{nid}"
            _add_node_once(tid, "E55_Type", fallback_type)
            normalized_edges.append((nid, tid, "P2_has_type", {}))

        if driver is not None:
            from .type_resolver import TypeResolver

            _merge_babelfy_e55_into_spec_grounding(spec, raw_text or "", wsd_profile)
            tr = TypeResolver(driver)
            tr.resolve_graph_spec(
                spec,
                existing=tr.get_existing_types(),
                journal_text=raw_text or "",
                wsd_profile=wsd_profile,
                llm_grounding=spec.get("_type_llm_grounding"),
            )
            spec.pop("_type_llm_grounding", None)
        auth_meta = spec.pop("_e55_authority_meta", None) or {}

        def _e55_merge_props(
            tname: str, node_props: Optional[Dict[str, Any]] = None
        ) -> Tuple[
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
        ]:
            np = node_props if isinstance(node_props, dict) else {}
            wid = str(np.get("wikidata_id", "") or "").strip()
            desc = str(np.get("description", "") or "").strip()
            aid = str(np.get("aat_id", "") or "").strip()
            bn = str(np.get("babel_synset_id", "") or "").strip()
            wn = str(np.get("wordnet_synset_id", "") or "").strip()
            bj = str(np.get("babelnet_sources_json", "") or "").strip()
            bg = str(np.get("babel_gloss", "") or "").strip()
            bru = str(np.get("babelnet_rdf_url", "") or "").strip()
            dpu = str(np.get("dbpedia_url", "") or "").strip()
            am = auth_meta.get(tname)
            if isinstance(am, dict):
                if not wid:
                    wid = str(am.get("wikidata_id", "") or "").strip()
                if not desc:
                    desc = str(am.get("description", "") or "").strip()
                if not aid:
                    aid = str(am.get("aat_id", "") or "").strip()
                if not bn:
                    bn = str(am.get("babel_synset_id", "") or "").strip()
                if not wn:
                    wn = str(am.get("wordnet_synset_id", "") or "").strip()
                if not bj:
                    bj = str(am.get("babelnet_sources_json", "") or "").strip()
                if not bg:
                    bg = str(am.get("babel_gloss", "") or "").strip()
                if not bru:
                    bru = str(am.get("babelnet_rdf_url", "") or "").strip()
                if not dpu:
                    dpu = str(am.get("dbpedia_url", "") or "").strip()
            if wid:
                aid = ""
            return (
                wid if wid else None,
                desc if desc else None,
                aid if aid else None,
                bn if bn.startswith("bn:") else None,
                wn if wn else None,
                bj if bj else None,
                bg if bg else None,
                bru if bru else None,
                dpu if dpu else None,
            )

        def _merge_e55_type(
            tx_inner, tname: str, node_props: Optional[Dict[str, Any]] = None
        ) -> None:
            (
                wid_p,
                desc_p,
                aid_p,
                bn_p,
                wn_p,
                bj_p,
                bg_p,
                bru_p,
                dpu_p,
            ) = _e55_merge_props(tname, node_props)
            tx_inner.run(
                """
                MERGE (n:E55_Type {name: $name})
                ON CREATE SET n.wikidata_id = $wid, n.description = $desc, n.aat_id = $aid,
                    n.babel_synset_id = $bn, n.wordnet_synset_id = $wn,
                    n.babelnet_sources_json = $bj,
                    n.babel_gloss = $bg, n.babelnet_rdf_url = $bru, n.dbpedia_url = $dpu
                SET n.wikidata_id = coalesce($wid, n.wikidata_id),
                    n.description = CASE
                        WHEN $desc IS NOT NULL AND $desc <> '' THEN $desc
                        ELSE n.description
                    END,
                    n.aat_id = CASE
                        WHEN $wid IS NOT NULL AND $wid <> '' THEN null
                        WHEN $aid IS NOT NULL AND $aid <> '' THEN $aid
                        ELSE n.aat_id
                    END,
                    n.babel_synset_id = coalesce($bn, n.babel_synset_id),
                    n.wordnet_synset_id = coalesce($wn, n.wordnet_synset_id),
                    n.babelnet_sources_json = coalesce($bj, n.babelnet_sources_json),
                    n.babel_gloss = coalesce($bg, n.babel_gloss),
                    n.babelnet_rdf_url = coalesce($bru, n.babelnet_rdf_url),
                    n.dbpedia_url = coalesce($dpu, n.dbpedia_url)
                """,
                name=tname,
                wid=wid_p,
                desc=desc_p,
                aid=aid_p,
                bn=bn_p,
                wn=wn_p,
                bj=bj_p,
                bg=bg_p,
                bru=bru_p,
                dpu=dpu_p,
            )

        short_name = raw_text[:60].strip()
        if len(raw_text) > 60:
            short_name += "..."
        tx.run(
            """
            MERGE (j:E73_Information_Object {id: $id})
            SET j.text = $text,
                j.input_time = datetime($ts),
                j.entry_kind = 'journal_entry',
                j.name = $short_name
            """,
            id=entry_id,
            text=raw_text[:5000],
            ts=ts,
            short_name=short_name,
        )

        if day_bucket:
            tx.run(
                """
                MERGE (d:E52_Time_Span {key: $day})
                SET d.date = $day, d.name = $day
                """,
                day=day_bucket,
            )

        id_to_key: Dict[str, str] = {}
        id_to_type_name: Dict[str, str] = {}
        id_to_person_name: Dict[str, str] = {}
        id_to_person_id: Dict[str, str] = {}

        for node in nodes:
            if not isinstance(node, dict):
                continue
            nid = str(node.get("id", ""))
            label = str(node.get("label", ""))
            name = str(node.get("name", ""))
            types = node.get("types", [])
            props = node.get("properties", {})
            if not isinstance(props, dict):
                props = {}
            if not nid or not label or not name:
                continue
            if label not in VALID_LABELS:
                logger.warning("Unknown label %s for node %s, skipping", label, nid)
                continue

            # E55 types are global vocabulary nodes and should be merged by name.
            if label == "E55_Type":
                _merge_e55_type(tx, name, props)
                id_to_type_name[nid] = name
            elif label == "E21_Person":
                # Merge people globally by name so user/known persons are not duplicated per entry.
                person_id = str(props.get("person_id", "") or "").strip()
                wid_person = str(props.get("wikidata_id") or "").strip() or None
                wdesc_person = str(props.get("wikidata_description") or "").strip() or None
                bn_person = str(props.get("babel_synset_id") or "").strip() or None
                wn_person = str(props.get("wordnet_synset_id") or "").strip() or None
                bj_person = str(props.get("babelnet_sources_json") or "").strip() or None
                bg_person = str(props.get("babel_gloss") or "").strip() or None
                bru_person = str(props.get("babelnet_rdf_url") or "").strip() or None
                dpu_person = str(props.get("dbpedia_url") or "").strip() or None
                if person_id:
                    row = tx.run(
                        """
                        MERGE (n:E21_Person:E39_Actor {id: $pid})
                        ON CREATE SET n.first_seen = datetime($ts)
                        SET n.last_seen = datetime($ts),
                            n.name = coalesce(n.name, $name),
                            n.wikidata_id = coalesce($wid, n.wikidata_id),
                            n.wikidata_description = coalesce($wdesc, n.wikidata_description),
                            n.babel_synset_id = coalesce($bn, n.babel_synset_id),
                            n.wordnet_synset_id = coalesce($wn, n.wordnet_synset_id),
                            n.babelnet_sources_json = coalesce($bj, n.babelnet_sources_json),
                            n.babel_gloss = coalesce($bg, n.babel_gloss),
                            n.babelnet_rdf_url = coalesce($bru, n.babelnet_rdf_url),
                            n.dbpedia_url = coalesce($dpu, n.dbpedia_url)
                        RETURN n.id as id
                        """,
                        pid=person_id,
                        name=name,
                        ts=ts,
                        wid=wid_person,
                        wdesc=wdesc_person,
                        bn=bn_person,
                        wn=wn_person,
                        bj=bj_person,
                        bg=bg_person,
                        bru=bru_person,
                        dpu=dpu_person,
                    ).single()
                else:
                    row = tx.run(
                        """
                        MERGE (n:E21_Person:E39_Actor {name: $name})
                        ON CREATE SET n.first_seen = datetime($ts)
                        SET n.last_seen = datetime($ts),
                            n.id = coalesce(n.id, randomUUID()),
                            n.wikidata_id = coalesce($wid, n.wikidata_id),
                            n.wikidata_description = coalesce($wdesc, n.wikidata_description),
                            n.babel_synset_id = coalesce($bn, n.babel_synset_id),
                            n.wordnet_synset_id = coalesce($wn, n.wordnet_synset_id),
                            n.babelnet_sources_json = coalesce($bj, n.babelnet_sources_json),
                            n.babel_gloss = coalesce($bg, n.babel_gloss),
                            n.babelnet_rdf_url = coalesce($bru, n.babelnet_rdf_url),
                            n.dbpedia_url = coalesce($dpu, n.dbpedia_url)
                        RETURN n.id as id
                        """,
                        name=name,
                        ts=ts,
                        wid=wid_person,
                        wdesc=wdesc_person,
                        bn=bn_person,
                        wn=wn_person,
                        bj=bj_person,
                        bg=bg_person,
                        bru=bru_person,
                        dpu=dpu_person,
                    ).single()
                id_to_person_name[nid] = name
                if row and row.get("id"):
                    id_to_person_id[nid] = str(row.get("id"))
            else:
                neo_label = MULTI_LABEL_MAP.get(label, label)
                key = f"{entry_id}|{nid}"
                id_to_key[nid] = key

                prop_sets = []
                prop_params: Dict[str, Any] = {"key": key, "name": name, "ts": ts}

                if "event_time_iso" in props:
                    prop_sets.append("n.event_time_iso = $eti")
                    prop_params["eti"] = str(props["event_time_iso"])
                if "event_time_text" in props:
                    prop_sets.append("n.event_time_text = $ett")
                    prop_params["ett"] = str(props["event_time_text"])
                if label in ("E53_Place", "E74_Group") and props.get("wikidata_id"):
                    prop_sets.append("n.wikidata_id = coalesce(n.wikidata_id, $wid)")
                    prop_params["wid"] = str(props["wikidata_id"])
                    if props.get("wikidata_description"):
                        prop_sets.append("n.wikidata_description = coalesce(n.wikidata_description, $wdesc)")
                        prop_params["wdesc"] = str(props["wikidata_description"])
                if label in ("E53_Place", "E74_Group"):
                    bn_e = str(props.get("babel_synset_id") or "").strip()
                    if bn_e.startswith("bn:"):
                        prop_sets.append("n.babel_synset_id = coalesce(n.babel_synset_id, $bn)")
                        prop_params["bn"] = bn_e
                    if str(props.get("wordnet_synset_id") or "").strip():
                        prop_sets.append("n.wordnet_synset_id = coalesce(n.wordnet_synset_id, $wn)")
                        prop_params["wn"] = str(props["wordnet_synset_id"])
                    if str(props.get("babelnet_sources_json") or "").strip():
                        prop_sets.append(
                            "n.babelnet_sources_json = coalesce(n.babelnet_sources_json, $bj)"
                        )
                        prop_params["bj"] = str(props["babelnet_sources_json"])
                    if str(props.get("babel_gloss") or "").strip():
                        prop_sets.append("n.babel_gloss = coalesce(n.babel_gloss, $bg)")
                        prop_params["bg"] = str(props["babel_gloss"])
                    if str(props.get("babelnet_rdf_url") or "").strip():
                        prop_sets.append(
                            "n.babelnet_rdf_url = coalesce(n.babelnet_rdf_url, $bru)"
                        )
                        prop_params["bru"] = str(props["babelnet_rdf_url"])
                    if str(props.get("dbpedia_url") or "").strip():
                        prop_sets.append("n.dbpedia_url = coalesce(n.dbpedia_url, $dpu)")
                        prop_params["dpu"] = str(props["dbpedia_url"])

                extra = (", " + ", ".join(prop_sets)) if prop_sets else ""

                tx.run(
                    f"""
                    MERGE (n:{neo_label} {{key: $key}})
                    ON CREATE SET n.first_seen = datetime($ts)
                    SET n.last_seen = datetime($ts),
                        n.name = $name{extra}
                    """,
                    **prop_params,
                )

            if isinstance(types, list):
                for t in types:
                    tname = _spec_type_item_str(t)
                    if not tname:
                        continue
                    if label == "E55_Type":
                        _merge_e55_type(tx, tname, {})
                        tx.run(
                            """
                            MATCH (n:E55_Type {name: $name})
                            MATCH (t:E55_Type {name: $tname})
                            MERGE (n)-[:P2_has_type]->(t)
                            """,
                            name=name,
                            tname=tname,
                        )
                    elif label == "E21_Person":
                        _merge_e55_type(tx, tname, {})
                        tx.run(
                            """
                            MATCH (n:E21_Person {id: $id})
                            MATCH (t:E55_Type {name: $tname})
                            MERGE (n)-[:P2_has_type]->(t)
                            """,
                            id=id_to_person_id.get(nid, ""),
                            tname=tname,
                        )
                    else:
                        _merge_e55_type(tx, tname, {})
                        tx.run(
                            """
                            MATCH (n {key: $key})
                            MATCH (t:E55_Type {name: $tname})
                            MERGE (n)-[:P2_has_type]->(t)
                            """,
                            key=key,
                            tname=tname,
                        )

            neo_label = MULTI_LABEL_MAP.get(label, label)
            is_activity = label in (
                "E7_Activity",
                "E10_Transfer_of_Custody",
            ) or "E7_Activity" in neo_label or "E10_Transfer_of_Custody" in neo_label
            if day_bucket and is_activity and nid in id_to_key:
                tx.run(
                    """
                    MATCH (n {key: $key})
                    MATCH (d:E52_Time_Span {key: $day})
                    MERGE (n)-[:P4_has_time_span]->(d)
                    """,
                    key=id_to_key[nid],
                    day=day_bucket,
                )

            # Do not attach journal -> E55 type by P67; types classify entities/events.
            if label == "E21_Person":
                tx.run(
                    """
                    MATCH (j:E73_Information_Object {id: $entry_id})
                    MATCH (n:E21_Person {id: $pid})
                    MERGE (j)-[:P67_refers_to {ref_type: 'about'}]->(n)
                    """,
                    entry_id=entry_id,
                    pid=id_to_person_id.get(nid, ""),
                )
            elif label != "E55_Type":
                tx.run(
                    """
                    MATCH (j:E73_Information_Object {id: $entry_id})
                    MATCH (n {key: $key})
                    MERGE (j)-[:P67_refers_to {ref_type: 'about'}]->(n)
                    """,
                    entry_id=entry_id,
                    key=id_to_key[nid],
                )

        for from_id, to_id, prop, eprops in normalized_edges:
            from_key = id_to_key.get(from_id)
            to_key = id_to_key.get(to_id)
            from_type_name = id_to_type_name.get(from_id)
            to_type_name = id_to_type_name.get(to_id)
            from_person_name = id_to_person_name.get(from_id)
            to_person_name = id_to_person_name.get(to_id)
            from_person_id = id_to_person_id.get(from_id)
            to_person_id = id_to_person_id.get(to_id)
            if not (from_key or from_type_name or from_person_name or from_person_id):
                continue
            if not (to_key or to_type_name or to_person_name or to_person_id):
                continue

            ref_type = str(eprops.get("ref_type", ""))
            if from_key:
                match_a = "MATCH (a {key: $fk})"
            elif from_type_name:
                match_a = "MATCH (a:E55_Type {name: $fn})"
            elif from_person_id:
                match_a = "MATCH (a:E21_Person {id: $fpid})"
            else:
                match_a = "MATCH (a:E21_Person {name: $fp})"
            if to_key:
                match_b = "MATCH (b {key: $tk})"
            elif to_type_name:
                match_b = "MATCH (b:E55_Type {name: $tn})"
            elif to_person_id:
                match_b = "MATCH (b:E21_Person {id: $tpid})"
            else:
                match_b = "MATCH (b:E21_Person {name: $tp})"
            params: Dict[str, Any] = {}
            if from_key:
                params["fk"] = from_key
            elif from_type_name:
                params["fn"] = from_type_name
            elif from_person_id:
                params["fpid"] = from_person_id
            else:
                params["fp"] = from_person_name
            if to_key:
                params["tk"] = to_key
            elif to_type_name:
                params["tn"] = to_type_name
            elif to_person_id:
                params["tpid"] = to_person_id
            else:
                params["tp"] = to_person_name
            if ref_type:
                tx.run(
                    f"""
                    {match_a}
                    {match_b}
                    MERGE (a)-[r:{prop} {{ref_type: $rt}}]->(b)
                    """,
                    rt=ref_type,
                    **params,
                )
            else:
                tx.run(
                    f"""
                    {match_a}
                    {match_b}
                    MERGE (a)-[:{prop}]->(b)
                    """,
                    **params,
                )

        if user_name:
            _merge_e55_type(tx, "User", {})
            tx.run(
                """
                MERGE (u:E21_Person:E39_Actor {name: $name})
                ON CREATE SET u.first_seen = datetime($ts)
                SET u.last_seen = datetime($ts)
                WITH u
                MATCH (ut:E55_Type {name: 'User'})
                MERGE (u)-[:P2_has_type]->(ut)
                """,
                name=user_name,
                ts=ts,
            )

        # Post-write CIDOC audit for this entry scope (entry node + keyed nodes).
        audit = {}
        # Neo4j subquery aliases need stable names; use separate run for clarity/safety.
        wrong_p140 = tx.run(
            """
            MATCH (j:E73_Information_Object {id: $entry_id})-[:P67_refers_to]->(s)
            WITH collect(DISTINCT s) as scope
            MATCH (a:E7_Activity)-[r:P140_assigned_attribute_to]->(b:E13_Attribute_Assignment)
            WHERE a IN scope OR b IN scope
            RETURN count(r) as c
            """,
            entry_id=entry_id,
        ).single()["c"]
        wrong_p141 = tx.run(
            """
            MATCH (j:E73_Information_Object {id: $entry_id})-[:P67_refers_to]->(s)
            WITH collect(DISTINCT s) as scope
            MATCH (a:E7_Activity)-[r:P141_assigned]->(b:E13_Attribute_Assignment)
            WHERE a IN scope OR b IN scope
            RETURN count(r) as c
            """,
            entry_id=entry_id,
        ).single()["c"]
        wrong_p14i = tx.run(
            """
            MATCH (j:E73_Information_Object {id: $entry_id})-[:P67_refers_to]->(s)
            WITH collect(DISTINCT s) as scope
            MATCH (a:E7_Activity)-[r:P14i_performed]->(b:E39_Actor)
            WHERE a IN scope OR b IN scope
            RETURN count(r) as c
            """,
            entry_id=entry_id,
        ).single()["c"]
        transfer_missing_object = tx.run(
            """
            MATCH (j:E73_Information_Object {id: $entry_id})-[:P67_refers_to]->(s)
            WITH collect(DISTINCT s) as scope
            MATCH (t:E10_Transfer_of_Custody)
            WHERE t IN scope AND NOT (t)-[:P30_transferred_custody_of]->(:E22_Human_Made_Object)
            RETURN count(t) as c
            """,
            entry_id=entry_id,
        ).single()["c"]
        transfer_count = tx.run(
            """
            MATCH (j:E73_Information_Object {id: $entry_id})-[:P67_refers_to]->(s:E10_Transfer_of_Custody)
            RETURN count(s) as c
            """,
            entry_id=entry_id,
        ).single()["c"]
        assignment_count = tx.run(
            """
            MATCH (j:E73_Information_Object {id: $entry_id})-[:P67_refers_to]->(s:E13_Attribute_Assignment)
            RETURN count(s) as c
            """,
            entry_id=entry_id,
        ).single()["c"]
        assignment_missing_p140 = tx.run(
            """
            MATCH (j:E73_Information_Object {id: $entry_id})-[:P67_refers_to]->(s:E13_Attribute_Assignment)
            WHERE NOT (s)-[:P140_assigned_attribute_to]->()
            RETURN count(s) as c
            """,
            entry_id=entry_id,
        ).single()["c"]
        assignment_missing_p141 = tx.run(
            """
            MATCH (j:E73_Information_Object {id: $entry_id})-[:P67_refers_to]->(s:E13_Attribute_Assignment)
            WHERE NOT (s)-[:P141_assigned]->(:E55_Type)
            RETURN count(s) as c
            """,
            entry_id=entry_id,
        ).single()["c"]
        audit = {
            "wrong_p140": int(wrong_p140),
            "wrong_p141": int(wrong_p141),
            "wrong_p14i": int(wrong_p14i),
            "transfer_missing_object": int(transfer_missing_object),
            "transfer_count": int(transfer_count),
            "assignment_count": int(assignment_count),
            "assignment_missing_p140": int(assignment_missing_p140),
            "assignment_missing_p141": int(assignment_missing_p141),
            "is_valid": int(wrong_p140) == 0
            and int(wrong_p141) == 0
            and int(wrong_p14i) == 0
            and int(transfer_missing_object) == 0
            and int(assignment_missing_p140) == 0
            and int(assignment_missing_p141) == 0,
        }
        wtasks = spec.get("_entity_linking_wikidata_tasks") or []
        if isinstance(wtasks, list):
            for item in wtasks:
                if not isinstance(item, dict):
                    continue
                mention = str(item.get("mention") or "").strip()
                place_key = str(item.get("place_key") or "").strip()
                eid = str(item.get("entry_id") or "").strip()
                entity_label = str(item.get("entity_label") or "E53_Place").strip()
                cands = item.get("candidates") or []
                if not mention or not place_key or not eid:
                    continue
                try:
                    cj = json.dumps(cands, ensure_ascii=False)
                except (TypeError, ValueError):
                    cj = "[]"
                dup = tx.run(
                    """
                    MATCH (e:DisambiguationTask {place_key: $place_key, type: 'place_wikidata'})
                    WHERE e.status = 'open'
                    RETURN e.id AS id LIMIT 1
                    """,
                    place_key=place_key,
                ).single()
                if dup and dup.get("id"):
                    continue
                tid = str(uuid.uuid4())
                tx.run(
                    """
                    CREATE (t:DisambiguationTask {
                      id: $id,
                      type: 'place_wikidata',
                      mention: $mention,
                      status: 'open',
                      created_at: datetime(),
                      entry_id: $entry_id,
                      place_key: $place_key,
                      entity_label: $entity_label,
                      candidates_json: $candidates_json
                    })
                    """,
                    id=tid,
                    mention=mention,
                    entry_id=eid,
                    place_key=place_key,
                    entity_label=entity_label or "E53_Place",
                    candidates_json=cj,
                )
        return audit
