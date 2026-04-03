"""Human-readable progress events for the agentic journal pipeline (LangGraph nodes)."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

# (graph_node_name, stage_id, short_label, cumulative pct after this step completes)
NODE_STAGES: List[Tuple[str, str, str, int]] = [
    ("prep", "prep", "Preparing text", 12),
    ("wsd", "wsd", "Word sense & context", 25),
    ("model", "model", "Building graph spec", 45),
    ("llm_disambiguate", "llm_disambiguate", "Disambiguating mentions", 58),
    ("babelnet_lookup", "babelnet_lookup", "Linking entities (Wikidata / BabelNet)", 72),
    ("disambiguate_persons", "disambiguate_persons", "Resolving people", 84),
    ("persist_graph", "persist_graph", "Writing to memory graph", 94),
    ("persist_vector", "persist_vector", "Indexing for search", 100),
]

_NODE_TO_ROW: Dict[str, Tuple[str, str, str, int]] = {row[0]: row for row in NODE_STAGES}


def connection_stage_event(text: str, entry_id: str) -> Dict[str, Any]:
    """First event: real stats so the UI can show data before LangGraph nodes finish."""
    t = (text or "").strip()
    n_chars = len(t)
    words = len(t.split()) if t else 0
    snippet = t[:240] + ("…" if len(t) > 240 else "")
    short = entry_id[:8] if len(entry_id) >= 8 else entry_id
    return {
        "type": "stage",
        "stage": "connect",
        "label": "Received your journal",
        "pct": 5,
        "detail": f"{words} words · {n_chars} characters · entry {short}… — loading AI pipeline…",
        "preview": {
            "snippet": snippet,
            "char_count": n_chars,
            "word_count": words,
            "entry_id": entry_id,
        },
    }


def pipeline_boot_stage_event() -> Dict[str, Any]:
    """Emitted after the agent graph is compiled; bridges the gap before prep returns."""
    return {
        "type": "stage",
        "stage": "boot",
        "label": "Pipeline ready — prep & extraction",
        "pct": 8,
        "detail": "Running the prep agent: decomposing text into micro-events and entity mentions (LLM)…",
    }


def _sample_names(nodes: List[Dict[str, Any]], label: str, limit: int = 4) -> List[str]:
    out: List[str] = []
    for n in nodes:
        if not isinstance(n, dict):
            continue
        if str(n.get("label") or "") != label:
            continue
        nm = str(n.get("name") or "").strip()
        if nm:
            out.append(nm)
        if len(out) >= limit:
            break
    return out


def _graph_spec_preview(gs: Dict[str, Any]) -> Dict[str, Any]:
    nodes = gs.get("nodes") if isinstance(gs, dict) else None
    edges = gs.get("edges") if isinstance(gs, dict) else None
    if not isinstance(nodes, list):
        nodes = []
    if not isinstance(edges, list):
        edges = []
    places = _sample_names(nodes, "E53_Place")
    people = _sample_names(nodes, "E21_Person")
    groups = _sample_names(nodes, "E74_Group")
    activities: List[str] = []
    for n in nodes:
        if not isinstance(n, dict):
            continue
        lab = str(n.get("label") or "")
        if lab not in {"E5_Event", "E7_Activity", "E10_Transfer_of_Custody", "E13_Attribute_Assignment"}:
            continue
        nm = str(n.get("name") or "").strip()
        if nm:
            activities.append(nm)
        if len(activities) >= 4:
            break
    types_ = _sample_names(nodes, "E55_Type", limit=6)
    return {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "places": places,
        "people": people + groups[: max(0, 4 - len(people))],
        "activities": activities,
        "types": types_,
    }


def stage_event_for_node(node_name: str, state: Dict[str, Any]) -> Dict[str, Any]:
    """Build one ``stage`` event after *node_name* has completed (``state`` is accumulated)."""
    row = _NODE_TO_ROW.get(node_name)
    if not row:
        sid, label, pct = node_name, node_name.replace("_", " ").title(), 50
    else:
        _, sid, label, pct = row

    detail = ""
    prep = state.get("prep") if isinstance(state.get("prep"), dict) else {}
    gs = state.get("graph_spec") if isinstance(state.get("graph_spec"), dict) else {}
    wsd = state.get("wsd_profile") if isinstance(state.get("wsd_profile"), dict) else {}

    if node_name == "prep":
        me = prep.get("micro_events") or []
        ent = prep.get("entities") or []
        n_me = len(me) if isinstance(me, list) else 0
        n_ent = len(ent) if isinstance(ent, list) else 0
        detail = f"Micro-events: {n_me} · Mentions: {n_ent}"
        if n_me and isinstance(me, list):
            bits: List[str] = []
            for ev in me[:3]:
                if not isinstance(ev, dict):
                    continue
                tx = str(ev.get("text") or "").strip()
                if tx:
                    bits.append(tx[:72] + ("…" if len(tx) > 72 else ""))
            if bits:
                detail += " — " + " · ".join(bits)
        if n_ent and isinstance(ent, list):
            names: List[str] = []
            for e in ent[:6]:
                if not isinstance(e, dict):
                    continue
                nm = str(e.get("name") or "").strip()
                if nm:
                    names.append(nm)
            if names:
                detail += f" — spotted: {', '.join(names)}"
    elif node_name == "wsd":
        ents = wsd.get("entities") or []
        n = len(ents) if isinstance(ents, list) else 0
        detail = f"Context tags for {n} entity mention(s)"
    elif node_name == "model":
        pv = _graph_spec_preview(gs)
        detail = f"Spec: {pv['node_count']} nodes, {pv['edge_count']} edges"
    elif node_name == "llm_disambiguate":
        dm = state.get("disambiguated_mentions") or []
        n = len(dm) if isinstance(dm, list) else 0
        need = state.get("clarifications_needed") or []
        n_need = len(need) if isinstance(need, list) else 0
        detail = f"Resolved {n} mention(s)" + (f" · {n_need} need your input later" if n_need else "")
    elif node_name == "babelnet_lookup":
        raw_bf = gs.get("_babelfy_entity_linking") if isinstance(gs, dict) else None
        linked = 0
        if isinstance(raw_bf, dict):
            linked = int(raw_bf.get("linked") or raw_bf.get("resolved") or 0)
        if not linked and isinstance(gs, dict):
            linked = len([x for x in (gs.get("nodes") or []) if isinstance(x, dict) and x.get("wikidata_id")])
        detail = (
            f"Linked {linked} entity reference(s)"
            if linked
            else "Entity linking (BabelNet / Wikidata)"
        )
    elif node_name == "disambiguate_persons":
        pr = state.get("person_resolution") if isinstance(state.get("person_resolution"), dict) else {}
        keys = list(pr.keys()) if pr else []
        detail = f"Person merge candidates: {len(keys)}" if keys else "No extra person merges"
    elif node_name == "persist_graph":
        status = str(state.get("graph_status") or "")
        detail = f"Graph store: {status or 'ok'}"
    elif node_name == "persist_vector":
        vs = str(state.get("vector_status") or "")
        detail = f"Vector index: {vs}"

    preview: Dict[str, Any] = {}
    if gs:
        preview = _graph_spec_preview(gs)

    return {
        "type": "stage",
        "stage": sid,
        "label": label,
        "pct": pct,
        "detail": detail,
        "preview": preview,
    }
