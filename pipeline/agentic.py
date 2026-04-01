"""Agentic orchestration (LangGraph) for the memory pipeline.

Flow: Prep → WSD (LLM JSON) → Model → LLM type grounding + TypeResolve → WriteGraph → WriteVector
"""
from __future__ import annotations

from datetime import datetime
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import StateGraph, END

from .prep_agent import PrepAgent
from .modeling_agent import ModelingAgent
from .type_resolver import TypeResolver
from .graph_writer import GraphWriter
from .graph_store import GraphStore
from .vector_store import VectorStore
from .llm_extractor import LLMExtractor
from .pipeline import MemoryPipeline
from .wsd_preprocess import WsdPreprocessor
from .type_grounding_llm import TypeGroundingLLM
from .type_resolver import (
    collect_e55_grounding_requests,
    collect_entity_linking_requests,
    apply_entity_linking,
    build_entity_linking_wikidata_tasks,
)


def _extract_user_profile(spec: Dict[str, Any], user_name: str) -> Dict[str, Any]:
    """Pull the author's profile properties from the E21_Person node in the spec."""
    for node in spec.get("nodes", []):
        if not isinstance(node, dict):
            continue
        if str(node.get("label", "")) != "E21_Person":
            continue
        if user_name and str(node.get("name", "")).lower() != user_name.lower():
            continue
        props = node.get("properties", {})
        if not isinstance(props, dict):
            return {}
        return {k: v for k, v in props.items() if v and not k.startswith("_")}
    return {}


class AgenticState(TypedDict, total=False):
    text: str
    entry_id: str
    day_bucket: str
    prep: Dict[str, Any]
    wsd_profile: Dict[str, Any]
    graph_spec: Dict[str, Any]
    person_resolution: Dict[str, Any]
    extraction: Any
    graph_status: str
    graph_audit: Dict[str, Any]
    vector_status: str


@dataclass
class AgenticRunner:
    prep_agent: Optional[PrepAgent]
    modeling_agent: Optional[ModelingAgent]
    type_resolver: Optional[TypeResolver]
    graph_writer: Optional[GraphWriter]
    graph_store: Optional[GraphStore]
    vector_store: Optional[VectorStore]
    extractor: Optional[LLMExtractor] = None
    wsd_preprocessor: Optional[WsdPreprocessor] = None
    type_grounding_llm: Optional[TypeGroundingLLM] = None
    user_name: str = ""

    def build(self):
        g: StateGraph = StateGraph(AgenticState)

        def prep_node(state: AgenticState) -> AgenticState:
            if not self.prep_agent:
                return {**state, "prep": {}}
            try:
                prep = self.prep_agent.run(state["text"])
                return {**state, "prep": prep}
            except Exception as e:
                return {**state, "prep": {"_error": str(e)}}

        def wsd_node(state: AgenticState) -> AgenticState:
            if not self.wsd_preprocessor:
                return {**state, "wsd_profile": {"entities": []}}
            try:
                prof = self.wsd_preprocessor.run(state.get("text") or "")
                if not isinstance(prof, dict):
                    prof = {"entities": []}
                if not isinstance(prof.get("entities"), list):
                    prof = {"entities": []}
                return {**state, "wsd_profile": prof}
            except Exception:
                return {**state, "wsd_profile": {"entities": []}}

        def model_node(state: AgenticState) -> AgenticState:
            prep = state.get("prep") or {}
            if not self.modeling_agent or not prep.get("micro_events"):
                return {**state, "graph_spec": {"nodes": [], "edges": []}}
            try:
                existing_types: List[str] = []
                if self.type_resolver:
                    existing_types = self.type_resolver.get_existing_types()
                spec = self.modeling_agent.run(
                    prep=prep,
                    user_name=self.user_name,
                    existing_types=existing_types,
                    day_bucket=state.get("day_bucket", ""),
                )
                llm_ground: Optional[Dict[str, Any]] = None
                if self.type_grounding_llm and self.type_resolver:
                    reqs = collect_e55_grounding_requests(spec)
                    if reqs:
                        # Skip grounding for types already resolved: seed vocab entries
                        # with known QIDs, and types cached in Neo4j from prior entries.
                        from .type_vocab import SEED_VOCAB
                        seeded_lower = {
                            k.lower()
                            for k, v in SEED_VOCAB.items()
                            if v.get("wikidata_id")
                        }
                        try:
                            neo4j_grounded_lower = {
                                n.lower()
                                for n in self.type_resolver.get_grounded_types()
                            }
                        except Exception:
                            neo4j_grounded_lower = set()
                        skip_lower = seeded_lower | neo4j_grounded_lower
                        reqs_to_ground = [
                            r for r in reqs
                            if (r.get("name") or "").lower() not in skip_lower
                        ]
                        if reqs_to_ground:
                            llm_ground = self.type_grounding_llm.run(
                                state.get("text") or "",
                                reqs_to_ground,
                                state.get("wsd_profile"),
                            )
                if self.type_resolver:
                    spec = self.type_resolver.resolve_graph_spec(
                        spec,
                        existing_types,
                        journal_text=state.get("text") or "",
                        wsd_profile=state.get("wsd_profile"),
                        llm_grounding=llm_ground,
                    )

                # Entity linking: E53_Place / E21_Person / E74_Group → Wikidata instance QIDs
                if self.type_grounding_llm:
                    el_reqs = collect_entity_linking_requests(spec, user_name=self.user_name)
                    if el_reqs:
                        user_profile = _extract_user_profile(spec, self.user_name)
                        el_bundle = self.type_grounding_llm.run_entity_linking(
                            state.get("text") or "",
                            el_reqs,
                            user_profile=user_profile,
                        )
                        # apply_entity_linking expects flat name -> {wikidata_id, description, ...}
                        el_confirmed = (
                            el_bundle.get("confirmed")
                            if isinstance(el_bundle, dict)
                            else None
                        )
                        if isinstance(el_confirmed, dict) and el_confirmed:
                            spec = apply_entity_linking(
                                spec, el_confirmed, user_name=self.user_name
                            )
                        el_pending = (
                            el_bundle.get("pending")
                            if isinstance(el_bundle, dict)
                            else None
                        )
                        if isinstance(el_pending, dict) and el_pending:
                            wtasks = build_entity_linking_wikidata_tasks(
                                spec,
                                str(state.get("entry_id") or ""),
                                el_pending,
                            )
                            if wtasks:
                                spec["_entity_linking_wikidata_tasks"] = wtasks

                return {**state, "graph_spec": spec}
            except Exception as e:
                return {**state, "graph_spec": {"nodes": [], "edges": [], "_error": str(e)}}

        def persist_graph_node(state: AgenticState) -> AgenticState:
            spec = state.get("graph_spec") or {}
            if not self.graph_writer or not spec.get("nodes"):
                return {**state, "graph_status": "skipped", "graph_audit": {"status": "skipped"}}
            try:
                audit = self.graph_writer.write(
                    spec=spec,
                    entry_id=state["entry_id"],
                    raw_text=state["text"],
                    user_name=self.user_name,
                    day_bucket=state.get("day_bucket", ""),
                    wsd_profile=state.get("wsd_profile"),
                )
                return {**state, "graph_status": "ok", "graph_audit": audit}
            except Exception as e:
                return {**state, "graph_status": f"error: {e}", "graph_audit": {"status": "error", "detail": str(e)}}

        def disambiguate_persons_node(state: AgenticState) -> AgenticState:
            spec = state.get("graph_spec") or {}
            if not self.graph_store or not isinstance(spec, dict):
                return {**state, "person_resolution": {"status": "skipped"}}
            nodes = spec.get("nodes", [])
            if not isinstance(nodes, list):
                return {**state, "person_resolution": {"status": "skipped"}}

            prep = state.get("prep") or {}
            entities = prep.get("entities", []) if isinstance(prep, dict) else []
            places_ctx: List[str] = []
            topics_ctx: List[str] = []
            for e in entities:
                if not isinstance(e, dict):
                    continue
                nm = str(e.get("name", "")).strip()
                tp = str(e.get("type", "")).strip().lower()
                if not nm:
                    continue
                if tp == "place":
                    places_ctx.append(nm)
                elif tp in {"concept", "object", "organization"}:
                    topics_ctx.append(nm)

            updated = 0
            try:
                for n in nodes:
                    if not isinstance(n, dict):
                        continue
                    if str(n.get("label", "")) != "E21_Person":
                        continue
                    mention = str(n.get("name", "")).strip()
                    if not mention:
                        continue
                    if self.user_name and mention.lower() == self.user_name.lower():
                        continue

                    props = n.get("properties", {})
                    if not isinstance(props, dict):
                        props = {}
                    role = str(props.get("role", "") or "").strip()

                    resolved = self.graph_store.resolve_person(
                        mention=mention,
                        entry_text=state.get("text", ""),
                        places=places_ctx,
                        topics=topics_ctx,
                        role=role,
                        entry_id=state.get("entry_id"),
                        interactive=False,
                    )
                    rid = str(resolved.get("id", "") or "").strip() if isinstance(resolved, dict) else ""
                    if rid:
                        props["person_id"] = rid
                        n["properties"] = props
                        if resolved.get("name"):
                            n["name"] = str(resolved["name"])
                        updated += 1
                return {**state, "graph_spec": spec, "person_resolution": {"status": "ok", "updated": updated}}
            except Exception as e:
                return {**state, "person_resolution": {"status": "error", "detail": str(e), "updated": updated}}

        def persist_vector_node(state: AgenticState) -> AgenticState:
            if not self.vector_store:
                return {**state, "vector_status": "skipped"}
            try:
                prep = state.get("prep") or {}
                entities = prep.get("entities", [])
                entity_names = [
                    e.get("name", "") for e in entities
                    if isinstance(e, dict) and e.get("name")
                ][:10]
                self.vector_store.add_entry(
                    entry_id=state["entry_id"],
                    text=state["text"],
                    metadata={
                        "entity_count": len(entities),
                        "entities": entity_names,
                    },
                )
                return {**state, "vector_status": "ok"}
            except Exception as e:
                return {**state, "vector_status": f"error: {e}"}

        g.add_node("prep", prep_node)
        g.add_node("wsd", wsd_node)
        g.add_node("model", model_node)
        g.add_node("disambiguate_persons", disambiguate_persons_node)
        g.add_node("persist_graph", persist_graph_node)
        g.add_node("persist_vector", persist_vector_node)

        g.set_entry_point("prep")
        g.add_edge("prep", "wsd")
        g.add_edge("wsd", "model")
        g.add_edge("model", "disambiguate_persons")
        g.add_edge("disambiguate_persons", "persist_graph")
        g.add_edge("persist_graph", "persist_vector")
        g.add_edge("persist_vector", END)

        return g.compile()
