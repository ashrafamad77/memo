"""Agentic orchestration (LangGraph) for the memory pipeline.

Flow: Prep → Model → TypeResolve → WriteGraph → WriteVector
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


class AgenticState(TypedDict, total=False):
    text: str
    entry_id: str
    day_bucket: str
    prep: Dict[str, Any]
    graph_spec: Dict[str, Any]
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
                if self.type_resolver:
                    spec = self.type_resolver.resolve_graph_spec(spec, existing_types)
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
                )
                return {**state, "graph_status": "ok", "graph_audit": audit}
            except Exception as e:
                return {**state, "graph_status": f"error: {e}", "graph_audit": {"status": "error", "detail": str(e)}}

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
        g.add_node("model", model_node)
        g.add_node("persist_graph", persist_graph_node)
        g.add_node("persist_vector", persist_vector_node)

        g.set_entry_point("prep")
        g.add_edge("prep", "model")
        g.add_edge("model", "persist_graph")
        g.add_edge("persist_graph", "persist_vector")
        g.add_edge("persist_vector", END)

        return g.compile()
