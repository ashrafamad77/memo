"""Agentic orchestration (LangGraph) for the memory pipeline.

This is intentionally minimal: Extract -> Normalize -> Persist.
We will extend it with a real Consolidator (entity resolution) next.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import StateGraph, END

from .llm_extractor import LLMExtractor
from .graph_store import GraphStore
from .vector_store import VectorStore
from .pipeline import MemoryPipeline


class AgenticState(TypedDict, total=False):
    text: str
    entry_id: str
    extraction: Any
    graph_status: str
    vector_status: str
    consolidation: Dict[str, Any]


@dataclass
class AgenticRunner:
    extractor: LLMExtractor
    graph_store: Optional[GraphStore]
    vector_store: Optional[VectorStore]
    user_name: str = ""

    def build(self):
        g: StateGraph = StateGraph(AgenticState)

        def extract_node(state: AgenticState) -> AgenticState:
            extraction = self.extractor.extract(state["text"])
            return {**state, "extraction": extraction}

        def normalize_node(state: AgenticState) -> AgenticState:
            extraction = state["extraction"]
            extraction = MemoryPipeline._resolve_relative_dates(extraction, input_dt=datetime.now())
            return {**state, "extraction": extraction}

        def consolidate_node(state: AgenticState) -> AgenticState:
            # Non-blocking HITL: consolidation happens during persistence (GraphStore.store_entry),
            # which can emit DisambiguationTask nodes for the UI Inbox.
            return {**state, "consolidation": {"status": "skipped"}}

        def persist_graph_node(state: AgenticState) -> AgenticState:
            if not self.graph_store:
                return {**state, "graph_status": "skipped"}
            try:
                self.graph_store.store_entry(
                    entry_id=state["entry_id"],
                    text=state["text"],
                    extraction=state["extraction"],
                    user_name=self.user_name,
                )
                return {**state, "graph_status": "ok"}
            except Exception as e:
                return {**state, "graph_status": f"error: {e}"}

        def persist_vector_node(state: AgenticState) -> AgenticState:
            if not self.vector_store:
                return {**state, "vector_status": "skipped"}
            try:
                extraction = state["extraction"]
                self.vector_store.add_entry(
                    entry_id=state["entry_id"],
                    text=state["text"],
                    metadata={
                        "entity_count": len(extraction.entities),
                        "entities": [e.text for e in extraction.entities if e.label != "Date"][:10],
                        "metadata": extraction.metadata,
                    },
                )
                return {**state, "vector_status": "ok"}
            except Exception as e:
                return {**state, "vector_status": f"error: {e}"}

        g.add_node("extract", extract_node)
        g.add_node("normalize", normalize_node)
        g.add_node("consolidate", consolidate_node)
        g.add_node("persist_graph", persist_graph_node)
        g.add_node("persist_vector", persist_vector_node)

        g.set_entry_point("extract")
        g.add_edge("extract", "normalize")
        g.add_edge("normalize", "consolidate")
        g.add_edge("consolidate", "persist_graph")
        g.add_edge("persist_graph", "persist_vector")
        g.add_edge("persist_vector", END)

        return g.compile()

