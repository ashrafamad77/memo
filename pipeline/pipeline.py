"""Main pipeline: text -> extract -> graph + vector store."""
from datetime import datetime, timedelta
from typing import List, Optional
import uuid

from .graph_store import GraphStore
from .llm_extractor import LLMExtractor
from .prep_agent import PrepAgent
from .vector_store import VectorStore

from config import (
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_ENDPOINT,
    AZURE_OPENAI_DEPLOYMENT,
    AZURE_OPENAI_API_VERSION,
    USER_NAME,
)


class MemoryPipeline:
    """
    Orchestrates the full pipeline:
    1. Extract entities from text (LLM uniquement)
    2. Store in Neo4j (graph)
    3. Store in Weaviate (vector search)
    """
    
    def __init__(
        self,
        use_graph: bool = True,
        use_vector: bool = True,
        use_llm: Optional[bool] = None,
    ):
        # LLM-only: Azure AI Foundry (utilisé exclusivement)
        use_azure = (AZURE_OPENAI_API_KEY or "").strip() and (AZURE_OPENAI_ENDPOINT or "").strip()
        if not use_azure:
            raise RuntimeError(
                "Configurer AZURE_OPENAI_API_KEY et AZURE_OPENAI_ENDPOINT dans .env pour utiliser la pipeline (LLM Azure-only)."
            )
        deployment = (AZURE_OPENAI_DEPLOYMENT or "gpt-4o-mini").strip()
        self.extractor = LLMExtractor(
            api_key=AZURE_OPENAI_API_KEY,
            model=deployment,
            azure_endpoint=AZURE_OPENAI_ENDPOINT.strip(),
            api_version=AZURE_OPENAI_API_VERSION,
            user_name=USER_NAME,
        )
        self.prep_agent = PrepAgent(
            api_key=AZURE_OPENAI_API_KEY,
            model=deployment,
            azure_endpoint=AZURE_OPENAI_ENDPOINT.strip(),
            api_version=AZURE_OPENAI_API_VERSION,
        )
        
        self.graph_store = None
        if use_graph:
            try:
                self.graph_store = GraphStore()
            except Exception:
                self.graph_store = None  # Neo4j non disponible
        
        self.vector_store = None
        self._vector_init_error: Optional[Exception] = None
        if use_vector:
            try:
                self.vector_store = VectorStore()
            except Exception as e:
                # Weaviate non disponible ou client incompatible
                self.vector_store = None
                self._vector_init_error = e
    
    def process(self, text: str, entry_id: Optional[str] = None) -> dict:
        """
        Process a journal entry through the full pipeline.
        
        Returns: dict with entry_id, extraction result, and status.
        """
        entry_id = entry_id or str(uuid.uuid4())

        # 1. Extract entities (LLM-only)
        prep = {}
        try:
            prep = self.prep_agent.run(text)
        except Exception:
            prep = {}
        extract_input = (prep.get("normalized_text") or "").strip() if isinstance(prep, dict) else ""
        extract_input = extract_input if extract_input else text
        extraction = self.extractor.extract(extract_input, prep_context=prep if isinstance(prep, dict) else None)
        if isinstance(extraction.metadata, dict):
            extraction.metadata["prep_v1"] = prep
            extraction.metadata["raw_text"] = text

        # Resolve relative dates like "aujourd'hui" -> absolute event_time_iso,
        # then drop those Date entities so we don't store them as Date nodes.
        extraction = self._resolve_relative_dates(extraction, input_dt=datetime.now())

        return self.persist_extraction(text=text, extraction=extraction, entry_id=entry_id)

    def process_agentic(self, text: str, entry_id: Optional[str] = None) -> dict:
        """Run the v2 pipeline: Prep → Model → TypeResolve → WriteGraph → WriteVector."""
        from .agentic import AgenticRunner
        from .modeling_agent import ModelingAgent
        from .type_resolver import TypeResolver
        from .graph_writer import GraphWriter
        from .wsd_preprocess import WsdPreprocessor
        from .type_grounding_llm import TypeGroundingLLM

        entry_id = entry_id or str(uuid.uuid4())
        deployment = (AZURE_OPENAI_DEPLOYMENT or "gpt-4o-mini").strip()

        wsd_preprocessor = WsdPreprocessor(
            api_key=AZURE_OPENAI_API_KEY,
            model=deployment,
            azure_endpoint=AZURE_OPENAI_ENDPOINT.strip(),
            api_version=AZURE_OPENAI_API_VERSION,
        )
        type_grounding_llm = TypeGroundingLLM(
            api_key=AZURE_OPENAI_API_KEY,
            model=deployment,
            azure_endpoint=AZURE_OPENAI_ENDPOINT.strip(),
            api_version=AZURE_OPENAI_API_VERSION,
        )

        modeling_agent = ModelingAgent(
            api_key=AZURE_OPENAI_API_KEY,
            model=deployment,
            azure_endpoint=AZURE_OPENAI_ENDPOINT.strip(),
            api_version=AZURE_OPENAI_API_VERSION,
        )

        type_resolver = None
        graph_writer = None
        if self.graph_store and self.graph_store.driver:
            type_resolver = TypeResolver(self.graph_store.driver)
            graph_writer = GraphWriter(self.graph_store.driver)

        day_bucket = datetime.now().strftime("%Y-%m-%d")

        runner = AgenticRunner(
            prep_agent=self.prep_agent,
            modeling_agent=modeling_agent,
            type_resolver=type_resolver,
            graph_writer=graph_writer,
            graph_store=self.graph_store,
            vector_store=self.vector_store,
            extractor=self.extractor,
            wsd_preprocessor=wsd_preprocessor,
            type_grounding_llm=type_grounding_llm,
            user_name=USER_NAME,
        )
        app = runner.build()
        out = app.invoke({
            "text": text,
            "entry_id": entry_id,
            "day_bucket": day_bucket,
        })

        prep = out.get("prep") or {}
        entities_raw = prep.get("entities", [])
        entities = [
            {"text": e.get("name", ""), "type": e.get("type", "")}
            for e in entities_raw if isinstance(e, dict)
        ]
        return {
            "entry_id": entry_id,
            "entities": entities,
            "relations": [],
            "graph": out.get("graph_status", "skipped"),
            "audit": out.get("graph_audit", {}),
            "vector": out.get("vector_status", "skipped"),
            "prep": prep,
            "graph_spec": out.get("graph_spec", {}),
            "wsd_profile": out.get("wsd_profile") or {"entities": []},
        }

    def persist_extraction(self, text: str, extraction, entry_id: Optional[str] = None) -> dict:
        """
        Persist a pre-computed extraction result to graph + vector stores.
        Used by chat clarifier flow to store only confirmed structured fields.
        """
        entry_id = entry_id or str(uuid.uuid4())

        # 1) Graph
        graph_status = "skipped"
        if self.graph_store:
            try:
                self.graph_store.store_entry(
                    entry_id=entry_id,
                    text=text,
                    extraction=extraction,
                    user_name=USER_NAME,
                )
                graph_status = "ok"
            except Exception as e:
                graph_status = f"error: {e}"

        # 2) Vector
        vector_status = "skipped"
        if self.vector_store:
            try:
                self.vector_store.add_entry(
                    entry_id=entry_id,
                    text=text,
                    metadata={
                        "entity_count": len(extraction.entities),
                        "entities": [e.text for e in extraction.entities if e.label != "Date"][:10],
                        "metadata": extraction.metadata,
                    },
                )
                vector_status = "ok"
            except Exception as e:
                vector_status = f"error: {e}"
        elif self._vector_init_error is not None:
            vector_status = f"init-error: {self._vector_init_error}"

        return {
            "entry_id": entry_id,
            "entities": [{"text": e.text, "type": e.label} for e in extraction.entities],
            "relations": [
                {"subject": r.subject, "predicate": r.predicate, "object": r.obj, "sentiment": r.sentiment}
                for r in extraction.relations
            ],
            "graph": graph_status,
            "vector": vector_status,
        }

    @staticmethod
    def _resolve_relative_dates(extraction, input_dt: datetime):
        meta = extraction.metadata or {}
        # Keep existing LLM-provided absolute time if present
        if not meta.get("event_time_iso"):
            for e in list(extraction.entities):
                if e.label != "Date":
                    continue
                t = e.text.strip().lower()
                t = t.replace("’", "'")
                if t in ("aujourd'hui", "aujourdhui"):
                    meta["event_time_iso"] = input_dt.replace(microsecond=0).isoformat() + "Z"
                    meta["event_time_confidence"] = max(float(meta.get("event_time_confidence", 0.0)), 0.9)
                elif t == "hier":
                    dt = input_dt - timedelta(days=1)
                    meta["event_time_iso"] = dt.replace(microsecond=0).isoformat() + "Z"
                    meta["event_time_confidence"] = max(float(meta.get("event_time_confidence", 0.0)), 0.9)
                elif t == "demain":
                    dt = input_dt + timedelta(days=1)
                    meta["event_time_iso"] = dt.replace(microsecond=0).isoformat() + "Z"
                    meta["event_time_confidence"] = max(float(meta.get("event_time_confidence", 0.0)), 0.9)

        # Drop relative date mentions from entities (not useful long-term)
        drop = {"aujourd'hui", "aujourdhui", "hier", "demain"}
        extraction.entities = [
            e for e in extraction.entities
            if not (e.label == "Date" and e.text.strip().lower().replace("’", "'") in drop)
        ]
        extraction.metadata = meta
        return extraction
    
    def search_semantic(self, query: str, n_results: int = 5) -> List[dict]:
        """Semantic search over journal entries."""
        if not self.vector_store:
            return []
        return self.vector_store.search(query, n_results=n_results)
    
    def search_by_entity(self, entity_name: str) -> List[dict]:
        """Find entries that mention an entity (graph query)."""
        if not self.graph_store:
            return []
        return self.graph_store.search_by_entity(entity_name)
    
    def list_entities(self, limit: int = 50) -> List[dict]:
        """List all known entities from the graph."""
        if not self.graph_store:
            return []
        return self.graph_store.query_entities(limit=limit)

    def reset_graph(self, *, keep_user_profile: bool = False) -> bool:
        """Clear all Neo4j data. Returns True if done, False if graph unavailable.

        If ``keep_user_profile`` is True, keep ``USER_NAME``'s person node and ``User`` E55_Type
        (profile properties) so UI onboarding is not required again.
        """
        if not self.graph_store:
            return False
        un = (USER_NAME or "").strip()
        if keep_user_profile and un:
            self.graph_store.reset_graph_keep_user_profile(un)
        else:
            self.graph_store.reset_graph()
        return True

    def reset_vector(self) -> bool:
        """Clear all Weaviate objects. Returns True if done, False if vector unavailable."""
        if not self.vector_store:
            return False
        self.vector_store.reset_vector()
        return True

    def reset_all(self, *, keep_user_profile: bool = False) -> dict:
        """Reset graph and vector stores."""
        return {
            "graph": self.reset_graph(keep_user_profile=keep_user_profile),
            "vector": self.reset_vector(),
        }

    def close(self):
        """Clean up connections."""
        if self.graph_store:
            self.graph_store.close()
