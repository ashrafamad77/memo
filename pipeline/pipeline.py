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
    BABELFY_API_KEY,
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

    def get_disambiguation_questions(self, text: str) -> list:
        """Dry-run prep → model → LLM disambiguation without persisting anything.

        Returns the ``clarifications_needed`` list (may be empty).  Call this before
        ``process_agentic`` so the caller can ask the user for clarifications first,
        then pass ``clarification_answers`` to ``process_agentic``.
        """
        from .modeling_agent import ModelingAgent
        from .type_resolver import TypeResolver, collect_e55_grounding_requests, collect_entity_linking_requests
        from .type_vocab import SEED_VOCAB
        from .llm_disambiguator import assign_mention_ids, disambiguate_mentions

        deployment = (AZURE_OPENAI_DEPLOYMENT or "gpt-4o-mini").strip()
        modeling_agent = ModelingAgent(
            api_key=AZURE_OPENAI_API_KEY,
            model=deployment,
            azure_endpoint=AZURE_OPENAI_ENDPOINT.strip(),
            api_version=AZURE_OPENAI_API_VERSION,
        )

        prep = {}
        try:
            prep = self.prep_agent.run(text)
        except Exception:
            return []

        if not prep.get("micro_events"):
            return []

        existing_types: list = []
        type_resolver = None
        if self.graph_store and self.graph_store.driver:
            try:
                type_resolver = TypeResolver(self.graph_store.driver)
                existing_types = type_resolver.get_existing_types()
            except Exception:
                pass

        try:
            spec = modeling_agent.run(
                prep=prep,
                user_name=USER_NAME,
                existing_types=existing_types,
                day_bucket=datetime.now().strftime("%Y-%m-%d"),
            )
        except Exception:
            return []

        if not spec.get("nodes"):
            return []

        type_reqs = collect_e55_grounding_requests(spec)
        entity_reqs = collect_entity_linking_requests(spec, user_name=USER_NAME)

        seeded_lower = {k.lower() for k, v in SEED_VOCAB.items() if v.get("wikidata_id")}
        neo4j_grounded_lower: set = set()
        if type_resolver:
            try:
                neo4j_grounded_lower = {n.lower() for n in type_resolver.get_grounded_types()}
            except Exception:
                pass
        skip_lower = seeded_lower | neo4j_grounded_lower

        raw_mentions: list = []
        for r in type_reqs:
            nm = str(r.get("name") or "").strip()
            if nm and nm.lower() not in skip_lower:
                raw_mentions.append({"name": nm, "cidoc_label": "E55_Type"})
        for r in entity_reqs:
            nm = str(r.get("name") or "").strip()
            if nm:
                raw_mentions.append({"name": nm, "cidoc_label": str(r.get("cidoc_label") or "")})

        if not raw_mentions:
            return []

        mentions = assign_mention_ids(raw_mentions)
        try:
            results = disambiguate_mentions(text, mentions)
        except Exception:
            return []

        # E55_Type: always auto-resolved, never ask the user.
        # Entities: use the LLM flag as a hint, BUT also apply a deterministic backup:
        # if the LLM had to change the surface form to produce a canonical label
        # (i.e., it did disambiguation work) AND didn't mark it as needing clarification,
        # force it — the LLM is overconfident and must not silently pick a sense.
        out = []
        for r in results:
            if r.get("cidoc_label") == "E55_Type":
                continue
            if r.get("needs_clarification"):
                out.append(r)
                continue
            # Backup: canonical_label differs from surface name → LLM resolved silently.
            # Treat as needing clarification so the user can confirm or correct.
            name = (r.get("name") or "").strip().lower()
            canonical = (r.get("canonical_label") or "").strip().lower()
            if canonical and canonical != name:
                r = dict(r)
                r["needs_clarification"] = True
                r["reason"] = (
                    r.get("reason")
                    or "Resolved automatically — please confirm this is correct."
                )
                out.append(r)
        return out

    def process_agentic(
        self,
        text: str,
        entry_id: Optional[str] = None,
        *,
        clarification_answers: Optional[dict] = None,
    ) -> dict:
        """Run the v2 agentic pipeline.

        Multi-turn clarification
        ------------------------
        If the result contains a non-empty ``clarifications_needed`` list, the caller
        should ask the user to resolve each item and then re-call with::

            clarification_answers = {
                "m0": "Victoria, London",   # keyed by mention id, NOT surface text
                "m3": "Victoria station",
            }

        Keying by mention id (not surface text) is required because the same surface
        form can appear multiple times in a single entry with different referents.
        """
        """Run the v2 pipeline: Prep → Model → TypeResolve → WriteGraph → WriteVector."""
        from .agentic import AgenticRunner
        from .modeling_agent import ModelingAgent
        from .type_resolver import TypeResolver
        from .graph_writer import GraphWriter
        from .wsd_preprocess import WsdPreprocessor

        entry_id = entry_id or str(uuid.uuid4())
        deployment = (AZURE_OPENAI_DEPLOYMENT or "gpt-4o-mini").strip()

        wsd_preprocessor = WsdPreprocessor(
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
            user_name=USER_NAME,
        )
        app = runner.build()
        initial_state: dict = {
            "text": text,
            "entry_id": entry_id,
            "day_bucket": day_bucket,
        }
        if clarification_answers:
            initial_state["clarification_answers"] = clarification_answers
        out = app.invoke(initial_state)

        prep = out.get("prep") or {}
        entities_raw = prep.get("entities", [])
        entities = [
            {"text": e.get("name", ""), "type": e.get("type", "")}
            for e in entities_raw if isinstance(e, dict)
        ]
        gs = out.get("graph_spec") or {}
        bf_stats = None
        bf_e55_stats = None
        if isinstance(gs, dict):
            raw_bf = gs.get("_babelfy_entity_linking")
            if isinstance(raw_bf, dict):
                bf_stats = dict(raw_bf)
            raw_e55 = gs.get("_babelfy_e55_grounding")
            if isinstance(raw_e55, dict):
                bf_e55_stats = dict(raw_e55)
        el_mode = "llm+babelnet" if (BABELFY_API_KEY or "").strip() else "off"
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
            "entity_linking_mode": el_mode,
            "babelfy_entity_linking": bf_stats,
            "babelfy_e55_grounding": bf_e55_stats,
            # Multi-turn clarification support:
            # If non-empty, the caller should ask the user to resolve these,
            # then re-call process_agentic(..., clarification_answers={name: sense, ...})
            "clarifications_needed": out.get("clarifications_needed") or [],
            "disambiguated_mentions": out.get("disambiguated_mentions") or [],
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
