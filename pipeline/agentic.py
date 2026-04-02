"""Agentic orchestration (LangGraph) for the memory pipeline.

Flow:
  Prep → WSD → Model → LLM-Disambiguate → BabelNet-Lookup
  → DisambiguatePersons → WriteGraph → WriteVector

The key insight:
  - LLM-Disambiguate: uses the full journal text to determine *what* each mention refers to
    (e.g. "Victoria" → "Victoria, London"), producing canonical labels.
  - BabelNet-Lookup: uses BabelNet getSenses on those canonical labels to get formal IDs
    (synset, Wikidata QID, WordNet) — no QID hallucination risk.

Multi-turn clarification:
  When the LLM cannot confidently disambiguate a mention, it sets needs_clarification=True.
  Those items are surfaced in ``state["clarifications_needed"]`` and returned from
  process_agentic so the caller can ask the user.  The caller re-invokes process_agentic
  with ``clarification_answers={"Victoria": "Victoria, London"}`` to override the LLM.
"""
from __future__ import annotations

import logging
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
from .type_resolver import collect_e55_grounding_requests

logger = logging.getLogger(__name__)


class AgenticState(TypedDict, total=False):
    text: str
    entry_id: str
    day_bucket: str
    prep: Dict[str, Any]
    wsd_profile: Dict[str, Any]
    graph_spec: Dict[str, Any]
    # LLM disambiguation results
    disambiguated_mentions: List[Dict[str, Any]]
    clarifications_needed: List[Dict[str, Any]]
    clarification_answers: Dict[str, str]   # name → user-confirmed canonical label
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
            """Run ModelingAgent → raw graph spec. No grounding here (moved to later nodes)."""
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
                return {**state, "graph_spec": spec}
            except Exception as e:
                return {**state, "graph_spec": {"nodes": [], "edges": [], "_error": str(e)}}

        def llm_disambiguate_node(state: AgenticState) -> AgenticState:
            """LLM reads the full journal text and assigns a canonical sense to each mention.

            Each mention gets a stable id (m0, m1, …) so callers can key
            clarification_answers by id rather than by surface text, avoiding
            collision bugs when the same name appears multiple times.

            Produces state["disambiguated_mentions"] and state["clarifications_needed"].
            """
            spec = state.get("graph_spec") or {}
            if not spec.get("nodes"):
                return {**state, "disambiguated_mentions": [], "clarifications_needed": []}

            from .type_resolver import collect_e55_grounding_requests, collect_entity_linking_requests
            from .llm_disambiguator import assign_mention_ids, disambiguate_mentions

            journal_text = state.get("text") or ""
            type_reqs = collect_e55_grounding_requests(spec)
            entity_reqs = collect_entity_linking_requests(spec, user_name=self.user_name)

            # Skip types already grounded (seed vocab with QID + Neo4j cache)
            from .type_vocab import SEED_VOCAB
            seeded_lower = {k.lower() for k, v in SEED_VOCAB.items() if v.get("wikidata_id")}
            try:
                neo4j_grounded_lower = {
                    n.lower()
                    for n in (self.type_resolver.get_grounded_types() if self.type_resolver else set())
                }
            except Exception:
                neo4j_grounded_lower = set()
            skip_lower = seeded_lower | neo4j_grounded_lower

            raw_mentions: List[Dict[str, Any]] = []
            for r in type_reqs:
                nm = str(r.get("name") or "").strip()
                if nm and nm.lower() not in skip_lower:
                    raw_mentions.append({"name": nm, "cidoc_label": "E55_Type"})
            for r in entity_reqs:
                nm = str(r.get("name") or "").strip()
                if nm:
                    raw_mentions.append({"name": nm, "cidoc_label": str(r.get("cidoc_label") or "")})

            if not raw_mentions:
                return {**state, "disambiguated_mentions": [], "clarifications_needed": []}

            # Assign stable ids before calling LLM and before checking clarification_answers
            mentions = assign_mention_ids(raw_mentions)

            try:
                results = disambiguate_mentions(
                    journal_text,
                    mentions,
                    # clarification_answers keyed by mention id ("m0", "m1", …)
                    clarification_answers=state.get("clarification_answers") or {},
                )
            except Exception as exc:
                logger.debug("agentic: llm_disambiguate_node failed: %s", exc)
                results = []

            clarifications_needed = [r for r in results if r.get("needs_clarification")]
            return {
                **state,
                "disambiguated_mentions": results,
                "clarifications_needed": clarifications_needed,
            }

        def babelnet_lookup_node(state: AgenticState) -> AgenticState:
            """BabelNet getSenses on canonical labels → formal IDs → apply to spec.

            For each disambiguated mention:
            - getSenses(canonical_label) → best synset → Wikidata QID, WordNet ID, gloss
            - E55_Type results feed into TypeResolver as llm_grounding rows
            - Entity results feed into apply_entity_linking
            """
            spec = state.get("graph_spec") or {}
            if not spec.get("nodes"):
                return state

            disambiguated = state.get("disambiguated_mentions") or []
            if not disambiguated:
                # No disambiguation ran — fall back to TypeResolver alone (seed vocab + Neo4j)
                if self.type_resolver:
                    try:
                        existing_types: List[str] = self.type_resolver.get_existing_types()
                        journal_text = state.get("text") or ""
                        spec = self.type_resolver.resolve_graph_spec(
                            spec,
                            existing_types,
                            journal_text=journal_text,
                            wsd_profile=state.get("wsd_profile"),
                        )
                    except Exception as exc:
                        logger.debug("agentic: TypeResolver fallback failed: %s", exc)
                return {**state, "graph_spec": spec}

            try:
                from config import BABELFY_API_KEY
                babelfy_key = (BABELFY_API_KEY or "").strip()
            except ImportError:
                import os
                babelfy_key = os.getenv("BABELFY_API_KEY", "").strip()

            from .babelnet_client import (
                bundle_to_sources_json,
                lookup_by_label,
                enrich_babel_synset,
            )
            from .babelfy_client import disambiguate as _babelfy_disambiguate
            from .type_grounding_embed import wikidata_fetch_labels_descriptions
            from .type_resolver import apply_entity_linking

            journal_text = state.get("text") or ""

            # ── Babelfy CONCEPTS prefetch (context-aware synset for E55_Type) ─────────
            # Run once on the full text; maps each annotated span → babelSynsetID so
            # that E55_Type grounding uses WSD context instead of context-free getSenses.
            concepts_by_span: Dict[str, str] = {}  # span_text.lower() -> babelSynsetID
            if babelfy_key and journal_text:
                try:
                    concept_anns = _babelfy_disambiguate(
                        journal_text, api_key=babelfy_key, ann_type="CONCEPTS"
                    )
                    for ann in concept_anns:
                        if not isinstance(ann, dict):
                            continue
                        cf = ann.get("charFragment") or {}
                        synset = str(ann.get("babelSynsetID") or "").strip()
                        if not synset or not isinstance(cf, dict):
                            continue
                        start = cf.get("start")
                        end = cf.get("end")
                        if start is None or end is None:
                            continue
                        span = journal_text[int(start):int(end) + 1].strip().lower()
                        if span:
                            concepts_by_span[span] = synset
                except Exception as _exc:
                    logger.debug("agentic: Babelfy CONCEPTS prefetch failed: %s", _exc)

            # ── Build grounding rows (E55_Type) and EL rows (entities) ──────────────
            e55_grounding_rows: Dict[str, Any] = {}
            el_rows: Dict[str, Any] = {}

            for item in disambiguated:
                name = str(item.get("name") or "").strip()
                canonical = str(item.get("canonical_label") or name).strip() or name
                cidoc = str(item.get("cidoc_label") or "")

                if not name or not babelfy_key:
                    continue

                if cidoc == "E55_Type":
                    # Bug 4: skip system-internal role types — no meaningful KB grounding
                    if name.lower() == "user" or (
                        self.user_name and name.lower() == self.user_name.lower()
                    ):
                        logger.debug("agentic: skipping BabelNet for role type %r", name)
                        continue

                    # Bug 2: prefer Babelfy CONCEPTS (context-aware WSD) over getSenses
                    synset_from_concepts = ""
                    if concepts_by_span:
                        # Try exact match on the surface form (as it appears in the text)
                        synset_from_concepts = concepts_by_span.get(name.lower(), "")
                        if not synset_from_concepts:
                            # Try each significant word from the canonical label
                            for word in canonical.lower().split():
                                if len(word) > 3 and word in concepts_by_span:
                                    synset_from_concepts = concepts_by_span[word]
                                    break

                    if synset_from_concepts:
                        logger.debug(
                            "agentic: E55 %r → Babelfy CONCEPTS synset %s", name, synset_from_concepts
                        )
                        bundle = enrich_babel_synset(synset_from_concepts, api_key=babelfy_key)
                        bundle["synset_id"] = synset_from_concepts
                        sid = synset_from_concepts
                        bundle["babelnet_rdf_url"] = (
                            f"https://babelnet.io/rdf/page/{sid}" if sid.startswith("bn:") else ""
                        )
                        wiki_keys = bundle.get("wikipedia_en_keys") or []
                        bundle["dbpedia_url"] = (
                            f"https://dbpedia.org/resource/{wiki_keys[0].replace(' ', '_')}"
                            if wiki_keys
                            else ""
                        )
                    else:
                        # Fallback: context-free getSenses on the LLM canonical label
                        logger.debug(
                            "agentic: E55 %r → getSenses fallback on canonical %r", name, canonical
                        )
                        bundle = lookup_by_label(canonical, api_key=babelfy_key)
                else:
                    # Entities: LLM already produced a specific canonical label
                    # (e.g. "Victoria, London") so getSenses is accurate here
                    bundle = lookup_by_label(canonical, api_key=babelfy_key)

                synset_id = str(bundle.get("synset_id") or "").strip()
                wd_qids = list(bundle.get("wikidata_qids") or [])
                wn_ids = list(bundle.get("wordnet_ids") or [])
                gloss = str(bundle.get("gloss") or "").strip()
                babelnet_rdf = str(bundle.get("babelnet_rdf_url") or "").strip()
                dbpedia = str(bundle.get("dbpedia_url") or "").strip()

                # Enrich Wikidata candidates with labels/descriptions
                wikidata_candidates: List[Dict[str, str]] = []
                if wd_qids:
                    fetched = wikidata_fetch_labels_descriptions(wd_qids[:6])
                    for q in wd_qids[:6]:
                        pair = fetched.get(q, ("", ""))
                        wikidata_candidates.append({
                            "qid": q,
                            "label": str(pair[0] or "").strip(),
                            "description": str(pair[1] or "").strip(),
                        })

                top_qid = wikidata_candidates[0]["qid"] if wikidata_candidates else ""
                top_desc = (
                    wikidata_candidates[0].get("description") or gloss
                    if wikidata_candidates
                    else gloss
                )
                conf = "high" if synset_id else "low"

                sources_json = bundle_to_sources_json(bundle) if synset_id else ""

                if cidoc == "E55_Type":
                    e55_grounding_rows[name] = {
                        "confidence": conf,
                        "wikidata_id": top_qid,
                        "wikidata_candidates": wikidata_candidates,
                        "babel_synset_id": synset_id,
                        "wordnet_synset_id": wn_ids[0] if wn_ids else "",
                        "babel_gloss": gloss,
                        "babelnet_rdf_url": babelnet_rdf,
                        "dbpedia_url": dbpedia,
                        "babelnet_sources_json": sources_json,
                        "aat_id": "",
                        "aat_label": "",
                        "aat_confidence": "low",
                        "description": top_desc,
                    }
                else:
                    el_rows[name] = {
                        "wikidata_id": top_qid,
                        "description": top_desc,
                        "babel_synset_id": synset_id,
                        "wordnet_synset_id": wn_ids[0] if wn_ids else "",
                        "babel_gloss": gloss,
                        "babelnet_rdf_url": babelnet_rdf,
                        "dbpedia_url": dbpedia,
                        "babelnet_sources_json": sources_json,
                    }

            # ── TypeResolver: normalise types + apply E55 grounding ──────────────────
            if self.type_resolver:
                try:
                    existing_types = self.type_resolver.get_existing_types()
                    spec = self.type_resolver.resolve_graph_spec(
                        spec,
                        existing_types,
                        journal_text=journal_text,
                        wsd_profile=state.get("wsd_profile"),
                        llm_grounding=e55_grounding_rows if e55_grounding_rows else None,
                    )
                except Exception as exc:
                    logger.warning("agentic: TypeResolver failed: %s", exc)

            # ── Apply entity linking results ─────────────────────────────────────────
            if el_rows:
                try:
                    spec = apply_entity_linking(spec, el_rows, user_name=self.user_name)
                except Exception as exc:
                    logger.warning("agentic: apply_entity_linking failed: %s", exc)

            return {**state, "graph_spec": spec}

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
        g.add_node("llm_disambiguate", llm_disambiguate_node)
        g.add_node("babelnet_lookup", babelnet_lookup_node)
        g.add_node("disambiguate_persons", disambiguate_persons_node)
        g.add_node("persist_graph", persist_graph_node)
        g.add_node("persist_vector", persist_vector_node)

        g.set_entry_point("prep")
        g.add_edge("prep", "wsd")
        g.add_edge("wsd", "model")
        g.add_edge("model", "llm_disambiguate")
        g.add_edge("llm_disambiguate", "babelnet_lookup")
        g.add_edge("babelnet_lookup", "disambiguate_persons")
        g.add_edge("disambiguate_persons", "persist_graph")
        g.add_edge("persist_graph", "persist_vector")
        g.add_edge("persist_vector", END)

        return g.compile()
