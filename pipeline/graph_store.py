"""Neo4j graph storage — event-centric ontology + Person consolidator."""
from datetime import datetime
from typing import List, Optional, Tuple
import uuid

from neo4j import GraphDatabase

from .extractor import ExtractionResult, ExtractedEntity
from .embedding_service import embed_text, embedding_dim
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


class GraphStore:
    """Event-centric graph: Entry -> Event -> (Person|Place|Concept)."""

    def __init__(
        self,
        uri: str = NEO4J_URI,
        user: str = NEO4J_USER,
        password: str = NEO4J_PASSWORD,
    ):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        with self.driver.session() as session:
            session.execute_write(self._init_schema)
            # Automatic one-way cleanup: legacy app labels/relations -> CIDOC equivalents.
            session.execute_write(self._purge_legacy_labels_and_relations)

    def close(self):
        self.driver.close()

    def reset_graph(self) -> None:
        """Delete all nodes and relationships. Use before migrating to new schema."""
        with self.driver.session() as session:
            # Drop legacy uniqueness constraint on :Person(name) if it exists (older schema).
            try:
                rows = session.run(
                    """
                    SHOW CONSTRAINTS
                    YIELD name, type, entityType, labelsOrTypes, properties
                    WHERE entityType = 'NODE'
                      AND type IN ['UNIQUENESS', 'NODE_PROPERTY_UNIQUENESS']
                      AND labelsOrTypes = ['Person']
                      AND properties = ['name']
                    RETURN name
                    """
                )
                for r in rows:
                    cname = r.get("name")
                    if cname:
                        session.run(f"DROP CONSTRAINT {cname} IF EXISTS")
            except Exception:
                # If SHOW CONSTRAINTS unsupported, ignore.
                pass

            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
            # Re-apply current schema (constraints + indexes)
            session.execute_write(self._init_schema)

    def _init_schema(self, tx):
        # People: use stable IDs (names are NOT unique)
        tx.run("""
            CREATE CONSTRAINT person_id IF NOT EXISTS
            FOR (p:E21_Person) REQUIRE p.id IS UNIQUE
        """)
        tx.run("""
            CREATE INDEX person_name IF NOT EXISTS
            FOR (p:E21_Person) ON (p.name)
        """)
        tx.run("""
            CREATE CONSTRAINT alias_id IF NOT EXISTS
            FOR (a:Alias) REQUIRE a.id IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT place_name IF NOT EXISTS
            FOR (p:E53_Place) REQUIRE p.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT org_name IF NOT EXISTS
            FOR (o:E74_Group) REQUIRE o.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT concept_name IF NOT EXISTS
            FOR (c:E28_Conceptual_Object) REQUIRE c.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT date_name IF NOT EXISTS
            FOR (d:E52_Time_Span) REQUIRE d.key IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT emotion_name IF NOT EXISTS
            FOR (e:E55_Type) REQUIRE e.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT event_type_name IF NOT EXISTS
            FOR (t:E55_Type) REQUIRE t.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT user_name IF NOT EXISTS
            FOR (u:E21_Person) REQUIRE u.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT event_key IF NOT EXISTS
            FOR (e:E7_Activity) REQUIRE e.key IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT day_date IF NOT EXISTS
            FOR (d:E52_Time_Span) REQUIRE d.key IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT disambiguation_task_id IF NOT EXISTS
            FOR (t:DisambiguationTask) REQUIRE t.id IS UNIQUE
        """)

        # Vector index for entity resolution (Neo4j 5+)
        try:
            dim = embedding_dim()
            tx.run(f"""
                CREATE VECTOR INDEX person_index IF NOT EXISTS
                FOR (p:E21_Person) ON (p.embedding)
                OPTIONS {{
                  indexConfig: {{
                    `vector.dimensions`: {dim},
                    `vector.similarity_function`: 'cosine'
                  }}
                }}
            """)
        except Exception:
            # If vector indexes unsupported on this Neo4j build, skip.
            pass

    def _purge_legacy_labels_and_relations(self, tx):
        """
        Idempotent migration pass that upgrades legacy app labels/relations to CIDOC.
        Safe to run on every startup.
        """
        # --- Label migrations ---
        tx.run("""
            MATCH (n:User)
            REMOVE n:User
            SET n:E21_Person:E39_Actor
        """)
        tx.run("""
            MATCH (n:Entry)
            REMOVE n:Entry
            SET n:E73_Information_Object,
                n.entry_kind = coalesce(n.entry_kind, 'journal_entry')
        """)
        tx.run("""
            MATCH (n:Place)
            REMOVE n:Place
            SET n:E53_Place
        """)
        tx.run("""
            MATCH (n:Concept)
            REMOVE n:Concept
            SET n:E28_Conceptual_Object
        """)
        tx.run("""
            MATCH (n:Organization)
            REMOVE n:Organization
            SET n:E74_Group
        """)
        tx.run("""
            MATCH (n:Day)
            REMOVE n:Day
            SET n:E52_Time_Span,
                n.key = coalesce(n.key, n.date),
                n.date = coalesce(n.date, n.key),
                n.name = coalesce(n.name, n.date, n.key)
        """)
        tx.run("""
            MATCH (n:Date)
            REMOVE n:Date
            SET n:E52_Time_Span,
                n.key = coalesce(n.key, n.name, n.date),
                n.date = coalesce(n.date, n.name),
                n.name = coalesce(n.name, n.date, n.key)
        """)
        tx.run("""
            MATCH (n:E52_Time_Span)
            WHERE n.date IS NOT NULL
            SET n.name = coalesce(n.name, n.date)
        """)
        tx.run("""
            MATCH (n:EventType)
            REMOVE n:EventType
            SET n:E55_Type
        """)
        tx.run("""
            MATCH (n:Emotion)
            REMOVE n:Emotion
            SET n:E55_Type
        """)
        tx.run("""
            MATCH (n:Person)
            REMOVE n:Person
            SET n:E21_Person
        """)

        # Ensure user typing exists when profile fields are present.
        tx.run("""
            MATCH (u:E21_Person)
            WHERE u.profile_current_city IS NOT NULL
               OR u.profile_home_country IS NOT NULL
               OR u.profile_nationality IS NOT NULL
               OR u.profile_timezone IS NOT NULL
               OR u.profile_work_context IS NOT NULL
            MERGE (ut:E55_Type {name:'User'})
            MERGE (u)-[:P2_has_type]->(ut)
        """)

        # --- Relationship migrations ---
        tx.run("""
            MATCH (a)-[r:REFERS_TO]->(b:E7_Activity)
            MERGE (a)-[:P67_refers_to {ref_type:'about_activity'}]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a)-[r:REFERS_TO]->(b:E21_Person)
            MERGE (a)-[:P67_refers_to {ref_type:'alias_of'}]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a)-[r:PARTICIPATED_IN]->(b:E7_Activity)
            MERGE (b)-[:P14_carried_out_by]->(a)
            MERGE (a)-[:P14i_performed]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a:E21_Person)-[r:P14_carried_out_by]->(b:E7_Activity)
            MERGE (b)-[:P14_carried_out_by]->(a)
            MERGE (a)-[:P14i_performed]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (ev:E7_Activity)-[:P14_carried_out_by]->(ac:E39_Actor)
            MERGE (ac)-[:P14i_performed]->(ev)
        """)
        tx.run("""
            MATCH (ac:E39_Actor)-[:P14i_performed]->(ev:E7_Activity)
            MERGE (ev)-[:P14_carried_out_by]->(ac)
        """)
        tx.run("""
            MATCH (a:E7_Activity)-[r:OCCURRED_AT]->(b:E53_Place)
            MERGE (a)-[:P7_took_place_at]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a:E7_Activity)-[r:HAS_TYPE]->(b:E55_Type)
            MERGE (a)-[:P2_has_type]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a:E7_Activity)-[r:ON_DAY]->(b:E52_Time_Span)
            MERGE (a)-[:P4_has_time_span]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a:E7_Activity)-[r:OCCURRED_ON]->(b:E52_Time_Span)
            MERGE (a)-[:P4_has_time_span]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a:E7_Activity)-[r:HAS_EMOTION]->(b:E55_Type)
            MERGE (a)-[:P67_refers_to {ref_type:'emotion'}]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a)-[r:HAS_TOPIC]->(b)
            MERGE (a)-[:P67_refers_to {ref_type:'topic'}]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a)-[r:HAS_CONTEXT]->(b)
            MERGE (a)-[:P67_refers_to {ref_type:'context'}]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a)-[r:MENTIONS]->(b)
            MERGE (a)-[:P67_refers_to {ref_type:'mention'}]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a:E7_Activity)-[r:WAS_MOTIVATED_BY]->(b:E28_Conceptual_Object)
            MERGE (a)-[:P17_was_motivated_by]->(b)
            DELETE r
        """)
        tx.run("""
            MATCH (a:E7_Activity)-[r:PRECEDES]->(b:E7_Activity)
            MERGE (a)-[n:P120_occurs_before]->(b)
            SET n.confidence = coalesce(r.confidence, n.confidence),
                n.evidence = coalesce(r.evidence, n.evidence),
                n.inference_type = 'PRECEDES'
            DELETE r
        """)
        tx.run("""
            MATCH (a:E7_Activity)-[r:CAUSES|ENABLES|IMPACTS|INFLUENCES]->(b:E7_Activity)
            MERGE (a)-[n:P15_was_influenced_by]->(b)
            SET n.confidence = coalesce(r.confidence, n.confidence),
                n.evidence = coalesce(r.evidence, n.evidence),
                n.inference_type = type(r)
            DELETE r
        """)
        tx.run("""
            MATCH ()-[r:CIDOC_EQUIVALENT]->()
            DELETE r
        """)

    def _person_profile_from_entry(self, mention: str, entry_text: str, places: List[str], topics: List[str]) -> str:
        # Short but rich profile for embedding
        parts = [f"Person: {mention}"]
        if places:
            parts.append("Places: " + ", ".join(places[:3]))
        if topics:
            parts.append("Topics: " + ", ".join(topics[:5]))
        parts.append("Context: " + entry_text[:500])
        return " | ".join(parts)

    def _query_person_candidates(self, tx, query_vec: List[float], k: int = 5) -> List[Tuple[dict, float]]:
        try:
            result = tx.run(
                """
                CALL db.index.vector.queryNodes('person_index', $k, $vec)
                YIELD node, score
                RETURN node.id as id, node.name as name, node.role as role, score as score
                ORDER BY score DESC
                """,
                k=k,
                vec=query_vec,
            )
            return [({"id": r["id"], "name": r["name"], "role": r.get("role")}, float(r["score"])) for r in result]
        except Exception:
            return []

    def _infer_role(self, mention: str, entry_text: str) -> str:
        """Very small heuristic to guess role from the sentence."""
        txt = (entry_text or "").lower()
        if "ma soeur" in txt or "ma sœur" in txt or "ma frangine" in txt:
            return "sister"
        if "mon frère" in txt or "mon frere" in txt:
            return "brother"
        if "ma mère" in txt or "ma mere" in txt:
            return "mother"
        if "mon père" in txt or "mon pere" in txt:
            return "father"
        if "ma femme" in txt or "mon mari" in txt or "mon époux" in txt or "mon epoux" in txt:
            return "partner"
        if "collègue" in txt or "collegue" in txt or "au travail" in txt or "au boulot" in txt:
            return "colleague"
        if "ami" in txt or "amie" in txt:
            return "friend"
        return ""

    def resolve_person(
        self,
        mention: str,
        entry_text: str,
        places: List[str],
        topics: List[str],
        threshold: float = 0.90,
        create_alias: bool = True,
        interactive: bool = False,
        role: str = "",
        entry_id: Optional[str] = None,
    ) -> dict:
        """Return canonical person node ({id,name,score,created})."""
        mention = (mention or "").strip()
        if not mention:
            return {"id": "", "name": "", "score": 0.0, "created": False}

        profile = self._person_profile_from_entry(mention, entry_text, places, topics)
        qvec = embed_text(profile)

        def _resolve(tx):
            def _create_new_person(new_role: str) -> dict:
                pid = str(uuid.uuid4())
                tx.run(
                    """
                    CREATE (p:E21_Person:E39_Actor {id: $id, name: $name, created_at: datetime(), first_seen: datetime(), last_seen: datetime(), mention_count: 0, embedding: $vec, role: $role})
                    """,
                    id=pid,
                    name=mention,
                    vec=qvec,
                    role=(new_role or "").strip() or None,
                )
                if create_alias:
                    aid = str(uuid.uuid4())
                    tx.run(
                        """
                        MATCH (p:E21_Person {id: $pid})
                        CREATE (a:Alias {id: $aid, text: $text, created_at: datetime()})
                        MERGE (a)-[:P67_refers_to {ref_type: 'alias_of'}]->(p)
                        """,
                        pid=pid,
                        aid=aid,
                        text=mention,
                    )
                return {"id": pid, "name": mention, "score": 0.0, "created": True}

            def _create_task(
                candidate_person_id: str,
                proposed_person_id: str,
                score: float,
                candidate_role: str,
                proposed_role: str,
            ) -> None:
                tid = str(uuid.uuid4())
                tx.run(
                    """
                    CREATE (t:DisambiguationTask {
                      id: $id,
                      type: 'person',
                      mention: $mention,
                      score: $score,
                      status: 'open',
                      created_at: datetime(),
                      candidate_role: $candidate_role,
                      proposed_role: $proposed_role,
                      entry_id: $entry_id
                    })
                    WITH t
                    MATCH (c:E21_Person {id: $cid})
                    MATCH (p:E21_Person {id: $pid})
                    MERGE (t)-[:CANDIDATE]->(c)
                    MERGE (t)-[:PROPOSED]->(p)
                    """,
                    id=tid,
                    mention=mention,
                    score=float(score),
                    cid=candidate_person_id,
                    pid=proposed_person_id,
                    candidate_role=candidate_role or None,
                    proposed_role=proposed_role or None,
                    entry_id=entry_id or None,
                )
                # NOTE: do not create a FROM_ENTRY relationship here because resolve_person
                # runs in a separate transaction and may not see the freshly created Entry yet.
                # We store entry_id as a property; the UI can use it to fetch entry details.

            # 1) Vector candidates
            candidates = self._query_person_candidates(tx, qvec, k=5)
            if candidates and candidates[0][0].get("id"):
                best, score = candidates[0]
                # Safety: do not auto-merge purely on embedding similarity unless we have a
                # strong lexical anchor (exact name match, or existing alias match).
                same_name = (best.get("name") or "").strip().lower() == mention.strip().lower()
                existing_role = (best.get("role") or "").strip().lower() if isinstance(best, dict) else ""
                alias_match = False
                if not same_name:
                    chk = tx.run(
                        """
                        MATCH (p:E21_Person {id: $pid})<-[:P67_refers_to]-(a:Alias)
                        WHERE toLower(a.text) = toLower($text)
                        RETURN count(a) > 0 as ok
                        """,
                        pid=best["id"],
                        text=mention,
                    ).single()
                    alias_match = bool(chk and chk.get("ok"))
                # 1) Clear conflict by role: force split
                kin_roles = {"sister", "brother", "mother", "father", "partner", "sibling"}
                new_role = (role or "").strip().lower()
                # Non-blocking HITL: for same-name (or alias-match) collisions where role is
                # conflicting/unknown or similarity is mid, create a proposed person + task.
                if same_name or alias_match:
                    role_conflict = bool(new_role and existing_role and new_role != existing_role)
                    kinship_change = bool((new_role in kin_roles) and new_role != (existing_role or ""))
                    unknown_role = bool((not new_role) or (not existing_role))
                    ambiguous = bool(score >= 0.75 and score < threshold)
                    if kinship_change or role_conflict or (unknown_role and score >= 0.80) or ambiguous:
                        proposed = _create_new_person(new_role)
                        _create_task(
                            candidate_person_id=best["id"],
                            proposed_person_id=proposed["id"],
                            score=score,
                            candidate_role=existing_role,
                            proposed_role=new_role,
                        )
                        return proposed
                if new_role and existing_role and new_role != existing_role and (
                    new_role in kin_roles or existing_role in kin_roles
                ):
                    # treat as different person
                    pass
                else:
                    # 2) High similarity AND lexical anchor -> likely same person
                    if score >= threshold and (same_name or alias_match):
                        if create_alias:
                            aid = str(uuid.uuid4())
                            tx.run(
                                """
                                MATCH (p:E21_Person {id: $pid})
                                CREATE (a:Alias {id: $aid, text: $text, created_at: datetime()})
                                MERGE (a)-[:P67_refers_to {ref_type: 'alias_of'}]->(p)
                                """,
                                pid=best["id"],
                                aid=aid,
                                text=mention,
                            )
                        # Update role if previously empty and we just inferred one
                        if new_role and not existing_role:
                            tx.run(
                                "MATCH (p:E21_Person {id: $id}) SET p.role = $role",
                                id=best["id"],
                                role=new_role,
                            )
                        return {"id": best["id"], "name": best["name"], "score": score, "created": False}
                    # otherwise, fall through to create new / exact-name path

            # 2) Fallback: exact name match (if exists) to seed vector index
            res = tx.run(
                "MATCH (p:E21_Person) WHERE toLower(p.name)=toLower($name) RETURN p.id as id, p.name as name, p.role as role LIMIT 1",
                name=mention,
            ).single()
            if res and res.get("id"):
                existing_role2 = (res.get("role") or "").strip().lower() if res.get("role") is not None else ""
                new_role2 = (role or "").strip().lower()
                kin_roles2 = {"sister", "brother", "mother", "father", "partner", "sibling"}
                role_conflict2 = bool(new_role2 and existing_role2 and new_role2 != existing_role2)
                kinship_change2 = bool((new_role2 in kin_roles2) and new_role2 != (existing_role2 or ""))
                unknown_role2 = bool((not new_role2) or (not existing_role2))
                if kinship_change2 or role_conflict2 or unknown_role2:
                    proposed2 = _create_new_person(new_role2)
                    _create_task(
                        candidate_person_id=res["id"],
                        proposed_person_id=proposed2["id"],
                        score=0.85,
                        candidate_role=existing_role2,
                        proposed_role=new_role2,
                    )
                    return proposed2

            if res and res.get("id"):
                pid = res["id"]
                # Update embedding with EMA
                tx.run(
                    """
                    MATCH (p:E21_Person {id: $id})
                    SET p.embedding = CASE
                      WHEN p.embedding IS NULL THEN $vec
                      ELSE [i IN range(0, size($vec)-1) | (p.embedding[i] * 0.7) + ($vec[i] * 0.3)]
                    END,
                    p.last_seen = datetime()
                    """,
                    id=pid,
                    vec=qvec,
                )
                if create_alias:
                    aid = str(uuid.uuid4())
                    tx.run(
                        """
                        MATCH (p:E21_Person {id: $pid})
                        CREATE (a:Alias {id: $aid, text: $text, created_at: datetime()})
                        MERGE (a)-[:P67_refers_to {ref_type: 'alias_of'}]->(p)
                        """,
                        pid=pid,
                        aid=aid,
                        text=mention,
                    )
                return {"id": pid, "name": mention, "score": 0.5, "created": False}

            # 3) Create new person
            return _create_new_person((role or "").strip().lower())

        with self.driver.session() as session:
            return session.execute_write(_resolve)

    def store_entry(
        self,
        entry_id: str,
        text: str,
        extraction: ExtractionResult,
        timestamp: Optional[datetime] = None,
        user_name: Optional[str] = None,
    ) -> str:
        """
        Event-centric storage. If user_name is set, creates a User node and links it
        to every Event (the journal owner is always a participant).
        """
        ts = timestamp or datetime.now()
        input_ts_str = ts.isoformat()
        user_name = (user_name or "").strip()
        def _is_placeholder_value(v: str) -> bool:
            x = (v or "").strip().lower()
            return x in {
                "", "none", "null", "n/a", "na", "unknown", "unk",
                "other", "autre", "event", "activity", "hi", "hello", "bonjour", "salut"
            }

        def _clean_event_type(v: str) -> str:
            x = (v or "").strip()
            if _is_placeholder_value(x):
                return "activity"
            return "".join(w.capitalize() for w in x.split())

        entities = extraction.entities
        relations = extraction.relations
        meta = extraction.metadata or {}
        events_meta = meta.get("events")
        event_relations_meta = meta.get("event_relations")
        causal_factors_meta = meta.get("causal_factors")
        prep_v1 = meta.get("prep_v1") if isinstance(meta.get("prep_v1"), dict) else {}
        # v2 ontology: when LLM provides `metadata.events`, we store micro-event occurrences
        # even if there's only 1 (still needed to separate physical_place vs context_places).
        has_multi_events = isinstance(events_meta, list) and len(events_meta) >= 1
        event_time_iso = meta.get("event_time_iso")
        event_time_conf = meta.get("event_time_confidence")
        event_type = _clean_event_type(str(meta.get("event_type") or "activity"))
        person_roles_map = meta.get("person_roles_map") or {}
        # Heuristic: when extractor gives generic type but text/prep clearly describes waking up,
        # prefer a specific activity label.
        try:
            generic_types = {"activity", "reflection", "academic", "event"}
            prep_blob = " ".join(
                [
                    " ".join(prep_v1.get("facts_today", []) if isinstance(prep_v1.get("facts_today"), list) else []),
                    " ".join(prep_v1.get("habits", []) if isinstance(prep_v1.get("habits"), list) else []),
                    str(prep_v1.get("normalized_text") or ""),
                    text,
                ]
            ).lower()
            wake_tokens = ["wake up", "woke up", "réveil", "reveil", "je me leve", "je me lève", "lever plus tot", "leve plus tot"]
            if event_type.strip().lower() in generic_types and any(tok in prep_blob for tok in wake_tokens):
                event_type = "wake up"
        except Exception:
            pass

        # Canonical day bucket: prefer resolved event_time_iso, else input date
        resolved_day = None
        if isinstance(event_time_iso, str) and len(event_time_iso) >= 10:
            resolved_day = event_time_iso[:10]
        day_bucket = resolved_day or ts.date().isoformat()
        key_people = sorted({e.text.strip().lower() for e in entities if e.label == "Person"})
        key_places = sorted({
            e.text.strip().lower()
            for e in entities
            if e.label == "Place" and not _is_placeholder_value(e.text)
        })
        key_concepts = sorted({e.text.strip().lower() for e in entities if e.label == "Concept"})[:5]
        event_key = "|".join([day_bucket, ",".join(key_people), ",".join(key_places), ",".join(key_concepts), str(event_type or "")])

        # Build sentiment map from relations: (subject, object) -> sentiment
        sentiment_map = {}
        for r in relations:
            sentiment_map[(r.subject.lower(), r.obj.lower())] = r.sentiment

        def _store(tx):
            short = (text or "")[:60].strip()
            if len(text or "") > 60:
                short += "..."
            tx.run("""
                MERGE (j:E73_Information_Object {id: $id})
                SET j.text = $text, j.input_time = datetime($input_ts), j.entry_kind = 'journal_entry',
                    j.name = $short_name
            """, id=entry_id, text=text[:5000], input_ts=input_ts_str, short_name=short)

            # Multi-event mode: create multiple Event occurrences and sequence/impact edges.
            # This is a best-effort implementation driven by LLM metadata.events / metadata.event_relations.
            if has_multi_events:
                # Clarification override (from onboarding clarifier agent):
                # force physical_place to current city and remote/context places accordingly.
                override_physical = (meta.get("clarified_physical_place") or "").strip().lower()
                override_remote_list = meta.get("clarified_remote_context_places") or []
                if isinstance(meta.get("clarified_remote_context_place"), str) and meta.get("clarified_remote_context_place").strip():
                    override_remote_list = [*override_remote_list, meta.get("clarified_remote_context_place")]
                override_context_places = {str(p).strip().lower() for p in override_remote_list if str(p).strip()}

                events_norm: List[dict] = []
                for i, ev in enumerate(events_meta or []):
                    if not isinstance(ev, dict):
                        continue
                    idx = int(ev.get("idx") or (i + 1))
                    ev_type = _clean_event_type(str(ev.get("event_type") or event_type or "activity"))
                    ev_time_iso_i = ev.get("event_time_iso") or event_time_iso
                    ev_time_text_i = ev.get("event_time_text") or ""
                    ev_time_conf_i = ev.get("event_time_confidence") or event_time_conf or 0.6

                    # v2 micro-event ontology
                    physical_places_raw = (
                        ev.get("physical_places")
                        or ev.get("physical_place")
                        or ev.get("place")
                        or ev.get("places")
                        or []
                    )
                    if isinstance(physical_places_raw, str):
                        physical_places_raw = [physical_places_raw]
                    context_places_raw = ev.get("context_places") or ev.get("remote_places") or []
                    if isinstance(context_places_raw, str):
                        context_places_raw = [context_places_raw]

                    people = ev.get("people") or ev.get("actors") or []
                    topics = ev.get("topics") or ev.get("concepts") or ev.get("topic_concepts") or []
                    context_concepts = ev.get("context_concepts") or ev.get("reflections") or []
                    context_text = str(ev.get("context_text") or "").strip()

                    def _norm_list(v):
                        if isinstance(v, list):
                            out = []
                            for x in v:
                                s = str(x).strip().lower()
                                if not s or _is_placeholder_value(s):
                                    continue
                                # Drop very short noise tokens that often come from bad extraction.
                                if len(s) <= 2 and s not in {"ai", "ia"}:
                                    continue
                                out.append(s)
                            return out
                        return []

                    # Preserve display spelling for each micro-event people[] (not only lowercase).
                    people_pairs: List[Tuple[str, str]] = []
                    seen_pl = set()
                    if isinstance(people, list):
                        for x in people:
                            disp = str(x).strip()
                            if not disp or _is_placeholder_value(disp):
                                continue
                            low = disp.lower()
                            if len(low) <= 2 and low not in {"ai", "ia"}:
                                continue
                            if low in seen_pl:
                                continue
                            seen_pl.add(low)
                            people_pairs.append((low, disp))
                    people_lower = [p[0] for p in people_pairs]
                    physical_places_lower = set(_norm_list(physical_places_raw))
                    context_places_lower = set(_norm_list(context_places_raw))
                    topics_lower = set(_norm_list(topics)[:8])
                    context_concepts_lower = set(_norm_list(context_concepts)[:10])

                    if override_physical:
                        physical_places_lower = {override_physical}
                    if override_context_places:
                        context_places_lower = override_context_places

                    day_bucket_ev = None
                    if isinstance(ev_time_iso_i, str) and len(ev_time_iso_i) >= 10:
                        day_bucket_ev = ev_time_iso_i[:10]
                    day_bucket_ev = day_bucket_ev or day_bucket

                    key_people_ev = sorted(people_lower) or key_people
                    # IMPORTANT: never fallback to global extracted places for event core.
                    # If physical place is unknown, keep it empty in event key.
                    key_places_ev = sorted(physical_places_lower)
                    # Event core key should not include context topics; context becomes a separate node.
                    key_concepts_ev: List[str] = []

                    event_key_ev = "|".join(
                        [
                            day_bucket_ev,
                            str(idx),
                            ",".join(key_people_ev),
                            ",".join(key_places_ev),
                            ",".join(key_concepts_ev),
                            ev_type,
                        ]
                    )

                    events_norm.append(
                        {
                            "idx": idx,
                            "event_type": ev_type,
                            "event_time_iso": ev_time_iso_i,
                            "event_time_text": ev_time_text_i,
                            "event_time_confidence": float(ev_time_conf_i),
                            "day_bucket": day_bucket_ev,
                            "event_key": event_key_ev,
                            "people_lower": people_lower,
                            "people_pairs": people_pairs,
                            "physical_places_lower": physical_places_lower,
                            "context_places_lower": context_places_lower,
                            "topics_lower": topics_lower,
                            "context_concepts_lower": context_concepts_lower,
                            "context_text": context_text,
                            "has_context_text": bool(context_text.strip()),
                            "ctx_key": f"{entry_id}|{idx}",
                        }
                    )

                if not events_norm:
                    return

                idx_to_event_key = {e["idx"]: e["event_key"] for e in events_norm}
                emotions = meta.get("emotions", [])
                if not isinstance(emotions, list):
                    emotions = []

                # Create CIDOC activity occurrences + connect Entry -> each occurrence.
                for evn in events_norm:
                    tx.run("""
                        MERGE (ev:E7_Activity:E5_Event {key: $event_key})
                        ON CREATE SET ev.first_seen = datetime($input_ts)
                        SET ev.last_seen = datetime($input_ts),
                            ev.event_time_iso = $event_time_iso,
                            ev.event_time_confidence = $event_time_conf,
                            ev.event_type = $event_type,
                            ev.event_time_text = $event_time_text,
                            ev.occurrence_index = $idx
                        WITH ev
                        MATCH (j:E73_Information_Object {id: $entry_id})
                        MERGE (j)-[:P67_refers_to {ref_type: 'about_activity'}]->(ev)
                    """,
                           event_key=evn["event_key"],
                           input_ts=input_ts_str,
                           event_time_iso=evn["event_time_iso"],
                           event_time_conf=evn["event_time_confidence"],
                           event_type=evn["event_type"],
                           event_time_text=evn["event_time_text"],
                           idx=evn["idx"],
                           entry_id=entry_id)

                    raw_type = str(evn.get("event_type") or "activity")
                    tx.run("""
                        MATCH (cev:E7_Activity {key: $event_key})
                        SET cev.name = coalesce($event_type, cev.name, 'activity')
                    """, event_key=evn["event_key"], event_type=raw_type)

                    # Time is stored on the activity node properties + day bucket only.
                    # No separate E52 for the event-specific time (avoids noise nodes).

                    # Context text is stored as metadata on the entry node, not as
                    # a separate E73 node (avoids noise nodes in the graph).

                    tx.run("""
                        MERGE (d:E52_Time_Span {key: $day})
                        SET d.date = $day,
                            d.name = $day
                        WITH d
                        MATCH (ev:E7_Activity {key: $event_key})
                        MERGE (ev)-[:P4_has_time_span]->(d)
                    """, day=evn["day_bucket"], event_key=evn["event_key"])

                    if evn.get("event_type") and not _is_placeholder_value(str(evn.get("event_type"))):
                        tx.run("""
                            MERGE (t:E55_Type {name: $name})
                            WITH t
                            MATCH (ev:E7_Activity {key: $event_key})
                            MERGE (ev)-[:P2_has_type]->(t)
                        """, name=str(evn["event_type"]).strip(), event_key=evn["event_key"])

                    if user_name:
                        tx.run("""
                            MERGE (u:E21_Person:E39_Actor {name: $name})
                            ON CREATE SET u.first_seen = datetime($ts)
                            ON MATCH SET u.last_seen = datetime($ts)
                            WITH u
                            MERGE (ut:E55_Type {name: 'User'})
                            MERGE (u)-[:P2_has_type]->(ut)
                            WITH u
                            MATCH (ev:E7_Activity {key: $event_key})
                            MERGE (ev)-[:P14_carried_out_by]->(u)
                            MERGE (u)-[:P14i_performed]->(ev)
                        """, name=user_name, ts=input_ts_str, event_key=evn["event_key"])

                    # Emotions: skip writing as separate nodes to avoid noise.
                    # They are preserved in extraction metadata for later use.

                # Pre-resolve persons to stable IDs (Consolidator).
                places_ctx = [e.text for e in entities if e.label == "Place"]
                topics_ctx = [e.text for e in entities if e.label == "Concept"]
                person_map = {}
                uname = (user_name or "").strip().lower()
                unique_person_mentions = {
                    e.text
                    for e in entities
                    if e.label == "Person"
                    and (e.text or "").strip()
                    and e.text.strip().lower() != uname
                }
                for mention in unique_person_mentions:
                    mkey = mention.strip().lower()
                    meta_role = ""
                    if isinstance(person_roles_map, dict):
                        meta_role = (person_roles_map.get(mkey) or "").strip()
                    person_map[mention] = self.resolve_person(
                        mention=mention,
                        entry_text=text,
                        places=places_ctx,
                        topics=topics_ctx,
                        role=meta_role or self._infer_role(mention, text),
                        entry_id=entry_id,
                        interactive=False,
                    )

                # metadata.events[].people often lists actors not repeated as top-level Person entities.
                for evn in events_norm:
                    for plow, pdis in evn.get("people_pairs") or []:
                        if plow == uname:
                            continue
                        if any(k.strip().lower() == plow for k in person_map.keys()):
                            continue
                        meta_pe = ""
                        if isinstance(person_roles_map, dict):
                            meta_pe = (person_roles_map.get(plow) or "").strip()
                        person_map[pdis] = self.resolve_person(
                            mention=pdis,
                            entry_text=text,
                            places=places_ctx,
                            topics=topics_ctx,
                            role=meta_pe or self._infer_role(pdis, text),
                            entry_id=entry_id,
                            interactive=False,
                        )

                # Link all extracted entities to each micro-event (best-effort).
                for ent in entities:
                    if user_name and ent.text.strip().lower() == user_name.lower():
                        node_type = "E39_Actor"
                    else:
                        node_type = self._get_node_type(ent)
                    ent_text = (ent.text or "").strip()
                    if not ent_text:
                        continue

                    if node_type == "E52_Time_Span":
                        continue
                    if node_type == "E21_Person":
                        resolved = person_map.get(ent_text) or {}
                        pid = resolved.get("id")
                        if pid:
                            tx.run("""
                                MATCH (p:E21_Person {id: $id})
                                SET p.last_seen = datetime($ts),
                                    p.mention_count = coalesce(p.mention_count, 0) + 1
                            """, id=pid, ts=input_ts_str)
                    elif node_type not in ("E39_Actor",):
                        tx.run(f"""
                            MERGE (e:{node_type} {{name: $name}})
                            ON CREATE SET e.first_seen = datetime($ts), e.mention_count = 1
                            ON MATCH SET e.last_seen = datetime($ts), e.mention_count = e.mention_count + 1
                        """, name=ent_text, ts=input_ts_str)

                    for evn in events_norm:
                        ev_key = evn["event_key"]
                        if node_type == "E39_Actor":
                            tx.run("""
                                MATCH (u:E21_Person {name: $name})
                                MATCH (ev:E7_Activity {key: $event_key})
                                MERGE (ev)-[:P14_carried_out_by]->(u)
                                MERGE (u)-[:P14i_performed]->(ev)
                            """, name=ent_text, event_key=ev_key)
                            tx.run("""
                                MERGE (ca:E39_Actor {name: $name})
                                WITH ca
                                MATCH (cev:E7_Activity {key: $event_key})
                                MERGE (cev)-[:P14_carried_out_by]->(ca)
                                MERGE (ca)-[:P14i_performed]->(cev)
                            """, name=ent_text, event_key=ev_key)
                        elif node_type == "E21_Person":
                            resolved = person_map.get(ent_text) or {}
                            pid = resolved.get("id")
                            if pid:
                                tx.run("""
                                    MATCH (p:E21_Person {id: $id})
                                    MATCH (ev:E7_Activity {key: $event_key})
                                    MERGE (ev)-[:P14_carried_out_by]->(p)
                                    MERGE (p)-[:P14i_performed]->(ev)
                                """, id=pid, event_key=ev_key)
                                tx.run("""
                                    MATCH (p:E21_Person {id: $id})
                                    MERGE (ca:E39_Actor {id: $id})
                                    SET ca.name = coalesce(p.name, ca.name, $fallback_name)
                                    WITH ca
                                    MATCH (cev:E7_Activity {key: $event_key})
                                    MERGE (cev)-[:P14_carried_out_by]->(ca)
                                    MERGE (ca)-[:P14i_performed]->(cev)
                                """, id=pid, fallback_name=ent_text, event_key=ev_key)
                        elif node_type == "E53_Place":
                            # All places are contextual mentions on the journal entry.
                            # Physical place assignment requires explicit clarification.
                            continue
                        elif node_type == "E52_Time_Span":
                            # ignore Date mentions in multi-event visualization; we use Day buckets
                            continue
                        else:
                            # All other entity types: skip per-event linking.
                            # They are linked to the journal entry in the entry-level references block.
                            continue

                # P14 + journal P67 for each micro-event's people[] (and mirror E39 on cast).
                for evn in events_norm:
                    ev_key = evn["event_key"]
                    for plow, pdis in evn.get("people_pairs") or []:
                        if plow == uname:
                            continue
                        resolved = next(
                            (person_map[k] for k in person_map if k.strip().lower() == plow),
                            None,
                        )
                        if not resolved:
                            continue
                        pid = resolved.get("id")
                        if not pid:
                            continue
                        tx.run(
                            """
                            MATCH (p:E21_Person {id: $id})
                            SET p.last_seen = datetime($ts), p.mention_count = coalesce(p.mention_count, 0) + 1
                            """,
                            id=pid,
                            ts=input_ts_str,
                        )
                        tx.run(
                            """
                            MATCH (p:E21_Person {id: $id})
                            MATCH (ev:E7_Activity {key: $event_key})
                            MERGE (ev)-[:P14_carried_out_by]->(p)
                            MERGE (p)-[:P14i_performed]->(ev)
                            """,
                            id=pid,
                            event_key=ev_key,
                        )
                        tx.run(
                            """
                            MATCH (p:E21_Person {id: $id})
                            MERGE (ca:E39_Actor {id: $id})
                            SET ca.name = coalesce(p.name, ca.name, $fallback_name)
                            WITH ca
                            MATCH (cev:E7_Activity {key: $event_key})
                            MERGE (cev)-[:P14_carried_out_by]->(ca)
                            MERGE (ca)-[:P14i_performed]->(cev)
                            """,
                            id=pid,
                            fallback_name=pdis,
                            event_key=ev_key,
                        )
                for _, resolved in person_map.items():
                    pid = resolved.get("id")
                    if not pid:
                        continue
                    tx.run(
                        """
                        MATCH (j:E73_Information_Object {id: $entry_id})
                        MATCH (p:E21_Person {id: $id})
                        MERGE (j)-[:P67_refers_to {ref_type: 'mention'}]->(p)
                        """,
                        entry_id=entry_id,
                        id=pid,
                    )

                # All places, topics, concepts from extraction are linked as P67_refers_to
                # on the main journal entry (E73_Information_Object), not on separate context nodes.
                try:
                    extracted_place_lowers = {
                        str(e.text or "").strip().lower()
                        for e in entities
                        if e.label == "Place" and (e.text or "").strip()
                    }
                    extracted_org_lowers = {
                        str(e.text or "").strip().lower()
                        for e in entities
                        if e.label == "Organization" and (e.text or "").strip()
                    }

                    def _infer_ctx_node_label(item_lower: str) -> str:
                        if item_lower in extracted_place_lowers:
                            return "E53_Place"
                        if item_lower in extracted_org_lowers:
                            return "E74_Group"
                        return "E28_Conceptual_Object"

                    all_context_items: set = set()
                    for evn in events_norm:
                        for p_l in evn.get("context_places_lower", set()):
                            all_context_items.add(("mention", p_l))
                        for topic_l in evn.get("topics_lower", set()):
                            all_context_items.add(("topic", topic_l))
                        for ctxc_l in evn.get("context_concepts_lower", set()):
                            all_context_items.add(("context", ctxc_l))

                    for ref_type, item_lower in all_context_items:
                        label = _infer_ctx_node_label(item_lower)
                        display = entity_name_map.get(item_lower, item_lower)
                        tx.run(
                            f"""
                            MATCH (j:E73_Information_Object {{id: $entry_id}})
                            MERGE (t:{label} {{name: $name}})
                            MERGE (j)-[:P67_refers_to {{ref_type: $rt}}]->(t)
                            """,
                            entry_id=entry_id,
                            name=display,
                            rt=ref_type,
                        )
                except Exception:
                    pass

                # Create micro-event relations using CIDOC-compatible properties.
                # PRECEDES -> P120_occurs_before
                # CAUSES/ENABLES/IMPACTS/INFLUENCES -> target event P15_was_influenced_by source event
                allowed_rel_types = {"PRECEDES", "CAUSES", "ENABLES", "IMPACTS", "INFLUENCES"}
                if isinstance(event_relations_meta, list):
                    for rel in event_relations_meta:
                        if not isinstance(rel, dict):
                            continue
                        pred = str(rel.get("predicate") or rel.get("type") or "").strip().upper()
                        if pred not in allowed_rel_types:
                            continue
                        from_idx = rel.get("from_idx") or rel.get("from")
                        to_idx = rel.get("to_idx") or rel.get("to")
                        try:
                            from_i = int(from_idx)
                            to_i = int(to_idx)
                        except Exception:
                            continue
                        if from_i not in idx_to_event_key or to_i not in idx_to_event_key:
                            continue
                        conf = rel.get("confidence", 0.5)
                        try:
                            conf_f = float(conf)
                        except Exception:
                            conf_f = 0.5
                        evidence = str(rel.get("evidence") or "").strip()

                        if pred == "PRECEDES":
                            tx.run(
                                """
                                MATCH (a:E7_Activity {key: $ka})
                                MATCH (b:E7_Activity {key: $kb})
                                MERGE (a)-[r:P120_occurs_before]->(b)
                                SET r.confidence = $conf, r.evidence = $evidence, r.inference_type = $pred
                                """,
                                ka=idx_to_event_key[from_i],
                                kb=idx_to_event_key[to_i],
                                conf=conf_f,
                                evidence=evidence,
                                pred=pred,
                            )
                        else:
                            tx.run(
                                """
                                MATCH (a:E7_Activity {key: $ka})
                                MATCH (b:E7_Activity {key: $kb})
                                MERGE (b)-[r:P15_was_influenced_by]->(a)
                                SET r.confidence = $conf, r.evidence = $evidence, r.inference_type = $pred
                                """,
                                ka=idx_to_event_key[from_i],
                                kb=idx_to_event_key[to_i],
                                conf=conf_f,
                                evidence=evidence,
                                pred=pred,
                            )

                # Causal factor policy (habit vs today-specific vs propositional).
                # Priority rule:
                # - if a today_specific factor exists for a target event, it is the causal driver.
                # - habit/propositional stay as context references (no direct P15 edge) for that target.
                def _looks_today_specific(s: str) -> bool:
                    x = (s or "").strip().lower()
                    return any(k in x for k in [
                        "no teaching today", "no lecture today", "no lectures today",
                        "pas de cours aujourd", "pas de cours aujourd'hui", "aucun cours aujourd",
                        "pas de conférence aujourd", "no class today"
                    ])

                cf_list = causal_factors_meta if isinstance(causal_factors_meta, list) else []
                # If extractor didn't provide explicit causal_factors, derive from Prep Agent hints.
                if not cf_list and isinstance(prep_v1, dict):
                    facts_today = prep_v1.get("facts_today") if isinstance(prep_v1.get("facts_today"), list) else []
                    habits = prep_v1.get("habits") if isinstance(prep_v1.get("habits"), list) else []
                    causal_rules = prep_v1.get("causal_rules") if isinstance(prep_v1.get("causal_rules"), list) else []
                    target_idx_default = 1
                    for f in facts_today:
                        txt = str(f or "").strip()
                        if not txt:
                            continue
                        if _looks_today_specific(txt):
                            cf_list.append(
                                {
                                    "target_idx": target_idx_default,
                                    "factor_kind": "today_specific",
                                    "text": txt,
                                    "relation": "INFLUENCES",
                                    "confidence": 0.7,
                                    "evidence": txt[:160],
                                }
                            )
                    for h in habits:
                        txt = str(h or "").strip()
                        if not txt:
                            continue
                        cf_list.append(
                            {
                                "target_idx": target_idx_default,
                                "factor_kind": "habit",
                                "text": txt,
                                "relation": "INFLUENCES",
                                "confidence": 0.6,
                                "evidence": txt[:160],
                            }
                        )
                    for c in causal_rules:
                        txt = str(c or "").strip()
                        if not txt:
                            continue
                        kind = "propositional"
                        if _looks_today_specific(txt):
                            kind = "today_specific"
                        cf_list.append(
                            {
                                "target_idx": target_idx_default,
                                "factor_kind": kind,
                                "text": txt,
                                "relation": "INFLUENCES",
                                "confidence": 0.6,
                                "evidence": txt[:160],
                            }
                        )
                if not cf_list:
                    # Fallback extraction from context_text when LLM omits causal_factors.
                    for evn in events_norm:
                        ctext = str(evn.get("context_text") or "").strip()
                        if not ctext:
                            continue
                        if _looks_today_specific(ctext):
                            cf_list.append(
                                {
                                    "target_idx": int(evn.get("idx", 0)),
                                    "factor_kind": "today_specific",
                                    "text": "no teaching today",
                                    "relation": "INFLUENCES",
                                    "confidence": 0.7,
                                    "evidence": ctext[:160],
                                }
                            )

                if isinstance(cf_list, list):
                    by_target = {}
                    for cf in cf_list:
                        if not isinstance(cf, dict):
                            continue
                        try:
                            t_idx = int(cf.get("target_idx"))
                        except Exception:
                            continue
                        t_key = idx_to_event_key.get(t_idx)
                        if not t_key:
                            continue
                        txt = str(cf.get("text") or "").strip()
                        if not txt:
                            continue
                        kind = str(cf.get("factor_kind") or "").strip().lower()
                        if kind not in {"habit", "today_specific", "propositional"}:
                            continue
                        evd = str(cf.get("evidence") or "").strip()
                        try:
                            cconf = float(cf.get("confidence", 0.6))
                        except Exception:
                            cconf = 0.6
                        by_target.setdefault(t_idx, {"event_key": t_key, "habit": [], "today_specific": [], "propositional": []})
                        by_target[t_idx][kind].append({"text": txt, "evidence": evd, "conf": cconf})

                    for t_idx, payload in by_target.items():
                        t_key = payload["event_key"]
                        has_today_specific = bool(payload["today_specific"])

                        # 1) today-specific conditions as propositional objects influencing event
                        for f in payload["today_specific"]:
                            tx.run(
                                """
                                MERGE (c:E89_Propositional_Object {name: $name})
                                MERGE (ct:E55_Type {name: 'NoTeachingTodayCondition'})
                                MERGE (c)-[:P2_has_type]->(ct)
                                WITH c
                                MATCH (ev:E7_Activity {key: $event_key})
                                MERGE (ev)-[r:P15_was_influenced_by]->(c)
                                SET r.confidence = $conf, r.evidence = $evidence, r.inference_type = 'TODAY_SPECIFIC'
                                """,
                                name=f["text"],
                                event_key=t_key,
                                conf=f["conf"],
                                evidence=f["evidence"],
                            )

                        # 2) habits as conceptual objects.
                        # The event does not directly influence/reference the habit.
                        # Instead a Reflection activity bridges them:
                        #   Event ←[P17_was_motivated_by]— Reflection —[P67_refers_to]→ Habit
                        for h in payload["habit"]:
                            refl_key = f"{t_key}|reflection|{h['text'][:40]}"
                            tx.run(
                                """
                                MERGE (hb:E28_Conceptual_Object {name: $hname})
                                MERGE (ht:E55_Type {name: 'Habit'})
                                MERGE (hb)-[:P2_has_type]->(ht)
                                WITH hb
                                MERGE (ref:E7_Activity {key: $rkey})
                                SET ref.name = 'reflection'
                                WITH ref, hb
                                MERGE (rt:E55_Type {name: 'Reflection'})
                                MERGE (ref)-[:P2_has_type]->(rt)
                                WITH ref, hb
                                MATCH (ev:E7_Activity {key: $event_key})
                                MERGE (ref)-[:P17_was_motivated_by]->(ev)
                                WITH ref, hb
                                MERGE (ref)-[:P67_refers_to {ref_type: 'about_habit'}]->(hb)
                                """,
                                hname=h["text"],
                                rkey=refl_key,
                                event_key=t_key,
                            )
                            if user_name:
                                tx.run(
                                    """
                                    MATCH (ref:E7_Activity {key: $rkey})
                                    MERGE (u:E21_Person:E39_Actor {name: $uname})
                                    MERGE (ref)-[:P14_carried_out_by]->(u)
                                    MERGE (u)-[:P14i_performed]->(ref)
                                    """,
                                    rkey=refl_key,
                                    uname=user_name,
                                )

                        # 3) propositions influence habit when habit exists; otherwise influence event directly.
                        # If a today-specific condition exists, do not create direct proposition->event links.
                        if payload["habit"] and payload["propositional"]:
                            for p in payload["propositional"]:
                                for h in payload["habit"]:
                                    tx.run(
                                        """
                                        MERGE (pr:E89_Propositional_Object {name: $pname})
                                        MERGE (hb:E28_Conceptual_Object {name: $hname})
                                        MERGE (hb)-[r:P15_was_influenced_by]->(pr)
                                        SET r.confidence = $conf, r.evidence = $evidence, r.inference_type = 'PROPOSITION_TO_HABIT'
                                        """,
                                        pname=p["text"],
                                        hname=h["text"],
                                        conf=min(p["conf"], h["conf"]),
                                        evidence=(p["evidence"] or h["evidence"]),
                                    )
                        else:
                            for p in payload["propositional"]:
                                if has_today_specific:
                                    tx.run(
                                        """
                                        MERGE (pr:E89_Propositional_Object {name: $name})
                                        """,
                                        name=p["text"],
                                    )
                                else:
                                    tx.run(
                                        """
                                        MERGE (pr:E89_Propositional_Object {name: $name})
                                        WITH pr
                                        MATCH (ev:E7_Activity {key: $event_key})
                                        MERGE (ev)-[r:P15_was_influenced_by]->(pr)
                                        SET r.confidence = $conf, r.evidence = $evidence, r.inference_type = 'PROPOSITION'
                                        """,
                                        name=p["text"],
                                        event_key=t_key,
                                        conf=p["conf"],
                                        evidence=p["evidence"],
                                    )

                    # Ensure journal entry context explicitly references causal nodes (target RDF style).
                    for cf in cf_list:
                        if not isinstance(cf, dict):
                            continue
                        kind = str(cf.get("factor_kind") or "").strip().lower()
                        nm = str(cf.get("text") or "").strip()
                        if not nm:
                            continue
                        if kind == "today_specific" or kind == "propositional":
                            tx.run(
                                """
                                MATCH (j:E73_Information_Object {id: $entry_id})
                                MERGE (n:E89_Propositional_Object {name: $name})
                                MERGE (j)-[:P67_refers_to {ref_type:'context'}]->(n)
                                """,
                                entry_id=entry_id,
                                name=nm,
                            )
                        elif kind == "habit":
                            tx.run(
                                """
                                MATCH (j:E73_Information_Object {id: $entry_id})
                                MERGE (n:E28_Conceptual_Object {name: $name})
                                MERGE (j)-[:P67_refers_to {ref_type:'reflection_about'}]->(n)
                                """,
                                entry_id=entry_id,
                                name=nm,
                            )

                    # Also keep entry-level references for explicitly extracted entities.
                    for ent in entities:
                        ent_text = (ent.text or "").strip()
                        if not ent_text or _is_placeholder_value(ent_text):
                            continue
                        if ent.label == "Place":
                            tx.run(
                                """
                                MATCH (j:E73_Information_Object {id: $entry_id})
                                MERGE (p:E53_Place {name: $name})
                                MERGE (j)-[:P67_refers_to {ref_type:'mention'}]->(p)
                                """,
                                entry_id=entry_id,
                                name=ent_text,
                            )
                        elif ent.label == "Organization":
                            tx.run(
                                """
                                MATCH (j:E73_Information_Object {id: $entry_id})
                                MERGE (g:E74_Group {name: $name})
                                MERGE (j)-[:P67_refers_to {ref_type:'topic'}]->(g)
                                """,
                                entry_id=entry_id,
                                name=ent_text,
                            )
                        elif ent.label == "Concept":
                            tx.run(
                                """
                                MATCH (j:E73_Information_Object {id: $entry_id})
                                MERGE (c:E28_Conceptual_Object {name: $name})
                                MERGE (j)-[:P67_refers_to {ref_type:'context'}]->(c)
                                """,
                                entry_id=entry_id,
                                name=ent_text,
                            )

                # Heuristic fallback for causal links when LLM provides a "because" style relation
                # at the entity-level but didn't emit event_relations.
                # Example: "I should be faster because I have a lecture at 9H40"
                try:
                    events_sorted = sorted(events_norm, key=lambda x: int(x.get("idx", 0)))

                    def _concept_segment(ev_key: str) -> str:
                        # day|idx|people|places|concepts|event_type
                        parts = (ev_key or "").split("|")
                        return parts[4] if len(parts) >= 5 else ""

                    text_l = (text or "").lower()
                    faster_kw = any(k in text_l for k in ["faster", "plus vite", "accélér", "acceler", "oblig", "devrais être plus vite"])

                    # If the sentence explicitly says you need to be faster and later mentions a lecture,
                    # infer that the "faster" decision impacts the lecture timing.
                    if faster_kw and "lecture" in text_l:
                        lecture_by_type = [e for e in events_sorted if "lecture" in str(e.get("event_type") or "").lower()]
                        target_ev = max(lecture_by_type, key=lambda e: int(e.get("idx", 0))) if lecture_by_type else None
                        if not target_ev:
                            lecture_by_concept = [e for e in events_sorted if "lecture" in _concept_segment(e.get("event_key") or "")]
                            target_ev = max(lecture_by_concept, key=lambda e: int(e.get("idx", 0))) if lecture_by_concept else None
                        if target_ev:
                            target_idx = int(target_ev.get("idx"))
                            cause_idx = target_idx - 1
                            if cause_idx in idx_to_event_key:
                                tx.run(
                                    """
                                    MATCH (a:E7_Activity {key: $ka})
                                    MATCH (b:E7_Activity {key: $kb})
                                    MERGE (b)-[rel:P15_was_influenced_by]->(a)
                                    SET rel.confidence = coalesce(rel.confidence, 0.0) + $delta,
                                        rel.inference_type = 'IMPACTS',
                                        rel.evidence = CASE
                                            WHEN rel.evidence IS NULL OR rel.evidence = '' THEN $evidence
                                            ELSE rel.evidence + ' | ' + $evidence
                                        END
                                    """,
                                    ka=idx_to_event_key[cause_idx],
                                    kb=idx_to_event_key[target_idx],
                                    delta=0.35,
                                    evidence="heuristic: faster/acceleration decision impacts next lecture",
                                )

                    for r in relations:
                        pred_u = str(r.predicate or "").upper()
                        obj_l = str(r.obj or "").lower()
                        if "BECAUSE" not in pred_u and "BECAUSE" not in obj_l:
                            continue

                        # Detect the common scenario where a "faster" decision is driven by a later lecture.
                        if "lecture" not in obj_l:
                            continue

                        target_ev = None
                        for evn in events_sorted:
                            et_l = str(evn.get("event_type") or "").lower()
                            if "lecture" in et_l:
                                target_ev = evn
                                break
                            if "lecture" in _concept_segment(evn.get("event_key") or ""):
                                target_ev = evn
                                break

                        if not target_ev:
                            continue

                        target_idx = int(target_ev.get("idx"))
                        cause_idx = target_idx - 1
                        if cause_idx not in idx_to_event_key:
                            continue

                        evidence = f"{r.predicate}: {r.obj}"
                        tx.run(
                            """
                            MATCH (a:E7_Activity {key: $ka})
                            MATCH (b:E7_Activity {key: $kb})
                            MERGE (b)-[rel:P15_was_influenced_by]->(a)
                            SET rel.confidence = coalesce(rel.confidence, 0.0) + $delta,
                                rel.inference_type = 'IMPACTS',
                                rel.evidence = CASE
                                    WHEN rel.evidence IS NULL OR rel.evidence = '' THEN $evidence
                                    ELSE rel.evidence + ' | ' + $evidence
                                END
                            """,
                            ka=idx_to_event_key[cause_idx],
                            kb=idx_to_event_key[target_idx],
                            delta=0.35,
                            evidence=evidence,
                        )
                except Exception:
                    pass

                return

            tx.run("""
                MERGE (ev:E7_Activity:E5_Event {key: $event_key})
                ON CREATE SET ev.first_seen = datetime($input_ts)
                SET ev.last_seen = datetime($input_ts),
                    ev.event_time_iso = $event_time_iso,
                    ev.event_time_confidence = $event_time_conf,
                    ev.event_type = $event_type,
                    ev.name = coalesce($event_type, ev.name, 'activity')
                WITH ev
                MATCH (j:E73_Information_Object {id: $entry_id})
                MERGE (j)-[:P67_refers_to {ref_type: 'about_activity'}]->(ev)
            """, event_key=event_key, input_ts=input_ts_str, event_time_iso=event_time_iso, event_time_conf=event_time_conf, event_type=event_type, entry_id=entry_id)

            # Day node (absolute date)
            tx.run("""
                MERGE (d:E52_Time_Span {key: $day})
                SET d.date = $day,
                    d.name = $day
                WITH d
                MATCH (ev:E7_Activity {key: $event_key})
                MERGE (ev)-[:P4_has_time_span]->(d)
            """, day=day_bucket, event_key=event_key)

            # Event type as node + relation
            if event_type and isinstance(event_type, str):
                tx.run("""
                    MERGE (t:E55_Type {name: $name})
                    WITH t
                    MATCH (ev:E7_Activity {key: $event_key})
                    MERGE (ev)-[:P2_has_type]->(t)
                """, name=event_type.strip(), event_key=event_key)

            # User (journal owner) always participates
            if user_name:
                tx.run("""
                    MERGE (u:E21_Person:E39_Actor {name: $name})
                    ON CREATE SET u.first_seen = datetime($ts)
                    ON MATCH SET u.last_seen = datetime($ts)
                    WITH u
                    MERGE (ut:E55_Type {name: 'User'})
                    MERGE (u)-[:P2_has_type]->(ut)
                    WITH u
                    MATCH (ev:E7_Activity {key: $event_key})
                    MERGE (ev)-[:P14_carried_out_by]->(u)
                    MERGE (u)-[:P14i_performed]->(ev)
                """, name=user_name, ts=input_ts_str, event_key=event_key)

            # Pre-resolve persons to stable IDs (Consolidator)
            places_ctx = [e.text for e in entities if e.label == "Place"]
            topics_ctx = [e.text for e in entities if e.label == "Concept"]
            person_map = {}
            uname = (user_name or "").strip().lower()
            unique_person_mentions = {
                e.text
                for e in entities
                if e.label == "Person"
                and (e.text or "").strip()
                and e.text.strip().lower() != uname
            }
            for mention in unique_person_mentions:
                mkey = mention.strip().lower()
                meta_role = ""
                if isinstance(person_roles_map, dict):
                    meta_role = (person_roles_map.get(mkey) or "").strip()
                person_map[mention] = self.resolve_person(
                    mention=mention,
                    entry_text=text,
                    places=places_ctx,
                    topics=topics_ctx,
                    role=meta_role or self._infer_role(mention, text),
                    entry_id=entry_id,
                    interactive=False,
                )

            for ent in entities:
                if user_name and ent.text.strip().lower() == user_name.lower():
                    node_type = "E39_Actor"
                else:
                    node_type = self._get_node_type(ent)
                if _is_placeholder_value(ent.text):
                    continue
                if node_type == "E52_Time_Span":
                    continue
                if node_type == "E21_Person":
                    resolved = person_map.get(ent.text) or {}
                    pid = resolved.get("id")
                    if not pid:
                        continue
                    tx.run(
                        """
                        MATCH (p:E21_Person {id: $id})
                        SET p.last_seen = datetime($ts), p.mention_count = coalesce(p.mention_count, 0) + 1
                        """,
                        id=pid,
                        ts=input_ts_str,
                    )
                elif node_type not in ("E39_Actor",):
                    tx.run(f"""
                        MERGE (e:{node_type} {{name: $name}})
                        ON CREATE SET e.first_seen = datetime($ts), e.mention_count = 1
                        ON MATCH SET e.last_seen = datetime($ts), e.mention_count = e.mention_count + 1
                    """, name=ent.text, ts=input_ts_str)

                if node_type in ("E21_Person", "E39_Actor"):
                    if node_type == "E39_Actor":
                        tx.run("""
                            MATCH (u:E21_Person {name: $name})
                            MATCH (ev:E7_Activity {key: $event_key})
                            MERGE (ev)-[:P14_carried_out_by]->(u)
                            MERGE (u)-[:P14i_performed]->(ev)
                        """, name=ent.text, event_key=event_key)
                    else:
                        resolved = person_map.get(ent.text) or {}
                        pid = resolved.get("id")
                        if pid:
                            tx.run("""
                                MATCH (p:E21_Person {id: $id})
                                MATCH (ev:E7_Activity {key: $event_key})
                                MERGE (ev)-[:P14_carried_out_by]->(p)
                                MERGE (p)-[:P14i_performed]->(ev)
                            """, id=pid, event_key=event_key)
                elif node_type == "E53_Place":
                    tx.run("""
                        MATCH (j:E73_Information_Object {id: $entry_id})
                        MERGE (pl:E53_Place {name: $name})
                        MERGE (j)-[:P67_refers_to {ref_type:'mention'}]->(pl)
                    """, name=ent.text, entry_id=entry_id)
                elif node_type == "E52_Time_Span":
                    continue
                else:
                    tx.run(f"""
                        MATCH (j:E73_Information_Object {{id: $entry_id}})
                        MERGE (t:{node_type} {{name: $name}})
                        MERGE (j)-[:P67_refers_to {{ref_type: 'mention'}}]->(t)
                    """, entry_id=entry_id, name=ent.text)

            for _, resolved in person_map.items():
                pid = resolved.get("id")
                if not pid:
                    continue
                tx.run(
                    """
                    MATCH (j:E73_Information_Object {id: $entry_id})
                    MATCH (p:E21_Person {id: $id})
                    MERGE (j)-[:P67_refers_to {ref_type: 'mention'}]->(p)
                    """,
                    entry_id=entry_id,
                    id=pid,
                )

            # Single-event causal factors: preserve CIDOC causal modeling even without metadata.events.
            cf_list = causal_factors_meta if isinstance(causal_factors_meta, list) else []
            if not cf_list and isinstance(prep_v1, dict):
                def _looks_today_specific_local(s: str) -> bool:
                    x = (s or "").strip().lower()
                    return any(k in x for k in [
                        "no teaching today", "no lecture today", "no lectures today",
                        "pas de cours aujourd", "pas de cours aujourd'hui", "aucun cours aujourd",
                        "pas de conférence aujourd", "no class today"
                    ])
                facts_today = prep_v1.get("facts_today") if isinstance(prep_v1.get("facts_today"), list) else []
                habits = prep_v1.get("habits") if isinstance(prep_v1.get("habits"), list) else []
                causal_rules = prep_v1.get("causal_rules") if isinstance(prep_v1.get("causal_rules"), list) else []
                for f in facts_today:
                    if isinstance(f, str) and f.strip():
                        kind = "today_specific" if _looks_today_specific_local(f) else "propositional"
                        cf_list.append({"factor_kind": kind, "text": f.strip(), "confidence": 0.7, "evidence": f.strip()[:160]})
                for h in habits:
                    if isinstance(h, str) and h.strip():
                        kind = "today_specific" if _looks_today_specific_local(h) else "habit"
                        cf_list.append({"factor_kind": kind, "text": h.strip(), "confidence": 0.6, "evidence": h.strip()[:160]})
                for c in causal_rules:
                    if isinstance(c, str) and c.strip():
                        kind = "today_specific" if _looks_today_specific_local(c) else "propositional"
                        cf_list.append({"factor_kind": kind, "text": c.strip(), "confidence": 0.6, "evidence": c.strip()[:160]})

            today = [c for c in cf_list if isinstance(c, dict) and str(c.get("factor_kind") or "").lower() == "today_specific"]
            habits = [c for c in cf_list if isinstance(c, dict) and str(c.get("factor_kind") or "").lower() == "habit"]
            props = [c for c in cf_list if isinstance(c, dict) and str(c.get("factor_kind") or "").lower() == "propositional"]

            for c in today:
                txt = str(c.get("text") or "").strip()
                if not txt:
                    continue
                tx.run("""
                    MERGE (cond:E89_Propositional_Object {name:$name})
                    MERGE (ct:E55_Type {name:'NoTeachingTodayCondition'})
                    MERGE (cond)-[:P2_has_type]->(ct)
                    WITH cond
                    MATCH (ev:E7_Activity {key:$event_key})
                    MERGE (ev)-[r:P15_was_influenced_by]->(cond)
                    SET r.inference_type='TODAY_SPECIFIC', r.confidence=$conf, r.evidence=$evidence
                """, name=txt, event_key=event_key, conf=float(c.get("confidence", 0.7) or 0.7), evidence=str(c.get("evidence") or "")[:220])

            has_today_specific = len(today) > 0
            for h in habits:
                txt = str(h.get("text") or "").strip()
                if not txt:
                    continue
                refl_key = f"{event_key}|reflection|{txt[:40]}"
                tx.run("""
                    MERGE (hb:E28_Conceptual_Object {name:$name})
                    MERGE (ht:E55_Type {name:'Habit'})
                    MERGE (hb)-[:P2_has_type]->(ht)
                    WITH hb
                    MERGE (ref:E7_Activity {key: $rkey})
                    SET ref.name = 'reflection'
                    WITH ref, hb
                    MERGE (rt:E55_Type {name: 'Reflection'})
                    MERGE (ref)-[:P2_has_type]->(rt)
                    WITH ref, hb
                    MATCH (ev:E7_Activity {key: $event_key})
                    MERGE (ref)-[:P17_was_motivated_by]->(ev)
                    WITH ref, hb
                    MERGE (ref)-[:P67_refers_to {ref_type: 'about_habit'}]->(hb)
                """, name=txt, rkey=refl_key, event_key=event_key)
                if user_name:
                    tx.run("""
                        MATCH (ref:E7_Activity {key: $rkey})
                        MERGE (u:E21_Person:E39_Actor {name: $uname})
                        MERGE (ref)-[:P14_carried_out_by]->(u)
                        MERGE (u)-[:P14i_performed]->(ref)
                    """, rkey=refl_key, uname=user_name)

            for p in props:
                ptxt = str(p.get("text") or "").strip()
                if not ptxt:
                    continue
                if habits:
                    for h in habits:
                        htxt = str(h.get("text") or "").strip()
                        if not htxt:
                            continue
                        tx.run("""
                            MERGE (pr:E89_Propositional_Object {name:$pname})
                            MERGE (hb:E28_Conceptual_Object {name:$hname})
                            MERGE (hb)-[r:P15_was_influenced_by]->(pr)
                            SET r.inference_type='PROPOSITION_TO_HABIT', r.confidence=$conf, r.evidence=$evidence
                        """, pname=ptxt, hname=htxt, conf=0.6, evidence=str(p.get("evidence") or "")[:220])
                elif not has_today_specific:
                    tx.run("""
                        MERGE (pr:E89_Propositional_Object {name:$name})
                        WITH pr
                        MATCH (ev:E7_Activity {key:$event_key})
                        MERGE (ev)-[r:P15_was_influenced_by]->(pr)
                        SET r.inference_type='PROPOSITION', r.confidence=$conf, r.evidence=$evidence
                    """, name=ptxt, event_key=event_key, conf=float(p.get("confidence", 0.6) or 0.6), evidence=str(p.get("evidence") or "")[:220])

        with self.driver.session() as session:
            session.execute_write(_store)

        return entry_id

    def _get_node_type(self, entity: ExtractedEntity) -> str:
        # Event entity text stored as Concept; Date stays Date
        if entity.label == "Event":
            return "E28_Conceptual_Object"
        mapping = {
            "Person": "E21_Person",
            "Place": "E53_Place",
            "Organization": "E74_Group",
            "Concept": "E28_Conceptual_Object",
            "Date": "E52_Time_Span",
        }
        return mapping.get(entity.label, "E28_Conceptual_Object")

    def query_entities(self, limit: int = 50) -> List[dict]:
        def _query(tx):
            result = tx.run("""
                MATCH (e) WHERE e:E21_Person OR e:E53_Place OR e:E74_Group OR e:E28_Conceptual_Object
                RETURN labels(e)[0] as type,
                       e.name as name,
                       e.mention_count as mentions,
                       e.last_seen as last_seen
                ORDER BY coalesce(e.mention_count, 0) DESC
                LIMIT $limit
            """, limit=limit)
            return [dict(record) for record in result]

        with self.driver.session() as session:
            return session.execute_read(_query)

    def search_by_entity(self, entity_name: str) -> List[dict]:
        def _query(tx):
            result = tx.run("""
                MATCH (j:E73_Information_Object)-[:P67_refers_to]->(ev:E7_Activity)
                WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(u:E21_Person {name: $name})
                OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
                OPTIONAL MATCH (a:Alias {text: $name})-[:P67_refers_to]->(p)
                OPTIONAL MATCH (pl:E53_Place {name: $name})<-[:P7_took_place_at]-(ev)
                OPTIONAL MATCH (c:E28_Conceptual_Object {name: $name})<-[:P67_refers_to]-(ev)
                WITH j, ev,
                     (u IS NOT NULL OR a IS NOT NULL) as personMatch,
                     (pl IS NOT NULL) as placeMatch,
                     (c IS NOT NULL) as conceptMatch
                WHERE personMatch OR placeMatch OR conceptMatch
                RETURN j.id as id, j.text as text, j.input_time as timestamp
                ORDER BY j.input_time DESC
                LIMIT 20
            """, name=entity_name)
            return [dict(record) for record in result]

        with self.driver.session() as session:
            return session.execute_read(_query)
