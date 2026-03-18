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
            FOR (p:Person) REQUIRE p.id IS UNIQUE
        """)
        tx.run("""
            CREATE INDEX person_name IF NOT EXISTS
            FOR (p:Person) ON (p.name)
        """)
        tx.run("""
            CREATE CONSTRAINT alias_id IF NOT EXISTS
            FOR (a:Alias) REQUIRE a.id IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT place_name IF NOT EXISTS
            FOR (p:Place) REQUIRE p.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT org_name IF NOT EXISTS
            FOR (o:Organization) REQUIRE o.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT concept_name IF NOT EXISTS
            FOR (c:Concept) REQUIRE c.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT date_name IF NOT EXISTS
            FOR (d:Date) REQUIRE d.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT emotion_name IF NOT EXISTS
            FOR (e:Emotion) REQUIRE e.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT event_type_name IF NOT EXISTS
            FOR (t:EventType) REQUIRE t.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT user_name IF NOT EXISTS
            FOR (u:User) REQUIRE u.name IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT event_key IF NOT EXISTS
            FOR (e:Event) REQUIRE e.key IS UNIQUE
        """)
        tx.run("""
            CREATE CONSTRAINT day_date IF NOT EXISTS
            FOR (d:Day) REQUIRE d.date IS UNIQUE
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
                FOR (p:Person) ON (p.embedding)
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
                    CREATE (p:Person {id: $id, name: $name, created_at: datetime(), first_seen: datetime(), last_seen: datetime(), mention_count: 0, embedding: $vec, role: $role})
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
                        MATCH (p:Person {id: $pid})
                        CREATE (a:Alias {id: $aid, text: $text, created_at: datetime()})
                        MERGE (a)-[:REFERS_TO]->(p)
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
                    MATCH (c:Person {id: $cid})
                    MATCH (p:Person {id: $pid})
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
                        MATCH (p:Person {id: $pid})<-[:REFERS_TO]-(a:Alias)
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
                                MATCH (p:Person {id: $pid})
                                CREATE (a:Alias {id: $aid, text: $text, created_at: datetime()})
                                MERGE (a)-[:REFERS_TO]->(p)
                                """,
                                pid=best["id"],
                                aid=aid,
                                text=mention,
                            )
                        # Update role if previously empty and we just inferred one
                        if new_role and not existing_role:
                            tx.run(
                                "MATCH (p:Person {id: $id}) SET p.role = $role",
                                id=best["id"],
                                role=new_role,
                            )
                        return {"id": best["id"], "name": best["name"], "score": score, "created": False}
                    # otherwise, fall through to create new / exact-name path

            # 2) Fallback: exact name match (if exists) to seed vector index
            res = tx.run(
                "MATCH (p:Person) WHERE toLower(p.name)=toLower($name) RETURN p.id as id, p.name as name, p.role as role LIMIT 1",
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
                    MATCH (p:Person {id: $id})
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
                        MATCH (p:Person {id: $pid})
                        CREATE (a:Alias {id: $aid, text: $text, created_at: datetime()})
                        MERGE (a)-[:REFERS_TO]->(p)
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

        entities = extraction.entities
        relations = extraction.relations
        meta = extraction.metadata or {}
        event_time_iso = meta.get("event_time_iso")
        event_time_conf = meta.get("event_time_confidence")
        event_type = meta.get("event_type")
        person_roles_map = meta.get("person_roles_map") or {}

        # Canonical day bucket: prefer resolved event_time_iso, else input date
        resolved_day = None
        if isinstance(event_time_iso, str) and len(event_time_iso) >= 10:
            resolved_day = event_time_iso[:10]
        day_bucket = resolved_day or ts.date().isoformat()
        key_people = sorted({e.text.strip().lower() for e in entities if e.label == "Person"})
        key_places = sorted({e.text.strip().lower() for e in entities if e.label == "Place"})
        key_concepts = sorted({e.text.strip().lower() for e in entities if e.label == "Concept"})[:5]
        event_key = "|".join([day_bucket, ",".join(key_people), ",".join(key_places), ",".join(key_concepts), str(event_type or "")])

        # Build sentiment map from relations: (subject, object) -> sentiment
        sentiment_map = {}
        for r in relations:
            sentiment_map[(r.subject.lower(), r.obj.lower())] = r.sentiment

        def _store(tx):
            tx.run("""
                MERGE (j:Entry {id: $id})
                SET j.text = $text, j.input_time = datetime($input_ts)
            """, id=entry_id, text=text[:5000], input_ts=input_ts_str)

            tx.run("""
                MERGE (ev:Event {key: $event_key})
                ON CREATE SET ev.first_seen = datetime($input_ts)
                SET ev.last_seen = datetime($input_ts),
                    ev.event_time_iso = $event_time_iso,
                    ev.event_time_confidence = $event_time_conf,
                    ev.event_type = $event_type
                WITH ev
                MATCH (j:Entry {id: $entry_id})
                MERGE (j)-[:REFERS_TO]->(ev)
            """, event_key=event_key, input_ts=input_ts_str, event_time_iso=event_time_iso, event_time_conf=event_time_conf, event_type=event_type, entry_id=entry_id)

            # Day node (absolute date)
            tx.run("""
                MERGE (d:Day {date: $day})
                WITH d
                MATCH (ev:Event {key: $event_key})
                MERGE (ev)-[:ON_DAY]->(d)
            """, day=day_bucket, event_key=event_key)

            # Event type as node + relation (instead of HAS_TOPIC)
            if event_type and isinstance(event_type, str):
                tx.run("""
                    MERGE (t:EventType {name: $name})
                    WITH t
                    MATCH (ev:Event {key: $event_key})
                    MERGE (ev)-[:HAS_TYPE]->(t)
                """, name=event_type.strip(), event_key=event_key)

            # Emotions as nodes + relations
            emotions = meta.get("emotions", [])
            if isinstance(emotions, list):
                for emo in emotions:
                    if isinstance(emo, str) and emo.strip():
                        tx.run("""
                            MERGE (e:Emotion {name: $name})
                            WITH e
                            MATCH (ev:Event {key: $event_key})
                            MERGE (ev)-[:HAS_EMOTION]->(e)
                        """, name=emo.strip().lower(), event_key=event_key)

            # User (journal owner) always participates
            if user_name:
                tx.run("""
                    MERGE (u:User {name: $name})
                    ON CREATE SET u.first_seen = datetime($ts)
                    ON MATCH SET u.last_seen = datetime($ts)
                    WITH u
                    MATCH (ev:Event {key: $event_key})
                    MERGE (u)-[:PARTICIPATED_IN]->(ev)
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
                    node_type = "User"
                else:
                    node_type = self._get_node_type(ent)
                if node_type == "Person":
                    resolved = person_map.get(ent.text) or {}
                    pid = resolved.get("id")
                    if not pid:
                        continue
                    tx.run(
                        """
                        MATCH (p:Person {id: $id})
                        SET p.last_seen = datetime($ts), p.mention_count = coalesce(p.mention_count, 0) + 1
                        """,
                        id=pid,
                        ts=input_ts_str,
                    )
                else:
                    tx.run(f"""
                        MERGE (e:{node_type} {{name: $name}})
                        ON CREATE SET e.first_seen = datetime($ts), e.mention_count = 1
                        ON MATCH SET e.last_seen = datetime($ts), e.mention_count = e.mention_count + 1
                    """, name=ent.text, ts=input_ts_str)

                if node_type in ("Person", "User"):
                    if node_type == "User":
                        tx.run("""
                            MATCH (u:User {name: $name})
                            MATCH (ev:Event {key: $event_key})
                            MERGE (u)-[:PARTICIPATED_IN]->(ev)
                        """, name=ent.text, event_key=event_key)
                    else:
                        resolved = person_map.get(ent.text) or {}
                        pid = resolved.get("id")
                        if pid:
                            tx.run("""
                                MATCH (p:Person {id: $id})
                                MATCH (ev:Event {key: $event_key})
                                MERGE (p)-[:PARTICIPATED_IN]->(ev)
                            """, id=pid, event_key=event_key)
                elif node_type == "Place":
                    tx.run("""
                        MATCH (pl:Place {name: $name})
                        MATCH (ev:Event {key: $event_key})
                        MERGE (ev)-[:OCCURRED_AT]->(pl)
                    """, name=ent.text, event_key=event_key)
                elif node_type == "Date":
                    tx.run("""
                        MATCH (d:Date {name: $name})
                        MATCH (ev:Event {key: $event_key})
                        MERGE (ev)-[:OCCURRED_ON]->(d)
                    """, name=ent.text, event_key=event_key)
                else:
                    score = 0.5
                    for r in relations:
                        if r.obj.lower() == ent.text.lower():
                            score = r.sentiment
                            break
                    # Concept / Organization -> HAS_TOPIC (topics only)
                    tx.run(f"""
                        MATCH (t:{node_type} {{name: $name}})
                        MATCH (ev:Event {{key: $event_key}})
                        MERGE (ev)-[r:HAS_TOPIC]->(t)
                        ON CREATE SET r.score = $score, r.last_updated = datetime($ts)
                        ON MATCH SET r.score = (r.score * 0.7) + ($score * 0.3), r.last_updated = datetime($ts)
                    """, name=ent.text, event_key=event_key, score=score, ts=input_ts_str)

        with self.driver.session() as session:
            session.execute_write(_store)

        return entry_id

    def _get_node_type(self, entity: ExtractedEntity) -> str:
        # Event entity text stored as Concept; Date stays Date
        if entity.label == "Event":
            return "Concept"
        valid = ("Person", "Place", "Organization", "Concept", "Date")
        return entity.label if entity.label in valid else "Concept"

    def query_entities(self, limit: int = 50) -> List[dict]:
        def _query(tx):
            result = tx.run("""
                MATCH (e) WHERE e:Person OR e:Place OR e:Organization OR e:Concept OR e:User
                RETURN labels(e)[0] as type,
                       CASE WHEN e:Person THEN e.name ELSE e.name END as name,
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
                MATCH (j:Entry)-[:REFERS_TO]->(ev:Event)
                OPTIONAL MATCH (u:User {name: $name})-[:PARTICIPATED_IN]->(ev)
                OPTIONAL MATCH (p:Person)-[:PARTICIPATED_IN]->(ev)
                OPTIONAL MATCH (a:Alias {text: $name})-[:REFERS_TO]->(p)
                OPTIONAL MATCH (pl:Place {name: $name})<-[:OCCURRED_AT]-(ev)
                OPTIONAL MATCH (c:Concept {name: $name})<-[:HAS_TOPIC]-(ev)
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
