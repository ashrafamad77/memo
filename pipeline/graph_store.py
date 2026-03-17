"""Neo4j graph storage — event-centric ontology."""
from datetime import datetime
from typing import List, Optional

from neo4j import GraphDatabase

from .extractor import ExtractionResult, ExtractedEntity
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
        def _reset(tx):
            tx.run("MATCH (n) DETACH DELETE n")

        with self.driver.session() as session:
            session.execute_write(_reset)

    def _init_schema(self, tx):
        tx.run("""
            CREATE CONSTRAINT person_name IF NOT EXISTS
            FOR (p:Person) REQUIRE p.name IS UNIQUE
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

            for ent in entities:
                if user_name and ent.text.strip().lower() == user_name.lower():
                    node_type = "User"
                else:
                    node_type = self._get_node_type(ent)
                tx.run(f"""
                    MERGE (e:{node_type} {{name: $name}})
                    ON CREATE SET e.first_seen = datetime($ts), e.mention_count = 1
                    ON MATCH SET e.last_seen = datetime($ts), e.mention_count = e.mention_count + 1
                """, name=ent.text, ts=input_ts_str)

                if node_type in ("Person", "User"):
                    tx.run(f"""
                        MATCH (n:{node_type} {{name: $name}})
                        MATCH (ev:Event {{key: $event_key}})
                        MERGE (n)-[:PARTICIPATED_IN]->(ev)
                    """, name=ent.text, event_key=event_key)
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
                RETURN labels(e)[0] as type, e.name as name,
                       e.mention_count as mentions, e.last_seen as last_seen
                ORDER BY e.mention_count DESC
                LIMIT $limit
            """, limit=limit)
            return [dict(record) for record in result]

        with self.driver.session() as session:
            return session.execute_read(_query)

    def search_by_entity(self, entity_name: str) -> List[dict]:
        def _query(tx):
            result = tx.run("""
                MATCH (e {name: $name})
                MATCH (j:Entry)-[:REFERS_TO]->(ev:Event)
                WHERE ((e:Person OR e:User) AND (e)-[:PARTICIPATED_IN]->(ev))
                   OR (e:Place AND (ev)-[:OCCURRED_AT]->(e))
                   OR (e:Concept AND (ev)-[:HAS_TOPIC]->(e))
                   OR (e:Organization AND (ev)-[:HAS_TOPIC]->(e))
                RETURN j.id as id, j.text as text, j.input_time as timestamp
                ORDER BY j.input_time DESC
                LIMIT 20
            """, name=entity_name)
            return [dict(record) for record in result]

        with self.driver.session() as session:
            return session.execute_read(_query)
