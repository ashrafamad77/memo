from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from neo4j import GraphDatabase

from config import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER


@dataclass
class Neo4jRepo:
    uri: str = NEO4J_URI
    user: str = NEO4J_USER
    password: str = NEO4J_PASSWORD

    def __post_init__(self) -> None:
        self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self) -> None:
        self._driver.close()

    def entry_count(self) -> int:
        q = "MATCH (e:Entry) RETURN count(e) as c"
        with self._driver.session() as s:
            row = s.run(q).single()
            return int(row["c"]) if row and row.get("c") is not None else 0

    def get_user_profile(self, user_name: str) -> Dict[str, Any]:
        q = """
        MATCH (u:User {name: $name})
        RETURN u.name as name,
               u.profile_current_city as current_city,
               u.profile_home_country as home_country,
               u.profile_nationality as nationality,
               u.profile_timezone as timezone,
               u.profile_work_context as work_context
        """
        with self._driver.session() as s:
            row = s.run(q, name=user_name).single()
            return dict(row) if row else {}

    def upsert_user_profile(self, user_name: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        q = """
        MERGE (u:User {name: $name})
        ON CREATE SET u.first_seen = datetime()
        SET u.last_seen = datetime(),
            u.profile_current_city = coalesce($current_city, u.profile_current_city),
            u.profile_home_country = coalesce($home_country, u.profile_home_country),
            u.profile_nationality = coalesce($nationality, u.profile_nationality),
            u.profile_timezone = coalesce($timezone, u.profile_timezone),
            u.profile_work_context = coalesce($work_context, u.profile_work_context)
        RETURN u.name as name,
               u.profile_current_city as current_city,
               u.profile_home_country as home_country,
               u.profile_nationality as nationality,
               u.profile_timezone as timezone,
               u.profile_work_context as work_context
        """
        with self._driver.session() as s:
            row = s.run(
                q,
                name=user_name,
                current_city=(fields.get("current_city") or None),
                home_country=(fields.get("home_country") or None),
                nationality=(fields.get("nationality") or None),
                timezone=(fields.get("timezone") or None),
                work_context=(fields.get("work_context") or None),
            ).single()
            return dict(row) if row else {}

    def timeline(self, limit: int = 50) -> List[Dict[str, Any]]:
        q = """
        MATCH (e:Entry)-[:REFERS_TO]->(ev:Event)
        OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
        WITH e,
             collect(DISTINCT ev.key) as event_keys,
             collect(DISTINCT d.date) as days
        RETURN e.id as id,
               e.text as text,
               toString(e.input_time) as input_time,
               event_keys[0] as event_key,
               days[0] as day
        ORDER BY e.input_time DESC
        LIMIT $limit
        """
        with self._driver.session() as s:
            return [dict(r) for r in s.run(q, limit=int(limit))]

    def entry_detail(self, entry_id: str) -> Optional[Dict[str, Any]]:
        q = """
        MATCH (e:Entry {id: $id})-[:REFERS_TO]->(ev:Event)
        OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
        OPTIONAL MATCH (u:User)-[:PARTICIPATED_IN]->(ev)
        OPTIONAL MATCH (p:Person)-[:PARTICIPATED_IN]->(ev)
        OPTIONAL MATCH (ev)-[:OCCURRED_AT]->(pl:Place)
        OPTIONAL MATCH (ev)-[:HAS_TOPIC]->(c:Concept)
        RETURN e.id as id,
               e.text as text,
               toString(e.input_time) as input_time,
               ev.key as event_key,
               ev.event_type as event_type,
               d.date as day,
               collect(DISTINCT u.name) as users,
               collect(DISTINCT {id: p.id, name: p.name, role: p.role}) as persons,
               collect(DISTINCT pl.name) as places,
               collect(DISTINCT c.name) as topics
        """
        with self._driver.session() as s:
            row = s.run(q, id=entry_id).single()
            return dict(row) if row else None

    def persons(self, query: str = "", limit: int = 50) -> List[Dict[str, Any]]:
        q = """
        MATCH (p:Person)
        WHERE $q = "" OR toLower(p.name) CONTAINS toLower($q)
        RETURN p.id as id,
               p.name as name,
               p.role as role,
               coalesce(p.mention_count, 0) as mentions,
               toString(p.last_seen) as last_seen
        ORDER BY mentions DESC, last_seen DESC
        LIMIT $limit
        """
        with self._driver.session() as s:
            return [dict(r) for r in s.run(q, q=query or "", limit=int(limit))]

    def person_detail(self, person_id: str, entry_limit: int = 30) -> Optional[Dict[str, Any]]:
        q = """
        MATCH (p:Person {id: $id})
        OPTIONAL MATCH (a:Alias)-[:REFERS_TO]->(p)
        WITH p, collect(DISTINCT a.text) as aliases
        OPTIONAL MATCH (p)-[:PARTICIPATED_IN]->(ev:Event)<-[:REFERS_TO]-(e:Entry)
        OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
        RETURN p.id as id,
               p.name as name,
               p.role as role,
               coalesce(p.mention_count, 0) as mentions,
               toString(p.first_seen) as first_seen,
               toString(p.last_seen) as last_seen,
               aliases as aliases,
               collect(DISTINCT {
                 id: e.id,
                 input_time: toString(e.input_time),
                 day: d.date,
                 text_preview: substring(e.text, 0, 220)
               })[0..$entry_limit] as entries
        """
        with self._driver.session() as s:
            row = s.run(q, id=person_id, entry_limit=int(entry_limit)).single()
            return dict(row) if row else None

    def person_timeline(self, person_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Interaction timeline for a person: (Person)-[:PARTICIPATED_IN]->(Event)<-[:REFERS_TO]-(Entry)
        Enriched with Day, Place, and EventType when available.
        """
        q = """
        MATCH (p:Person {id: $id})
        MATCH (p)-[:PARTICIPATED_IN]->(ev:Event)<-[:REFERS_TO]-(e:Entry)
        OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
        OPTIONAL MATCH (ev)-[:OCCURRED_AT]->(pl:Place)
        OPTIONAL MATCH (ev)-[:HAS_TYPE]->(t:EventType)
        WITH e, ev, d, collect(DISTINCT pl.name) as places, collect(DISTINCT t.name) as types
        RETURN e.id as entry_id,
               toString(e.input_time) as input_time,
               d.date as day,
               coalesce(types[0], ev.event_type, '') as event_type,
               places as places,
               substring(e.text, 0, 260) as text_preview
        ORDER BY e.input_time DESC
        LIMIT $limit
        """
        with self._driver.session() as s:
            return [dict(r) for r in s.run(q, id=person_id, limit=int(limit))]

    def entities(self, limit: int = 100, query: str = "") -> List[Dict[str, Any]]:
        """
        Mixed entity list for UI browsing.
        Returns items with:
          - type: primary Neo4j label (Person, Event, Place, Concept, User, Day, EventType, Emotion)
          - name: human display name
          - ref: a stable ref string usable with /entity/overview (ex: Person:<uuid>, Event:<key>, Day:<yyyy-mm-dd>, Context:<key>)
        """
        q = """
        MATCH (e)
        WHERE e:Person OR e:Place OR e:Concept OR e:User OR e:Event OR e:Day OR e:EventType OR e:Emotion OR e:Context
        WITH e, labels(e)[0] as type
        WITH e, type,
          CASE
            WHEN type = "Person" THEN e.name
            WHEN type = "Place" THEN e.name
            WHEN type = "Concept" THEN e.name
            WHEN type = "User" THEN e.name
            WHEN type = "Event" THEN coalesce(e.event_type, "event")
            WHEN type = "Day" THEN toString(e.date)
            WHEN type = "EventType" THEN e.name
            WHEN type = "Emotion" THEN e.name
            WHEN type = "Context" THEN coalesce(e.name, substring(coalesce(e.text, ''), 0, 60))
            ELSE coalesce(e.name, type)
          END as name,
          CASE
            WHEN type = "Person" THEN "Person:" + e.id
            WHEN type = "Event" THEN "Event:" + e.key
            WHEN type = "Day" THEN "Day:" + toString(e.date)
            WHEN type IN ["Place","Concept","User","EventType","Emotion"] THEN type + ":" + e.name
            WHEN type = "Context" THEN "Context:" + e.key
            ELSE type + ":" + coalesce(e.name, toString(e.id))
          END as ref,
          coalesce(e.mention_count, 0) as mentions,
          CASE
            WHEN type = "Day" THEN toString(e.date)
            ELSE toString(coalesce(e.last_seen, e.first_seen, e.created_at))
          END as last_seen
        WHERE $q = "" OR toLower(name) CONTAINS toLower($q)
        RETURN type as type, name as name, ref as ref, mentions as mentions, last_seen as last_seen
        ORDER BY mentions DESC, last_seen DESC
        LIMIT $limit
        """
        with self._driver.session() as s:
            return [dict(r) for r in s.run(q, limit=int(limit), q=query or "")]

    def entity_overview(self, ref: str, limit: int = 120) -> Dict[str, Any]:
        """
        Unified overview endpoint for UI navigation.
        Supported:
          - Person:<uuid> => returns kind="Person" and items timeline
          - Event:<key> => returns kind="Event" and participants + entries
        """
        if not ref or ":" not in ref:
            raise ValueError("ref must be like 'Person:<id>' or 'Event:<key>'")

        label, key = self._parse_ref(ref)

        if label == "Person":
            # timeline enriched with Day/Place/EventType
            items = self.person_timeline(person_id=key, limit=limit)
            # also include display name for header
            with self._driver.session() as s:
                row = s.run(
                    "MATCH (p:Person {id: $id}) RETURN p.name as name, p.role as role, coalesce(p.mention_count,0) as mentions",
                    id=key,
                ).single()
                name = row.get("name") if row else "Person"
                role = row.get("role") if row else None
                mentions = row.get("mentions") if row else 0
            return {"kind": "Person", "ref": ref, "name": name, "role": role, "mentions": mentions, "items": items}

        if label == "Event":
            q_participants = """
            MATCH (ev:Event {key: $key})
            OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
            OPTIONAL MATCH (ev)-[:OCCURRED_AT]->(pl:Place)
            OPTIONAL MATCH (ev)-[:HAS_TYPE]->(t:EventType)
            OPTIONAL MATCH (p:Person)-[:PARTICIPATED_IN]->(ev)
            OPTIONAL MATCH (u:User)-[:PARTICIPATED_IN]->(ev)
            WITH toString(d.date) as day,
                 coalesce(t.name, ev.event_type, "") as event_type,
                 collect(DISTINCT pl.name) as places,
                 [x IN collect(DISTINCT {id: p.id, name: p.name, role: p.role, mentions: coalesce(p.mention_count,0)}) WHERE x.id IS NOT NULL] as persons,
                 [y IN collect(DISTINCT {name: u.name}) WHERE y.name IS NOT NULL] as users
            RETURN day as day,
                   event_type as event_type,
                   places as places,
                   persons as persons,
                   users as users
            """

            q_entries = """
            MATCH (ev:Event {key: $key})<-[:REFERS_TO]-(e:Entry)
            OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
            RETURN e.id as entry_id,
                   toString(e.input_time) as input_time,
                   d.date as day,
                   substring(e.text, 0, 260) as text_preview
            ORDER BY e.input_time DESC
            LIMIT $limit
            """

            with self._driver.session() as s:
                p_row = s.run(q_participants, key=key).single()
                participants = dict(p_row) if p_row else {"day": None, "event_type": None, "places": [], "persons": [], "users": []}
                entries = [dict(r) for r in s.run(q_entries, key=key, limit=int(limit))]

            # Flatten a bit for the UI
            return {
                "kind": "Event",
                "ref": ref,
                "event_type": participants.get("event_type") or "",
                "day": participants.get("day") or "",
                "places": participants.get("places") or [],
                "persons": participants.get("persons") or [],
                "users": participants.get("users") or [],
                "entries": entries,
            }

        if label == "Context":
            q_ctx = """
            MATCH (ctx:Context {key: $key})
            OPTIONAL MATCH (ev:Event)-[:HAS_CONTEXT]->(ctx)
            OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
            OPTIONAL MATCH (ev)-[:HAS_TYPE]->(t:EventType)
            WITH ctx,
                 coalesce(t.name, ev.event_type, '') as event_type,
                 d.date as day,
                 substring(ctx.text, 0, 120) as context_preview
            RETURN event_type as event_type,
                   day as day,
                   ctx.name as name,
                   ctx.text as text_preview_long,
                   context_preview as context_preview,
                   ctx.key as ckey
            LIMIT 1
            """

            q_entries = """
            MATCH (ctx:Context {key: $key})<-[:HAS_CONTEXT]-(ev:Event)<-[:REFERS_TO]-(e:Entry)
            OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
            RETURN e.id as entry_id,
                   toString(e.input_time) as input_time,
                   d.date as day,
                   substring(e.text, 0, 260) as text_preview
            ORDER BY e.input_time DESC
            LIMIT $limit
            """

            q_entities = """
            MATCH (ctx:Context {key: $key})
            OPTIONAL MATCH (ctx)-[:HAS_TOPIC]->(t)
            OPTIONAL MATCH (ctx)-[:MENTIONS]->(m)
            WITH ctx,
                 collect(DISTINCT {type: labels(t)[0], name: t.name}) as topics,
                 collect(DISTINCT {type: labels(m)[0], name: m.name}) as mentions
            RETURN topics, mentions
            """

            with self._driver.session() as s:
                row = s.run(q_ctx, key=key).single()
                ents = s.run(q_entities, key=key).single()
                entries = [dict(r) for r in s.run(q_entries, key=key, limit=int(limit))]
                ctx_row = dict(row) if row else {}
                topics = (ents or {}).get("topics") or [] if ents else []
                mentions = (ents or {}).get("mentions") or [] if ents else []

            return {
                "kind": "Context",
                "ref": ref,
                "name": ctx_row.get("name") or "Context",
                "event_type": ctx_row.get("event_type") or "",
                "day": ctx_row.get("day") or "",
                "text": ctx_row.get("text_preview_long") or ctx_row.get("context_preview") or "",
                "topics": topics,
                "mentions": mentions,
                "entries": entries,
            }

        if label == "Concept":
            # Concept occurrences: (Concept)<-[:HAS_TOPIC]-(Event)<-[:REFERS_TO]-(Entry)
            q_participants = """
            MATCH (c:Concept {name: $name})<-[:HAS_TOPIC]-(ev:Event)
            OPTIONAL MATCH (p:Person)-[:PARTICIPATED_IN]->(ev)
            OPTIONAL MATCH (u:User)-[:PARTICIPATED_IN]->(ev)
            RETURN coalesce(
                     [x IN collect(DISTINCT {id: p.id, name: p.name, role: p.role, mentions: coalesce(p.mention_count,0)}) WHERE x.id IS NOT NULL],
                     []
                   ) as persons,
                   coalesce([y IN collect(DISTINCT {name: u.name}) WHERE y.name IS NOT NULL], []) as users
            """
            q_entries = """
            MATCH (c:Concept {name: $name})<-[:HAS_TOPIC]-(ev:Event)
            MATCH (ev)<-[:REFERS_TO]-(e:Entry)
            OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
            OPTIONAL MATCH (ev)-[:OCCURRED_AT]->(pl:Place)
            OPTIONAL MATCH (ev)-[:HAS_TYPE]->(t:EventType)
            WITH e,
                 collect(DISTINCT d.date) as days,
                 collect(DISTINCT coalesce(t.name, ev.event_type, '')) as event_types,
                 collect(DISTINCT pl.name) as places
            RETURN e.id as entry_id,
                   toString(e.input_time) as input_time,
                   days[0] as day,
                   event_types[0] as event_type,
                   places[0..3] as places,
                   substring(e.text, 0, 260) as text_preview
            ORDER BY e.input_time DESC
            LIMIT $limit
            """

            with self._driver.session() as s:
                p_row = s.run(q_participants, name=key).single()
                participants = dict(p_row) if p_row else {"persons": [], "users": []}
                entries = [dict(r) for r in s.run(q_entries, name=key, limit=int(limit))]

            first = entries[0] if entries else {}
            return {
                "kind": "Event",
                "ref": ref,
                "event_type": first.get("event_type") or key,
                "day": first.get("day") or "",
                "places": first.get("places") or [],
                "persons": participants.get("persons") or [],
                "users": participants.get("users") or [],
                "entries": entries,
            }

        if label == "Day":
            # Day overview: show entries whose event occurred on this day,
            # plus all participants connected to those events.
            q_participants = """
            MATCH (d:Day {date: $name})<-[:ON_DAY]-(ev:Event)
            OPTIONAL MATCH (p:Person)-[:PARTICIPATED_IN]->(ev)
            OPTIONAL MATCH (u:User)-[:PARTICIPATED_IN]->(ev)
            RETURN coalesce(
                     [x IN collect(DISTINCT {id: p.id, name: p.name, role: p.role, mentions: coalesce(p.mention_count,0)}) WHERE x.id IS NOT NULL],
                     []
                   ) as persons,
                   coalesce(
                     [y IN collect(DISTINCT {name: u.name}) WHERE y.name IS NOT NULL],
                     []
                   ) as users
            """

            q_entries = """
            MATCH (d:Day {date: $name})<-[:ON_DAY]-(ev:Event)<-[:REFERS_TO]-(e:Entry)
            OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
            OPTIONAL MATCH (ev)-[:OCCURRED_AT]->(pl:Place)
            OPTIONAL MATCH (ev)-[:HAS_TYPE]->(t:EventType)
            WITH e,
                 collect(DISTINCT d.date) as days,
                 collect(DISTINCT coalesce(t.name, ev.event_type, '')) as event_types,
                 collect(DISTINCT pl.name) as places
            RETURN e.id as entry_id,
                   toString(e.input_time) as input_time,
                   days[0] as day,
                   event_types[0] as event_type,
                   places[0..3] as places,
                   substring(e.text, 0, 260) as text_preview
            ORDER BY e.input_time DESC
            LIMIT $limit
            """

            with self._driver.session() as s:
                p_row = s.run(q_participants, name=key).single()
                participants = dict(p_row) if p_row else {"persons": [], "users": []}
                entries = [dict(r) for r in s.run(q_entries, name=key, limit=int(limit))]

            return {
                "kind": "Event",
                "ref": ref,
                "event_type": "day",
                "day": key,
                "places": [],
                "persons": participants.get("persons") or [],
                "users": participants.get("users") or [],
                "entries": entries,
            }

        if label == "Place":
            # Treat "place occurrences" as an Event-like overview so the UI can reuse
            # the same rendering (participants + entries list).
            q_entries = """
            MATCH (pl:Place {name: $name})<-[:OCCURRED_AT]-(ev:Event)<-[:REFERS_TO]-(e:Entry)
            OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
            OPTIONAL MATCH (ev)-[:HAS_TYPE]->(t:EventType)
            WITH e,
                 collect(DISTINCT d.date) as days,
                 collect(DISTINCT coalesce(t.name, ev.event_type, '')) as event_types
            RETURN e.id as entry_id,
                   toString(e.input_time) as input_time,
                   days[0] as day,
                   event_types[0] as event_type,
                   substring(e.text, 0, 260) as text_preview
            ORDER BY e.input_time DESC
            LIMIT $limit
            """
            q_participants = """
            MATCH (pl:Place {name: $name})<-[:OCCURRED_AT]-(ev:Event)
            OPTIONAL MATCH (p:Person)-[:PARTICIPATED_IN]->(ev)
            OPTIONAL MATCH (u:User)-[:PARTICIPATED_IN]->(ev)
            RETURN coalesce([x IN collect(DISTINCT {id: p.id, name: p.name, role: p.role, mentions: coalesce(p.mention_count,0)}) WHERE x.id IS NOT NULL], []) as persons,
                   coalesce([y IN collect(DISTINCT {name: u.name}) WHERE y.name IS NOT NULL], []) as users
            """
            with self._driver.session() as s:
                p_row = s.run(q_participants, name=key).single()
                participants = dict(p_row) if p_row else {"persons": [], "users": []}
                entries = [dict(r) for r in s.run(q_entries, name=key, limit=int(limit))]

            first = entries[0] if entries else {}
            return {
                "kind": "Event",
                "ref": ref,
                "event_type": first.get("event_type") or "",
                "day": first.get("day") or "",
                "places": [key],
                "persons": participants.get("persons") or [],
                "users": participants.get("users") or [],
                "entries": [
                    {
                        "entry_id": e.get("entry_id"),
                        "input_time": e.get("input_time"),
                        "day": e.get("day"),
                        "text_preview": e.get("text_preview"),
                    }
                    for e in entries
                ],
            }

        if label == "EventType":
            q_entries = """
            MATCH (ev:Event)-[:HAS_TYPE]->(t:EventType {name: $name})
            MATCH (ev)<-[:REFERS_TO]-(e:Entry)
            OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
            WITH e, collect(DISTINCT ev) as evs, collect(DISTINCT d.date) as days, collect(DISTINCT t.name) as tnames
            ORDER BY e.input_time DESC
            LIMIT $limit
            CALL {
              WITH evs
              UNWIND evs as ev
              OPTIONAL MATCH (ev)-[:OCCURRED_AT]->(pl:Place)
              RETURN collect(DISTINCT pl.name)[0..3] as places
            }
            RETURN e.id as entry_id,
                   toString(e.input_time) as input_time,
                   days[0] as day,
                   coalesce(tnames[0], '') as event_type,
                   places,
                   substring(e.text, 0, 260) as text_preview
            """
            q_participants = """
            MATCH (ev:Event)-[:HAS_TYPE]->(t:EventType {name: $name})
            OPTIONAL MATCH (p:Person)-[:PARTICIPATED_IN]->(ev)
            OPTIONAL MATCH (u:User)-[:PARTICIPATED_IN]->(ev)
            RETURN coalesce([x IN collect(DISTINCT {id: p.id, name: p.name, role: p.role, mentions: coalesce(p.mention_count,0)}) WHERE x.id IS NOT NULL], []) as persons,
                   coalesce([y IN collect(DISTINCT {name: u.name}) WHERE y.name IS NOT NULL], []) as users
            """
            with self._driver.session() as s:
                p_row = s.run(q_participants, name=key).single()
                participants = dict(p_row) if p_row else {"persons": [], "users": []}
                rows = [dict(r) for r in s.run(q_entries, name=key, limit=int(limit))]

            first = rows[0] if rows else {}
            return {
                "kind": "Event",
                "ref": ref,
                "event_type": first.get("event_type") or key,
                "day": first.get("day") or "",
                "places": first.get("places") or [],
                "persons": participants.get("persons") or [],
                "users": participants.get("users") or [],
                "entries": rows,
            }

        # Fallback: return minimal info rather than 500.
        with self._driver.session() as s:
            row = s.run(
                f"MATCH (e:{label}) WHERE e.name = $key RETURN e LIMIT 1",
                key=key,
            ).single()
        return {"kind": "Event", "ref": ref, "event_type": label, "day": "", "places": [], "persons": [], "users": [], "entries": []}

    def neighborhood(self, ref: str, depth: int = 1, limit: int = 200) -> Dict[str, Any]:
        """
        ref format: Label:Key where key maps to a well-known property:
          - Person:<uuid> (p.id)
          - Entry:<uuid> (e.id)
          - Event:<key> (ev.key)
          - User:<name> (u.name)
          - Place:<name> (pl.name)
          - Concept:<name> (c.name)
          - Day:<yyyy-mm-dd> (d.date)
        """
        label, key = self._parse_ref(ref)
        prop = self._label_key_prop(label)
        depth = max(1, min(int(depth), 2))
        limit = max(10, min(int(limit), 1000))

        q = f"""
        MATCH (n:{label} {{{prop}: $key}})
        MATCH (n)-[r*1..{depth}]-(m)
        WITH collect(DISTINCT n) + collect(DISTINCT m) as ns, r
        UNWIND ns as node
        WITH collect(DISTINCT node) as nodes, collect(DISTINCT r) as rels
        RETURN nodes, rels
        LIMIT 1
        """

        def _node_to_dict(node) -> Dict[str, Any]:
            d = dict(node)
            d["_labels"] = list(node.labels)
            d["_elementId"] = node.element_id
            return d

        edges: List[Dict[str, Any]] = []
        nodes_out: List[Dict[str, Any]] = []
        with self._driver.session() as s:
            row = s.run(q, key=key).single()
            if not row:
                return {"nodes": [], "edges": []}
            nodes = row["nodes"]
            rels = row["rels"]
            nodes_out = [_node_to_dict(n) for n in nodes][:limit]
            # rels is a list-of-lists when collected from r*; flatten
            flat = []
            for rpath in rels:
                if isinstance(rpath, list):
                    flat.extend(rpath)
            for r in flat[:limit]:
                edges.append(
                    {
                        "type": r.type,
                        "start": r.start_node.element_id,
                        "end": r.end_node.element_id,
                        "properties": dict(r),
                    }
                )
        return {"nodes": nodes_out, "edges": edges}

    def inbox(self, status: str = "open", limit: int = 50) -> List[Dict[str, Any]]:
        q = """
        MATCH (t:DisambiguationTask)
        WHERE t.status = $status
        OPTIONAL MATCH (t)-[:CANDIDATE]->(c:Person)
        OPTIONAL MATCH (t)-[:PROPOSED]->(p:Person)
        RETURN t.id as id,
               t.type as type,
               t.mention as mention,
               t.score as score,
               toString(t.created_at) as created_at,
               t.status as status,
               c.id as candidate_person_id,
               c.name as candidate_name,
               c.role as candidate_role,
               p.id as proposed_person_id,
               p.name as proposed_name,
               p.role as proposed_role,
               t.entry_id as entry_id
        ORDER BY t.created_at DESC
        LIMIT $limit
        """
        with self._driver.session() as s:
            return [dict(r) for r in s.run(q, status=status, limit=int(limit))]

    def resolve_task(
        self,
        task_id: str,
        decision: str,
        target_person_id: Optional[str] = None,
        decided_by: str = "user",
    ) -> Dict[str, Any]:
        decision = (decision or "").strip().lower()
        if decision not in {"merge", "split"}:
            raise ValueError("decision must be merge|split")

        q_get = """
        MATCH (t:DisambiguationTask {id: $id})
        OPTIONAL MATCH (t)-[:CANDIDATE]->(c:Person)
        OPTIONAL MATCH (t)-[:PROPOSED]->(p:Person)
        RETURN t as t, c.id as candidate_id, p.id as proposed_id
        """
        with self._driver.session() as s:
            row = s.run(q_get, id=task_id).single()
            if not row:
                raise ValueError("task not found")
            candidate_id = row.get("candidate_id")
            proposed_id = row.get("proposed_id")

            if decision == "merge":
                # Merge proposed into candidate by default; target_person_id can override.
                dst = target_person_id or candidate_id
                src = proposed_id if dst == candidate_id else candidate_id
                if not src or not dst:
                    raise ValueError("task missing candidate/proposed person")
                if src == dst:
                    # nothing to do
                    pass
                else:
                    self.merge_persons(src_person_id=src, dst_person_id=dst)

            # Mark task resolved
            s.run(
                """
                MATCH (t:DisambiguationTask {id: $id})
                SET t.status = 'resolved',
                    t.decision = $decision,
                    t.decided_by = $decided_by,
                    t.resolved_at = datetime()
                """,
                id=task_id,
                decision=decision,
                decided_by=decided_by,
            )
            return {"ok": True, "task_id": task_id, "decision": decision}

    def merge_persons(self, src_person_id: str, dst_person_id: str) -> None:
        """
        Merge src into dst (limited to relationships used in this project).
        Moves aliases and PARTICIPATED_IN edges, then deletes src.
        """
        if not src_person_id or not dst_person_id or src_person_id == dst_person_id:
            return
        q = """
        MATCH (src:Person {id: $src}), (dst:Person {id: $dst})
        // Move aliases (subquery avoids FOREACH/WITH pitfalls)
        CALL {
          WITH src, dst
          OPTIONAL MATCH (a:Alias)-[:REFERS_TO]->(src)
          WITH dst, collect(DISTINCT a) AS aliases
          UNWIND aliases AS a
          WITH dst, a
          WHERE a IS NOT NULL
          MERGE (a)-[:REFERS_TO]->(dst)
        }
        // Move participation edges
        CALL {
          WITH src, dst
          OPTIONAL MATCH (src)-[:PARTICIPATED_IN]->(ev:Event)
          WITH dst, collect(DISTINCT ev) AS events
          UNWIND events AS ev
          WITH dst, ev
          WHERE ev IS NOT NULL
          MERGE (dst)-[:PARTICIPATED_IN]->(ev)
        }
        // Best-effort mention_count + last_seen
        SET dst.mention_count = coalesce(dst.mention_count, 0) + coalesce(src.mention_count, 0),
            dst.last_seen = CASE
              WHEN dst.last_seen IS NULL THEN src.last_seen
              WHEN src.last_seen IS NULL THEN dst.last_seen
              WHEN datetime(dst.last_seen) >= datetime(src.last_seen) THEN dst.last_seen
              ELSE src.last_seen
            END
        DETACH DELETE src
        """
        with self._driver.session() as s:
            s.execute_write(lambda tx: tx.run(q, src=src_person_id, dst=dst_person_id))

    @staticmethod
    def _parse_ref(ref: str) -> Tuple[str, str]:
        if not ref or ":" not in ref:
            raise ValueError("ref must be like 'Person:<id>'")
        label, key = ref.split(":", 1)
        label = label.strip()
        key = key.strip()
        if not label or not key:
            raise ValueError("invalid ref")
        return label, key

    @staticmethod
    def _label_key_prop(label: str) -> str:
        mapping = {
            "Person": "id",
            "Entry": "id",
            "Event": "key",
            "User": "name",
            "Place": "name",
            "Concept": "name",
            "Day": "date",
            "Context": "key",
            "EventType": "name",
        }
        if label not in mapping:
            raise ValueError("unsupported label for neighborhood")
        return mapping[label]

