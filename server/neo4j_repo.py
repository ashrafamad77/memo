from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from neo4j import GraphDatabase

from config import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER

POSITIVE_EMOTION_TAGS = {
    "joy",
    "joie",
    "gratitude",
    "reconnaissance",
    "relief",
    "soulagement",
    "calm",
    "calme",
    "confidence",
    "confiance",
    "fierte",
    "pride",
    "satisfaction",
    "motivation",
    "hope",
    "espoir",
}

NEGATIVE_EMOTION_TAGS = {
    "pain",
    "douleur",
    "stress",
    "anxiety",
    "anxiete",
    "fear",
    "peur",
    "triste",
    "sadness",
    "deception",
    "disappointment",
    "frustration",
    "colere",
    "anger",
    "burnout",
    "emotionalpain",
}


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
        q = "MATCH (e:E73_Information_Object) WHERE coalesce(e.entry_kind,'') = 'journal_entry' RETURN count(e) as c"
        with self._driver.session() as s:
            row = s.run(q).single()
            return int(row["c"]) if row and row.get("c") is not None else 0

    def get_user_profile(self, user_name: str) -> Dict[str, Any]:
        q = """
        MATCH (u:E21_Person {name: $name})-[:P2_has_type]->(:E55_Type {name:'User'})
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
        MERGE (u:E21_Person:E39_Actor {name: $name})
        ON CREATE SET u.first_seen = datetime()
        SET u.last_seen = datetime(),
            u.profile_current_city = coalesce($current_city, u.profile_current_city),
            u.profile_home_country = coalesce($home_country, u.profile_home_country),
            u.profile_nationality = coalesce($nationality, u.profile_nationality),
            u.profile_timezone = coalesce($timezone, u.profile_timezone),
            u.profile_work_context = coalesce($work_context, u.profile_work_context)
        WITH u
        MERGE (ut:E55_Type {name:'User'})
        MERGE (u)-[:P2_has_type]->(ut)
        WITH u
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
        MATCH (e:E73_Information_Object)-[:P67_refers_to]->(ev:E7_Activity)
        WHERE coalesce(e.entry_kind,'') = 'journal_entry'
        OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
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
        MATCH (e:E73_Information_Object {id: $id})-[:P67_refers_to]->(ev:E7_Activity)
        OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
        OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(u:E21_Person)
        OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
        OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
        OPTIONAL MATCH (ev)-[:P67_refers_to]->(c:E28_Conceptual_Object)
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
        MATCH (p:E21_Person)
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
        MATCH (p:E21_Person {id: $id})
        OPTIONAL MATCH (a:Alias)-[:P67_refers_to]->(p)
        WITH p, collect(DISTINCT a.text) as aliases
        OPTIONAL MATCH (ev:E7_Activity)-[:P14_carried_out_by]->(p)
        OPTIONAL MATCH (ev)<-[:P67_refers_to]-(e:E73_Information_Object {entry_kind: 'journal_entry'})
        OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
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
        Interaction timeline for a person: (Person)-[:P14_carried_out_by]->(E7_Activity)<-[:P67_refers_to]-(Entry)
        Enriched with Day, Place, and EventType when available.
        """
        q = """
        MATCH (p:E21_Person {id: $id})
        MATCH (ev:E7_Activity)-[:P14_carried_out_by]->(p:E21_Person {id: $id})
        MATCH (ev)<-[:P67_refers_to]-(e:E73_Information_Object)
        WHERE coalesce(e.entry_kind,'') = 'journal_entry'
        OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
        OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
        OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
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
        WHERE e:E21_Person OR e:E53_Place OR e:E28_Conceptual_Object OR e:E7_Activity OR e:E52_Time_Span OR e:E55_Type OR e:E73_Information_Object OR e:E74_Group
        WITH e, labels(e)[0] as type
        WITH e, type,
          CASE
            WHEN type = "E21_Person" THEN e.name
            WHEN type = "E53_Place" THEN e.name
            WHEN type = "E28_Conceptual_Object" THEN e.name
            WHEN type = "E74_Group" THEN e.name
            WHEN type = "E7_Activity" THEN coalesce(e.event_type, "event")
            WHEN type = "E52_Time_Span" THEN toString(coalesce(e.date, e.key))
            WHEN type = "E55_Type" THEN e.name
            WHEN type = "E73_Information_Object" THEN coalesce(e.name, substring(coalesce(e.content, ''), 0, 60))
            ELSE coalesce(e.name, type)
          END as name,
          CASE
            WHEN type = "E21_Person" THEN "E21_Person:" + e.id
            WHEN type = "E7_Activity" THEN "Event:" + e.key
            WHEN type = "E52_Time_Span" THEN "E52_Time_Span:" + toString(coalesce(e.key, e.date))
            WHEN type IN ["E53_Place","E28_Conceptual_Object","E74_Group","E55_Type"] THEN type + ":" + e.name
            WHEN type = "E73_Information_Object" THEN "E73_Information_Object:" + e.key
            ELSE type + ":" + coalesce(e.name, toString(e.id))
          END as ref,
          coalesce(e.mention_count, 0) as mentions,
          CASE
            WHEN type = "E52_Time_Span" THEN toString(coalesce(e.date, e.key))
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

        if label == "E21_Person":
            # timeline enriched with Day/Place/EventType
            items = self.person_timeline(person_id=key, limit=limit)
            # also include display name for header
            with self._driver.session() as s:
                row = s.run(
                    "MATCH (p:E21_Person {id: $id}) RETURN p.name as name, p.role as role, coalesce(p.mention_count,0) as mentions",
                    id=key,
                ).single()
                name = row.get("name") if row else "Person"
                role = row.get("role") if row else None
                mentions = row.get("mentions") if row else 0
            return {"kind": "Person", "ref": ref, "name": name, "role": role, "mentions": mentions, "items": items}

        if label == "Event":
            q_participants = """
            MATCH (ev:E7_Activity {key: $key})
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
            OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
            WITH toString(d.date) as day,
                 coalesce(t.name, ev.event_type, "") as event_type,
                 collect(DISTINCT pl.name) as places,
                 [x IN collect(DISTINCT {id: p.id, name: p.name, role: p.role, mentions: coalesce(p.mention_count,0)}) WHERE x.id IS NOT NULL] as persons,
                 [] as users
            RETURN day as day,
                   event_type as event_type,
                   places as places,
                   persons as persons,
                   users as users
            """

            q_entries = """
            MATCH (ev:E7_Activity {key: $key})<-[:P67_refers_to]-(e:E73_Information_Object)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
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

        if label == "E73_Information_Object":
            q_ctx = """
            MATCH (ctx:E73_Information_Object {key: $key})
            OPTIONAL MATCH (ctx)-[:P67_refers_to {ref_type: 'context_of'}]->(ev:E7_Activity)
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
            WITH ctx,
                 coalesce(t.name, ev.event_type, '') as event_type,
                 d.date as day,
                 substring(ctx.content, 0, 120) as context_preview
            RETURN event_type as event_type,
                   day as day,
                   ctx.name as name,
                   ctx.content as text_preview_long,
                   context_preview as context_preview,
                   ctx.key as ckey
            LIMIT 1
            """

            q_entries = """
            MATCH (ctx:E73_Information_Object {key: $key})-[:P67_refers_to {ref_type: 'context_of'}]->(ev:E7_Activity)<-[:P67_refers_to]-(e:E73_Information_Object)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            RETURN e.id as entry_id,
                   toString(e.input_time) as input_time,
                   d.date as day,
                   substring(e.text, 0, 260) as text_preview
            ORDER BY e.input_time DESC
            LIMIT $limit
            """

            q_entities = """
            MATCH (ctx:E73_Information_Object {key: $key})
            OPTIONAL MATCH (ctx)-[rt:P67_refers_to {ref_type: 'topic'}]->(t)
            OPTIONAL MATCH (ctx)-[rc:P67_refers_to {ref_type: 'context'}]->(c)
            OPTIONAL MATCH (ctx)-[rm:P67_refers_to {ref_type: 'mention'}]->(m)
            WITH ctx,
                 collect(DISTINCT {type: labels(t)[0], name: t.name}) as topics,
                 collect(DISTINCT {type: labels(c)[0], name: c.name}) as concepts,
                 collect(DISTINCT {type: labels(m)[0], name: m.name}) as mentions
            RETURN topics, concepts, mentions
            """

            with self._driver.session() as s:
                row = s.run(q_ctx, key=key).single()
                ents = s.run(q_entities, key=key).single()
                entries = [dict(r) for r in s.run(q_entries, key=key, limit=int(limit))]
                ctx_row = dict(row) if row else {}
                topics = (ents or {}).get("topics") or [] if ents else []
                concepts = (ents or {}).get("concepts") or [] if ents else []
                mentions = (ents or {}).get("mentions") or [] if ents else []

            return {
                "kind": "E73_Information_Object",
                "ref": ref,
                "name": ctx_row.get("name") or "Context",
                "event_type": ctx_row.get("event_type") or "",
                "day": ctx_row.get("day") or "",
                "text": ctx_row.get("text_preview_long") or ctx_row.get("context_preview") or "",
                "topics": topics,
                "concepts": concepts,
                "mentions": mentions,
                "entries": entries,
            }

        if label == "E28_Conceptual_Object":
            # Concept occurrences through CIDOC reference links.
            q_participants = """
            MATCH (c:E28_Conceptual_Object {name: $name})<-[:P67_refers_to]-(ev:E7_Activity)
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(u:E21_Person)
            RETURN coalesce(
                     [x IN collect(DISTINCT {id: p.id, name: p.name, role: p.role, mentions: coalesce(p.mention_count,0)}) WHERE x.id IS NOT NULL],
                     []
                   ) as persons,
                   coalesce([y IN collect(DISTINCT {name: u.name}) WHERE y.name IS NOT NULL], []) as users
            """
            q_entries = """
            MATCH (c:E28_Conceptual_Object {name: $name})<-[:P67_refers_to]-(ev:E7_Activity)
            MATCH (ev)<-[:P67_refers_to]-(e:E73_Information_Object)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
            OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
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

        if label == "E52_Time_Span":
            # Day overview: show entries whose event occurred on this day,
            # plus all participants connected to those events.
            q_participants = """
            MATCH (d:E52_Time_Span {key: $name})<-[:P4_has_time_span]-(ev:E7_Activity)
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(u:E21_Person)
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
            MATCH (d:E52_Time_Span {key: $name})<-[:P4_has_time_span]-(ev:E7_Activity)<-[:P67_refers_to]-(e:E73_Information_Object)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
            OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
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

        if label == "E53_Place":
            # Treat "place occurrences" as an Event-like overview so the UI can reuse
            # the same rendering (participants + entries list).
            q_entries = """
            MATCH (pl:E53_Place {name: $name})<-[:P7_took_place_at]-(ev:E7_Activity)<-[:P67_refers_to]-(e:E73_Information_Object)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
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
            MATCH (pl:E53_Place {name: $name})<-[:P7_took_place_at]-(ev:E7_Activity)
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(u:E21_Person)
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

        if label == "E55_Type":
            q_entries = """
            MATCH (ev:E7_Activity)-[:P2_has_type]->(t:E55_Type {name: $name})
            MATCH (ev)<-[:P67_refers_to]-(e:E73_Information_Object)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            WITH e, collect(DISTINCT ev) as evs, collect(DISTINCT d.date) as days, collect(DISTINCT t.name) as tnames
            ORDER BY e.input_time DESC
            LIMIT $limit
            CALL {
              WITH evs
              UNWIND evs as ev
              OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
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
            MATCH (ev:E7_Activity)-[:P2_has_type]->(t:E55_Type {name: $name})
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(u:E21_Person)
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

        match_label = "E7_Activity" if label == "Event" else label
        q = f"""
        MATCH (n:{match_label} {{{prop}: $key}})
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

    @staticmethod
    def _emotion_polarity(tag: str) -> str:
        t = (tag or "").strip().lower()
        if t in NEGATIVE_EMOTION_TAGS:
            return "negative"
        if t in POSITIVE_EMOTION_TAGS:
            return "positive"
        return "neutral"

    def insights(self, user_name: str, days: int = 30, people_limit: int = 12) -> Dict[str, Any]:
        days = max(7, min(int(days), 365))
        people_limit = max(5, min(int(people_limit), 50))
        since_expr = f"datetime() - duration('P{days}D')"

        emotion_rows: List[Dict[str, Any]] = []
        people_rows: List[Dict[str, Any]] = []
        custody_rows: List[Dict[str, Any]] = []
        expectation_rows: List[Dict[str, Any]] = []
        entry_count_window = 0

        with self._driver.session() as s:
            emotion_rows = [
                dict(r)
                for r in s.run(
                    f"""
                    MATCH (j:E73_Information_Object)-[:P67_refers_to]->(a:E13_Attribute_Assignment)
                    WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                      AND j.input_time >= {since_expr}
                    OPTIONAL MATCH (a)-[:P141_assigned]->(t1:E55_Type)
                    OPTIONAL MATCH (a)-[:P2_has_type]->(t2:E55_Type)
                    RETURN toString(date(j.input_time)) as day,
                           coalesce(toLower(t1.name), toLower(t2.name), toLower(a.name), '') as tag,
                           count(*) as c
                    ORDER BY day ASC
                    """
                )
            ]

            people_rows = [
                dict(r)
                for r in s.run(
                    f"""
                    MATCH (j:E73_Information_Object)-[:P67_refers_to]->(a:E13_Attribute_Assignment)-[:P15_was_influenced_by]->(ev:E7_Activity)-[:P14_carried_out_by]->(p:E21_Person)
                    WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                      AND j.input_time >= {since_expr}
                    OPTIONAL MATCH (a)-[:P141_assigned]->(t1:E55_Type)
                    OPTIONAL MATCH (a)-[:P2_has_type]->(t2:E55_Type)
                    RETURN p.name as person,
                           coalesce(toLower(t1.name), toLower(t2.name), toLower(a.name), '') as tag,
                           count(*) as c
                    """
                )
            ]

            custody_rows = [
                dict(r)
                for r in s.run(
                    f"""
                    MATCH (j:E73_Information_Object)-[:P67_refers_to]->(tr:E10_Transfer_of_Custody)-[:P30_transferred_custody_of]->(o:E22_Human_Made_Object)
                    WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                      AND j.input_time >= {since_expr}
                    OPTIONAL MATCH (ret:E7_Activity)-[:P129_is_about]->(o)
                    WHERE toLower(coalesce(ret.name,'')) CONTAINS 'retour'
                       OR toLower(coalesce(ret.name,'')) CONTAINS 'return'
                    OPTIONAL MATCH (a:E13_Attribute_Assignment)-[:P17_was_motivated_by]->(tr)
                    OPTIONAL MATCH (a)-[:P141_assigned]->(t1:E55_Type)
                    OPTIONAL MATCH (a)-[:P2_has_type]->(t2:E55_Type)
                    WITH j, tr, o, count(ret) as returns,
                         collect(toLower(coalesce(t1.name, t2.name, a.name, ''))) as tags
                    WITH j, tr, o, returns,
                         any(tag IN tags WHERE tag CONTAINS 'expect' OR tag CONTAINS 'attente' OR tag CONTAINS 'returnexpectation') as has_return_expectation
                    WHERE returns = 0
                      AND has_return_expectation
                    RETURN tr.key as transfer_key,
                           coalesce(tr.name,'transfer') as transfer_name,
                           o.name as object_name,
                           toString(j.input_time) as input_time
                    ORDER BY j.input_time DESC
                    LIMIT 30
                    """
                )
            ]

            expectation_rows = [
                dict(r)
                for r in s.run(
                    f"""
                    MATCH (j:E73_Information_Object)-[:P67_refers_to]->(a:E13_Attribute_Assignment)
                    WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                      AND j.input_time >= {since_expr}
                    OPTIONAL MATCH (a)-[:P141_assigned]->(t1:E55_Type)
                    OPTIONAL MATCH (a)-[:P2_has_type]->(t2:E55_Type)
                    WITH j, a, toLower(coalesce(t1.name, t2.name, a.name, '')) as tag
                    WHERE tag CONTAINS 'expect' OR tag CONTAINS 'attente' OR tag CONTAINS 'returnexpectation'
                    RETURN a.key as assignment_key,
                           coalesce(a.name, 'expectation') as assignment_name,
                           toString(j.input_time) as input_time
                    ORDER BY j.input_time DESC
                    LIMIT 30
                    """
                )
            ]

            row = s.run(
                f"""
                MATCH (j:E73_Information_Object)
                WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                  AND j.input_time >= {since_expr}
                RETURN count(j) as c
                """
            ).single()
            entry_count_window = int((row or {}).get("c") or 0)

        # Emotions by day
        by_day: Dict[str, Dict[str, int]] = {}
        total_pos = 0
        total_neg = 0
        total_neu = 0
        for r in emotion_rows:
            d = str(r.get("day") or "")
            tag = str(r.get("tag") or "")
            c = int(r.get("c") or 0)
            pol = self._emotion_polarity(tag)
            if d not in by_day:
                by_day[d] = {"positive": 0, "negative": 0, "neutral": 0}
            by_day[d][pol] += c
            if pol == "positive":
                total_pos += c
            elif pol == "negative":
                total_neg += c
            else:
                total_neu += c
        emotions_per_day = [
            {"day": d, **vals}
            for d, vals in sorted(by_day.items(), key=lambda x: x[0])
        ]

        # People impact
        people_acc: Dict[str, Dict[str, Any]] = {}
        for r in people_rows:
            person = str(r.get("person") or "").strip()
            if not person:
                continue
            tag = str(r.get("tag") or "")
            c = int(r.get("c") or 0)
            pol = self._emotion_polarity(tag)
            if person not in people_acc:
                people_acc[person] = {"person": person, "positive": 0, "negative": 0, "neutral": 0, "sample_size": 0}
            people_acc[person][pol] += c
            people_acc[person]["sample_size"] += c

        people_impact: List[Dict[str, Any]] = []
        for person, v in people_acc.items():
            total = max(1, int(v["sample_size"]))
            net = (int(v["positive"]) - int(v["negative"])) / float(total)
            if total < 2:
                label = "Uncertain"
            elif net >= 0.25:
                label = "Supportive"
            elif net <= -0.25:
                label = "Draining"
            else:
                label = "Mixed"
            people_impact.append(
                {
                    "person": person,
                    "positive": int(v["positive"]),
                    "negative": int(v["negative"]),
                    "neutral": int(v["neutral"]),
                    "sample_size": int(v["sample_size"]),
                    "net_score": round(net, 3),
                    "label": label,
                }
            )
        people_impact.sort(key=lambda x: (x["net_score"], x["sample_size"]), reverse=True)
        people_impact = people_impact[:people_limit]

        # Pulse
        emotion_total = max(1, total_pos + total_neg + total_neu)
        neg_ratio = total_neg / float(emotion_total)
        support_count = len([p for p in people_impact if p["label"] == "Supportive"])
        draining_count = len([p for p in people_impact if p["label"] == "Draining"])
        support_ratio = support_count / float(max(1, support_count + draining_count))
        open_obligations = len(custody_rows) + len(expectation_rows)
        raw_pulse = 100.0
        raw_pulse -= neg_ratio * 40.0
        raw_pulse -= min(open_obligations, 10) / 10.0 * 30.0
        raw_pulse += support_ratio * 30.0
        # Confidence calibration for sparse datasets:
        # with few entries, shrink toward neutral baseline to avoid overconfident scores.
        confidence = min(1.0, entry_count_window / 12.0)
        pulse = (confidence * raw_pulse) + ((1.0 - confidence) * 60.0)
        pulse = max(0.0, min(100.0, pulse))

        # Recommendations
        recs: List[Dict[str, Any]] = []
        if total_neg > total_pos:
            recs.append(
                {
                    "title": "Stabilize emotional load",
                    "why": "Negative emotional assignments currently exceed positive ones.",
                    "action": "Plan one low-friction recovery block this week (walk, deep rest, or journaling closure).",
                    "confidence": "medium",
                }
            )
        if open_obligations > 0:
            recs.append(
                {
                    "title": "Close open obligations",
                    "why": f"There are {open_obligations} unresolved custody/expectation items.",
                    "action": "Pick one unresolved item and close it in the next 48 hours.",
                    "confidence": "high",
                }
            )
        best_supportive = next((p for p in people_impact if p["label"] == "Supportive"), None)
        if best_supportive:
            recs.append(
                {
                    "title": "Leverage a supportive relationship",
                    "why": f"{best_supportive['person']} is associated with better emotional outcomes.",
                    "action": f"Schedule one intentional interaction with {best_supportive['person']} this week.",
                    "confidence": "medium",
                }
            )
        recs = recs[:3]

        return {
            "window_days": days,
            "life_pulse": {
                "score": round(pulse, 1),
                "confidence": round(confidence, 3),
                "entries_in_window": entry_count_window,
                "emotion_load_negative_ratio": round(neg_ratio, 3),
                "open_obligations": open_obligations,
                "support_ratio": round(support_ratio, 3),
            },
            "emotions_per_day": emotions_per_day,
            "people_impact": people_impact,
            "open_obligations": {
                "custody_open": custody_rows,
                "expectations_open": expectation_rows,
            },
            "weekly_recommendations": recs,
        }

    def inbox(self, status: str = "open", limit: int = 50) -> List[Dict[str, Any]]:
        q = """
        MATCH (t:DisambiguationTask)
        WHERE t.status = $status
        OPTIONAL MATCH (t)-[:CANDIDATE]->(c:E21_Person)
        OPTIONAL MATCH (t)-[:PROPOSED]->(p:E21_Person)
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
        OPTIONAL MATCH (t)-[:CANDIDATE]->(c:E21_Person)
        OPTIONAL MATCH (t)-[:PROPOSED]->(p:E21_Person)
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
        MATCH (src:E21_Person {id: $src}), (dst:E21_Person {id: $dst})
        // Move aliases (subquery avoids FOREACH/WITH pitfalls)
        CALL {
          WITH src, dst
          OPTIONAL MATCH (a:Alias)-[:P67_refers_to]->(src)
          WITH dst, collect(DISTINCT a) AS aliases
          UNWIND aliases AS a
          WITH dst, a
          WHERE a IS NOT NULL
          MERGE (a)-[:P67_refers_to {ref_type: 'alias_of'}]->(dst)
        }
        // Move participation edges
        CALL {
          WITH src, dst
          OPTIONAL MATCH (ev:E7_Activity)-[:P14_carried_out_by]->(src)
          WITH dst, collect(DISTINCT ev) AS events
          UNWIND events AS ev
          WITH dst, ev
          WHERE ev IS NOT NULL
          MERGE (ev)-[:P14_carried_out_by]->(dst)
          MERGE (dst)-[:P14i_performed]->(ev)
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
            "E21_Person": "id",
            "E73_Information_Object": "id",
            "Event": "key",
            "E53_Place": "name",
            "E28_Conceptual_Object": "name",
            "E52_Time_Span": "key",
            "E55_Type": "name",
        }
        if label not in mapping:
            raise ValueError("unsupported label for neighborhood")
        return mapping[label]

