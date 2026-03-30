from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, ClassVar, Dict, List, Optional, Tuple

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

    def proposal_persons_at_place_like(
        self, user_name: str, city_substring: str, days: int = 365, limit: int = 80
    ) -> List[str]:
        """
        Names of people (non-User) who P14-carried-out activities at E53_Place whose name
        contains city_substring — used to narrow “see someone” proposals to likely-local context.
        """
        _ = user_name  # reserved for future: co-with-user constraints
        cs = (city_substring or "").strip()
        if len(cs) < 2:
            return []
        days = max(7, min(int(days), 730))
        limit = max(5, min(int(limit), 200))
        since_expr = f"datetime() - duration('P{days}D')"
        q = f"""
        MATCH (j:E73_Information_Object)-[:P67_refers_to]->(a:E13_Attribute_Assignment)-[:P15_was_influenced_by]->(ev:E7_Activity)
        WHERE coalesce(j.entry_kind,'') = 'journal_entry'
          AND j.input_time >= {since_expr}
        MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
        WHERE toLower(coalesce(pl.name,'')) CONTAINS toLower($city)
        MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
        WHERE NOT (p)-[:P2_has_type]->(:E55_Type {{name:'User'}})
        RETURN DISTINCT p.name as person
        LIMIT $limit
        """
        with self._driver.session() as s:
            rows = [dict(r) for r in s.run(q, city=cs, limit=limit)]
        return [str(r["person"]).strip() for r in rows if r.get("person")]

    def semantic_proposal_fragments(
        self,
        *,
        city_substring: str = "",
        place_hints: Optional[List[str]] = None,
        days: int = 400,
        max_activities: int = 55,
        max_feelings: int = 45,
        entry_cap: int = 100,
    ) -> Dict[str, Any]:
        """
        Context-triggered history for the semantic proposer: E7 activities and E13 feelings
        with E55_Type meaning (P2 on E7, P141/P2 on E13). Rows match profile city substring,
        common place hints (bureau, office, …), or have no place / no linked activity on the
        feeling path — so chains like office → hunger → shop remain retrievable.
        """
        days = max(30, min(int(days), 730))
        max_activities = max(5, min(int(max_activities), 120))
        max_feelings = max(5, min(int(max_feelings), 120))
        entry_cap = max(20, min(int(entry_cap), 200))
        cs = (city_substring or "").strip()
        hints = [str(h).strip() for h in (place_hints or []) if str(h).strip()]
        no_city_filter = len(cs) < 2 and len(hints) == 0
        since_expr = f"datetime() - duration('P{days}D')"

        q_activities = f"""
        MATCH (j:E73_Information_Object)
        WHERE coalesce(j.entry_kind,'') = 'journal_entry'
          AND j.input_time >= {since_expr}
        WITH j ORDER BY j.input_time DESC LIMIT $entry_cap
        MATCH (j)-[:P67_refers_to]->(ev:E7_Activity)
        OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
        OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
        OPTIONAL MATCH (ev)-[:P4_has_time_span]->(span:E52_Time_Span)
        OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
        WITH j, ev, pl, span, t, p,
             ($no_city_filter OR pl IS NULL OR
              toLower(coalesce(pl.name,'')) CONTAINS toLower($city) OR
              (size($hints) > 0 AND ANY(h IN $hints WHERE toLower(coalesce(pl.name,'')) CONTAINS toLower(h)))) AS place_ok
        WHERE place_ok
        WITH j, ev, pl, span, collect(DISTINCT t.name) AS act_type_names,
             collect(DISTINCT p) AS actors_raw
        RETURN
          j.id AS entry_id,
          toString(j.input_time) AS entry_time,
          ev.key AS activity_key,
          coalesce(ev.id, '') AS activity_node_id,
          coalesce(ev.name, '') AS activity_name,
          [x IN act_type_names WHERE x IS NOT NULL AND toString(x) <> ''] AS activity_meaning_types,
          pl.name AS place_name,
          coalesce(pl.id, '') AS place_node_id,
          span.date AS calendar_day,
          actors_raw AS actors_raw
        ORDER BY entry_time DESC
        LIMIT $lim_act
        """

        q_feelings = f"""
        MATCH (j:E73_Information_Object)
        WHERE coalesce(j.entry_kind,'') = 'journal_entry'
          AND j.input_time >= {since_expr}
        WITH j ORDER BY j.input_time DESC LIMIT $entry_cap
        MATCH (j)-[:P67_refers_to]->(a:E13_Attribute_Assignment)
        OPTIONAL MATCH (a)-[:P141_assigned]->(tg1:E55_Type)
        OPTIONAL MATCH (a)-[:P2_has_type]->(tg2:E55_Type)
        OPTIONAL MATCH (a)-[:P15_was_influenced_by]->(ev:E7_Activity)
        OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
        WITH j, a, ev, pl, tg1, tg2,
             ($no_city_filter OR pl IS NULL OR ev IS NULL OR
              toLower(coalesce(pl.name,'')) CONTAINS toLower($city) OR
              (size($hints) > 0 AND ANY(h IN $hints WHERE toLower(coalesce(pl.name,'')) CONTAINS toLower(h)))) AS place_ok
        WHERE place_ok
          AND coalesce(tg1.name, tg2.name, '') <> ''
          AND coalesce(tg1.name, tg2.name, '') <> 'User'
        RETURN
          j.id AS entry_id,
          toString(j.input_time) AS entry_time,
          coalesce(a.key, a.name, '') AS assignment_ref,
          coalesce(tg1.name, '') AS type_via_p141,
          coalesce(tg2.name, '') AS type_via_p2,
          coalesce(tg1.name, tg2.name, '') AS feeling_meaning,
          ev.key AS influenced_by_activity_key,
          coalesce(ev.name, '') AS influenced_by_activity_name,
          pl.name AS place_name,
          coalesce(pl.id, '') AS place_node_id
        ORDER BY entry_time DESC
        LIMIT $lim_feel
        """

        params = {
            "city": cs,
            "hints": hints,
            "no_city_filter": no_city_filter,
            "entry_cap": entry_cap,
            "lim_act": max_activities,
            "lim_feel": max_feelings,
        }
        def _person_nodes(raw: Any) -> List[Dict[str, str]]:
            if not raw:
                return []
            seen: set = set()
            acc: List[Dict[str, str]] = []
            for x in raw:
                if x is None:
                    continue
                try:
                    props = dict(x)
                except (TypeError, ValueError):
                    continue
                nm = str(props.get("name") or "").strip()
                if not nm:
                    continue
                pid = str(props.get("id") or "").strip()
                sk = (pid, nm.casefold())
                if sk in seen:
                    continue
                seen.add(sk)
                acc.append({"id": pid, "name": nm})
            return acc

        activities: List[Dict[str, Any]] = []
        feelings: List[Dict[str, Any]] = []
        with self._driver.session() as s:
            for r in s.run(q_activities, **params):
                d = dict(r)
                d["actors"] = _person_nodes(d.pop("actors_raw", []))
                cd = d.get("calendar_day")
                if cd is not None:
                    d["calendar_day"] = str(cd)
                activities.append(d)
            for r in s.run(q_feelings, **params):
                feelings.append(dict(r))

        return {
            "activities": activities,
            "feelings": feelings,
            "meta": {
                "days": days,
                "city_filter": cs,
                "place_hints": hints,
                "no_city_filter": no_city_filter,
                "activity_count": len(activities),
                "feeling_count": len(feelings),
            },
        }

    def briefing_activity_focus(self, hours: int = 24) -> Dict[str, Any]:
        """
        Fast daily-briefing signal: distinct E7_Activity nodes linked from journal entries
        whose input_time falls within the last `hours` hours, with human-readable labels
        from activity name, event_type, or P2_has_type → E55_Type.
        """
        hours = max(1, min(int(hours), 168))
        dur = f"PT{hours}H"
        q = f"""
        MATCH (j:E73_Information_Object)
        WHERE coalesce(j.entry_kind,'') = 'journal_entry'
          AND j.input_time >= datetime() - duration('{dur}')
        MATCH (j)-[:P67_refers_to]->(ev:E7_Activity)
        OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
        WITH ev, trim(coalesce(ev.name, ev.event_type, t.name, '')) AS raw_lbl
        WITH ev, CASE WHEN raw_lbl = '' THEN 'Activity' ELSE raw_lbl END AS activity_label
        RETURN count(DISTINCT ev) AS activity_count,
               collect(DISTINCT activity_label) AS raw_labels
        """
        with self._driver.session() as s:
            row = s.run(q).single()
        r = dict(row) if row else {}
        raw_labels = r.get("raw_labels") or []
        seen = set()
        sample_labels: List[str] = []
        for x in raw_labels:
            lab = str(x).strip()
            if not lab or lab in seen:
                continue
            seen.add(lab)
            sample_labels.append(lab)
            if len(sample_labels) >= 14:
                break
        return {
            "window_hours": hours,
            "activity_count": int(r.get("activity_count") or 0),
            "sample_labels": sample_labels,
        }

    def timeline(self, limit: int = 50) -> List[Dict[str, Any]]:
        q = """
        MATCH (e:E73_Information_Object)
        WHERE coalesce(e.entry_kind,'') = 'journal_entry'
        OPTIONAL MATCH (e)-[:P67_refers_to]->(ev:E7_Activity)
        OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
        WITH e,
             collect(DISTINCT ev.key) as event_keys,
             collect(DISTINCT d.date) as days
        RETURN e.id as id,
               e.text as text,
               toString(e.input_time) as input_time,
               event_keys[0] as event_key,
               coalesce(days[0], toString(date(e.input_time))) as day
        ORDER BY e.input_time DESC
        LIMIT $limit
        """
        with self._driver.session() as s:
            return [dict(r) for r in s.run(q, limit=int(limit))]

    def delete_journal_entry(self, entry_id: str) -> Dict[str, Any]:
        """
        Remove one journal entry (E73) and all entry-scoped graph nodes (key starts with entry_id|).
        Shared nodes (people, places, E55_Type, day bucket) are kept; only P67 edges from the journal go away.
        Also removes DisambiguationTask nodes tagged with this entry_id.
        """
        eid = (entry_id or "").strip()
        if not eid:
            return {"ok": False, "reason": "empty_id", "entry_id": ""}
        prefix = f"{eid}|"

        def work(tx):
            row = tx.run(
                """
                MATCH (j:E73_Information_Object {id: $id})
                WHERE coalesce(j.entry_kind, '') = 'journal_entry'
                RETURN j.id AS id
                """,
                id=eid,
            ).single()
            if not row:
                return False
            tx.run(
                """
                MATCH (t:DisambiguationTask)
                WHERE t.entry_id = $eid
                DETACH DELETE t
                """,
                eid=eid,
            )
            tx.run(
                """
                MATCH (n)
                WHERE n.key STARTS WITH $prefix
                DETACH DELETE n
                """,
                prefix=prefix,
            )
            tx.run(
                """
                MATCH (j:E73_Information_Object {id: $id})
                WHERE coalesce(j.entry_kind, '') = 'journal_entry'
                DETACH DELETE j
                """,
                id=eid,
            )
            return True

        with self._driver.session() as s:
            ok = s.execute_write(work)
        if not ok:
            return {"ok": False, "reason": "not_found", "entry_id": eid}
        return {"ok": True, "entry_id": eid}

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
               substring(e.text, 0, 260) as text_preview,
               coalesce(ev.key, '') as event_key,
               coalesce(ev.name, '') as activity_name
        ORDER BY e.input_time DESC
        LIMIT $limit
        """
        with self._driver.session() as s:
            return [dict(r) for r in s.run(q, id=person_id, limit=int(limit))]

    ENTITY_CATEGORY_PREDICATES: ClassVar[Dict[str, str]] = {
        "person": "e:E21_Person",
        "feeling_tag": "e:E55_Type",
        "situation": "e:E7_Activity",
        "place": "e:E53_Place",
        "day": "e:E52_Time_Span",
        "idea": "e:E28_Conceptual_Object",
        "note": "e:E73_Information_Object",
        "group": "e:E74_Group",
    }

    def entities(self, limit: int = 100, query: str = "", category: str = "") -> List[Dict[str, Any]]:
        """
        Mixed entity list for UI browsing.
        Optional category filters to one family (person, feeling_tag, situation, place, day, idea, note, group).
        Returns items with:
          - type: primary Neo4j label (Person, Event, Place, Concept, User, Day, EventType, Emotion)
          - name: human display name
          - ref: a stable ref string usable with /entity/overview (ex: Person:<uuid>, Event:<key>, Day:<yyyy-mm-dd>, Context:<key>)
        """
        cat = (category or "").strip().lower()
        if cat and cat not in self.ENTITY_CATEGORY_PREDICATES:
            cat = ""
        if cat:
            where_types = self.ENTITY_CATEGORY_PREDICATES[cat]
        else:
            where_types = (
                "e:E21_Person OR e:E53_Place OR e:E28_Conceptual_Object OR e:E7_Activity OR "
                "e:E52_Time_Span OR e:E55_Type OR e:E73_Information_Object OR e:E74_Group"
            )

        q = f"""
        MATCH (e)
        WHERE {where_types}
        WITH e,
          CASE
            WHEN e:E21_Person THEN 'E21_Person'
            WHEN e:E7_Activity THEN 'E7_Activity'
            WHEN e:E53_Place THEN 'E53_Place'
            WHEN e:E28_Conceptual_Object THEN 'E28_Conceptual_Object'
            WHEN e:E74_Group THEN 'E74_Group'
            WHEN e:E52_Time_Span THEN 'E52_Time_Span'
            WHEN e:E55_Type THEN 'E55_Type'
            WHEN e:E73_Information_Object THEN 'E73_Information_Object'
            ELSE coalesce(labels(e)[0], '')
          END AS type
        WITH e, type,
          CASE
            WHEN type = "E21_Person" THEN e.name
            WHEN type = "E53_Place" THEN e.name
            WHEN type = "E28_Conceptual_Object" THEN e.name
            WHEN type = "E74_Group" THEN e.name
            WHEN type = "E7_Activity" THEN
              CASE
                WHEN e.name IS NOT NULL AND trim(e.name) <> ''
                 AND NOT toLower(trim(e.name)) IN ['activity', 'event', 'misc'] THEN trim(e.name)
                WHEN e.event_type IS NOT NULL AND trim(e.event_type) <> ''
                 AND NOT toLower(trim(e.event_type)) IN ['activity', 'event', 'misc'] THEN trim(e.event_type)
                WHEN e.event_time_text IS NOT NULL AND trim(e.event_time_text) <> '' THEN trim(e.event_time_text)
                WHEN e.event_time_iso IS NOT NULL AND trim(toString(e.event_time_iso)) <> ''
                  THEN trim(replace(toString(e.event_time_iso), 'T', ' '))
                WHEN e.key IS NOT NULL AND trim(e.key) <> '' THEN
                  CASE
                    WHEN size(split(e.key, '|')) > 1 THEN
                      trim(split(e.key, '|')[0]) + ' · ' + trim(reverse(split(e.key, '|'))[0])
                    ELSE substring(e.key, 0, 56)
                  END
                ELSE 'Situation'
              END
            WHEN type = "E52_Time_Span" THEN toString(coalesce(e.date, e.key))
            WHEN type = "E55_Type" THEN e.name
            WHEN type = "E73_Information_Object" THEN coalesce(e.name, substring(coalesce(e.text, e.content, ''), 0, 60))
            ELSE coalesce(e.name, type)
          END as name,
          CASE
            WHEN type = "E73_Information_Object" AND coalesce(e.entry_kind,'') = 'journal_entry' THEN 'journal'
            WHEN type = "E73_Information_Object" THEN 'context'
            ELSE null
          END as note_role,
          CASE
            WHEN type = "E21_Person" THEN "E21_Person:" + e.id
            WHEN type = "E7_Activity" THEN "Event:" + e.key
            WHEN type = "E52_Time_Span" THEN "E52_Time_Span:" + toString(coalesce(e.key, e.date))
            WHEN type IN ["E53_Place","E28_Conceptual_Object","E74_Group","E55_Type"] THEN type + ":" + e.name
            WHEN type = "E73_Information_Object" THEN "E73_Information_Object:" + coalesce(e.id, e.key, elementId(e))
            ELSE type + ":" + coalesce(e.name, toString(e.id))
          END as ref,
          coalesce(e.mention_count, 0) as mentions,
          CASE
            WHEN type = "E52_Time_Span" THEN toString(coalesce(e.date, e.key))
            ELSE toString(coalesce(e.last_seen, e.first_seen, e.created_at))
          END as last_seen
        WHERE $q = "" OR toLower(name) CONTAINS toLower($q)
        RETURN type as type, name as name, ref as ref, mentions as mentions, last_seen as last_seen, note_role as note_role
        ORDER BY mentions DESC, last_seen DESC
        LIMIT $limit
        """
        with self._driver.session() as s:
            return [dict(r) for r in s.run(q, limit=int(limit), q=query or "")]

    def _person_feeling_tags(self, person_id: str, limit: int = 40) -> List[Dict[str, Any]]:
        """
        E55_Type tags (feelings / attributes) co-occurring with this person via:
        - journal → E13 → tag, and the same journal or assignment ties to an activity
          that lists this person (P14), including P15(ev) or direct j→ev.
        """
        q = """
        MATCH (p:E21_Person {id: $id})
        MATCH (j:E73_Information_Object)-[:P67_refers_to]->(a:E13_Attribute_Assignment)
        WHERE coalesce(j.entry_kind,'') = 'journal_entry'
        MATCH (a)-[:P141_assigned]->(tg:E55_Type)
        WHERE coalesce(tg.name,'') <> 'User'
        MATCH (ev:E7_Activity)-[:P14_carried_out_by]->(p)
        WHERE (a)-[:P15_was_influenced_by]->(ev) OR (j)-[:P67_refers_to]->(ev)
        RETURN tg.name as name, count(DISTINCT a) as cnt
        ORDER BY cnt DESC
        LIMIT $limit
        """
        out: List[Dict[str, Any]] = []
        with self._driver.session() as s:
            for r in s.run(q, id=person_id, limit=int(limit)):
                nm = (r.get("name") or "").strip()
                if not nm:
                    continue
                out.append(
                    {
                        "name": nm,
                        "count": int(r.get("cnt") or 0),
                        "ref": f"E55_Type:{nm}",
                    }
                )
        return out

    def _normalize_explore_ref(self, ref: str) -> str:
        """
        Browse lists used labels(e)[0], so :E21_Person:E39_Actor nodes could surface as E39_Actor:name.
        Map those (and Person:) to E21_Person:id so nav-options and overview match.
        """
        ref = (ref or "").strip()
        if not ref or ":" not in ref:
            return ref
        label, key = self._parse_ref(ref)
        if label == "Person":
            return f"E21_Person:{key}"
        if label == "E21_Person":
            return ref
        if label != "E39_Actor":
            return ref
        with self._driver.session() as s:
            row = s.run(
                """
                MATCH (p:E21_Person)
                WHERE p.id = $k OR toLower(toString(p.name)) = toLower($k)
                RETURN p.id as id
                LIMIT 1
                """,
                k=key,
            ).single()
            if row and row.get("id"):
                return f"E21_Person:{row['id']}"
        return ref

    def _person_id_from_anchor(self, anchor_person: str) -> Optional[str]:
        ap = (anchor_person or "").strip()
        if not ap or ":" not in ap:
            return None
        ap = self._normalize_explore_ref(ap)
        label, key = self._parse_ref(ap)
        return key if label == "E21_Person" else None

    def entity_navigation_options(self, ref: str, anchor_person: str = "") -> Dict[str, Any]:
        """
        High-level exploration choices for a ref (counts + enabled flags for empty-safe UI).
        """
        if not ref or ":" not in ref:
            raise ValueError("ref required")
        ref = self._normalize_explore_ref(ref)
        label, key = self._parse_ref(ref)
        anchor_pid = self._person_id_from_anchor(anchor_person) or ""
        options: List[Dict[str, Any]] = []
        display_name = key

        with self._driver.session() as s:
            if label == "E21_Person":
                row = s.run(
                    "MATCH (p:E21_Person {id: $id}) RETURN coalesce(p.name, '') as name, coalesce(p.mention_count, 0) as mentions",
                    id=key,
                ).single()
                display_name = (row.get("name") or "").strip() or key
                mentions = int(row.get("mentions") or 0) if row else 0
                crow = s.run(
                    """
                    MATCH (p:E21_Person {id: $id})
                    MATCH (ev:E7_Activity)-[:P14_carried_out_by]->(p)
                    MATCH (ev)<-[:P67_refers_to]-(e:E73_Information_Object)
                    WHERE coalesce(e.entry_kind,'') = 'journal_entry'
                    RETURN count(DISTINCT e) as c
                    """,
                    id=key,
                ).single()
                n = int(crow.get("c") or 0) if crow else 0
                # Always allow opening: people appear in browse lists from mentions/aliases even when
                # nothing is wired yet as Activity→P14→Person→journal (count can be 0).
                desc = (
                    "Each note where they appear, linked to a situation when the graph has one."
                    if n > 0
                    else (
                        "No journal notes are linked through a situation yet for this person."
                        + (f" They still have {mentions} recorded mention(s) in the graph." if mentions else "")
                    )
                )
                options.append(
                    {
                        "key": "moments",
                        "title": "Journal moments",
                        "description": desc,
                        "count": n,
                        "enabled": True,
                    }
                )

            elif label == "E55_Type":
                row = s.run(
                    """
                    MATCH (t:E55_Type)
                    WHERE t.name = $name OR toLower(t.name) = toLower($name)
                    WITH t ORDER BY CASE WHEN t.name = $name THEN 0 ELSE 1 END, t.name
                    LIMIT 1
                    RETURN coalesce(t.name, $name) as name
                    """,
                    name=key,
                ).single()
                display_name = (row.get("name") or key).strip() if row else key
                crow = s.run(
                    """
                    MATCH (tag:E55_Type)
                    WHERE tag.name = $name OR toLower(tag.name) = toLower($name)
                    WITH tag ORDER BY CASE WHEN tag.name = $name THEN 0 ELSE 1 END, tag.name
                    LIMIT 1
                    MATCH (j:E73_Information_Object)-[:P67_refers_to]->(a:E13_Attribute_Assignment)
                    WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                      AND (a)-[:P141_assigned]->(tag)
                      AND (
                        $pid = ''
                        OR (a)-[:P140_assigned_attribute_to]->(:E21_Person {id: $pid})
                        OR EXISTS {
                          MATCH (a)-[:P15_was_influenced_by]->(:E7_Activity)-[:P14_carried_out_by]->(:E21_Person {id: $pid})
                        }
                      )
                    RETURN count(DISTINCT a) as c
                    """,
                    name=key,
                    pid=anchor_pid,
                ).single()
                n = int(crow.get("c") or 0) if crow else 0
                options.append(
                    {
                        "key": "feelings",
                        "title": "How this shows in your notes",
                        "description": "Journal assignments (E13) that use this type as the recorded value (P141).",
                        "count": n,
                        "enabled": n > 0,
                    }
                )
                act = s.run(
                    """
                    MATCH (tag:E55_Type)
                    WHERE tag.name = $name OR toLower(tag.name) = toLower($name)
                    WITH tag ORDER BY CASE WHEN tag.name = $name THEN 0 ELSE 1 END, tag.name
                    LIMIT 1
                    MATCH (ev:E7_Activity)-[:P2_has_type]->(tag)
                    MATCH (ev)<-[:P67_refers_to]-(j:E73_Information_Object)
                    WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                      AND (
                        $pid = ''
                        OR (ev)-[:P14_carried_out_by]->(:E21_Person {id: $pid})
                      )
                    RETURN count(DISTINCT ev) as c
                    """,
                    name=key,
                    pid=anchor_pid,
                ).single()
                n_act = int(act.get("c") or 0) if act else 0
                options.append(
                    {
                        "key": "activity_type",
                        "title": "Situations typed like this",
                        "description": "Activities classified with this E55_Type (P2_has_type), e.g. lunch as “Consumption”.",
                        "count": n_act,
                        "enabled": n_act > 0,
                    }
                )

            elif label == "Event":
                row = s.run(
                    "MATCH (ev:E7_Activity {key: $k}) RETURN coalesce(ev.name, ev.key, $k) as name",
                    k=key,
                ).single()
                display_name = (row.get("name") or key).strip() if row else key
                options.append(
                    {
                        "key": "hub",
                        "title": "Situation overview",
                        "description": "Who was involved and which journal notes mention it.",
                        "count": 1,
                        "enabled": True,
                    }
                )

            elif label == "E73_Information_Object":
                # Journal entries use `id`; context snippets may use `key` only.
                row = s.run(
                    """
                    MATCH (x:E73_Information_Object)
                    WHERE x.id = $k OR x.key = $k
                    RETURN coalesce(x.name, '') as name
                    LIMIT 1
                    """,
                    k=key,
                ).single()
                display_name = (row.get("name") or "Note").strip() or key
                erow = s.run(
                    """
                    MATCH (ctx:E73_Information_Object)
                    WHERE ctx.id = $k OR ctx.key = $k
                    MATCH (ctx)-[:P67_refers_to {ref_type: 'context_of'}]->(ev:E7_Activity)
                    MATCH (ev)<-[:P67_refers_to]-(e:E73_Information_Object)
                    WHERE coalesce(e.entry_kind,'') = 'journal_entry'
                    RETURN count(DISTINCT e) as c
                    """,
                    k=key,
                ).single()
                n = int(erow.get("c") or 0) if erow else 0
                options.append(
                    {
                        "key": "context",
                        "title": "Context & related notes",
                        "description": "Phrases linked from this context plus related journal entries.",
                        "count": max(1, n),
                        "enabled": True,
                    }
                )

            elif label == "E53_Place":
                display_name = key
                # Places from "physical_location" link via Activity P7; "remote_context" links
                # journal P67→Place only (no P7). Count both so Entity Timeline is not empty for those.
                crow = s.run(
                    """
                    MATCH (pl:E53_Place)
                    WHERE pl.name = $name OR toLower(toString(pl.name)) = toLower($name)
                    WITH pl ORDER BY CASE WHEN pl.name = $name THEN 0 ELSE 1 END, pl.name LIMIT 1
                    CALL {
                      WITH pl
                      MATCH (pl)<-[:P7_took_place_at]-(ev:E7_Activity)<-[:P67_refers_to]-(e:E73_Information_Object)
                      WHERE coalesce(e.entry_kind,'') = 'journal_entry'
                      RETURN e.id AS entry_id
                      UNION
                      WITH pl
                      MATCH (e:E73_Information_Object)-[:P67_refers_to]->(pl)
                      WHERE coalesce(e.entry_kind,'') = 'journal_entry'
                      RETURN e.id AS entry_id
                    }
                    RETURN count(DISTINCT entry_id) AS c
                    """,
                    name=key,
                ).single()
                n = int(crow.get("c") or 0) if crow else 0
                options.append(
                    {
                        "key": "hub",
                        "title": "What happened here",
                        "description": (
                            "Journal notes at this place (via activities) or mentioning it in the text (remote context)."
                            if n > 0
                            else "No journal notes linked yet—neither as a situation location nor as a mentioned place."
                        ),
                        "count": n,
                        "enabled": n > 0,
                    }
                )

            elif label == "E28_Conceptual_Object":
                display_name = key
                # Pipeline links topics/concepts as journal—P67→concept; older path is concept←P67←activity←P67←journal.
                crow = s.run(
                    """
                    MATCH (c:E28_Conceptual_Object {name: $name})
                    CALL {
                      WITH c
                      MATCH (e:E73_Information_Object)-[:P67_refers_to]->(c)
                      WHERE coalesce(e.entry_kind,'') = 'journal_entry'
                      RETURN e.id as id
                      UNION
                      WITH c
                      MATCH (c)<-[:P67_refers_to]-(ev:E7_Activity)
                      MATCH (ev)<-[:P67_refers_to]-(e:E73_Information_Object)
                      WHERE coalesce(e.entry_kind,'') = 'journal_entry'
                      RETURN e.id as id
                    }
                    RETURN count(DISTINCT id) as c
                    """,
                    name=key,
                ).single()
                n = int(crow.get("c") or 0) if crow else 0
                options.append(
                    {
                        "key": "hub",
                        "title": "Linked moments",
                        "description": "Journal notes that mention this topic, or activities tied to it.",
                        "count": n,
                        "enabled": n > 0,
                    }
                )

            elif label == "E52_Time_Span":
                display_name = key
                dc = self._e52_day_counts(key)
                n_any = sum(dc.values())
                options.append(
                    {
                        "key": "all",
                        "title": "Full day",
                        "description": "Situations (filterable), journal notes, people, and feelings linked to this date.",
                        "count": n_any,
                        "enabled": True,
                    }
                )
                options.append(
                    {
                        "key": "situations",
                        "title": "Situations",
                        "description": "Activities with this day as time span (direct). Open one for cast and notes.",
                        "count": dc["situations"],
                        "enabled": dc["situations"] > 0,
                    }
                )
                options.append(
                    {
                        "key": "journal",
                        "title": "Journal notes",
                        "description": "Entries written this day or tied to activities on this day (direct + via situation).",
                        "count": dc["journal"],
                        "enabled": dc["journal"] > 0,
                    }
                )
                options.append(
                    {
                        "key": "people",
                        "title": "People",
                        "description": "Everyone on the cast of activities dated this day (via situations).",
                        "count": dc["people"],
                        "enabled": dc["people"] > 0,
                    }
                )
                options.append(
                    {
                        "key": "feelings",
                        "title": "Feelings & tags",
                        "description": "Emotion assignments on notes that belong to this day (proxy via journal / situation).",
                        "count": dc["feelings"],
                        "enabled": dc["feelings"] > 0,
                    }
                )

            elif label == "E74_Group":
                grow = s.run(
                    """
                    MATCH (g:E74_Group)
                    WHERE g.name = $name OR toLower(toString(g.name)) = toLower($name)
                    WITH g ORDER BY CASE WHEN g.name = $name THEN 0 ELSE 1 END, g.name
                    LIMIT 1
                    RETURN coalesce(g.name, $name) AS disp
                    """,
                    name=key,
                ).single()
                display_name = (grow.get("disp") or key).strip() if grow else key
                crow = s.run(
                    """
                    MATCH (g:E74_Group)
                    WHERE g.name = $name OR toLower(toString(g.name)) = toLower($name)
                    WITH g ORDER BY CASE WHEN g.name = $name THEN 0 ELSE 1 END, g.name LIMIT 1
                    MATCH (e:E73_Information_Object)-[:P67_refers_to]->(g)
                    WHERE coalesce(e.entry_kind,'') = 'journal_entry'
                    RETURN count(DISTINCT e) AS c
                    """,
                    name=key,
                ).single()
                n = int(crow.get("c") or 0) if crow else 0
                options.append(
                    {
                        "key": "hub",
                        "title": "Notes mentioning this group",
                        "description": (
                            "Journal entries that link to this organization (from your notes)."
                            if n > 0
                            else "No journal notes link to this group yet."
                        ),
                        "count": n,
                        "enabled": n > 0,
                    }
                )

            else:
                options.append(
                    {
                        "key": "unknown",
                        "title": "Explore",
                        "description": "This type is not fully supported in the guided flow.",
                        "count": 0,
                        "enabled": False,
                    }
                )

        return {"ref": ref, "display_name": display_name, "options": options}

    @staticmethod
    def _e52_day_match() -> str:
        return """
        MATCH (d:E52_Time_Span)
        WHERE toString(coalesce(d.key, d.date)) = $dk OR toString(d.date) = $dk
        """

    def _e52_day_counts(self, dk: str) -> Dict[str, int]:
        m = self._e52_day_match()
        with self._driver.session() as s:
            if not s.run(m + " RETURN 1 AS ok LIMIT 1", dk=dk).single():
                return {"situations": 0, "journal": 0, "people": 0, "feelings": 0}
            sit = s.run(
                m + """
                MATCH (d)<-[:P4_has_time_span]-(ev:E7_Activity)
                RETURN count(DISTINCT ev) AS c
                """,
                dk=dk,
            ).single()
            ent = s.run(
                m + """
                WITH d, toString(coalesce(d.key, d.date)) AS dayStr
                MATCH (j:E73_Information_Object)
                WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                  AND (
                    toString(date(j.input_time)) = dayStr
                    OR EXISTS { (j)-[:P67_refers_to]->(:E7_Activity)-[:P4_has_time_span]->(d) }
                  )
                RETURN count(DISTINCT j) AS c
                """,
                dk=dk,
            ).single()
            peo = s.run(
                m + """
                MATCH (d)<-[:P4_has_time_span]-(ev:E7_Activity)
                MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
                RETURN count(DISTINCT p) AS c
                """,
                dk=dk,
            ).single()
            tag = s.run(
                m + """
                WITH d, toString(coalesce(d.key, d.date)) AS dayStr
                MATCH (j:E73_Information_Object)
                WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                  AND (
                    toString(date(j.input_time)) = dayStr
                    OR EXISTS { (j)-[:P67_refers_to]->(:E7_Activity)-[:P4_has_time_span]->(d) }
                  )
                MATCH (j)-[:P67_refers_to]->(a:E13_Attribute_Assignment)
                MATCH (a)-[:P141_assigned]->(tg:E55_Type)
                WHERE coalesce(tg.name,'') <> 'User'
                RETURN count(DISTINCT tg) AS c
                """,
                dk=dk,
            ).single()
            return {
                "situations": int((sit or {}).get("c") or 0),
                "journal": int((ent or {}).get("c") or 0),
                "people": int((peo or {}).get("c") or 0),
                "feelings": int((tag or {}).get("c") or 0),
            }

    def _overview_day(self, ref: str, day_key: str, limit: int, focus: str) -> Dict[str, Any]:
        dk = day_key
        fo = (focus or "all").strip().lower()
        if fo not in ("all", "situations", "journal", "people", "feelings"):
            fo = "all"
        m = self._e52_day_match()
        out: Dict[str, Any] = {
            "kind": "Day",
            "ref": ref,
            "day": dk,
            "focus": fo,
            "situations": [],
            "entries": [],
            "persons": [],
            "users": [],
            "feeling_tags": [],
        }
        with self._driver.session() as s:
            if not s.run(m + " RETURN d LIMIT 1", dk=dk).single():
                return out

            if fo in ("all", "situations"):
                rows = s.run(
                    m + """
                    MATCH (d)<-[:P4_has_time_span]-(ev:E7_Activity)
                    OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
                    OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
                    RETURN DISTINCT ev.key AS event_key,
                           coalesce(ev.name, ev.event_type, t.name, 'Situation') AS title,
                           coalesce(t.name, ev.event_type, '') AS event_type,
                           collect(DISTINCT pl.name) AS places
                    ORDER BY title
                    LIMIT 80
                    """,
                    dk=dk,
                )
                out["situations"] = [
                    {
                        "ref": f"Event:{dict(r).get('event_key') or ''}",
                        "event_key": str(dict(r).get("event_key") or ""),
                        "title": str(dict(r).get("title") or "Situation"),
                        "event_type": str(dict(r).get("event_type") or ""),
                        "places": [x for x in (dict(r).get("places") or []) if x],
                    }
                    for r in rows
                    if dict(r).get("event_key")
                ]

            if fo in ("all", "journal"):
                rows = s.run(
                    m + """
                    MATCH (d)<-[:P4_has_time_span]-(ev:E7_Activity)<-[:P67_refers_to]-(e:E73_Information_Object)
                    WHERE coalesce(e.entry_kind,'') = 'journal_entry'
                    OPTIONAL MATCH (ev)-[:P4_has_time_span]->(dd:E52_Time_Span)
                    OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
                    OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
                    WITH e, ev, collect(DISTINCT dd.date) AS days,
                         collect(DISTINCT coalesce(t.name, ev.event_type, '')) AS event_types,
                         collect(DISTINCT pl.name) AS places
                    RETURN e.id AS entry_id,
                           toString(e.input_time) AS input_time,
                           days[0] AS day,
                           event_types[0] AS event_type,
                           places[0..3] AS places,
                           substring(e.text, 0, 260) AS text_preview,
                           coalesce(ev.key, '') AS event_key,
                           coalesce(ev.name, '') AS activity_name
                    ORDER BY e.input_time DESC
                    LIMIT $lim
                    """,
                    dk=dk,
                    lim=int(limit),
                )
                out["entries"] = [dict(r) for r in rows]

            if fo in ("all", "people"):
                p_row = s.run(
                    m + """
                    MATCH (d)<-[:P4_has_time_span]-(ev:E7_Activity)
                    OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
                    RETURN coalesce(
                      [x IN collect(DISTINCT {id: p.id, name: p.name, role: p.role, mentions: coalesce(p.mention_count,0)})
                       WHERE x.id IS NOT NULL],
                      []
                    ) AS persons
                    """,
                    dk=dk,
                ).single()
                persons = (p_row or {}).get("persons") or []
                out["persons"] = [dict(x) for x in persons if isinstance(x, dict) and x.get("id")]
                out["users"] = []

            if fo in ("all", "feelings"):
                rows = s.run(
                    m + """
                    WITH d, toString(coalesce(d.key, d.date)) AS dayStr
                    MATCH (j:E73_Information_Object)
                    WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                      AND (
                        toString(date(j.input_time)) = dayStr
                        OR EXISTS { (j)-[:P67_refers_to]->(:E7_Activity)-[:P4_has_time_span]->(d) }
                      )
                    MATCH (j)-[:P67_refers_to]->(a:E13_Attribute_Assignment)
                    MATCH (a)-[:P141_assigned]->(tg:E55_Type)
                    WHERE coalesce(tg.name,'') <> 'User'
                    RETURN tg.name AS name, count(DISTINCT a) AS cnt
                    ORDER BY cnt DESC
                    LIMIT 40
                    """,
                    dk=dk,
                )
                out["feeling_tags"] = [
                    {
                        "name": str(dict(r).get("name") or ""),
                        "count": int(dict(r).get("cnt") or 0),
                        "ref": f"E55_Type:{str(dict(r).get('name') or '')}",
                    }
                    for r in rows
                    if (dict(r).get("name") or "").strip()
                ]


        if fo == "situations":
            out["entries"] = []
            out["persons"] = []
            out["users"] = []
            out["feeling_tags"] = []
        elif fo == "journal":
            out["situations"] = []
            out["persons"] = []
            out["users"] = []
            out["feeling_tags"] = []
        elif fo == "people":
            out["situations"] = []
            out["entries"] = []
            out["feeling_tags"] = []
        elif fo == "feelings":
            out["situations"] = []
            out["entries"] = []
            out["persons"] = []
            out["users"] = []

        return out

    @staticmethod
    def _journal_explore_links_from_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Deduplicate journal P67 rows; prefer direct links over people/places reached only via an activity."""
        by_ref: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            er = str(r.get("explore_ref") or "").strip()
            if not er:
                continue
            src = str(r.get("src") or "")
            prev = by_ref.get(er)
            if prev is None:
                by_ref[er] = r
                continue
            prev_src = str(prev.get("src") or "")
            if prev_src != "direct" and src == "direct":
                by_ref[er] = r
        order = {"person": 0, "situation": 1, "place": 2, "idea": 3, "group": 4, "tag": 5, "day": 6, "other": 7}
        out = list(by_ref.values())
        out.sort(
            key=lambda x: (
                order.get(str(x.get("bucket") or ""), 99),
                str(x.get("display_name") or ""),
            )
        )
        return [
            {
                "ref": str(x.get("explore_ref") or ""),
                "name": (str(x.get("display_name") or "").strip() or str(x.get("explore_ref"))),
                "bucket": str(x.get("bucket") or "other"),
                "ref_type": str(x.get("ref_type") or ""),
                "source": str(x.get("src") or ""),
            }
            for x in out
        ]

    @staticmethod
    def _merge_person_rows_by_id(*lists: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge {id, name, role, mentions} rows; first wins for duplicates."""
        by_id: Dict[str, Dict[str, Any]] = {}
        for lst in lists:
            for r in lst:
                pid = str(r.get("id") or "").strip()
                if not pid:
                    continue
                name = str(r.get("name") or "").strip() or pid
                if pid not in by_id:
                    by_id[pid] = {
                        "id": pid,
                        "name": name,
                        "role": r.get("role"),
                        "mentions": int(r.get("mentions") or 0),
                    }
        return sorted(by_id.values(), key=lambda x: str(x.get("name") or "").lower())

    def entity_overview(self, ref: str, limit: int = 120, anchor_person: str = "", focus: str = "") -> Dict[str, Any]:
        """
        Unified overview endpoint for UI navigation.
        Supported:
          - Person:<uuid> => returns kind="Person" and items timeline
          - Event:<key> => returns kind="Event" and participants + entries
          - E53_Place:<name> => kind="Event" (journal via activity P7 and/or direct P67 mention)
          - E74_Group:<name> => kind="Event" (journal entries with P67→group)
          - E55_Type:<name> => returns kind="Feeling" (two lenses: focus=feelings for E13→P141, or focus=activity_type for E7→P2_has_type→tag with journal P67→ev)
        anchor_person: optional E21_Person:… ref — for Feeling, only assignments where that person is the assignee
        (P140) or appears on the linked situation (P14); for activity_type lens, only situations where that person is on P14.
        focus: optional lens — days (E52_Time_Span): all | situations | journal | people | feelings; E55_Type: feelings | activity_type.
        """
        if not ref or ":" not in ref:
            raise ValueError("ref must be like 'Person:<id>' or 'Event:<key>'")

        ref = self._normalize_explore_ref(ref)
        label, key = self._parse_ref(ref)
        anchor_pid = self._person_id_from_anchor(anchor_person) or ""
        anchor_display = ""
        if anchor_pid:
            with self._driver.session() as s:
                pr = s.run(
                    "MATCH (p:E21_Person {id: $id}) RETURN coalesce(p.name, '') as name",
                    id=anchor_pid,
                ).single()
                anchor_display = (pr.get("name") or "").strip() if pr else ""

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
            feeling_tags = self._person_feeling_tags(person_id=key, limit=40)
            return {
                "kind": "Person",
                "ref": ref,
                "name": name,
                "role": role,
                "mentions": mentions,
                "items": items,
                "feeling_tags": feeling_tags,
            }

        if label == "Event":
            q_participants = """
            MATCH (ev:E7_Activity {key: $key})
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
            OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(performer)
            WHERE performer IS NULL OR performer:E21_Person OR performer:E39_Actor
            WITH toString(d.date) as day,
                 coalesce(t.name, ev.event_type, "") as event_type,
                 coalesce(ev.name, "") as activity_name,
                 toString(ev.event_time_iso) as event_time_iso,
                 ev.event_time_text as event_time_text,
                 collect(DISTINCT pl.name) as places,
                 [x IN collect(DISTINCT {id: performer.id, name: performer.name, role: performer.role, mentions: coalesce(performer.mention_count,0)})
                  WHERE x.id IS NOT NULL] as persons,
                 [] as users
            RETURN day as day,
                   event_type as event_type,
                   activity_name as activity_name,
                   event_time_iso as event_time_iso,
                   event_time_text as event_time_text,
                   places as places,
                   persons as persons,
                   users as users
            """

            q_evt_journal_people = """
            MATCH (ev:E7_Activity {key: $key})<-[:P67_refers_to]-(e:E73_Information_Object)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            MATCH (e)-[:P67_refers_to]->(p:E21_Person)
            RETURN DISTINCT p.id as id,
                   coalesce(p.name, p.id) as name,
                   p.role as role,
                   coalesce(p.mention_count, 0) as mentions
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
                participants = dict(p_row) if p_row else {}
                if not participants:
                    participants = {
                        "day": None,
                        "event_type": None,
                        "activity_name": None,
                        "event_time_iso": None,
                        "event_time_text": None,
                        "places": [],
                        "persons": [],
                        "users": [],
                    }
                jp_rows = [dict(r) for r in s.run(q_evt_journal_people, key=key)]
                entries = [dict(r) for r in s.run(q_entries, key=key, limit=int(limit))]

            persons = self._merge_person_rows_by_id(participants.get("persons") or [], jp_rows)
            first_e = entries[0] if entries else {}

            # Flatten a bit for the UI
            return {
                "kind": "Event",
                "ref": ref,
                "activity_name": participants.get("activity_name") or "",
                "summary_preview": str(first_e.get("text_preview") or "").strip(),
                "event_type": participants.get("event_type") or "",
                "day": participants.get("day") or "",
                "event_time_iso": participants.get("event_time_iso") or "",
                "event_time_text": participants.get("event_time_text") or "",
                "places": participants.get("places") or [],
                "persons": persons,
                "users": participants.get("users") or [],
                "entries": entries,
            }

        if label == "E73_Information_Object":
            q_ctx = """
            MATCH (ctx:E73_Information_Object)
            WHERE ctx.id = $key OR ctx.key = $key
            OPTIONAL MATCH (ctx)-[:P67_refers_to {ref_type: 'context_of'}]->(ev:E7_Activity)
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
            WITH ctx,
                 coalesce(t.name, ev.event_type, '') as event_type,
                 d.date as day,
                 substring(coalesce(ctx.text, ctx.content, ''), 0, 120) as context_preview
            RETURN event_type as event_type,
                   day as day,
                   ctx.name as name,
                   coalesce(ctx.text, ctx.content, '') as text_preview_long,
                   context_preview as context_preview,
                   coalesce(ctx.key, ctx.id, '') as ckey,
                   coalesce(ctx.entry_kind, '') as entry_kind
            LIMIT 1
            """

            q_entries = """
            MATCH (ctx:E73_Information_Object)
            WHERE ctx.id = $key OR ctx.key = $key
            MATCH (ctx)-[:P67_refers_to {ref_type: 'context_of'}]->(ev:E7_Activity)<-[:P67_refers_to]-(e:E73_Information_Object)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            RETURN e.id as entry_id,
                   toString(e.input_time) as input_time,
                   d.date as day,
                   substring(e.text, 0, 260) as text_preview
            ORDER BY e.input_time DESC
            LIMIT $limit
            """

            # All explorable P67 targets (any ref_type) plus cast/places on linked activities.
            q_links = """
            MATCH (ctx:E73_Information_Object)
            WHERE ctx.id = $key OR ctx.key = $key
            CALL {
              WITH ctx
              MATCH (ctx)-[r:P67_refers_to]->(n)
              WHERE n <> ctx AND n:E21_Person AND n.id IS NOT NULL
              RETURN coalesce(r.ref_type, '') AS ref_type, 'person' AS bucket, ('E21_Person:' + n.id) AS explore_ref,
                     coalesce(n.name, n.id) AS display_name, 'direct' AS src
              UNION
              WITH ctx
              MATCH (ctx)-[r:P67_refers_to]->(n)
              WHERE n:E7_Activity AND n.key IS NOT NULL
              RETURN coalesce(r.ref_type, '') AS ref_type, 'situation' AS bucket, ('Event:' + n.key) AS explore_ref,
                     coalesce(n.name, n.event_type, n.key) AS display_name, 'direct' AS src
              UNION
              WITH ctx
              MATCH (ctx)-[r:P67_refers_to]->(n)
              WHERE n:E53_Place AND n.name IS NOT NULL
              RETURN coalesce(r.ref_type, '') AS ref_type, 'place' AS bucket, ('E53_Place:' + n.name) AS explore_ref,
                     n.name AS display_name, 'direct' AS src
              UNION
              WITH ctx
              MATCH (ctx)-[r:P67_refers_to]->(n)
              WHERE n:E28_Conceptual_Object AND n.name IS NOT NULL
              RETURN coalesce(r.ref_type, '') AS ref_type, 'idea' AS bucket, ('E28_Conceptual_Object:' + n.name) AS explore_ref,
                     n.name AS display_name, 'direct' AS src
              UNION
              WITH ctx
              MATCH (ctx)-[r:P67_refers_to]->(n)
              WHERE n:E74_Group AND n.name IS NOT NULL
              RETURN coalesce(r.ref_type, '') AS ref_type, 'group' AS bucket, ('E74_Group:' + n.name) AS explore_ref,
                     n.name AS display_name, 'direct' AS src
              UNION
              WITH ctx
              MATCH (ctx)-[r:P67_refers_to]->(n)
              WHERE n:E55_Type AND n.name IS NOT NULL
              RETURN coalesce(r.ref_type, '') AS ref_type, 'tag' AS bucket, ('E55_Type:' + n.name) AS explore_ref,
                     n.name AS display_name, 'direct' AS src
              UNION
              WITH ctx
              MATCH (ctx)-[r:P67_refers_to]->(n)
              WHERE n:E52_Time_Span
              RETURN coalesce(r.ref_type, '') AS ref_type, 'day' AS bucket,
                     ('E52_Time_Span:' + toString(coalesce(n.key, n.date))) AS explore_ref,
                     toString(coalesce(n.date, n.key)) AS display_name, 'direct' AS src
              UNION
              WITH ctx
              MATCH (ctx)-[:P67_refers_to]->(ev:E7_Activity)
              MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
              WHERE p.id IS NOT NULL
              RETURN 'via_activity' AS ref_type, 'person' AS bucket, ('E21_Person:' + p.id) AS explore_ref,
                     coalesce(p.name, p.id) AS display_name, 'situation' AS src
              UNION
              WITH ctx
              MATCH (ctx)-[:P67_refers_to]->(ev:E7_Activity)
              MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
              WHERE pl.name IS NOT NULL
              RETURN 'via_activity' AS ref_type, 'place' AS bucket, ('E53_Place:' + pl.name) AS explore_ref,
                     pl.name AS display_name, 'situation' AS src
            }
            RETURN ref_type, bucket, explore_ref, display_name, src
            """

            with self._driver.session() as s:
                row = s.run(q_ctx, key=key).single()
                link_rows = [dict(r) for r in s.run(q_links, key=key)]
                entries = [dict(r) for r in s.run(q_entries, key=key, limit=int(limit))]
                ctx_row = dict(row) if row else {}
                linked = self._journal_explore_links_from_rows(link_rows)

            body = ctx_row.get("text_preview_long") or ctx_row.get("context_preview") or ""
            ek = str(ctx_row.get("entry_kind") or "").strip()
            # Legacy shape: narrow ref_type buckets (still used if linked is empty elsewhere).
            topics = [{"type": "E28_Conceptual_Object", "name": x["name"]} for x in linked if x["bucket"] == "idea" and x["ref_type"] == "topic"]
            concepts = [{"type": "E28_Conceptual_Object", "name": x["name"]} for x in linked if x["bucket"] == "idea" and x["ref_type"] == "context"]
            mentions = [{"type": "E21_Person", "name": x["name"]} for x in linked if x["bucket"] == "person" and x["ref_type"] == "mention"]

            return {
                "kind": "E73_Information_Object",
                "ref": ref,
                "name": ctx_row.get("name") or ("Journal" if ek == "journal_entry" else "Context"),
                "event_type": ctx_row.get("event_type") or "",
                "day": ctx_row.get("day") or "",
                "text": body,
                "entry_kind": ek,
                "linked": linked,
                "topics": topics,
                "concepts": concepts,
                "mentions": mentions,
                "entries": entries,
            }

        if label == "E28_Conceptual_Object":
            # Concept: journal—P67→concept (topics from extraction) or concept←P67←activity←P67←journal.
            q_participants = """
            MATCH (c:E28_Conceptual_Object {name: $name})
            CALL {
              WITH c
              MATCH (journal:E73_Information_Object)-[:P67_refers_to]->(c)
              WHERE coalesce(journal.entry_kind,'') = 'journal_entry'
              MATCH (journal)-[:P67_refers_to]->(p:E21_Person)
              RETURN p
              UNION
              WITH c
              MATCH (c)<-[:P67_refers_to]-(ev:E7_Activity)
              MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
              RETURN p
            }
            WITH collect(DISTINCT {id: p.id, name: p.name, role: p.role, mentions: coalesce(p.mention_count,0)}) as raw
            RETURN [x IN raw WHERE x.id IS NOT NULL] as persons,
                   [] as users
            """
            q_entries = """
            MATCH (c:E28_Conceptual_Object {name: $name})
            CALL {
              WITH c
              MATCH (journal:E73_Information_Object)-[:P67_refers_to]->(c)
              WHERE coalesce(journal.entry_kind,'') = 'journal_entry'
              RETURN journal
              UNION
              WITH c
              MATCH (c)<-[:P67_refers_to]-(ev:E7_Activity)
              MATCH (journal:E73_Information_Object)-[:P67_refers_to]->(ev)
              WHERE coalesce(journal.entry_kind,'') = 'journal_entry'
              RETURN journal
            }
            WITH DISTINCT journal
            OPTIONAL MATCH (journal)-[:P67_refers_to]->(ev:E7_Activity)
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            OPTIONAL MATCH (ev)-[:P7_took_place_at]->(pl:E53_Place)
            OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
            WITH journal,
                 collect(DISTINCT d.date) as days,
                 collect(DISTINCT coalesce(t.name, ev.event_type, '')) as event_types,
                 collect(DISTINCT pl.name) as places
            RETURN journal.id as entry_id,
                   toString(journal.input_time) as input_time,
                   days[0] as day,
                   event_types[0] as event_type,
                   places[0..3] as places,
                   substring(journal.text, 0, 260) as text_preview
            ORDER BY journal.input_time DESC
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
            return self._overview_day(ref=ref, day_key=key, limit=limit, focus=focus)

        if label == "E53_Place":
            # Treat "place occurrences" as an Event-like overview so the UI can reuse
            # the same rendering (participants + entries list).
            # Include (1) journals via Activity P7 and (2) journals that P67→place (e.g. remote_context).
            q_entries = """
            MATCH (pl:E53_Place)
            WHERE pl.name = $name OR toLower(toString(pl.name)) = toLower($name)
            WITH pl ORDER BY CASE WHEN pl.name = $name THEN 0 ELSE 1 END, pl.name LIMIT 1
            CALL {
              WITH pl
              MATCH (pl)<-[:P7_took_place_at]-(ev:E7_Activity)<-[:P67_refers_to]-(e:E73_Information_Object)
              WHERE coalesce(e.entry_kind,'') = 'journal_entry'
              OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
              OPTIONAL MATCH (ev)-[:P2_has_type]->(t:E55_Type)
              WITH e,
                   collect(DISTINCT d.date) AS days,
                   collect(DISTINCT coalesce(t.name, ev.event_type, '')) AS event_types
              RETURN e,
                     days[0] AS day,
                     event_types[0] AS event_type
              UNION
              WITH pl
              MATCH (e:E73_Information_Object)-[:P67_refers_to]->(pl)
              WHERE coalesce(e.entry_kind,'') = 'journal_entry'
              RETURN e,
                     coalesce(e.day, '') AS day,
                     'mentioned in note' AS event_type
            }
            WITH DISTINCT e, day, event_type
            ORDER BY e.input_time DESC
            LIMIT $limit
            RETURN e.id AS entry_id,
                   toString(e.input_time) AS input_time,
                   day,
                   event_type,
                   substring(e.text, 0, 260) AS text_preview
            """
            # Cast may be E21_Person and/or E39_Actor (pipeline merges both). Also pick up people
            # the journal links (P67) even when the activity has no P14 row yet.
            q_cast = """
            MATCH (pl:E53_Place)
            WHERE pl.name = $name OR toLower(toString(pl.name)) = toLower($name)
            WITH pl ORDER BY CASE WHEN pl.name = $name THEN 0 ELSE 1 END, pl.name LIMIT 1
            MATCH (pl)<-[:P7_took_place_at]-(ev:E7_Activity)
            MATCH (ev)-[:P14_carried_out_by]->(n)
            WHERE n:E21_Person OR n:E39_Actor
            RETURN DISTINCT n.id AS id,
                   coalesce(n.name, n.id) AS name,
                   n.role AS role,
                   coalesce(n.mention_count, 0) AS mentions
            """
            q_journal_people = """
            MATCH (pl:E53_Place)
            WHERE pl.name = $name OR toLower(toString(pl.name)) = toLower($name)
            WITH pl ORDER BY CASE WHEN pl.name = $name THEN 0 ELSE 1 END, pl.name LIMIT 1
            MATCH (pl)<-[:P7_took_place_at]-(ev:E7_Activity)
            MATCH (ev)<-[:P67_refers_to]-(e:E73_Information_Object)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            MATCH (e)-[:P67_refers_to]->(p:E21_Person)
            RETURN DISTINCT p.id AS id,
                   coalesce(p.name, p.id) AS name,
                   p.role AS role,
                   coalesce(p.mention_count, 0) AS mentions
            """
            q_direct_journal_people = """
            MATCH (pl:E53_Place)
            WHERE pl.name = $name OR toLower(toString(pl.name)) = toLower($name)
            WITH pl ORDER BY CASE WHEN pl.name = $name THEN 0 ELSE 1 END, pl.name LIMIT 1
            MATCH (e:E73_Information_Object)-[:P67_refers_to]->(pl)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            MATCH (e)-[:P67_refers_to]->(p:E21_Person)
            RETURN DISTINCT p.id AS id,
                   coalesce(p.name, p.id) AS name,
                   p.role AS role,
                   coalesce(p.mention_count, 0) AS mentions
            """
            q_place_headline = """
            MATCH (pl:E53_Place)
            WHERE pl.name = $name OR toLower(toString(pl.name)) = toLower($name)
            WITH pl ORDER BY CASE WHEN pl.name = $name THEN 0 ELSE 1 END, pl.name LIMIT 1
            MATCH (pl)<-[:P7_took_place_at]-(ev:E7_Activity)
            RETURN coalesce(ev.name, '') AS ev_name,
                   coalesce(ev.event_type, '') AS ev_type,
                   coalesce(ev.key, '') AS ev_key
            ORDER BY coalesce(ev.last_seen, datetime('1970-01-01T00:00:00Z')) DESC
            LIMIT 1
            """

            with self._driver.session() as s:
                entries = [dict(r) for r in s.run(q_entries, name=key, limit=int(limit))]
                cast_rows = [dict(r) for r in s.run(q_cast, name=key)]
                note_rows = [dict(r) for r in s.run(q_journal_people, name=key)]
                direct_rows = [dict(r) for r in s.run(q_direct_journal_people, name=key)]
                persons = self._merge_person_rows_by_id(cast_rows, note_rows, direct_rows)
                hrow = s.run(q_place_headline, name=key).single()
                hdict = dict(hrow) if hrow else {}

            first = entries[0] if entries else {}
            evn = str(hdict.get("ev_name") or "").strip()
            evt = str(hdict.get("ev_type") or "").strip()
            evk = str(hdict.get("ev_key") or "").strip()
            junk = {"activity", "event", "misc"}
            headline = ""
            if evn and evn.lower() not in junk:
                headline = evn
            elif evt and evt.lower() not in junk:
                headline = evt
            elif evk:
                headline = evk[:56] + ("…" if len(evk) > 56 else "")
            if not headline:
                ft = str(first.get("event_type") or "").strip()
                if ft and ft.lower() not in junk:
                    headline = ft
            activity_name = f"{key}" if not headline else f"{key} · {headline}"
            summary_preview = str(first.get("text_preview") or "").strip()

            return {
                "kind": "Event",
                "ref": ref,
                "activity_name": activity_name,
                "summary_preview": summary_preview,
                "event_type": first.get("event_type") or "",
                "day": first.get("day") or "",
                "places": [key],
                "persons": persons,
                "users": [],
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
            # feelings: P141_assigned only (avoid P2 on E13 mixing category vs value).
            # activity_type: situations (E7) classified with this tag via P2_has_type, with a journal entry P67→ev.
            fo = (focus or "feelings").strip().lower()
            if fo not in ("feelings", "activity_type"):
                fo = "feelings"

            q_feeling = """
            MATCH (tag:E55_Type)
            WHERE tag.name = $tag_name OR toLower(tag.name) = toLower($tag_name)
            WITH tag ORDER BY CASE WHEN tag.name = $tag_name THEN 0 ELSE 1 END, tag.name
            LIMIT 1
            MATCH (j:E73_Information_Object)-[:P67_refers_to]->(a:E13_Attribute_Assignment)
            WHERE coalesce(j.entry_kind,'') = 'journal_entry'
              AND (a)-[:P141_assigned]->(tag)
              AND (
                $pid = ''
                OR (a)-[:P140_assigned_attribute_to]->(:E21_Person {id: $pid})
                OR EXISTS {
                  MATCH (a)-[:P15_was_influenced_by]->(:E7_Activity)-[:P14_carried_out_by]->(:E21_Person {id: $pid})
                }
              )
            WITH DISTINCT j, a, tag
            OPTIONAL MATCH (a)-[:P15_was_influenced_by]->(ev:E7_Activity)
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            OPTIONAL MATCH (ev)-[:P2_has_type]->(et:E55_Type)
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
            WITH j, a, tag, ev, d, et,
                 collect(DISTINCT {id: p.id, name: p.name, role: p.role}) as raw_persons
            ORDER BY j.input_time DESC
            LIMIT $limit
            RETURN tag.name as tag_resolved_name,
                   coalesce(a.key, '') as assignment_key,
                   coalesce(a.name, tag.name) as assignment_label,
                   toString(j.input_time) as input_time,
                   toString(date(j.input_time)) as day,
                   coalesce(j.id, '') as entry_id,
                   substring(coalesce(j.text, ''), 0, 140) as entry_preview,
                   coalesce(ev.key, '') as event_key,
                   coalesce(ev.name, '') as activity_name,
                   coalesce(et.name, ev.event_type, '') as activity_kind,
                   toString(d.date) as activity_day,
                   [x IN raw_persons WHERE x.id IS NOT NULL] as persons
            """
            q_activity_type = """
            MATCH (tag:E55_Type)
            WHERE tag.name = $tag_name OR toLower(tag.name) = toLower($tag_name)
            WITH tag ORDER BY CASE WHEN tag.name = $tag_name THEN 0 ELSE 1 END, tag.name
            LIMIT 1
            MATCH (ev:E7_Activity)-[:P2_has_type]->(tag)
            MATCH (j:E73_Information_Object)-[:P67_refers_to]->(ev)
            WHERE coalesce(j.entry_kind,'') = 'journal_entry'
              AND (
                $pid = ''
                OR (ev)-[:P14_carried_out_by]->(:E21_Person {id: $pid})
              )
            WITH DISTINCT j, ev, tag
            OPTIONAL MATCH (ev)-[:P4_has_time_span]->(d:E52_Time_Span)
            OPTIONAL MATCH (ev)-[:P2_has_type]->(et:E55_Type)
            OPTIONAL MATCH (ev)-[:P14_carried_out_by]->(p:E21_Person)
            WITH j, ev, tag, d, et,
                 collect(DISTINCT {id: p.id, name: p.name, role: p.role}) as raw_persons
            ORDER BY j.input_time DESC
            LIMIT $limit
            RETURN tag.name as tag_resolved_name,
                   coalesce(ev.key, '') + '|' + coalesce(j.id, '') as assignment_key,
                   coalesce(ev.name, ev.event_type, tag.name) as assignment_label,
                   toString(j.input_time) as input_time,
                   toString(date(j.input_time)) as day,
                   coalesce(j.id, '') as entry_id,
                   substring(coalesce(j.text, ''), 0, 140) as entry_preview,
                   coalesce(ev.key, '') as event_key,
                   coalesce(ev.name, '') as activity_name,
                   coalesce(et.name, ev.event_type, tag.name) as activity_kind,
                   toString(d.date) as activity_day,
                   [x IN raw_persons WHERE x.id IS NOT NULL] as persons
            """
            q_use = q_activity_type if fo == "activity_type" else q_feeling
            with self._driver.session() as s:
                rows = [dict(r) for r in s.run(q_use, tag_name=key, pid=anchor_pid, limit=int(limit))]

            occurrences: List[Dict[str, Any]] = []
            resolved_tag = str(rows[0].get("tag_resolved_name") or key) if rows else key
            for r in rows:
                persons = r.get("persons") or []
                if isinstance(persons, list):
                    persons_out = [dict(p) for p in persons if isinstance(p, dict) and p.get("id")]
                else:
                    persons_out = []
                occurrences.append(
                    {
                        "assignment_key": str(r.get("assignment_key") or ""),
                        "assignment_label": str(r.get("assignment_label") or key),
                        "input_time": str(r.get("input_time") or ""),
                        "day": str(r.get("day") or ""),
                        "entry_id": str(r.get("entry_id") or ""),
                        "entry_preview": str(r.get("entry_preview") or ""),
                        "event_key": str(r.get("event_key") or ""),
                        "activity_name": str(r.get("activity_name") or ""),
                        "activity_kind": str(r.get("activity_kind") or ""),
                        "activity_day": str(r.get("activity_day") or ""),
                        "persons": persons_out,
                    }
                )

            out: Dict[str, Any] = {
                "kind": "Feeling",
                "ref": ref,
                "name": resolved_tag,
                "occurrences": occurrences,
            }
            if anchor_pid:
                out["anchor_person_id"] = anchor_pid
                out["anchor_person_name"] = anchor_display or anchor_pid
                out["anchor_person_ref"] = f"E21_Person:{anchor_pid}"
            return out

        if label == "E74_Group":
            # Organizations are linked from journal entries via P67 (same as places/topics in prep).
            q_gname = """
            MATCH (g:E74_Group)
            WHERE g.name = $name OR toLower(toString(g.name)) = toLower($name)
            WITH g ORDER BY CASE WHEN g.name = $name THEN 0 ELSE 1 END, g.name LIMIT 1
            RETURN coalesce(g.name, $name) AS gn
            """
            q_entries = """
            MATCH (g:E74_Group)
            WHERE g.name = $name OR toLower(toString(g.name)) = toLower($name)
            WITH g ORDER BY CASE WHEN g.name = $name THEN 0 ELSE 1 END, g.name LIMIT 1
            MATCH (e:E73_Information_Object)-[:P67_refers_to]->(g)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            RETURN e.id AS entry_id,
                   toString(e.input_time) AS input_time,
                   coalesce(e.day, '') AS day,
                   'mentioned in note' AS event_type,
                   substring(e.text, 0, 260) AS text_preview
            ORDER BY e.input_time DESC
            LIMIT $limit
            """
            q_journal_people = """
            MATCH (g:E74_Group)
            WHERE g.name = $name OR toLower(toString(g.name)) = toLower($name)
            WITH g ORDER BY CASE WHEN g.name = $name THEN 0 ELSE 1 END, g.name LIMIT 1
            MATCH (e:E73_Information_Object)-[:P67_refers_to]->(g)
            WHERE coalesce(e.entry_kind,'') = 'journal_entry'
            MATCH (e)-[:P67_refers_to]->(p:E21_Person)
            RETURN DISTINCT p.id AS id,
                   coalesce(p.name, p.id) AS name,
                   p.role AS role,
                   coalesce(p.mention_count, 0) AS mentions
            """
            with self._driver.session() as s:
                gr = s.run(q_gname, name=key).single()
                gname = str((gr.get("gn") or key)).strip() if gr else key
                entries = [dict(r) for r in s.run(q_entries, name=key, limit=int(limit))]
                persons = [dict(r) for r in s.run(q_journal_people, name=key)]
            first = entries[0] if entries else {}
            summary_preview = str(first.get("text_preview") or "").strip()
            activity_name = f"{gname} · mentioned in your notes" if summary_preview else gname
            return {
                "kind": "Event",
                "ref": ref,
                "activity_name": activity_name,
                "summary_preview": summary_preview,
                "event_type": first.get("event_type") or "",
                "day": first.get("day") or "",
                "places": [],
                "persons": persons,
                "users": [],
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
                      AND NOT (p)-[:P2_has_type]->(:E55_Type {{name:'User'}})
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

        # Emerging support: low-signal people with positive early trend.
        emerging_support = [
            {
                "person": p["person"],
                "net_score": p["net_score"],
                "signals": p["sample_size"],
            }
            for p in people_impact
            if p["label"] == "Uncertain" and p["net_score"] > 0
        ]
        emerging_support.sort(key=lambda x: (x["net_score"], x["signals"]), reverse=True)
        emerging_support = emerging_support[:5]

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
            "emerging_support": emerging_support,
            "open_obligations": {
                "custody_open": custody_rows,
                "expectations_open": expectation_rows,
            },
            "weekly_recommendations": recs,
        }

    def insights_person_detail(self, person_name: str, days: int = 30, limit: int = 40) -> Dict[str, Any]:
        person_name = (person_name or "").strip()
        if not person_name:
            return {
                "person": "",
                "window_days": 0,
                "counts": {"positive": 0, "negative": 0, "neutral": 0, "signals_total": 0},
                "net_score": 0.0,
                "label": "Uncertain",
                "confidence": 0.0,
                "formula": "net_score = (positive - negative) / max(1, signals_total)",
                "signals_per_day": [],
                "evidence": [],
            }

        days = max(7, min(int(days), 365))
        limit = max(10, min(int(limit), 200))
        since_expr = f"datetime() - duration('P{days}D')"

        aggregate_rows: List[Dict[str, Any]] = []
        day_rows: List[Dict[str, Any]] = []
        evidence_rows: List[Dict[str, Any]] = []
        with self._driver.session() as s:
            aggregate_rows = [
                dict(r)
                for r in s.run(
                    f"""
                    MATCH (j:E73_Information_Object)-[:P67_refers_to]->(a:E13_Attribute_Assignment)-[:P15_was_influenced_by]->(ev:E7_Activity)-[:P14_carried_out_by]->(p:E21_Person)
                    WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                      AND j.input_time >= {since_expr}
                      AND toLower(coalesce(p.name,'')) = toLower($person)
                    OPTIONAL MATCH (a)-[:P141_assigned]->(t1:E55_Type)
                    OPTIONAL MATCH (a)-[:P2_has_type]->(t2:E55_Type)
                    RETURN coalesce(toLower(t1.name), toLower(t2.name), toLower(a.name), '') as tag,
                           count(*) as c
                    """,
                    person=person_name,
                )
            ]

            day_rows = [
                dict(r)
                for r in s.run(
                    f"""
                    MATCH (j:E73_Information_Object)-[:P67_refers_to]->(a:E13_Attribute_Assignment)-[:P15_was_influenced_by]->(ev:E7_Activity)-[:P14_carried_out_by]->(p:E21_Person)
                    WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                      AND j.input_time >= {since_expr}
                      AND toLower(coalesce(p.name,'')) = toLower($person)
                    OPTIONAL MATCH (a)-[:P141_assigned]->(t1:E55_Type)
                    OPTIONAL MATCH (a)-[:P2_has_type]->(t2:E55_Type)
                    WITH toString(date(j.input_time)) as day,
                         coalesce(toLower(t1.name), toLower(t2.name), toLower(a.name), '') as tag
                    RETURN day, tag, count(*) as c
                    ORDER BY day ASC
                    """,
                    person=person_name,
                )
            ]

            evidence_rows = [
                dict(r)
                for r in s.run(
                    f"""
                    MATCH (j:E73_Information_Object)-[:P67_refers_to]->(a:E13_Attribute_Assignment)-[:P15_was_influenced_by]->(ev:E7_Activity)-[:P14_carried_out_by]->(p:E21_Person)
                    WHERE coalesce(j.entry_kind,'') = 'journal_entry'
                      AND j.input_time >= {since_expr}
                      AND toLower(coalesce(p.name,'')) = toLower($person)
                    OPTIONAL MATCH (a)-[:P141_assigned]->(t1:E55_Type)
                    OPTIONAL MATCH (a)-[:P2_has_type]->(t2:E55_Type)
                    RETURN coalesce(j.id, '') as entry_id,
                           toString(j.input_time) as input_time,
                           toString(date(j.input_time)) as day,
                           coalesce(toLower(t1.name), toLower(t2.name), toLower(a.name), '') as tag,
                           coalesce(a.name, '') as assignment_name,
                           coalesce(ev.name, '') as event_name,
                           coalesce(ev.key, '') as event_key,
                           substring(coalesce(j.text,''), 0, 220) as text_preview
                    ORDER BY j.input_time DESC
                    LIMIT $limit
                    """,
                    person=person_name,
                    limit=limit,
                )
            ]

        positive = 0
        negative = 0
        neutral = 0
        for r in aggregate_rows:
            tag = str(r.get("tag") or "")
            c = int(r.get("c") or 0)
            pol = self._emotion_polarity(tag)
            if pol == "positive":
                positive += c
            elif pol == "negative":
                negative += c
            else:
                neutral += c

        signals_total = positive + negative + neutral
        denom = max(1, signals_total)
        net = (positive - negative) / float(denom)
        if signals_total < 2:
            label = "Uncertain"
        elif net >= 0.25:
            label = "Supportive"
        elif net <= -0.25:
            label = "Draining"
        else:
            label = "Mixed"

        by_day: Dict[str, Dict[str, int]] = {}
        for r in day_rows:
            d = str(r.get("day") or "")
            if not d:
                continue
            tag = str(r.get("tag") or "")
            c = int(r.get("c") or 0)
            pol = self._emotion_polarity(tag)
            if d not in by_day:
                by_day[d] = {"positive": 0, "negative": 0, "neutral": 0}
            by_day[d][pol] += c

        end_d = date.today()
        start_d = end_d - timedelta(days=days - 1)
        signals_per_day: List[Dict[str, Any]] = []
        cur = start_d
        while cur <= end_d:
            ds = cur.isoformat()
            vals = by_day.get(ds, {"positive": 0, "negative": 0, "neutral": 0})
            signals_per_day.append({"day": ds, **vals})
            cur += timedelta(days=1)

        evidence: List[Dict[str, Any]] = []
        for r in evidence_rows:
            tag = str(r.get("tag") or "")
            evidence.append(
                {
                    "entry_id": str(r.get("entry_id") or ""),
                    "input_time": str(r.get("input_time") or ""),
                    "day": str(r.get("day") or ""),
                    "tag": tag,
                    "polarity": self._emotion_polarity(tag),
                    "assignment_name": str(r.get("assignment_name") or ""),
                    "event_name": str(r.get("event_name") or ""),
                    "event_key": str(r.get("event_key") or ""),
                    "text_preview": str(r.get("text_preview") or ""),
                }
            )

        confidence = min(1.0, signals_total / 6.0)
        return {
            "person": person_name,
            "window_days": days,
            "counts": {
                "positive": positive,
                "negative": negative,
                "neutral": neutral,
                "signals_total": signals_total,
            },
            "net_score": round(net, 3),
            "label": label,
            "confidence": round(confidence, 3),
            "formula": "net_score = (positive - negative) / max(1, signals_total)",
            "signals_per_day": signals_per_day,
            "evidence": evidence,
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
            "E74_Group": "name",
            "E13_Attribute_Assignment": "key",
        }
        if label not in mapping:
            raise ValueError("unsupported label for neighborhood")
        return mapping[label]

