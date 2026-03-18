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

    def timeline(self, limit: int = 50) -> List[Dict[str, Any]]:
        q = """
        MATCH (e:Entry)-[:REFERS_TO]->(ev:Event)
        OPTIONAL MATCH (ev)-[:ON_DAY]->(d:Day)
        RETURN e.id as id,
               e.text as text,
               toString(e.input_time) as input_time,
               ev.key as event_key,
               d.date as day
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
        }
        if label not in mapping:
            raise ValueError("unsupported label for neighborhood")
        return mapping[label]

