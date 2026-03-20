"""Generic CIDOC CRM Graph Writer.

Executes a graph spec (nodes + edges) produced by the Modeling Agent.
No hardcoded semantic logic — all intelligence is in the Modeling Agent.
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

VALID_LABELS = {
    "E5_Event",
    "E7_Activity",
    "E10_Transfer_of_Custody",
    "E13_Attribute_Assignment",
    "E21_Person",
    "E22_Human_Made_Object",
    "E28_Conceptual_Object",
    "E39_Actor",
    "E52_Time_Span",
    "E53_Place",
    "E55_Type",
    "E73_Information_Object",
    "E74_Group",
    "E89_Propositional_Object",
}

VALID_PROPERTIES = {
    "P2_has_type",
    "P4_has_time_span",
    "P7_took_place_at",
    "P14_carried_out_by",
    "P14i_performed",
    "P15_was_influenced_by",
    "P17_was_motivated_by",
    "P28_custody_surrendered_by",
    "P29_custody_received_by",
    "P30_transferred_custody_of",
    "P67_refers_to",
    "P120_occurs_before",
    "P140_assigned_attribute_to",
    "P141_assigned",
}

MULTI_LABEL_MAP = {
    "E7_Activity": "E7_Activity:E5_Event",
    "E10_Transfer_of_Custody": "E10_Transfer_of_Custody:E7_Activity:E5_Event",
    "E21_Person": "E21_Person:E39_Actor",
}


class GraphWriter:
    """Writes a CIDOC CRM graph spec to Neo4j."""

    def __init__(self, driver):
        self.driver = driver

    def write(
        self,
        spec: Dict[str, Any],
        entry_id: str,
        raw_text: str,
        user_name: str = "",
        day_bucket: str = "",
        input_ts: Optional[str] = None,
    ) -> None:
        if not spec or (not spec.get("nodes") and not spec.get("edges")):
            return
        ts = input_ts or datetime.now().astimezone().isoformat()

        with self.driver.session() as session:
            session.execute_write(
                self._write_tx,
                spec=spec,
                entry_id=entry_id,
                raw_text=raw_text,
                user_name=user_name,
                day_bucket=day_bucket,
                ts=ts,
            )

    @staticmethod
    def _write_tx(
        tx,
        spec: Dict[str, Any],
        entry_id: str,
        raw_text: str,
        user_name: str,
        day_bucket: str,
        ts: str,
    ) -> None:
        nodes: List[Dict] = spec.get("nodes", [])
        edges: List[Dict] = spec.get("edges", [])

        short_name = raw_text[:60].strip()
        if len(raw_text) > 60:
            short_name += "..."
        tx.run(
            """
            MERGE (j:E73_Information_Object {id: $id})
            SET j.text = $text,
                j.input_time = datetime($ts),
                j.entry_kind = 'journal_entry',
                j.name = $short_name
            """,
            id=entry_id,
            text=raw_text[:5000],
            ts=ts,
            short_name=short_name,
        )

        if day_bucket:
            tx.run(
                """
                MERGE (d:E52_Time_Span {key: $day})
                SET d.date = $day, d.name = $day
                """,
                day=day_bucket,
            )

        id_to_key: Dict[str, str] = {}

        for node in nodes:
            if not isinstance(node, dict):
                continue
            nid = str(node.get("id", ""))
            label = str(node.get("label", ""))
            name = str(node.get("name", ""))
            types = node.get("types", [])
            props = node.get("properties", {})
            if not isinstance(props, dict):
                props = {}
            if not nid or not label or not name:
                continue
            if label not in VALID_LABELS:
                logger.warning("Unknown label %s for node %s, skipping", label, nid)
                continue

            neo_label = MULTI_LABEL_MAP.get(label, label)
            key = f"{entry_id}|{nid}"
            id_to_key[nid] = key

            prop_sets = []
            prop_params: Dict[str, Any] = {"key": key, "name": name, "ts": ts}

            if "event_time_iso" in props:
                prop_sets.append("n.event_time_iso = $eti")
                prop_params["eti"] = str(props["event_time_iso"])
            if "event_time_text" in props:
                prop_sets.append("n.event_time_text = $ett")
                prop_params["ett"] = str(props["event_time_text"])

            extra = (", " + ", ".join(prop_sets)) if prop_sets else ""

            tx.run(
                f"""
                MERGE (n:{neo_label} {{key: $key}})
                ON CREATE SET n.first_seen = datetime($ts)
                SET n.last_seen = datetime($ts),
                    n.name = $name{extra}
                """,
                **prop_params,
            )

            if isinstance(types, list):
                for t in types:
                    tname = str(t or "").strip()
                    if not tname:
                        continue
                    tx.run(
                        """
                        MATCH (n {key: $key})
                        MERGE (t:E55_Type {name: $tname})
                        MERGE (n)-[:P2_has_type]->(t)
                        """,
                        key=key,
                        tname=tname,
                    )

            is_activity = label in (
                "E7_Activity",
                "E10_Transfer_of_Custody",
            ) or "E7_Activity" in neo_label or "E10_Transfer_of_Custody" in neo_label
            if day_bucket and is_activity:
                tx.run(
                    """
                    MATCH (n {key: $key})
                    MATCH (d:E52_Time_Span {key: $day})
                    MERGE (n)-[:P4_has_time_span]->(d)
                    """,
                    key=key,
                    day=day_bucket,
                )

            tx.run(
                """
                MATCH (j:E73_Information_Object {id: $entry_id})
                MATCH (n {key: $key})
                MERGE (j)-[:P67_refers_to {ref_type: 'about'}]->(n)
                """,
                entry_id=entry_id,
                key=key,
            )

        for edge in edges:
            if not isinstance(edge, dict):
                continue
            from_id = str(edge.get("from", ""))
            to_id = str(edge.get("to", ""))
            prop = str(edge.get("property", ""))
            eprops = edge.get("properties", {})
            if not isinstance(eprops, dict):
                eprops = {}

            if not from_id or not to_id or not prop:
                continue
            if prop not in VALID_PROPERTIES:
                logger.warning("Unknown property %s, skipping edge", prop)
                continue

            from_key = id_to_key.get(from_id)
            to_key = id_to_key.get(to_id)
            if not from_key or not to_key:
                continue

            ref_type = str(eprops.get("ref_type", ""))
            if ref_type:
                tx.run(
                    f"""
                    MATCH (a {{key: $fk}})
                    MATCH (b {{key: $tk}})
                    MERGE (a)-[r:{prop} {{ref_type: $rt}}]->(b)
                    """,
                    fk=from_key,
                    tk=to_key,
                    rt=ref_type,
                )
            else:
                tx.run(
                    f"""
                    MATCH (a {{key: $fk}})
                    MATCH (b {{key: $tk}})
                    MERGE (a)-[:{prop}]->(b)
                    """,
                    fk=from_key,
                    tk=to_key,
                )

        if user_name:
            tx.run(
                """
                MERGE (u:E21_Person:E39_Actor {name: $name})
                ON CREATE SET u.first_seen = datetime($ts)
                SET u.last_seen = datetime($ts)
                WITH u
                MERGE (ut:E55_Type {name: 'User'})
                MERGE (u)-[:P2_has_type]->(ut)
                """,
                name=user_name,
                ts=ts,
            )
