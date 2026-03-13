"""Neo4j graph storage for entities and journal entries."""
from datetime import datetime
from typing import List, Optional

from neo4j import GraphDatabase

from .extractor import ExtractionResult, ExtractedEntity
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


class GraphStore:
    """Store and query entities in Neo4j."""
    
    def __init__(
        self,
        uri: str = NEO4J_URI,
        user: str = NEO4J_USER,
        password: str = NEO4J_PASSWORD,
    ):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        # Initialize schema (constraints/indexes) in a dedicated transaction.
        # Neo4j does not allow mixing schema modifications with data writes
        # in the same transaction, so we run this once here.
        with self.driver.session() as session:
            session.execute_write(self._init_schema)
    
    def close(self):
        self.driver.close()
    
    def _init_schema(self, tx):
        """Create constraints and indexes if they don't exist."""
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
    
    def store_entry(
        self,
        entry_id: str,
        text: str,
        extraction: ExtractionResult,
        timestamp: Optional[datetime] = None,
    ) -> str:
        """
        Store a journal entry and its extracted entities in the graph.
        Creates JournalEntry node, entity nodes, and MENTIONS relationships.
        """
        ts = timestamp or datetime.now()
        ts_str = ts.isoformat()
        
        def _store(tx):
            # Create JournalEntry node
            tx.run("""
                MERGE (j:JournalEntry {id: $id})
                SET j.text = $text, j.timestamp = datetime($timestamp)
            """, id=entry_id, text=text[:5000], timestamp=ts_str)
            
            # Create entity nodes and MENTIONS relationships
            for ent in extraction.entities:
                node_type = self._get_node_type(ent)
                tx.run(f"""
                    MERGE (e:{node_type} {{name: $name}})
                    ON CREATE SET e.first_seen = datetime($timestamp), e.mention_count = 1
                    ON MATCH SET e.last_seen = datetime($timestamp), e.mention_count = e.mention_count + 1
                    WITH e
                    MATCH (j:JournalEntry {{id: $entry_id}})
                    MERGE (j)-[:MENTIONS]->(e)
                """, name=ent.text, timestamp=ts_str, entry_id=entry_id)
            
            # Create MENTIONS relationships between co-mentioned entities
            if len(extraction.entities) > 1:
                for i, e1 in enumerate(extraction.entities):
                    for e2 in extraction.entities[i+1:]:
                        t1, t2 = self._get_node_type(e1), self._get_node_type(e2)
                        tx.run(f"""
                            MATCH (a:{t1} {{name: $name1}})
                            MATCH (b:{t2} {{name: $name2}})
                            MERGE (a)-[r:CO_MENTIONED_WITH]->(b)
                            ON CREATE SET r.count = 1, r.last_seen = datetime($ts)
                            ON MATCH SET r.count = r.count + 1, r.last_seen = datetime($ts)
                        """, name1=e1.text, name2=e2.text, ts=ts_str)
        
        with self.driver.session() as session:
            session.execute_write(_store)
        
        return entry_id
    
    def _get_node_type(self, entity: ExtractedEntity) -> str:
        """Map entity to Neo4j node label."""
        valid = ("Person", "Place", "Organization", "Concept", "Event", "Date")
        return entity.label if entity.label in valid else "Concept"
    
    def query_entities(self, limit: int = 50) -> List[dict]:
        """Get all entities with mention counts."""
        def _query(tx):
            result = tx.run("""
                MATCH (e)
                WHERE e:Person OR e:Place OR e:Organization OR e:Concept OR e:Event OR e:Date
                RETURN labels(e)[0] as type, e.name as name,
                       e.mention_count as mentions,
                       e.last_seen as last_seen
                ORDER BY e.mention_count DESC
                LIMIT $limit
            """, limit=limit)
            return [dict(record) for record in result]
        
        with self.driver.session() as session:
            return session.execute_read(_query)
    
    def search_by_entity(self, entity_name: str) -> List[dict]:
        """Find journal entries that mention an entity."""
        def _query(tx):
            result = tx.run("""
                MATCH (j:JournalEntry)-[:MENTIONS]->(e {name: $name})
                RETURN j.id as id, j.text as text, j.timestamp as timestamp
                ORDER BY j.timestamp DESC
                LIMIT 20
            """, name=entity_name)
            return [dict(record) for record in result]
        
        with self.driver.session() as session:
            return session.execute_read(_query)
