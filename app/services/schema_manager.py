import yaml
import logging
from typing import Dict, List, Optional, Any
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DynamicSchemaManager:
    def __init__(self, driver: GraphDatabase.driver):
        self.driver = driver
        self.schema = {}
        self._discover_schema()

    def _discover_schema(self):
        """Discover the complete schema from the database"""
        try:
            with self.driver.session() as session:
                # Get all node labels
                node_query = """
                MATCH (n)
                WITH DISTINCT labels(n) as labels
                UNWIND labels as label
                RETURN DISTINCT label
                """
                result = session.run(node_query)
                labels = [record["label"] for record in result]
                logger.info(f"Discovered node labels: {labels}")

                # For each label, get its properties
                for label in labels:
                    prop_query = f"""
                    MATCH (n:{label})
                    WITH n LIMIT 1000
                    UNWIND keys(n) as prop
                    RETURN DISTINCT prop
                    """
                    prop_result = session.run(prop_query)
                    properties = [record["prop"] for record in prop_result]
                    logger.info(f"Discovered properties for {label}: {properties}")

                    # Build schema for this label
                    self.schema[label] = {
                        "represented_as": "node",
                        "input_label": label,
                        "properties": self._process_properties(
                            {prop: {"type": "str"} for prop in properties}
                        ),
                    }

                # Discover relationships
                self._discover_relationships()

                logger.info(
                    f"Schema discovery completed. Found {len(self.schema)} types"
                )
                logger.info(f"Schema: {self.schema}")
        except Exception as e:
            logger.error(f"Error discovering schema: {e}")
            raise

    def _process_properties(self, properties: Dict) -> Dict:
        """Process raw properties into schema format"""
        processed = {}
        for prop_name, prop_info in properties.items():
            processed[prop_name] = {
                "type": prop_info.get("type", "str"),
                "is_identifier": prop_name.lower() in ["id", "identifier"],
                "is_display_name": prop_name.lower() in ["name", "title", "label"],
            }
        return processed

    def _discover_relationships(self):
        """Discover relationships from the database"""
        with self.driver.session() as session:
            # Get all relationship types and their properties
            rel_query = """
            MATCH ()-[r]->()
            WITH DISTINCT type(r) as rel_type, keys(r) as props
            RETURN rel_type, props
            """
            result = session.run(rel_query)

            for record in result:
                rel_type = record["rel_type"]
                props = record["props"]

                # Get source and target types
                source_target_query = f"""
                MATCH (a)-[r:{rel_type}]->(b)
                WITH DISTINCT labels(a)[0] as source_type, labels(b)[0] as target_type
                RETURN source_type, target_type
                LIMIT 1
                """
                st_result = session.run(source_target_query)
                st_record = st_result.single()

                if st_record:
                    self.schema[rel_type] = {
                        "represented_as": "edge",
                        "input_label": rel_type,
                        "source": st_record["source_type"],
                        "target": st_record["target_type"],
                        "properties": self._process_properties(
                            {p: {"type": "str"} for p in props}
                        ),
                    }

    def get_node_type(self, type_name: str) -> Dict:
        """Get schema for a node type"""
        return self.schema.get(type_name, {})

    def get_relationship_type(self, type_name: str) -> Dict:
        """Get schema for a relationship type"""
        return self.schema.get(type_name, {})

    def get_identifier_field(self, node_type: str) -> str:
        """Get the identifier field for a node type"""
        node_schema = self.get_node_type(node_type)
        if not node_schema:
            return "id"

        for prop_name, prop_config in node_schema.get("properties", {}).items():
            if prop_config.get("is_identifier"):
                return prop_name
        return "id"

    def get_display_field(self, node_type: str) -> Optional[str]:
        """Get the display field for a node type"""
        node_schema = self.get_node_type(node_type)
        if not node_schema:
            return None

        for prop_name, prop_config in node_schema.get("properties", {}).items():
            if prop_config.get("is_display_name"):
                return prop_name
        return None

    def is_valid_property(self, node_type: str, property_name: str) -> bool:
        """Check if property exists for node type"""
        node_schema = self.get_node_type(node_type)
        return property_name in node_schema.get("properties", {})

    def get_all_properties(self, node_type: str) -> Dict:
        """Get all properties for a node type including inherited ones"""
        properties = {}
        current_type = node_type

        while current_type in self.schema:
            type_def = self.schema[current_type]
            if "properties" in type_def:
                properties.update(type_def["properties"])

            if "is_a" in type_def:
                current_type = type_def["is_a"]
            else:
                break

        return properties
