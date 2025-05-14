import yaml
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime


class SchemaManager:
    """A generic schema manager that handles any domain's data model."""

    def __init__(self, schema_config_path: str = None, db_instance=None):
        """
        Initialize the schema manager with optional database connection.

        Args:
            schema_config_path (str): Path to the schema configuration YAML file
            db_instance: Neo4j database instance
        """
        self.db_instance = db_instance
        self.schema_config_path = schema_config_path or str(
            Path(__file__).parent.parent.parent
            / "config"
            / "schema"
            / "generic_schema.yaml"
        )
        self.schema = self._load_schema()
        self.nodes = self.schema.get("nodes", {})
        self.relationships = self.schema.get("relationships", {})
        self.property_types = self.schema.get("property_types", {})

    def _load_schema(self) -> dict:
        """Load and validate the schema configuration."""
        try:
            with open(self.schema_config_path, "r") as f:
                schema = yaml.safe_load(f)

            # Validate schema structure
            required_sections = ["nodes", "relationships", "property_types"]
            for section in required_sections:
                if section not in schema:
                    raise ValueError(f"Missing required section '{section}' in schema")

            return schema
        except Exception as e:
            raise ValueError(f"Error loading schema: {str(e)}")

    def get_node_type(self, node_type: str) -> dict:
        """Get node type definition from schema."""
        node_def = self.nodes.get(node_type)
        if not node_def:
            return None

        # Handle inheritance
        if "inherits" in node_def:
            parent_def = self.get_node_type(node_def["inherits"])
            if parent_def:
                # Merge parent and child properties
                merged_def = parent_def.copy()
                merged_def.update(node_def)
                return merged_def

        return node_def

    def get_relationship_type(self, rel_type: str) -> dict:
        """Get relationship type definition from schema."""
        return self.relationships.get(rel_type)

    def get_identifier_field(self, node_type: str) -> str:
        """Get the identifier field for a node type."""
        node_def = self.get_node_type(node_type)
        return node_def.get("identifier_field") if node_def else None

    def is_valid_property(
        self, entity_type: str, property_name: str, is_relationship: bool = False
    ) -> bool:
        """
        Check if a property is valid for an entity type.

        Args:
            entity_type (str): Node or relationship type
            property_name (str): Name of the property to validate
            is_relationship (bool): Whether the entity is a relationship
        """
        entity_def = (
            self.get_relationship_type(entity_type)
            if is_relationship
            else self.get_node_type(entity_type)
        )

        if not entity_def:
            return False

        # Check if property is defined in schema
        properties = entity_def.get("properties", {})
        if property_name not in properties:
            return False

        # Validate property type
        prop_type = properties[property_name]
        return prop_type in self.property_types

    def is_valid_identifier(self, node_type: str, identifier: str) -> bool:
        """Check if an identifier value is valid for a node type."""
        node_def = self.get_node_type(node_type)
        if not node_def:
            return False

        identifier_field = node_def.get("identifier_field")
        if not identifier_field:
            return False

        # Get property type for identifier field
        prop_type = node_def["properties"].get(identifier_field)
        if not prop_type:
            return False

        # Validate against property type rules
        type_rules = self.property_types.get(prop_type, {})
        pattern = type_rules.get("pattern")
        if pattern:
            import re

            return bool(re.match(pattern, identifier))

        return True

    def validate_node(self, node_type: str, properties: dict) -> bool:
        """Validate a node's properties against the schema."""
        node_def = self.get_node_type(node_type)
        if not node_def:
            return False

        # Check required properties
        required_props = {
            name
            for name, rules in node_def.get("properties", {}).items()
            if rules.get("required", False)
        }

        if not all(prop in properties for prop in required_props):
            return False

        # Validate property types
        for prop_name, value in properties.items():
            if not self.is_valid_property(node_type, prop_name):
                return False

        return True

    def validate_relationship(self, rel_type: str, properties: dict) -> bool:
        """Validate a relationship's properties against the schema."""
        rel_def = self.get_relationship_type(rel_type)
        if not rel_def:
            return False

        # Check required properties
        required_props = {
            name
            for name, rules in rel_def.get("properties", {}).items()
            if rules.get("required", False)
        }

        if not all(prop in properties for prop in required_props):
            return False

        # Validate property types
        for prop_name, value in properties.items():
            if not self.is_valid_property(rel_type, prop_name, is_relationship=True):
                return False

        return True

    def get_node_type(self, type_name: str) -> Optional[Dict]:
        """Get node type definition including inherited properties."""
        node_type = self.nodes.get(type_name)
        if not node_type:
            return None

        # Handle inheritance
        properties = {}
        current_type = node_type
        while current_type:
            if "properties" in current_type:
                properties.update(current_type["properties"])
            if "inherits" in current_type:
                current_type = self.nodes.get(current_type["inherits"])
            else:
                current_type = None

        node_type["properties"] = properties
        return node_type

    def get_relationship_type(self, type_name: str) -> Optional[Dict]:
        """Get relationship type definition."""
        return self.relationships.get(type_name)

    def validate_node(self, node_type: str, properties: Dict) -> List[str]:
        """
        Validate node properties against schema.

        Returns:
            List[str]: List of validation errors, empty if valid
        """
        errors = []
        node_schema = self.get_node_type(node_type)

        if not node_schema:
            errors.append(f"Invalid node type: {node_type}")
            return errors

        # Validate required properties
        for prop_name, prop_type in node_schema["properties"].items():
            if prop_name not in properties:
                if not self.property_types[prop_type].get("default") is None:
                    continue
                errors.append(f"Missing required property: {prop_name}")
                continue

            # Validate property type and constraints
            prop_value = properties[prop_name]
            prop_errors = self._validate_property(prop_name, prop_type, prop_value)
            errors.extend(prop_errors)

        return errors

    def validate_relationship(
        self, rel_type: str, source_type: str, target_type: str, properties: Dict
    ) -> List[str]:
        """
        Validate relationship properties and connectivity against schema.

        Returns:
            List[str]: List of validation errors, empty if valid
        """
        errors = []
        rel_schema = self.get_relationship_type(rel_type)

        if not rel_schema:
            errors.append(f"Invalid relationship type: {rel_type}")
            return errors

        # Validate source and target node types
        if source_type not in rel_schema["source"]:
            errors.append(
                f"Invalid source node type {source_type} for relationship {rel_type}"
            )

        if target_type not in rel_schema["target"]:
            errors.append(
                f"Invalid target node type {target_type} for relationship {rel_type}"
            )

        # Validate properties
        for prop_name, prop_type in rel_schema["properties"].items():
            if prop_name not in properties:
                if not self.property_types[prop_type].get("default") is None:
                    continue
                errors.append(f"Missing required property: {prop_name}")
                continue

            prop_value = properties[prop_name]
            prop_errors = self._validate_property(prop_name, prop_type, prop_value)
            errors.extend(prop_errors)

        return errors

    def _validate_property(self, name: str, type_name: str, value: Any) -> List[str]:
        """Validate a property value against its type definition."""
        errors = []
        type_def = self.property_types.get(type_name)

        if not type_def:
            errors.append(f"Unknown property type: {type_name}")
            return errors

        # Type validation
        try:
            if type_name == "datetime":
                if isinstance(value, str):
                    datetime.strptime(value, type_def["format"])
            elif type_name in ["string", "integer", "float", "boolean", "list", "dict"]:
                type_class = eval(type_def["python_type"])
                if not isinstance(value, type_class):
                    errors.append(
                        f"Invalid type for {name}: expected {type_def['python_type']}, got {type(value)}"
                    )
        except Exception as e:
            errors.append(f"Invalid value for {name}: {str(e)}")

        # Validate constraints
        if "validators" in type_def:
            for validator in type_def["validators"]:
                if validator["type"] == "length":
                    if len(value) > validator["max"]:
                        errors.append(
                            f"Value too long for {name}: max length is {validator['max']}"
                        )
                elif validator["type"] == "range":
                    if value < validator["min"] or value > validator["max"]:
                        errors.append(
                            f"Value out of range for {name}: must be between {validator['min']} and {validator['max']}"
                        )

        return errors

    def get_default_properties(self, type_name: str, is_node: bool = True) -> Dict:
        """Get default properties for a node or relationship type."""
        type_def = (
            self.get_node_type(type_name)
            if is_node
            else self.get_relationship_type(type_name)
        )
        if not type_def:
            return {}

        defaults = {}
        for prop_name, prop_type in type_def["properties"].items():
            type_def = self.property_types[prop_type]
            defaults[prop_name] = type_def["default"]

        return defaults

    def get_property_type_info(self, type_name: str) -> Optional[Dict]:
        """Get detailed information about a property type."""
        return self.property_types.get(type_name)

    def get_all_node_types(self) -> List[str]:
        """Get list of all defined node types."""
        return list(self.nodes.keys())

    def get_all_relationship_types(self) -> List[str]:
        """Get list of all defined relationship types."""
        return list(self.relationships.keys())

    def get_valid_relationships(self, source_type: str, target_type: str) -> List[str]:
        """Get list of valid relationship types between given node types."""
        valid_rels = []
        for rel_name, rel_def in self.relationships.items():
            if source_type in rel_def["source"] and target_type in rel_def["target"]:
                valid_rels.append(rel_name)
        return valid_rels

    def get_identifier_fields(self) -> List[str]:
        """Get all identifier fields defined in the schema."""
        identifier_fields = set()
        for node_type in self.nodes.values():
            if "identifier_field" in node_type:
                identifier_fields.add(node_type["identifier_field"])
        return list(identifier_fields)

    def is_valid_identifier(self, node_type: str, identifier: str) -> bool:
        """Check if an identifier is valid for a node type."""
        node_schema = self.get_node_type(node_type)
        if not node_schema:
            return False

        identifier_field = self.get_identifier_field(node_type)
        if not identifier_field:
            return False

        # Get property type for identifier field
        property_type = node_schema["properties"].get(identifier_field)
        if not property_type:
            return False

        # Get type definition
        type_def = self.property_types.get(property_type)
        if not type_def:
            return False

        # Validate against type definition
        try:
            if "pattern" in type_def:
                import re

                return bool(re.match(type_def["pattern"], identifier))

            if "validators" in type_def:
                for validator in type_def["validators"]:
                    if validator["type"] == "pattern":
                        import re

                        if not re.match(validator["pattern"], identifier):
                            return False
                    elif validator["type"] == "length":
                        if len(identifier) > validator.get("max", float("inf")) or len(
                            identifier
                        ) < validator.get("min", 0):
                            return False
            return True
        except Exception:
            return False

    def generate_schema_from_db(self):
        """Generate schema by inspecting the Neo4j database structure."""
        if not self.db_instance:
            raise ValueError("Database instance not provided")

        try:
            # Get all node labels and their properties
            node_query = """
            CALL db.schema.nodeTypeProperties()
            YIELD nodeType, propertyName, propertyTypes
            RETURN nodeType, collect({name: propertyName, types: propertyTypes}) as properties
            """
            node_results = self.db_instance.run_query(node_query)

            # Get all relationship types and their properties
            rel_query = """
            CALL db.schema.relationshipTypeProperties()
            YIELD relationType, propertyName, propertyTypes
            RETURN relationType, collect({name: propertyName, types: propertyTypes}) as properties
            """
            rel_results = self.db_instance.run_query(rel_query)

            # Build schema
            schema = {
                "version": "1.0",
                "property_types": self._get_base_property_types(),
                "nodes": {},
                "relationships": {},
            }

            # Process nodes
            for record in node_results:
                node_type = record["nodeType"]
                properties = record["properties"]

                schema["nodes"][node_type] = {
                    "label": node_type,
                    "inherits": "entity",
                    "properties": self._convert_properties(properties),
                }

            # Process relationships
            for record in rel_results:
                rel_type = record["relationType"]
                properties = record["properties"]

                schema["relationships"][rel_type] = {
                    "description": f"Relationship of type {rel_type}",
                    "properties": self._convert_properties(properties),
                }

            # Save schema to file
            with open(self.schema_config_path, "w") as f:
                yaml.safe_dump(schema, f, default_flow_style=False)

            # Reload schema
            self.schema = schema
            self.nodes = schema["nodes"]
            self.relationships = schema["relationships"]
            self.property_types = schema["property_types"]

            return schema

        except Exception as e:
            raise ValueError(f"Error generating schema from database: {str(e)}")

    def _convert_properties(self, properties):
        """Convert Neo4j property types to schema property types."""
        schema_properties = {}
        for prop in properties:
            prop_name = prop["name"]
            neo4j_types = prop["types"]

            # Map Neo4j types to our schema types
            schema_type = self._map_neo4j_type(
                neo4j_types[0] if neo4j_types else "String"
            )
            schema_properties[prop_name] = schema_type

        return schema_properties

    def _map_neo4j_type(self, neo4j_type):
        """Map Neo4j data types to schema property types."""
        type_mapping = {
            "String": "string",
            "Integer": "integer",
            "Float": "float",
            "Boolean": "boolean",
            "DateTime": "datetime",
            "List": "list",
            "Point": "string",  # Store as string representation
            "Date": "datetime",
            "LocalDateTime": "datetime",
            "LocalTime": "string",
            "Time": "string",
            "Duration": "string",
        }
        return type_mapping.get(neo4j_type, "string")

    def _get_base_property_types(self):
        """Get base property type definitions."""
        return {
            "string": {"description": "Text value", "pattern": ".*"},
            "integer": {"description": "Whole number", "pattern": "^-?\\d+$"},
            "float": {"description": "Decimal number", "pattern": "^-?\\d*\\.?\\d+$"},
            "boolean": {"description": "True/False value", "pattern": "^(true|false)$"},
            "datetime": {
                "description": "Date and time",
                "pattern": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(Z|[+-]\\d{2}:?\\d{2})?$",
            },
            "list": {"description": "List of values"},
        }

    def refresh_schema(self):
        """Refresh schema from database and update cache."""
        return self.generate_schema_from_db()
