import glob
import os
from typing import Dict, List, Optional, Any
from hyperon import MeTTa, SymbolAtom, ExpressionAtom, GroundedAtom
import logging
from .query_generator_interface import QueryGeneratorInterface
from .metta import Metta_Ground, metta_seralizer
import uuid

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MeTTaQueryGenerator(QueryGeneratorInterface):
    """MeTTa query generator that supports generic data models."""

    def __init__(self, dataset_path: str):
        """Initialize MeTTa environment and load dataset."""
        self.metta = MeTTa()
        self.initialize_space()
        self.dataset_path = dataset_path
        self.initialize_groundatoms()
        self.schema_manager = None  # Will be set by application

    def initialize_space(self):
        """Initialize MeTTa space."""
        self.metta.run("!(bind! &space (new-space))")

    def initialize_groundatoms(self):
        """Initialize grounded atoms."""
        Metta_Ground(self.metta)

    def load_dataset(self, path: str) -> None:
        """Load MeTTa dataset from files."""
        if not os.path.exists(path):
            raise ValueError(f"Dataset path '{path}' does not exist.")

        paths = glob.glob(os.path.join(path, "**/*.metta"), recursive=True)
        if not paths:
            raise ValueError(f"No .metta files found in dataset path '{path}'.")

        for file_path in paths:
            logger.info(f"Loading dataset from '{file_path}'...")
            try:
                self.metta.run(f"!(load-ascii &space {file_path})")
            except Exception as e:
                logger.error(f"Error loading dataset from '{file_path}': {e}")

    def generate_query(
        self,
        nodes: List[Dict],
        relationships: List[Dict],
        filters: Optional[Dict] = None,
        limit: Optional[int] = None,
        node_only: bool = False,
    ) -> str:
        """Generate a MeTTa query based on nodes and relationships."""
        # Build match patterns
        match_patterns = []
        return_patterns = []

        # Process nodes
        for node in nodes:
            node_var = f"${node.get('id', str(uuid.uuid4())[:8])}"
            node_type = node["type"].replace(" ", "_")

            # Build node pattern with properties
            node_pattern = f"({node_type} {node_var})"
            if node.get("properties"):
                for prop, value in node["properties"].items():
                    match_patterns.append(f"({prop} {node_pattern} {value})")
            else:
                match_patterns.append(node_pattern)

            return_patterns.append(node_pattern)

        # Process relationships if not node_only
        if not node_only and relationships:
            for rel in relationships:
                source_var = (
                    f"${next(n['id'] for n in nodes if n['id'] == rel['source'])}"
                )
                target_var = (
                    f"${next(n['id'] for n in nodes if n['id'] == rel['target'])}"
                )
                rel_type = rel["type"].replace(" ", "_")

                # Build relationship pattern
                rel_pattern = f"({rel_type} ({rel['source_type']} {source_var}) ({rel['target_type']} {target_var}))"
                match_patterns.append(rel_pattern)
                return_patterns.append(rel_pattern)

                # Add relationship properties
                if rel.get("properties"):
                    for prop, value in rel["properties"].items():
                        match_patterns.append(f"({prop} {rel_pattern} {value})")

        # Construct query
        query = f"""!(match &space (,{' '.join(match_patterns)}) ({' '.join(return_patterns)}))"""

        return query

    def execute_query(self, query: str) -> List[Dict]:
        """Execute a MeTTa query and return results."""
        try:
            results = self.metta.run(query)
            return self._convert_results(results)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []

    def _convert_results(self, results: List[Any]) -> List[Dict]:
        """Convert MeTTa results to dictionary format."""
        converted = []
        for result in results:
            if isinstance(result, (SymbolAtom, ExpressionAtom, GroundedAtom)):
                converted.append(self._atom_to_dict(result))
        return converted

    def _atom_to_dict(self, atom: Any) -> Dict:
        """Convert a MeTTa atom to dictionary."""
        if isinstance(atom, SymbolAtom):
            return {"type": "symbol", "value": str(atom)}
        elif isinstance(atom, ExpressionAtom):
            return {
                "type": "expression",
                "operator": str(atom.get_operator()),
                "arguments": [self._atom_to_dict(arg) for arg in atom.get_arguments()],
            }
        elif isinstance(atom, GroundedAtom):
            return {"type": "grounded", "value": str(atom)}
        return {"type": "unknown", "value": str(atom)}

    def parse_results(
        self,
        results: List[Dict],
        schema: Dict,
        components: Dict,
        result_type: str = "graph",
    ) -> Dict:
        """Parse and format query results."""
        if result_type == "graph":
            return self._parse_graph_results(results, schema, components)
        elif result_type == "count":
            return self._parse_count_results(results, schema, components)
        else:
            raise ValueError(f"Unsupported result type: {result_type}")

    def set_schema_manager(self, schema_manager):
        """Set the schema manager instance."""
        self.schema_manager = schema_manager

    def _parse_graph_results(
        self, results: List[Dict], schema: Dict, components: Dict
    ) -> Dict:
        """Parse results into graph format with schema validation."""
        nodes = []
        edges = []
        node_ids = set()
        node_to_dict = {}
        edge_to_dict = {}

        for result in results:
            if result["type"] == "expression":
                if len(result["arguments"]) == 2:  # Node
                    node_type = result["arguments"][0]["value"]
                    node_id = result["arguments"][1]["value"]

                    # Validate node type against schema
                    if self.schema_manager and not self.schema_manager.get_node_type(
                        node_type
                    ):
                        logger.warning(f"Skipping invalid node type: {node_type}")
                        continue

                    if node_id not in node_ids:
                        node_data = {
                            "data": {
                                "id": node_id,
                                "type": node_type,
                                "name": node_id,  # Default name
                            }
                        }

                        # Add schema-based properties if available
                        if self.schema_manager:
                            node_schema = self.schema_manager.get_node_type(node_type)
                            if node_schema:
                                # Get identifier field from schema
                                identifier_field = (
                                    self.schema_manager.get_identifier_field(node_type)
                                )
                                if identifier_field and identifier_field in result.get(
                                    "properties", {}
                                ):
                                    node_data["data"]["name"] = result["properties"][
                                        identifier_field
                                    ]

                                # Add valid properties
                                for prop, value in result.get("properties", {}).items():
                                    if self.schema_manager.is_valid_property(
                                        node_type, prop
                                    ):
                                        node_data["data"][prop] = value

                        nodes.append(node_data)
                        node_ids.add(node_id)

                        # Track nodes by type
                        if node_type not in node_to_dict:
                            node_to_dict[node_type] = []
                        node_to_dict[node_type].append(node_data)

                elif len(result["arguments"]) == 3:  # Edge
                    source_id = result["arguments"][0]["value"]
                    target_id = result["arguments"][1]["value"]
                    edge_type = result["operator"]

                    # Validate relationship type against schema
                    if (
                        self.schema_manager
                        and not self.schema_manager.get_relationship_type(edge_type)
                    ):
                        logger.warning(
                            f"Skipping invalid relationship type: {edge_type}"
                        )
                        continue

                    edge_data = {
                        "data": {
                            "source": source_id,
                            "target": target_id,
                            "label": edge_type,
                            "edge_id": f"{source_id}_{edge_type}_{target_id}",
                        }
                    }

                    # Add schema-based properties if available
                    if self.schema_manager:
                        rel_schema = self.schema_manager.get_relationship_type(
                            edge_type
                        )
                        if rel_schema:
                            for prop, value in result.get("properties", {}).items():
                                if self.schema_manager.is_valid_property(
                                    edge_type, prop, is_relationship=True
                                ):
                                    edge_data["data"][prop] = value

                    edges.append(edge_data)

                    # Track edges by type
                    if edge_type not in edge_to_dict:
                        edge_to_dict[edge_type] = []
                    edge_to_dict[edge_type].append(edge_data)

        return {
            "nodes": nodes,
            "edges": edges,
            "node_to_dict": node_to_dict,
            "edge_to_dict": edge_to_dict,
        }

    def _parse_count_results(
        self, results: List[Dict], schema: Dict, components: Dict
    ) -> Dict:
        """Parse count query results with schema validation."""
        counts = {
            "total_nodes": 0,
            "total_edges": 0,
            "nodes_by_label": {},
            "edges_by_type": {},
        }

        for result in results:
            if result["type"] == "expression":
                if result["operator"] == "node_count":
                    node_type = result["arguments"][0]["value"]
                    count = int(result["arguments"][1]["value"])

                    # Only count valid node types
                    if not self.schema_manager or self.schema_manager.get_node_type(
                        node_type
                    ):
                        counts["total_nodes"] += count
                        counts["nodes_by_label"][node_type] = count

                elif result["operator"] == "edge_count":
                    edge_type = result["arguments"][0]["value"]
                    count = int(result["arguments"][1]["value"])

                    # Only count valid relationship types
                    if (
                        not self.schema_manager
                        or self.schema_manager.get_relationship_type(edge_type)
                    ):
                        counts["total_edges"] += count
                        counts["edges_by_type"][edge_type] = count

        return counts

    def validate_query(self, query: str) -> bool:
        """Validate a MeTTa query."""
        try:
            # Try to parse the query without executing
            self.metta.parse(query)
            return True
        except Exception as e:
            logger.error(f"Query validation failed: {e}")
            return False

    def get_query_metadata(self, query: str) -> Dict:
        """Get metadata about a MeTTa query."""
        try:
            # Parse query to analyze complexity
            parsed = self.metta.parse(query)
            return {
                "complexity": self._calculate_query_complexity(parsed),
                "type": self._determine_query_type(parsed),
                "estimated_cost": self._estimate_query_cost(parsed),
            }
        except Exception as e:
            logger.error(f"Failed to get query metadata: {e}")
            return {}

    def _calculate_query_complexity(self, parsed_query: Any) -> int:
        """Calculate query complexity based on number of patterns."""
        if isinstance(parsed_query, (SymbolAtom, GroundedAtom)):
            return 1
        elif isinstance(parsed_query, ExpressionAtom):
            return 1 + sum(
                self._calculate_query_complexity(arg)
                for arg in parsed_query.get_arguments()
            )
        return 0

    def _determine_query_type(self, parsed_query: Any) -> str:
        """Determine the type of query (match, count, etc.)."""
        if isinstance(parsed_query, ExpressionAtom):
            op = str(parsed_query.get_operator())
            if op.startswith("match"):
                return "match"
            elif op.startswith("count"):
                return "count"
        return "unknown"

    def _estimate_query_cost(self, parsed_query: Any) -> Dict:
        """Estimate query execution cost."""
        complexity = self._calculate_query_complexity(parsed_query)
        return {
            "patterns": complexity,
            "estimated_time": f"{complexity * 0.1:.2f}ms",
            "memory_impact": (
                "low" if complexity < 10 else "medium" if complexity < 50 else "high"
            ),
        }
