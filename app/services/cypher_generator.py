from typing import List, Dict, Optional, Any
import logging
from dotenv import load_dotenv
import neo4j
from app.services.query_generator_interface import QueryGeneratorInterface
from neo4j import GraphDatabase
import glob
import os
from neo4j.graph import Node, Relationship
from app.error import ThreadStopException
import json

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CypherQueryGenerator(QueryGeneratorInterface):
    def __init__(self, dataset_path: str):
        """Initialize the Cypher query generator."""
        self.driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI"),
            auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
        )
        self.dataset_path = dataset_path
        self.schema_manager = None

    def set_schema_manager(self, schema_manager):
        """Set the schema manager instance."""
        if not schema_manager:
            raise ValueError("Schema manager is required for query generation")
        self.schema_manager = schema_manager

    def close(self):
        """Close the Neo4j driver connection."""
        self.driver.close()

    def load_dataset(self, path: str) -> None:
        """Load dataset from Cypher files."""
        if not os.path.exists(path):
            raise ValueError(f"Dataset path '{path}' does not exist.")

        paths = glob.glob(os.path.join(path, "**/*.cypher"), recursive=True)
        if not paths:
            raise ValueError(f"No .cypher files found in dataset path '{path}'.")

        # Process nodes first, then relationships
        for file_type in ["nodes", "edges"]:
            files = [p for p in paths if p.endswith(f"{file_type}.cypher")]
            for file_path in files:
                logger.info(f"Loading {file_type} from '{file_path}'...")
                try:
                    with open(file_path, "r") as file:
                        for line in file:
                            if line.strip():
                                self.execute_query(line)
                except Exception as e:
                    logger.error(f"Error loading {file_type} from '{file_path}': {e}")

    def generate_query(
        self,
        nodes: List[Dict],
        relationships: List[Dict],
        filters: Optional[Dict] = None,
        limit: Optional[int] = None,
        node_only: bool = False,
    ) -> str:
        """Generate a Cypher query based on nodes and relationships."""
        # Build MATCH clauses for nodes
        match_clauses = []
        where_clauses = []
        return_items = []

        # Process nodes
        for node in nodes:
            node_var = f"n{len(match_clauses)}"
            node_label = node["type"].replace(" ", "_")
            match_clause = f"({node_var}:{node_label})"
            match_clauses.append(match_clause)
            return_items.append(node_var)

            # Add property filters
            if node.get("properties"):
                for prop, value in node["properties"].items():
                    where_clauses.append(f"{node_var}.{prop} = {json.dumps(value)}")

        # Process relationships if not node_only
        if not node_only and relationships:
            for rel in relationships:
                source_var = f"n{nodes.index(next(n for n in nodes if n['id'] == rel['source']))}"
                target_var = f"n{nodes.index(next(n for n in nodes if n['id'] == rel['target']))}"
                rel_var = f"r{len(match_clauses)}"
                rel_type = rel["type"].replace(" ", "_")

                match_clause = f"({source_var})-[{rel_var}:{rel_type}]->({target_var})"
                match_clauses.append(match_clause)
                return_items.append(rel_var)

                # Add relationship property filters
                if rel.get("properties"):
                    for prop, value in rel["properties"].items():
                        where_clauses.append(f"{rel_var}.{prop} = {json.dumps(value)}")

        # Construct the final query
        query = f"MATCH {', '.join(match_clauses)}"
        if where_clauses:
            query += f"\nWHERE {' AND '.join(where_clauses)}"
        query += f"\nRETURN {', '.join(return_items)}"

        if limit:
            query += f"\nLIMIT {limit}"

        return query

    def execute_query(self, query: str) -> List[Dict]:
        """Execute a Cypher query and return results."""
        results = []
        with self.driver.session() as session:
            result = session.run(query)
            for record in result:
                results.append(record)
        return results

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

    def _parse_graph_results(
        self, results: List[Dict], schema: Dict, components: Dict
    ) -> Dict:
        """Parse results into graph format."""
        nodes = []
        edges = []
        node_dict = {}
        node_to_dict = {}
        edge_to_dict = {}
        node_type = set()
        edge_type = set()
        visited_relations = set()

        for record in results:
            for item in record.values():
                if isinstance(item, neo4j.graph.Node):
                    node_id = f"{list(item.labels)[0]} {item['id']}"
                    if node_id not in node_dict:
                        node_type_name = list(item.labels)[0]
                        node_data = {
                            "data": {
                                "id": node_id,
                                "type": node_type_name,
                            }
                        }

                        # Process properties based on schema
                        if self.schema_manager:
                            node_schema = self.schema_manager.get_node_type(
                                node_type_name
                            )
                            if node_schema:
                                # Add all valid properties from schema
                                for key, value in item.items():
                                    if graph_components["properties"]:
                                        if (
                                            key != "id"
                                            and self.schema_manager.is_valid_property(
                                                node_type_name, key
                                            )
                                        ):
                                            node_data["data"][key] = value
                                    else:
                                        # Use schema-defined identifier field
                                        identifier_field = (
                                            self.schema_manager.get_identifier_field(
                                                node_type_name
                                            )
                                        )
                                        if identifier_field and key == identifier_field:
                                            node_data["data"]["name"] = value

                        if "name" not in node_data["data"]:
                            node_data["data"]["name"] = node_id

                        nodes.append(node_data)
                        if node_data["data"]["type"] not in node_type:
                            node_type.add(node_data["data"]["type"])
                            node_to_dict[node_data["data"]["type"]] = []
                        node_to_dict[node_data["data"]["type"]].append(node_data)
                        node_dict[node_id] = node_data

                elif isinstance(item, neo4j.graph.Relationship):
                    source_label = list(item.start_node.labels)[0]
                    target_label = list(item.end_node.labels)[0]
                    source_id = f"{source_label} {item.start_node['id']}"
                    target_id = f"{target_label} {item.end_node['id']}"
                    rel_type = item.type

                    edge_data = {
                        "data": {
                            "edge_id": f"{source_label}_{rel_type}_{target_label}",
                            "label": rel_type,
                            "source": source_id,
                            "target": target_id,
                        }
                    }

                    # Process edge properties based on schema
                    if self.schema_manager:
                        rel_schema = self.schema_manager.get_relationship_type(rel_type)
                        if rel_schema:
                            for key, value in item.items():
                                if self.schema_manager.is_valid_property(
                                    rel_type, key, is_relationship=True
                                ):
                                    edge_data["data"][key] = value

                    temp_relation_id = f"{source_id} - {rel_type} - {target_id}"
                    if temp_relation_id not in visited_relations:
                        edges.append(edge_data)
                        visited_relations.add(temp_relation_id)

                        if edge_data["data"]["label"] not in edge_type:
                            edge_type.add(edge_data["data"]["label"])
                            edge_to_dict[edge_data["data"]["label"]] = []
                        edge_to_dict[edge_data["data"]["label"]].append(edge_data)

        return {"nodes": nodes, "edges": edges}

    def _parse_count_results(
        self, results: List[Dict], schema: Dict, components: Dict
    ) -> Dict:
        """Parse count query results."""
        counts = {
            "total_nodes": 0,
            "total_edges": 0,
            "nodes_by_label": {},
            "edges_by_type": {},
        }

        for record in results:
            for key, value in record.items():
                if key.startswith("nodes_"):
                    label = key[6:]  # Remove 'nodes_' prefix
                    counts["nodes_by_label"][label] = value
                    counts["total_nodes"] += value
                elif key.startswith("edges_"):
                    type_ = key[6:]  # Remove 'edges_' prefix
                    counts["edges_by_type"][type_] = value
                    counts["total_edges"] += value

        return counts

    def validate_query(self, query: str) -> bool:
        """Validate a Cypher query."""
        try:
            # Try to parse and explain the query without executing it
            with self.driver.session() as session:
                session.run(f"EXPLAIN {query}")
            return True
        except Exception as e:
            logger.error(f"Query validation failed: {e}")
            return False

    def get_query_metadata(self, query: str) -> Dict:
        """Get metadata about a Cypher query."""
        try:
            with self.driver.session() as session:
                result = session.run(f"PROFILE {query}")
                plan = result.consume().plan
                return {
                    "estimated_rows": plan.arguments.get("EstimatedRows", 0),
                    "pipeline": plan.arguments.get("Pipeline", ""),
                    "indexes_used": plan.arguments.get("IndexesUsed", []),
                    "time": plan.arguments.get("Time", 0),
                }
        except Exception as e:
            logger.error(f"Failed to get query metadata: {e}")
            return {}

    def query_Generator(self, requests, node_map, limit=None, node_only=False):
        nodes = requests["nodes"]
        predicate_map = {}

        if "predicates" in requests and len(requests["predicates"]) > 0:
            predicates = requests["predicates"]

            init_pred = predicates[0]

            if "predicate_id" not in init_pred:
                for idx, pred in enumerate(predicates):
                    pred["predicate_id"] = f"p{idx}"
                for predicate in predicates:
                    predicate_map[predicate["predicate_id"]] = predicate
            else:
                for predicate in predicates:
                    predicate_map[predicate["predicate_id"]] = predicate
        else:
            predicates = None

        cypher_queries = []
        match_preds = []
        return_preds = []
        where_preds = []
        match_no_preds = []
        return_no_preds = []
        where_no_preds = []
        node_ids = set()
        clause_list = []

        if not predicates:
            list_of_node_ids = []
            # Case when there are no predicates
            for node in nodes:
                var_name = f"{node['node_id']}"
                match_no_preds.append(self.match_node(node, var_name))
                if node["properties"]:
                    where_no_preds.extend(self.where_construct(node, var_name))
                return_no_preds.append(var_name)
                list_of_node_ids.append(var_name)
            if node_only:
                cypher_query = self.construct_optional_clause(
                    match_no_preds, return_no_preds, where_no_preds, limit
                )
            else:
                cypher_query = self.construct_clause(
                    match_no_preds, return_no_preds, where_no_preds, limit
                )
            cypher_queries.append(cypher_query)
            query_clauses = {
                "match_no_preds": match_no_preds,
                "return_no_preds": return_no_preds,
                "where_no_preds": where_no_preds,
                "list_of_node_ids": list_of_node_ids,
                "predicates": predicates,
            }
            count = self.construct_count_clause(query_clauses, node_map, predicate_map)
            cypher_queries.extend(count)
        else:
            for i, predicate in enumerate(predicates):
                predicate_id = predicate["predicate_id"]
                predicate_type = predicate["type"].replace(" ", "_").lower()
                source_node = node_map[predicate["source"]]
                target_node = node_map[predicate["target"]]
                source_var = source_node["node_id"]
                target_var = target_node["node_id"]

                source_match = self.match_node(source_node, source_var)
                target_match = self.match_node(target_node, target_var)

                tmp_where_preds = []
                if source_var not in node_ids:
                    tmp_where_preds.extend(
                        self.where_construct(source_node, source_var)
                    )
                    where_preds.extend(self.where_construct(source_node, source_var))
                if target_var not in node_ids:
                    tmp_where_preds.extend(
                        self.where_construct(target_node, target_var)
                    )
                    where_preds.extend(self.where_construct(target_node, target_var))

                return_preds.append(predicate_id)
                node_ids.add(source_var)
                node_ids.add(target_var)

                match_preds.append(
                    f"{source_match}-[{predicate_id}:{predicate_type}]->{target_match}"
                )

                # Construct the MATCH clause
                match_clause = f"MATCH {source_match}-[{predicate_id}:{predicate_type}]->{target_match}"

                # Construct the WHERE clause if there are conditions
                where_clause = (
                    f"WHERE {' AND '.join(tmp_where_preds)}"
                    if len(tmp_where_preds) >= 1
                    else ""
                )

                if i == len(predicates) - 1:
                    # Construct the RETURN clause
                    return_clause = (
                        f"RETURN {', '.join(return_preds)}, {', '.join(node_ids)}"
                    )

                    # Combine all clauses into a single query
                    clause_list.append(f"{match_clause} {where_clause} {return_clause}")
                else:
                    with_clause = (
                        f"WITH {', '.join(return_preds)}, {', '.join(node_ids)}"
                    )

                    clause_list.append(f"{match_clause} {where_clause} {with_clause}")

            list_of_node_ids = list(node_ids)
            list_of_node_ids.sort()
            full_return_preds = return_preds + list_of_node_ids

            cypher_query = " ".join(clause_list)
            cypher_queries.append(cypher_query)
            query_clauses = {
                "match_preds": match_preds,
                "full_return_preds": full_return_preds,
                "where_preds": where_preds,
                "list_of_node_ids": list_of_node_ids,
                "return_preds": return_preds,
                "predicates": predicates,
            }
            count = self.construct_count_clause(query_clauses, node_map, predicate_map)
            cypher_queries.extend(count)
        return cypher_queries

    def construct_clause(self, match_clause, return_clause, where_no_preds, limit):
        match_clause = f"MATCH {', '.join(match_clause)}"
        return_clause = f"RETURN {', '.join(return_clause)}"
        if len(where_no_preds) > 0:
            where_clause = f"WHERE {' AND '.join(where_no_preds)}"
            return f"{match_clause} {where_clause} {return_clause} {self.limit_query(limit)}"
        return f"{match_clause} {return_clause} {self.limit_query(limit)}"

    def construct_optional_clause(
        self, match_clause, return_clause, where_no_preds, limit
    ):
        optional_clause = ""

        for match in match_clause:
            optional_clause += f"OPTIONAL MATCH {match} "

        return_clause = f"RETURN {', '.join(return_clause)}"
        if len(where_no_preds) > 0:
            where_clause = f"WHERE {' AND '.join(where_no_preds)}"
            return f"{optional_clause} {where_clause} {return_clause} {self.limit_query(limit)}"
        return f"{optional_clause} {return_clause} {self.limit_query(limit)}"

    def construct_count_clause(self, query_clauses, node_map, predicate_map):
        match_no_clause = ""
        where_no_clause = ""
        match_clause = ""
        where_clause = ""
        return_preds = []
        collect_node_and_edge = ""

        # Construct clause for match with no predicates
        if "match_no_preds" in query_clauses and query_clauses["match_no_preds"]:
            match_no_clause = f"MATCH {', '.join(query_clauses['match_no_preds'])}"
            if "where_no_preds" in query_clauses and query_clauses["where_no_preds"]:
                where_no_clause = (
                    f"WHERE {' AND '.join(query_clauses['where_no_preds'])}"
                )

        # Construct clause for match with predicates
        if "match_preds" in query_clauses and query_clauses["match_preds"]:
            match_clause = f"MATCH {', '.join(query_clauses['match_preds'])}"
            if "where_preds" in query_clauses and query_clauses["where_preds"]:
                where_clause = f"WHERE {' AND '.join(query_clauses['where_preds'])}"

        if "return_no_preds" in query_clauses and "return_preds" in query_clauses:
            query_clauses["list_of_node_ids"].extend(query_clauses["return_no_preds"])

        if "return_preds" in query_clauses:
            return_preds = query_clauses["return_preds"]

        for node_ids in query_clauses["list_of_node_ids"]:
            collect_node_and_edge += (
                f"COLLECT(DISTINCT {node_ids}) AS {node_ids}_count, "
            )

        if "return_preds" in query_clauses:
            for predicate in query_clauses["predicates"]:
                predicate_id = predicate["predicate_id"]
                collect_node_and_edge += (
                    f"COLLECT(DISTINCT {predicate_id}) AS {predicate_id}_count, "
                )
        collect_node_and_edge = f"WITH {collect_node_and_edge.rstrip(', ')}"

        # Construct the WITH and UNWIND clauses
        combined_nodes = " + ".join(
            [f"{var}_count" for var in query_clauses["list_of_node_ids"]]
        )
        combined_edges = None
        if "return_preds" in query_clauses:
            combined_edges = " + ".join(
                [f"{var}_count" for var in query_clauses["return_preds"]]
            )
        with_clause = f"WITH {combined_nodes} AS combined_nodes {f',{combined_edges} AS combined_edges' if combined_edges else ''}"
        unwind_clause = f"UNWIND combined_nodes AS nodes"

        # Construct the RETURN clause
        return_clause = f"RETURN COUNT(DISTINCT nodes) AS total_nodes {', SIZE(combined_edges) AS total_edges ' if combined_edges else ''}"

        # build the query for total node and edge count
        total_count = f"""
            {match_no_clause}
            {where_no_clause}
            {match_clause}
            {where_clause}
            {collect_node_and_edge}
            {with_clause}
            {unwind_clause}
            {return_clause}
        """

        # start building query for counting by label for both ndoe and edges

        if return_preds:
            # count query
            count_clause = ""
            for node in query_clauses["list_of_node_ids"]:
                count_clause += (
                    f"COUNT(DISTINCT {node}) AS {node}_{node_map[node]['type']}, "
                )
            for edge in query_clauses["predicates"]:
                edge_id = edge["predicate_id"]
                count_clause += f"COUNT(DISTINCT {edge_id}) AS {edge_id}_{predicate_map[edge_id]['type'].replace(' ', '_')}, "
            return_clause = "RETURN " + count_clause.rstrip(", ")
            label_count_query = f"""{match_no_clause} {where_no_clause} {match_clause} {where_clause} {return_clause}"""
        else:
            count_clause = ""
            for node in query_clauses["list_of_node_ids"]:
                count_clause += (
                    f"COUNT(DISTINCT {node}) AS {node}_{node_map[node]['type']}, "
                )
            return_clause = "RETURN " + count_clause.rstrip(", ")
            label_count_query = (
                f"""{match_no_clause} {where_no_clause} {return_clause}"""
            )

        return [total_count, label_count_query]

    def limit_query(self, limit):
        """
        for now remove the limit from the backend
        and handle it from the client side
        """
        # if limit:
        # curr_limit = min(1000, int(limit))
        # else:
        # curr_limit = 1000
        if limit:
            return f"LIMIT {limit}"
        return f""

    def match_node(self, node, var_name):
        if node["id"]:
            return f"({var_name}:{node['type']} {{id: '{node['id']}'}})"
        else:
            return f"({var_name}:{node['type']})"

    def where_construct(self, node, var_name):
        properties = []
        if node["id"]:
            return properties
        for key, property in node["properties"].items():
            if key == "start":
                properties.append(f"{var_name}.{key} >= {property}")
            elif key == "end":
                properties.append(f"{var_name}.{key} <= {property}")
            else:
                properties.append(f"{var_name}.{key} =~ '(?i){property}'")
        return properties

    def parse_neo4j_results(self, results, graph_components, result_type):
        (nodes, edges, _, _, meta_data) = self.process_result(
            results, graph_components, result_type
        )
        return {
            "nodes": nodes,
            "edges": edges,
            "node_count": meta_data.get("node_count", 0),
            "edge_count": meta_data.get("edge_count", 0),
            "node_count_by_label": meta_data.get("node_count_by_label", []),
            "edge_count_by_label": meta_data.get("edge_count_by_label", []),
        }

    def parse_and_serialize(self, input, schema, graph_components, result_type):
        parsed_result = self.parse_neo4j_results(input, graph_components, result_type)
        return parsed_result

    def convert_to_dict(self, results, schema, graph_components):
        graph_components["properties"] = True
        (_, _, node_dict, edge_dict, _) = self.process_result(results, graph_components)
        return (node_dict, edge_dict)

    def process_result_graph(self, results, graph_components):
        """Parse results into graph format."""
        nodes = []
        edges = []
        node_dict = {}
        node_to_dict = {}
        edge_to_dict = {}
        node_type = set()
        edge_type = set()
        visited_relations = set()

        for record in results:
            for item in record.values():
                if isinstance(item, neo4j.graph.Node):
                    node_id = f"{list(item.labels)[0]} {item['id']}"
                    if node_id not in node_dict:
                        node_type_name = list(item.labels)[0]
                        node_data = {
                            "data": {
                                "id": node_id,
                                "type": node_type_name,
                            }
                        }

                        # Process properties based on schema
                        if self.schema_manager:
                            node_schema = self.schema_manager.get_node_type(
                                node_type_name
                            )
                            if node_schema:
                                # Add all valid properties from schema
                                for key, value in item.items():
                                    if graph_components["properties"]:
                                        if (
                                            key != "id"
                                            and self.schema_manager.is_valid_property(
                                                node_type_name, key
                                            )
                                        ):
                                            node_data["data"][key] = value
                                    else:
                                        # Use schema-defined identifier field
                                        identifier_field = (
                                            self.schema_manager.get_identifier_field(
                                                node_type_name
                                            )
                                        )
                                        if identifier_field and key == identifier_field:
                                            node_data["data"]["name"] = value

                        if "name" not in node_data["data"]:
                            node_data["data"]["name"] = node_id

                        nodes.append(node_data)
                        if node_data["data"]["type"] not in node_type:
                            node_type.add(node_data["data"]["type"])
                            node_to_dict[node_data["data"]["type"]] = []
                        node_to_dict[node_data["data"]["type"]].append(node_data)
                        node_dict[node_id] = node_data

                elif isinstance(item, neo4j.graph.Relationship):
                    source_label = list(item.start_node.labels)[0]
                    target_label = list(item.end_node.labels)[0]
                    source_id = f"{source_label} {item.start_node['id']}"
                    target_id = f"{target_label} {item.end_node['id']}"
                    rel_type = item.type

                    edge_data = {
                        "data": {
                            "edge_id": f"{source_label}_{rel_type}_{target_label}",
                            "label": rel_type,
                            "source": source_id,
                            "target": target_id,
                        }
                    }

                    # Process edge properties based on schema
                    if self.schema_manager:
                        rel_schema = self.schema_manager.get_relationship_type(rel_type)
                        if rel_schema:
                            for key, value in item.items():
                                if self.schema_manager.is_valid_property(
                                    rel_type, key, is_relationship=True
                                ):
                                    edge_data["data"][key] = value

                    temp_relation_id = f"{source_id} - {rel_type} - {target_id}"
                    if temp_relation_id not in visited_relations:
                        edges.append(edge_data)
                        visited_relations.add(temp_relation_id)

                        if edge_data["data"]["label"] not in edge_type:
                            edge_type.add(edge_data["data"]["label"])
                            edge_to_dict[edge_data["data"]["label"]] = []
                        edge_to_dict[edge_data["data"]["label"]].append(edge_data)

        return (nodes, edges, node_to_dict, edge_to_dict)

    def process_result_count(
        self, node_and_edge_count, count_by_label, graph_components
    ):
        node_count_by_label = []
        edge_count_by_label = []
        node_count = 0
        edge_count = 0

        node_count += node_and_edge_count.get("total_nodes", 0)
        edge_count += node_and_edge_count.get("total_edges", 0)
        # build edge type set
        node_count_aggregate = {}
        ege_count_aggregate = {}

        if len(count_by_label) != 0:
            # initialize node count aggreate dictionary where the key is the label.
            for node in graph_components["nodes"]:
                node_type = node["type"]
                node_count_aggregate[node_type] = {"count": 0}

            # initialize edge count aggreate dictionary where the key is the label.
            for predicate in graph_components["predicates"]:
                edge_type = predicate["type"].replace(" ", "_").lower()
                ege_count_aggregate[edge_type] = {"count": 0}

            # update node count aggregate dictionary with the count of each label
            for key, value in count_by_label.items():
                node_type_key = "_".join(key.split("_")[1:])
                if node_type_key in node_count_aggregate:
                    node_count_aggregate[node_type_key]["count"] += value

            # update edge count aggregate dictionary with the count of each label
            for key, value in count_by_label.items():
                edge_type_key = "_".join(key.split("_")[1:])
                if edge_type_key in ege_count_aggregate:
                    ege_count_aggregate[edge_type_key]["count"] += value

            # update the way node count by label and edge count by label are represented
            for key, value in node_count_aggregate.items():
                node_count_by_label.append({"label": key, "count": value["count"]})

            for key, value in ege_count_aggregate.items():
                edge_count_by_label.append({"label": key, "count": value["count"]})

        meta_data = {
            "node_count": node_count,
            "edge_count": edge_count,
            "node_count_by_label": node_count_by_label,
            "edge_count_by_label": edge_count_by_label,
        }

        return meta_data

    def process_result(self, results, graph_components, result_type):
        match_result = results
        node_and_edge_count = {}
        count_by_label = {}
        nodes = []
        edges = []
        node_to_dict = {}
        edge_to_dict = {}
        meta_data = {}

        if len(results) > 0:
            node_and_edge_count = results[0]

        if len(results) > 1:
            count_by_label = results[1]

        if result_type == "graph":
            nodes, edges, node_to_dict, edge_to_dict = self.process_result_graph(
                match_result, graph_components
            )

        if result_type == "count":
            meta_data = self.process_result_count(
                node_and_edge_count, count_by_label, graph_components
            )

        return (nodes, edges, node_to_dict, edge_to_dict, meta_data)

    def parse_id(self, request):
        """Parse and normalize entity IDs based on schema."""
        if not self.schema_manager:
            raise ValueError("Schema manager is required for ID parsing")

        nodes = request["nodes"]
        for node in nodes:
            node_type = node["type"]
            # Validate node type exists in schema
            if not self.schema_manager.get_node_type(node_type):
                raise ValueError(f"Node type '{node_type}' not found in schema")

            # Handle ID normalization based on schema
            if node["id"]:
                identifier_field = self.schema_manager.get_identifier_field(node_type)
                if identifier_field:
                    # If there's a schema-defined identifier field, use it
                    if self.schema_manager.is_valid_identifier(node_type, node["id"]):
                        node["properties"][identifier_field] = node["id"].lower()
                        node["id"] = ""
                    else:
                        node["id"] = node["id"].lower()
                else:
                    # If no identifier field defined, just normalize the ID
                    node["id"] = node["id"].lower()

        return request
