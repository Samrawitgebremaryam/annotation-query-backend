from typing import List
import logging
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase, GraphDatabase, graph
import glob
import os
from neo4j.graph import Node, Relationship
from app.services.query_generator_interface import QueryGeneratorInterface

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CypherQueryGenerator(QueryGeneratorInterface):
    def __init__(self, dataset_path: str):
        self.sync_driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI"),
            auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
        )
        self.async_driver = AsyncGraphDatabase.driver(
            os.getenv("NEO4J_URI"),
            auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
        )
        # self.dataset_path = dataset_path
        # self.load_dataset(self.dataset_path)

    async def close(self):
        await self.async_driver.close()
        self.sync_driver.close()  # sync driver doesn't need await

    async def run_query_async(self, query_code, run_count=True):
        results = []
        if isinstance(query_code, list):
            find_query = query_code[0]
            total_count_query = query_code[1]
            label_count_query = query_code[2]
        else:
            find_query = query_code
            total_count_query = None
            label_count_query = None

        async with self.async_driver.session() as session:
            result = await session.run(find_query)
            records = await result.values()
            results.append([dict(zip(result.keys(), record)) for record in records])

        if run_count and total_count_query:
            try:
                async with self.async_driver.session() as session:
                    result = await session.run(total_count_query)
                    records = await result.values()
                    results.append(
                        [dict(zip(result.keys(), record)) for record in records]
                    )
            except Exception as e:
                logger.error(f"Error running total count query: {str(e)}")
                results.append([])

        if run_count and label_count_query:
            try:
                async with self.async_driver.session() as session:
                    result = await session.run(label_count_query)
                    records = await result.values()
                    results.append(
                        [dict(zip(result.keys(), record)) for record in records]
                    )
            except Exception as e:
                logger.error(f"Error running label count query: {str(e)}")
                results.append([])

        return results

    def run_query(self, query_code, run_count=True):
        results = []
        if isinstance(query_code, list):
            find_query = query_code[0]
            total_count_query = query_code[1]
            label_count_query = query_code[2]
        else:
            find_query = query_code
            total_count_query = None
            label_count_query = None

        with self.sync_driver.session() as session:
            result = session.run(find_query)
            results.append([dict(zip(result.keys(), record)) for record in result])

        if run_count:
            if total_count_query:
                try:
                    with self.sync_driver.session() as session:
                        result = session.run(total_count_query)
                        results.append(
                            [dict(zip(result.keys(), record)) for record in result]
                        )
                except Exception as e:
                    logger.error(f"Error running total count query: {str(e)}")
                    results.append([])
            if label_count_query:
                try:
                    with self.sync_driver.session() as session:
                        result = session.run(label_count_query)
                        results.append(
                            [dict(zip(result.keys(), record)) for record in result]
                        )
                except Exception as e:
                    logger.error(f"Error running label count query: {str(e)}")
                    results.append([])

        return results

    def load_dataset(self, path: str) -> None:
        if not os.path.exists(path):
            raise ValueError(f"Dataset path '{path}' does not exist.")

        paths = glob.glob(os.path.join(path, "**/*.cypher"), recursive=True)
        if not paths:
            raise ValueError(f"No .cypher files found in dataset path '{path}'.")

        # Separate nodes and edges
        nodes_paths = [p for p in paths if p.endswith("nodes.cypher")]
        edges_paths = [p for p in paths if p.endswith("edges.cypher")]

        # Helper function to process files
        def process_files(file_paths, file_type):
            for file_path in file_paths:
                logger.info(f"Start loading {file_type} dataset from '{file_path}'...")
                try:
                    with open(file_path, "r") as file:
                        data = file.read()
                        for line in data.splitlines():
                            self.run_query(line)
                except Exception as e:
                    logger.error(
                        f"Error loading {file_type} dataset from '{file_path}': {e}"
                    )

        # Process nodes and edges files
        process_files(nodes_paths, "nodes")
        process_files(edges_paths, "edges")

        logger.info(
            f"Finished loading {len(nodes_paths)} nodes and {len(edges_paths)} edges datasets."
        )

    async def query_Generator(self, requests, node_map, limit=None, node_only=False):
        """Generate Cypher queries based on the request"""
        try:
            nodes = requests["nodes"]

            if "predicates" in requests:
                predicates = requests["predicates"]
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
            used_nodes = set()
            list_of_node_ids = []

            if not predicates:
                # Case when there are no predicates
                for node in nodes:
                    var_name = f"n_{node['node_id']}"
                    match_no_preds.append(self.match_node(node, var_name))
                    if node.get("properties"):
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

                if not isinstance(cypher_query, str):
                    logger.error(f"Generated query is not a string: {cypher_query}")
                    raise ValueError(f"Generated query is not a string: {cypher_query}")

                cypher_queries.append(cypher_query)

                # Prepare query clauses for count query
                query_clauses = {
                    "match_no_preds": match_no_preds,
                    "return_no_preds": return_no_preds,
                    "where_no_preds": where_no_preds,
                    "list_of_node_ids": list_of_node_ids,
                    "predicates": predicates,
                }

                try:
                    count_queries = self.construct_count_clause(query_clauses)
                    if count_queries and isinstance(count_queries, (list, tuple)):
                        cypher_queries.extend(count_queries)
                    else:
                        logger.warning("Count queries were not generated correctly")
                except Exception as e:
                    logger.error(f"Error generating count queries: {str(e)}")
                    # Continue without count queries if they fail
                    pass

            else:
                # Case when there are predicates
                query_clauses = {
                    "match_preds": match_preds,
                    "where_preds": where_preds,
                    "match_no_preds": match_no_preds,
                    "return_no_preds": return_no_preds,
                    "where_no_preds": where_no_preds,
                    "list_of_node_ids": list_of_node_ids,
                    "return_preds": return_preds,
                    "predicates": predicates,
                    "full_return_preds": return_preds,
                }
                cypher_query = self.construct_union_clause(query_clauses, limit)
                if not isinstance(cypher_query, str):
                    raise ValueError(f"Generated query is not a string: {cypher_query}")
                cypher_queries.append(cypher_query)

            return cypher_queries

        except Exception as e:
            logger.error(f"Error in query generation: {str(e)}")
            raise

    def construct_clause(self, match_clause, return_clause, where_no_preds, limit):
        try:
            if not match_clause or not isinstance(match_clause, (list, tuple)):
                raise ValueError("match_clause must be a non-empty list")
            if not return_clause or not isinstance(return_clause, (list, tuple)):
                raise ValueError("return_clause must be a non-empty list")

            match_str = f"MATCH {', '.join(str(m) for m in match_clause)}"
            return_str = f"RETURN {', '.join(str(r) for r in return_clause)}"

            if where_no_preds and len(where_no_preds) > 0:
                where_str = f"WHERE {' AND '.join(str(w) for w in where_no_preds)}"
                query = f"{match_str} {where_str} {return_str}"
            else:
                query = f"{match_str} {return_str}"

            if limit is not None:
                query = f"{query} {self.limit_query(limit)}"

            return query.strip()
        except Exception as e:
            logger.error(f"Error constructing clause: {str(e)}")
            raise ValueError(f"Failed to construct query clause: {str(e)}")

    def construct_optional_clause(
        self, match_clause, return_clause, where_no_preds, limit
    ):
        try:
            if not match_clause or not isinstance(match_clause, (list, tuple)):
                raise ValueError("match_clause must be a non-empty list")
            if not return_clause or not isinstance(return_clause, (list, tuple)):
                raise ValueError("return_clause must be a non-empty list")

            optional_parts = []
            for match in match_clause:
                optional_parts.append(f"OPTIONAL MATCH {str(match)}")

            optional_str = " ".join(optional_parts)
            return_str = f"RETURN {', '.join(str(r) for r in return_clause)}"

            if where_no_preds and len(where_no_preds) > 0:
                where_str = f"WHERE {' AND '.join(str(w) for w in where_no_preds)}"
                query = f"{optional_str} {where_str} {return_str}"
            else:
                query = f"{optional_str} {return_str}"

            if limit is not None:
                query = f"{query} {self.limit_query(limit)}"

            return query.strip()
        except Exception as e:
            logger.error(f"Error constructing optional clause: {str(e)}")
            raise ValueError(f"Failed to construct optional query clause: {str(e)}")

    def construct_union_clause(self, query_clauses, limit):
        match_no_clause = ""
        where_no_clause = ""
        return_count_no_preds_clause = ""
        match_clause = ""
        where_clause = ""
        return_count_preds_clause = ""

        # Check and construct clause for match with no predicates
        if "match_no_preds" in query_clauses and query_clauses["match_no_preds"]:
            match_no_clause = f"MATCH {', '.join(query_clauses['match_no_preds'])}"
            if "where_no_preds" in query_clauses and query_clauses["where_no_preds"]:
                where_no_clause = (
                    f"WHERE {' AND '.join(query_clauses['where_no_preds'])}"
                )
            return_count_no_preds_clause = "RETURN " + ", ".join(
                query_clauses["return_no_preds"]
            )

        # Construct a clause for match with predicates
        if "match_preds" in query_clauses and query_clauses["match_preds"]:
            match_clause = f"MATCH {', '.join(query_clauses['match_preds'])}"
            if "where_preds" in query_clauses and query_clauses["where_preds"]:
                where_clause = f"WHERE {' AND '.join(query_clauses['where_preds'])}"
            return_count_preds_clause = "RETURN " + ", ".join(
                query_clauses["full_return_preds"]
            )

        clauses = {}

        # Update the query_clauses dictionary with the constructed clauses
        clauses["match_no_clause"] = match_no_clause
        clauses["where_no_clause"] = where_no_clause
        clauses["return_no_clause"] = return_count_no_preds_clause
        clauses["match_clause"] = match_clause
        clauses["where_clause"] = where_clause
        clauses["return_clause"] = return_count_preds_clause

        query = self.construct_call_clause(clauses, limit)
        return query

    def construct_count_clause(self, query_clauses):
        try:
            match_no_clause = ""
            where_no_clause = ""
            match_clause = ""
            where_clause = ""
            return_preds = []
            collect_node_and_edge = ""

            # Validate input
            if not query_clauses or not isinstance(query_clauses, dict):
                raise ValueError("Invalid query_clauses parameter")

            # Construct clause for match with no predicates
            if "match_no_preds" in query_clauses and query_clauses["match_no_preds"]:
                match_no_clause = f"MATCH {', '.join(query_clauses['match_no_preds'])}"
                if (
                    "where_no_preds" in query_clauses
                    and query_clauses["where_no_preds"]
                ):
                    where_no_clause = (
                        f"WHERE {' AND '.join(query_clauses['where_no_preds'])}"
                    )

            # Construct clause for match with predicates
            if "match_preds" in query_clauses and query_clauses["match_preds"]:
                match_clause = f"MATCH {', '.join(query_clauses['match_preds'])}"
                if "where_preds" in query_clauses and query_clauses["where_preds"]:
                    where_clause = f"WHERE {' AND '.join(query_clauses['where_preds'])}"

            if "return_no_preds" in query_clauses and "return_preds" in query_clauses:
                query_clauses["list_of_node_ids"].extend(
                    query_clauses["return_no_preds"]
                )

            if "return_preds" in query_clauses:
                return_preds = query_clauses["return_preds"]

            # Ensure list_of_node_ids exists
            if (
                "list_of_node_ids" not in query_clauses
                or not query_clauses["list_of_node_ids"]
            ):
                logger.warning("No node IDs found in query_clauses")
                return None

            for node_ids in query_clauses["list_of_node_ids"]:
                collect_node_and_edge += (
                    f"COLLECT(DISTINCT {node_ids}) AS {node_ids}_count, "
                )

            if "return_preds" in query_clauses and query_clauses.get("predicates"):
                for i, _ in enumerate(query_clauses["predicates"]):
                    collect_node_and_edge += f"COLLECT(DISTINCT r{i}) AS r{i}_count, "

            collect_node_and_edge = f"WITH {collect_node_and_edge.rstrip(', ')}"

            # Construct the WITH and UNWIND clauses
            combined_nodes = " + ".join(
                [f"{var}_count" for var in query_clauses["list_of_node_ids"]]
            )
            combined_edges = None
            if return_preds:
                combined_edges = " + ".join(
                    [
                        f"r{i}_count"
                        for i in range(len(query_clauses.get("predicates", [])))
                    ]
                )

            with_clause = f"WITH {combined_nodes} AS combined_nodes {f', {combined_edges} AS combined_edges' if combined_edges else ''}"
            unwind_clause = f"UNWIND combined_nodes AS nodes"

            # Construct the RETURN clause
            return_clause = f"RETURN COUNT(DISTINCT nodes) AS total_nodes {', SIZE(combined_edges) AS total_edges' if combined_edges else ''}"

            # Build the query for total node and edge count
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

            # Validate generated query
            if not isinstance(total_count, str):
                raise ValueError(
                    f"Generated total_count query is not a string: {total_count}"
                )

            # Build query for counting by label
            label_clause = (
                "WITH DISTINCT "
                + " + ".join(
                    [f"labels({n})" for n in query_clauses["list_of_node_ids"]]
                )
                + " AS all_labels"
            )

            if not return_preds:
                label_clause += ", " + ", ".join(query_clauses["list_of_node_ids"])
            else:
                label_clause += ", " + ", ".join(return_preds)

            unwind_label_clause = "UNWIND all_labels AS label"

            # Return both queries
            return [total_count.strip(), label_clause + "\n" + unwind_label_clause]

        except Exception as e:
            logger.error(f"Error in construct_count_clause: {str(e)}")
            return None

    def limit_query(self, limit):
        try:
            if limit is None:
                return "LIMIT 1000"  # Default limit

            # Ensure limit is an integer and bounded
            curr_limit = min(1000, int(limit))
            if curr_limit <= 0:
                curr_limit = 1000

            return f"LIMIT {curr_limit}"
        except (ValueError, TypeError) as e:
            logger.warning(
                f"Invalid limit value: {limit}, using default. Error: {str(e)}"
            )
            return "LIMIT 1000"

    def construct_call_clause(self, clauses, limit=None):
        if not ("match_no_clause" in clauses or "match_clause" in clauses):
            raise Exception(
                "Either 'match_clause' or 'match_no_clause' must be present"
            )

        # Build CALL clauses
        call_clauses = []

        # For both nodes without predicate and with predicate
        if "match_no_clause" in clauses and clauses["match_no_clause"]:
            call_clauses.append(
                f'CALL() {{ {clauses["match_no_clause"]} '
                f'{clauses.get("where_no_clause", "")} '
                f'{clauses["return_no_clause"]} '
                f'{self.limit_query(limit) if "return_count_sum" not in clauses else ""} }}'
            )

        if "match_clause" in clauses and clauses["match_clause"]:
            call_clauses.append(
                f'CALL() {{ {clauses["match_clause"]} '
                f'{clauses.get("where_clause", "")} '
                f'{clauses["return_clause"]} '
                f'{self.limit_query(limit) if "return_count_sum" not in clauses else ""} }}'
            )

        # Add any additional return clause sum/normal query
        final_clause = clauses.get("return_count_sum", "RETURN *")
        call_clauses.append(final_clause)

        # Combine clauses into a single string
        return " ".join(call_clauses)

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
            properties.append(f"{var_name}.{key} =~ '(?i){property}'")
        return properties

    def parse_neo4j_results(self, results, all_properties):
        (nodes, edges, _, _, meta_data) = self.process_result(results, all_properties)
        return {
            "nodes": nodes,
            "edges": edges,
            "node_count": meta_data["node_count"],
            "edge_count": meta_data["edge_count"],
            "node_count_by_label": meta_data["node_count_by_label"],
            "edge_count_by_label": meta_data["edge_count_by_label"],
        }

    def parse_and_serialize(self, input, schema, all_properties):
        parsed_result = self.parse_neo4j_results(input, all_properties)
        return parsed_result

    def convert_to_dict(self, results, schema):
        (_, _, node_dict, edge_dict, _) = self.process_result(results, True)
        return (node_dict, edge_dict)

    def process_result(self, results, all_properties):
        match_result = results[0]
        node_count_by_label = []
        edge_count_by_label = []
        node_count = 0
        edge_count = 0
        node_and_edge_count = []
        count_by_label = []

        if len(results) > 2:
            node_and_edge_count = results[1]
        if len(results) > 1:
            count_by_label = results[2]

        nodes = []
        edges = []
        node_dict = {}
        node_to_dict = {}
        edge_to_dict = {}
        node_type = set()
        edge_type = set()
        visited_relations = set()

        named_types = [
            "gene_name",
            "transcript_name",
            "protein_name",
            "pathway_name",
            "term_name",
        ]

        for record in match_result:
            for item in record.values():
                if isinstance(item, graph.Node):
                    node_id = f"{list(item.labels)[0]} {item['id']}"
                    if node_id not in node_dict:
                        node_data = {
                            "data": {
                                "id": node_id,
                                "type": list(item.labels)[0],
                            }
                        }

                        for key, value in item.items():
                            if all_properties:
                                if key != "id" and key != "synonyms":
                                    node_data["data"][key] = value
                            else:
                                if key in named_types:
                                    node_data["data"]["name"] = value
                        if "name" not in node_data["data"]:
                            node_data["data"]["name"] = node_id
                        nodes.append(node_data)
                        if node_data["data"]["type"] not in node_type:
                            node_type.add(node_data["data"]["type"])
                            node_to_dict[node_data["data"]["type"]] = []
                        node_to_dict[node_data["data"]["type"]].append(node_data)
                        node_dict[node_id] = node_data
                elif isinstance(item, graph.Relationship):
                    source_label = list(item.start_node.labels)[0]
                    target_label = list(item.end_node.labels)[0]
                    source_id = (
                        f"{list(item.start_node.labels)[0]} {item.start_node['id']}"
                    )
                    target_id = f"{list(item.end_node.labels)[0]} {item.end_node['id']}"
                    edge_data = {
                        "data": {
                            # "id": item.id,
                            "edge_id": f"{source_label}_{item.type}_{target_label}",
                            "label": item.type,
                            "source": source_id,
                            "target": target_id,
                        }
                    }
                    temp_relation_id = f"{source_id} - {item.type} - {target_id}"
                    if temp_relation_id in visited_relations:
                        continue
                    visited_relations.add(temp_relation_id)

                    for key, value in item.items():
                        if key == "source":
                            edge_data["data"]["source_data"] = value
                        else:
                            edge_data["data"][key] = value
                    edges.append(edge_data)
                    if edge_data["data"]["label"] not in edge_type:
                        edge_type.add(edge_data["data"]["label"])
                        edge_to_dict[edge_data["data"]["label"]] = []
                    edge_to_dict[edge_data["data"]["label"]].append(edge_data)

        if node_and_edge_count:
            for count_record in node_and_edge_count:
                node_count += count_record.get("total_nodes", 0)
                edge_count += count_record.get("total_edges", 0)

        if count_by_label:
            for count_record in count_by_label:
                node_count_by_label.extend(count_record.get("nodes_count_by_label", []))
                edge_count_by_label.extend(count_record.get("edges_count_by_type", []))

        meta_data = {
            "node_count": node_count,
            "edge_count": edge_count,
            "node_count_by_label": node_count_by_label,
            "edge_count_by_label": edge_count_by_label,
        }

        return (nodes, edges, node_to_dict, edge_to_dict, meta_data)

    def parse_id(self, request):
        nodes = request["nodes"]
        named_types = {"gene": "gene_name", "transcript": "transcript_name"}
        prefixes = ["ensg", "enst"]

        for node in nodes:
            is_named_type = node["type"] in named_types
            id = node["id"].lower()
            is_name_as_id = all(not id.startswith(prefix) for prefix in prefixes)
            no_id = node["id"] != ""
            if is_named_type and is_name_as_id and no_id:
                node_type = named_types[node["type"]]
                node["properties"][node_type] = node["id"]
                node["id"] = ""
            node["id"] = node["id"].lower()
        return request
