from typing import List
import logging
from dotenv import load_dotenv
import neo4j
from app.services.query_generator_interface import QueryGeneratorInterface
from neo4j import GraphDatabase
import glob
import os
from neo4j.graph import Node, Relationship
load_dotenv()
# what is the value 
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CypherQueryGenerator(QueryGeneratorInterface):
    def __init__(self, dataset_path: str):
        self.driver = GraphDatabase.driver(
            os.getenv('NEO4J_URI'),
            auth=(os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))
        )
        # self.dataset_path = dataset_path
        # self.load_dataset(self.dataset_path)

    def close(self):
        self.driver.close()

    async def load_dataset(self, path: str) -> None:
        if not os.path.exists(path):
            raise ValueError(f"Dataset path '{path}' does not exist.")

        paths = glob.glob(os.path.join(path, "**/*.cypher"), recursive=True)
        if not paths:
            raise ValueError(f"No .cypher files found in dataset path '{path}'.")

        # Separate nodes and edges
        nodes_paths = [p for p in paths if p.endswith("nodes.cypher")]
        edges_paths = [p for p in paths if p.endswith("edges.cypher")]

        # Helper function to process files
        async def process_files(file_paths, file_type):
            for file_path in file_paths:
                logger.info(f"Start loading {file_type} dataset from '{file_path}'...")
                try:
                    with open(file_path, 'r') as file:
                        data = file.read()
                        for line in data.splitlines():
                            await self.run_query(line)
                except Exception as e:
                    logger.error(f"Error loading {file_type} dataset from '{file_path}': {e}")

        # Process nodes and edges files
        await process_files(nodes_paths, "nodes")
        await process_files(edges_paths, "edges")

        logger.info(f"Finished loading {len(nodes_paths)} nodes and {len(edges_paths)} edges datasets.")

    async def run_query(self, query_code):
        if isinstance(query_code, list):
            find_query = query_code[0]
            count_query = query_code[1]
        else:
            find_query = query_code
            count_query = None
    
        async with self.driver.session() as session:  # Changed from async_session to session
            # Execute main query
            main_results = await session.run(find_query)
            results = [await main_results.data()]
            
            # Start count query if it exists
            if count_query:
                count_results = await session.run(count_query)
                count_data = await count_results.data()
                results.append(count_data)
            
            return results
    
    def logic_detail(self, requests):
        logic = requests.get('logic', None)  
        sign_operator = "="  
        logic_predicate = False 
        entities = [] 

        if logic and 'children' in logic:
            for logic_condition in logic['children']:
                operator = logic_condition.get('operator') 

                # Handle different operators
                if operator == 'NOT':
                    sign_operator = "<>" 
                elif operator == 'OR':
                    sign_operator = "OR"  

                if 'predicates' in logic_condition:
                    logic_predicate = True  
                    entities.append(logic_condition['predicates'])
                    
                elif 'nodes' in logic_condition:
                    logic_predicate = False  
                    entities.append(logic_condition['nodes'])

        return sign_operator, logic_predicate, entities  

    def query_Generator(self, requests, node_map, limit=None):
        nodes = requests['nodes']

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

        # Create a mapping of predicate IDs to their types 
        predicate_type_map = {predicate['predicate_id']: predicate['type'] for predicate in predicates}

        # Get the operator and determine if we have a predicate inside our logic
        sign_operator, logic_predicate, entities = self.logic_detail(requests)
        print("#######################################################################################################")
        print(f"Sign operator: {sign_operator}") 
        print(f"Entities: {entities}") 
        print(f"is predicate inside logic: {logic_predicate}")
        print("#######################################################################################################")
        
        logic_nodes = [entity for entity in entities if 'node_id' in entity]
        logic_predicates = []

        for entity in entities:
            if 'predicate_id' in entity:
                logic_predicates.append(entity)
            elif isinstance(entity, list):
                for value in entity:
                    logic_predicates.append({'predicate_id': value})

        if not predicates:
            for node in nodes:
                var_name = f"{node['node_id']}"
                if any('properties' in ln for ln in logic_nodes) or node['properties']:
                    match_no_preds.append(self.match_node(node, var_name))
                    for logic_node in logic_nodes:
                        where_no_preds.extend(self.where_construct(logic_node, var_name, sign_operator))
                else:
                    match_no_preds.append(self.match_label_node(node, var_name))
                    where_no_preds.extend(self.where_label_construct(node, var_name, sign_operator))

                return_no_preds.append(var_name)
                list_of_node_ids.append(var_name)

            print(f"Match no predicates (no predicates case): {match_no_preds}")  
            print(f"Where no predicates (no predicates case): {where_no_preds}")  

            cypher_query = self.construct_clause(match_no_preds, return_no_preds, where_no_preds, limit)
            cypher_queries.append(cypher_query)
            query_clauses = {
                    "match_no_preds": match_no_preds,
                    "return_no_preds": return_no_preds,
                    "where_no_preds": where_no_preds,
                    "list_of_node_ids": list_of_node_ids,
                }
            count = self.construct_count_clause(query_clauses)
            cypher_queries.append(count)
        else:
            for i, predicate in enumerate(predicates):
                predicate_type = predicate['type'].replace(" ", "_").lower()
                source_node = node_map[predicate['source']]
                target_node = node_map[predicate['target']]
                source_var = source_node['node_id']
                target_var = target_node['node_id']
                source_match = self.match_node(source_node, source_var)
                match_preds.append(source_match)
                target_match = self.match_node(target_node, target_var)
                match_preds.append(f"({source_var})-[r{i}:{predicate_type}]->{target_match}")
                return_preds.append(f"r{i}")
                used_nodes.add(predicate['source'])
                used_nodes.add(predicate['target'])
                node_ids.add(source_var)
                node_ids.add(target_var)

            # WHERE clauses using logic nodes and predicates
            for logic_node in logic_nodes:
                var_name = f"{logic_node['node_id']}"
                print(f"Processing logic_node: {logic_node}")
                where_conditions = self.where_construct(logic_node, var_name, sign_operator)
                print(f"Constructed WHERE: {where_conditions}")
                where_preds.extend(where_conditions)  
            print(f"Updated where_preds: {where_preds}")

            or_conditions = []
            for i , logic_predicate in enumerate(logic_predicates):
                # Get the predicate type based on the predicate_id
                predicate_type = predicate_type_map.get(logic_predicate['predicate_id'], logic_predicate['predicate_id'])
                print(f"Processing logic_predicate with predicate_id: {logic_predicate['predicate_id']}")
                print(f"Predicate type for this logic predicate: {predicate_type}")

                # Check if the sign_operator is OR
                if sign_operator == "OR":
                    or_conditions.append(f"type(r{i}) = '{predicate_type}'")  # Collect OR conditions
                else:
                    # Handle other operators (e.g., AND, =, <>) by iterating through predicates
                    for i, predicate in enumerate(predicates):
                        where_preds.extend(self.where_predicate([predicate_type], sign_operator, f"r{i}"))
                        print(f"Extended where_preds with normal conditions: {where_preds}")

            if sign_operator == "OR":
                where_preds.append(f"({' OR '.join(or_conditions)})")  

            for node_id, node in node_map.items():
                if node_id not in used_nodes:
                    var_name = f"{node_id}"
                    match_no_preds.append(self.match_node(node, var_name))
                    where_preds.extend(self.where_construct(node, var_name, sign_operator))  
                    return_no_preds.append(var_name)

            list_of_node_ids = sorted(list(node_ids))
            full_return_preds = return_preds + list_of_node_ids

            if len(match_no_preds) == 0:
                print("=== Debug: Calling construct_clause (No Predicates) ===")
                print(f"match_preds: {match_preds}")
                print(f"full_return_preds: {full_return_preds}")
                print(f"where_preds: {where_preds}")
                print(f"limit: {limit}")

                cypher_query = self.construct_clause(match_preds, full_return_preds, where_preds, limit)
                print(f"Constructed Cypher Query (No Predicates): {cypher_query}")
                cypher_queries.append(cypher_query)
                query_clauses = {
                    "match_preds": match_preds,
                    "full_return_preds": full_return_preds,
                    "where_preds": where_preds,
                    "list_of_node_ids": list_of_node_ids,
                    "return_preds": return_preds
                }
                count = self.construct_count_clause(query_clauses)
                cypher_queries.append(count)
            else:
                print("=== Debug: Calling construct_union_clause ===")
                print(f"match_preds: {match_preds}")
                print(f"full_return_preds: {full_return_preds}")
                print(f"where_preds: {where_preds}")
                print(f"match_no_preds: {match_no_preds}")
                print(f"return_no_preds: {return_no_preds}")
                print(f"where_no_preds: {where_no_preds}")
                print(f"limit: {limit}")
                query_clauses = {
                    "match_preds": match_preds,
                    "full_return_preds": full_return_preds,
                    "where_preds": where_preds,
                    "match_no_preds": match_no_preds,
                    "return_no_preds": return_no_preds,
                    "where_no_preds": where_no_preds,
                    "list_of_node_ids": list_of_node_ids,
                    "return_preds": return_preds
                }
                cypher_query = self.construct_union_clause(query_clauses, limit)
                print(f"Constructed Cypher Query (Union Clause): {cypher_query}")
                cypher_queries.append(cypher_query)
                count = self.construct_count_clause(query_clauses)
                cypher_queries.append(count)

        return cypher_queries

                        
    def construct_clause(self, match_clause, return_clause, where_no_preds, limit):
        match_clause = f"MATCH {', '.join(match_clause)}"
        return_clause = f"RETURN {', '.join(return_clause)}"
        if len(where_no_preds) > 0:
            where_clause = f"WHERE {' AND '.join(where_no_preds)}"
            return f"{match_clause} {where_clause} {return_clause} {self.limit_query(limit)}"
        return f"{match_clause} {return_clause} {self.limit_query(limit)}"

    def construct_union_clause(self, query_clauses, limit):
        match_no_clause = ''
        where_no_clause = ''
        return_count_no_preds_clause = ''
        match_clause = ''
        where_clause = ''
        return_count_preds_clause = ''

        # Check and construct clause for match with no predicates
        if 'match_no_preds' in query_clauses and query_clauses['match_no_preds']:
            match_no_clause = f"MATCH {', '.join(query_clauses['match_no_preds'])}"
            if 'where_no_preds' in query_clauses and query_clauses['where_no_preds']:
                where_no_clause = f"WHERE {' AND '.join(query_clauses['where_no_preds'])}"
            return_count_no_preds_clause = "RETURN " + ', '.join(query_clauses['return_no_preds'])

        # Construct a clause for match with predicates
        if 'match_preds' in query_clauses and query_clauses['match_preds']:
            match_clause = f"MATCH {', '.join(query_clauses['match_preds'])}"
            if 'where_preds' in query_clauses and query_clauses['where_preds']:
                where_clause = f"WHERE {' AND '.join(query_clauses['where_preds'])}"
            return_count_preds_clause = "RETURN " + ', '.join(query_clauses['full_return_preds'])

        clauses = {}

        # Update the query_clauses dictionary with the constructed clauses
        clauses['match_no_clause'] = match_no_clause
        clauses['where_no_clause'] = where_no_clause
        clauses['return_no_clause'] = return_count_no_preds_clause
        clauses['match_clause'] = match_clause
        clauses['where_clause'] = where_clause
        clauses['return_clause'] = return_count_preds_clause
        
        query = self.construct_call_clause(clauses, limit)
        return query

    def construct_count_clause(self, query_clauses):
        match_no_clause = ''
        where_no_clause = ''
        match_clause = ''
        where_clause = ''
        count_clause = ''
        with_clause = ''
        unwind_clause = ''
        return_clause = ''

        # Check and construct clause for match with no predicates
        if 'match_no_preds' in query_clauses and query_clauses['match_no_preds']:
            match_no_clause = f"MATCH {', '.join(query_clauses['match_no_preds'])}"
            if 'where_no_preds' in query_clauses and query_clauses['where_no_preds']:
                where_no_clause = f"WHERE {' AND '.join(query_clauses['where_no_preds'])}"

        # Construct a clause for match with predicates
        if 'match_preds' in query_clauses and query_clauses['match_preds']:
            match_clause = f"MATCH {', '.join(query_clauses['match_preds'])}"
            if 'where_preds' in query_clauses and query_clauses['where_preds']:
                where_clause = f"WHERE {' AND '.join(query_clauses['where_preds'])}"

        # Construct the COUNT clause
        if 'return_no_preds' in query_clauses and 'return_preds' in query_clauses:
            query_clauses['list_of_node_ids'].extend(query_clauses['return_no_preds'])
        for node_ids in query_clauses['list_of_node_ids']:
            count_clause += f"COLLECT(DISTINCT {node_ids}) AS {node_ids}_count, "
        if 'return_preds' in query_clauses:
            for edge_ids in query_clauses['return_preds']:
                count_clause += f"COLLECT(DISTINCT {edge_ids}) AS {edge_ids}_count, "
        count_clause = f"WITH {count_clause.rstrip(', ')}"


        # Construct the WITH and UNWIND clauses
        combined_nodes = ' + '.join([f"{var}_count" for var in query_clauses['list_of_node_ids']])
        combined_edges = None
        if 'return_preds' in query_clauses:
            combined_edges = ' + '.join([f"{var}_count" for var in query_clauses['return_preds']])
        with_clause = f"WITH {combined_nodes} AS combined_nodes {f',{combined_edges} AS combined_edges' if combined_edges else ''}"
        unwind_clause = f"UNWIND combined_nodes AS nodes"

        # Construct the RETURN clause
        return_clause = f"RETURN COUNT(DISTINCT nodes) AS total_nodes {', SIZE(combined_edges) AS total_edges ' if combined_edges else ''}"

        query = f'''
            {match_no_clause}
            {where_no_clause}
            {match_clause}
            {where_clause}
            {count_clause}
            {with_clause}
            {unwind_clause}
            {return_clause}
        '''
        return query


    def limit_query(self, limit):
        if limit:
            curr_limit = min(1000, int(limit))
        else:
            curr_limit = 1000
        return f"LIMIT {curr_limit}"

    def construct_call_clause(self, clauses, limit=None):
        if not ("match_no_clause" in clauses or "match_clause" in clauses):
            raise Exception("Either 'match_clause' or 'match_no_clause' must be present")

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
        if node['id']:
            return f"({var_name}:{node['type']} {{id: '{node['id']}'}})"
        else:
            return f"({var_name}:{node['type']})"
    
    def match_label_node(self, node, var_name):
        if node['id']:
            return f"({var_name} {{id: '{node['id']}'}})"
        else:
            return f"({var_name})"
        

    def where_construct(self, node, var_name, sign_operator):
        properties = []
        
        if sign_operator == "=" and node['id']: 
            return properties

        if sign_operator == "OR":
            or_conditions = []  

            # Loop through properties of the node
            for key, values in node['properties'].items():
                if isinstance(values, list):  
                    or_conditions.extend([f"{var_name}.{key} = '{value}'" for value in values])
                else:
                    or_conditions.append(f"{var_name}.{key} = '{values}'")
            
            # Join conditions with OR and wrap in parentheses
            if or_conditions:
                properties.append(f"({' OR '.join(or_conditions)})")
        
        else:
            for key, value in node['properties'].items():
                properties.append(f"{var_name}.{key} {sign_operator} '{value}'")
        
        return properties



    def where_label_construct(self, node, var_name, sign_operator):
        properties = []

        if sign_operator == "=" and node['id']: 
            return properties

        elif sign_operator == "<>":
            properties.append(f"NOT ({var_name}:{node['type']})")
        else:
            properties.append(f"({var_name}:{node['type']})")

        return properties


    def where_predicate(self, predicate_types, sign_operator, relationship_variable):
        predicate_conditions = []

        if sign_operator == "=":
            for predicate_type in predicate_types:
                predicate_conditions.append(f"type({relationship_variable}) = '{predicate_type}'")

        # Handle inequality condition (<>)
        elif sign_operator == "<>":
            for predicate_type in predicate_types:
                predicate_conditions.append(f"NOT type({relationship_variable}) = '{predicate_type}'")
        
        return predicate_conditions


    async def parse_neo4j_results(self, results, all_properties):
        (nodes, edges, _, _, node_count, edge_count) = await self.process_result(results, all_properties)
        return {"nodes": nodes, "edges": edges, "node_count": node_count, "edge_count": edge_count}

    async def parse_and_serialize(self, input, schema, all_properties):
        parsed_result = await self.parse_neo4j_results(input, all_properties)
        return parsed_result

    async def convert_to_dict(self, results, schema):
        (_, _, node_dict, edge_dict, _, _) = await self.process_result(results, True)
        return (node_dict, edge_dict)

    async def process_result(self, results, all_properties):
        match_result = results[0]
        count_result = results[1] if len(results) > 1 else None
        
        # Initialize values
        nodes = []
        edges = []
        node_dict = {}
        node_to_dict = {}
        edge_to_dict = {}
        node_type = set()
        edge_type = set()
        visited_relations = set()
        node_count = 0
        edge_count = 0
        
        # Process nodes and edges from main query
        for record in match_result:
            for key, item in record.items():
                if isinstance(item, neo4j.graph.Node):
                    # Process node (existing node processing logic)
                    node_id = f"{list(item.labels)[0]} {item['id']}"
                    if node_id not in node_dict:
                        node_data = {
                            "data": {
                                "id": node_id,
                                "type": list(item.labels)[0],
                            }
                        }
                        # Add properties
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
                            node_to_dict[node_data['data']['type']] = []
                        node_to_dict[node_data['data']['type']].append(node_data)
                
                elif isinstance(item, neo4j.graph.Relationship):
                    # Process relationship (existing edge processing logic)
                    source_id = f"{list(item.start_node.labels)[0]} {item.start_node['id']}"
                    target_id = f"{list(item.end_node.labels)[0]} {item.end_node['id']}"
                    temp_relation_id = f"{source_id} - {item.type} - {target_id}"
                    
                    if temp_relation_id not in visited_relations:
                        edge_data = {
                            "data": {
                                "label": item.type,
                                "source": source_id,
                                "target": target_id,
                            }
                        }
                        visited_relations.add(temp_relation_id)
                        
                        for key, value in item.items():
                            if key == 'source':
                                edge_data["data"]["source_data"] = value
                            else:
                                edge_data["data"][key] = value
                        edges.append(edge_data)
                        
                        if edge_data["data"]["label"] not in edge_type:
                            edge_type.add(edge_data["data"]["label"])
                            edge_to_dict[edge_data['data']['label']] = []
                        edge_to_dict[edge_data['data']['label']].append(edge_data)
        
        # Process count results if available
        if count_result:
            for record in count_result:
                node_count = record.get('total_nodes', 0)
                edge_count = record.get('total_edges', 0)
        
        return (nodes, edges, node_to_dict, edge_to_dict, node_count, edge_count)

    def parse_id(self, request):
        nodes = request["nodes"]
        named_types = {"gene": "gene_name", "transcript": "transcript_name"}
        prefixes = ["ensg", "enst"]

        for node in nodes:
            is_named_type = node['type'] in named_types
            id = node["id"].lower()
            is_name_as_id = all(not id.startswith(prefix) for prefix in prefixes)
            no_id = node["id"] != ''
            if is_named_type and is_name_as_id and no_id:
                node_type = named_types[node['type']]
                node['properties'][node_type] = node["id"]
                node['id'] = ''
            node["id"] = node["id"].lower()
        return request