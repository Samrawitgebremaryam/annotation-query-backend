from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from collections import defaultdict
from neo4j import GraphDatabase
import nanoid

MINIMUM_EDGES_TO_COLLAPSE = 2

@dataclass
class Edge:
    data: Dict[str, Any]

@dataclass
class Node:
    data: Dict[str, Any]

@dataclass
class Graph:
    nodes: List[Node]
    edges: List[Edge]
    
    @classmethod
    def from_dict(cls, data: Dict) -> "Graph":
        return cls(
            nodes=[Node(data=n["data"]) for n in data.get("nodes", [])],
            edges=[Edge(data=e["data"]) for e in data.get("edges", [])]
        )

    def to_dict(self) -> Dict:
        return {
            "nodes": [{"data": n.data} for n in self.nodes],
            "edges": [{"data": e.data} for e in self.edges]
        }

class Neo4jConnection:
    def __init__(self, uri: str, user: str, password: str, max_retries: int = 3):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.max_retries = max_retries

    def close(self):
        if self.driver:
            self.driver.close()

    def get_graph_data(self, request_data: Dict, limit: int = 1000) -> Dict[str, Any]:
        try:
            with self.driver.session() as session:
                # Build the query based on the request
                query_parts = []
                parameters = {}

                # Process nodes
                for node in request_data.get("nodes", []):
                    node_type = node.get("type", "").lower()
                    node_id = node.get("node_id")
                    properties = node.get("properties", {})

                    # Add node match clause
                    node_conditions = []
                    for prop, value in properties.items():
                        param_key = f"{node_id}_{prop}"
                        parameters[param_key] = value
                        node_conditions.append(f"toLower({node_id}.{prop}) = toLower(${param_key})")

                    match_clause = f"MATCH ({node_id}:{node_type})"
                    if node_conditions:
                        match_clause += f" WHERE {' AND '.join(node_conditions)}"
                    query_parts.append(match_clause)

                # Process relationships
                for predicate in request_data.get("predicates", []):
                    source = predicate.get("source")
                    target = predicate.get("target")
                    rel_type = predicate.get("type")
                    query_parts.append(f"MATCH ({source})-[r:{rel_type}]->({target})")

                # Combine query parts
                query = " ".join(query_parts)
                query += " RETURN collect(distinct n1) + collect(distinct n2) as nodes, collect(distinct r) as relationships"
                query += " LIMIT $limit"

                # Execute query
                result = session.run(query, parameters={**parameters, "limit": limit})
                record = result.single()

                if not record:
                    return {"nodes": [], "edges": []}

                nodes = []
                edges = []

                # Process nodes
                for node in record["nodes"]:
                    node_properties = dict(node.items())
                    node_type = list(node.labels)[0].lower()
                    raw_node_id = node_properties.get("id", str(node.id))
                    
                    # Create formatted node data with node_type in id
                    formatted_data = {
                        "id": f"{node_type} {raw_node_id}",  
                        "type": node_type,
                        "label": node_type,
                        "name": f"{node_type} {raw_node_id}"
                    }

                    # Add all properties from Neo4j node
                    for key, value in node_properties.items():
                        # Skip synonyms field
                        if key == "synonyms":
                            continue
                        elif isinstance(value, (int, float)):
                            formatted_data[key] = str(value)
                        else:
                            formatted_data[key] = value

                    # Add gene_name to transcript nodes
                    if node_type == "transcript" and "gene_name" not in formatted_data:
                        # Find the gene node and get its gene_name
                        for gene_node in record["nodes"]:
                            if "gene" in gene_node.labels:
                                formatted_data["gene_name"] = dict(gene_node.items()).get("gene_name")
                                break

                    nodes.append({"data": formatted_data})

                # Process relationships
                for rel in record["relationships"]:
                    start_node_type = list(rel.start_node.labels)[0].lower()
                    end_node_type = list(rel.end_node.labels)[0].lower()
                    
                    start_node_id = rel.start_node.get("id", str(rel.start_node.id))
                    end_node_id = rel.end_node.get("id", str(rel.end_node.id))
                    
                    start_id = f"{start_node_type} {start_node_id}"
                    end_id = f"{end_node_type} {end_node_id}"

                    edges.append({
                        "data": {
                            "id": f"e{nanoid.generate(size=10)}",
                            "edge_id": f"{start_node_type}_{rel.type.lower()}_{end_node_type}",
                            "label": rel.type.lower(),
                            "source": start_id,
                            "target": end_id
                        }
                    })

                return {"nodes": nodes, "edges": edges}

        except Exception as e:
            print(f"Error getting graph data: {str(e)}")
            raise

def group_edges(result_graph: Graph, request: Dict) -> List[Dict]:
    """Group edges by edge_id and handle any node types"""
    edge_groups = defaultdict(list)

    for edge in result_graph.edges:
        edge_id = edge.data.get("edge_id")
        if edge_id:
            edge_groups[edge_id].append(edge)

    edge_groupings = []
    for edge_id, edges in edge_groups.items():
        if len(edges) >= MINIMUM_EDGES_TO_COLLAPSE:
            source_type, relationship, target_type = edge_id.split("_", 2)

            source_groups = {}
            target_groups = {}

            for edge in edges:
                source = edge.data.get("source")
                target = edge.data.get("target")

                if source not in source_groups:
                    source_groups[source] = []
                if target not in target_groups:
                    target_groups[target] = []

                source_groups[source].append(edge)
                target_groups[target].append(edge)

            grouped_by = "target" if len(source_groups) > len(target_groups) else "source"
            groups = target_groups if grouped_by == "target" else source_groups

            edge_groupings.append({
                "count": len(edges),
                "edge_id": edge_id,
                "edge_type": edges[0].data.get("label"),
                "grouped_by": grouped_by,
                "groups": groups,
            })

    return edge_groupings

def group_graph(result_graph: Graph, request: Dict) -> Graph:
    """Group nodes and edges based on request data"""
    new_graph = Graph(nodes=[], edges=[])
    
    # Get source and target types from request predicates
    predicates = request.get("predicates", [])
    if not predicates:
        return result_graph
        
    source_type = None
    target_type = None
    relationship_type = None
    
    for predicate in predicates:
        # Get node types from request nodes
        for node in request.get("nodes", []):
            if node.get("node_id") == predicate.get("source"):
                source_type = node.get("type", "").lower()
            elif node.get("node_id") == predicate.get("target"):
                target_type = node.get("type", "").lower()
        relationship_type = predicate.get("type", "").lower()
        break  # Use first predicate for grouping
    
    if not (source_type and target_type and relationship_type):
        return result_graph
    
    # Get source and target nodes
    source_nodes = [node for node in result_graph.nodes if node.data["type"] == source_type]
    target_nodes = [node for node in result_graph.nodes if node.data["type"] == target_type]
    
    if len(target_nodes) >= MINIMUM_EDGES_TO_COLLAPSE:
        # Create parent node with nanoid
        parent_id = nanoid.generate(size=10)
        parent_node = Node(data={
            "id": parent_id,  
            "type": "parent",
            "name": f"{len(target_nodes)} {target_type} nodes"
        })

        # Add parent node first
        new_graph.nodes.append(parent_node)

        # Add source nodes (ensuring they have node type in ID)
        for source_node in source_nodes:
            source_data = source_node.data.copy()
            if not source_data["id"].startswith(f"{source_type} "):
                source_data["id"] = f"{source_type} {source_data['id']}"
            new_graph.nodes.append(Node(data=source_data))

        # Add target nodes with parent reference
        for node in target_nodes:
            node_data = node.data.copy()
            if not node_data["id"].startswith(f"{target_type} "):
                node_data["id"] = f"{target_type} {node_data['id']}"
            node_data["parent"] = parent_id
            new_graph.nodes.append(Node(data=node_data))

        # Create single edge from source to parent
        if source_nodes:
            source_node = source_nodes[0].data
            source_id = source_node["id"]
            if not source_id.startswith(f"{source_type} "):
                source_id = f"{source_type} {source_id}"
            
            new_edge = Edge(data={
                "id": f"e{nanoid.generate(size=10)}",
                "edge_id": f"{source_type}_{relationship_type}_{target_type}",
                "label": relationship_type,
                "source": source_id,
                "target": parent_id
            })
            new_graph.edges = [new_edge]
    else:
        return result_graph
    
    return new_graph
