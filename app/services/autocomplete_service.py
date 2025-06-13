import logging
from typing import List, Dict, Optional, Any
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from neo4j import GraphDatabase
import yaml
import os
from dotenv import load_dotenv
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AutocompleteService:
    def __init__(self, config_path: str = "config/elasticsearch_config.yaml"):
        """Initialize AutocompleteService with Elasticsearch configuration."""
        load_dotenv()
        self._load_config(config_path)
        self._init_elasticsearch()
        self.node_types = set()
        self.last_indexed_time = None

    def _load_config(self, config_path: str) -> None:
        """Load Elasticsearch configuration from YAML file."""
        try:
            with open(config_path, "r") as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise ValueError(f"Invalid configuration: {e}")

    def _init_elasticsearch(self) -> None:
        """Initialize Elasticsearch client with authentication."""
        try:
            es_username = os.getenv("ES_USERNAME")
            es_password = os.getenv("ES_PASSWORD")
            if not es_username or not es_password:
                raise ValueError("ES_USERNAME and ES_PASSWORD must be set")

            self.es = Elasticsearch(
                hosts=[self.config["elasticsearch"]["host"]],
                basic_auth=(es_username, es_password),
                verify_certs=False,  # TODO: Use ca_certs in production
                request_timeout=60,
                retry_on_timeout=True,
                max_retries=3,
            )

            if not self.es.ping():
                raise ConnectionError("Failed to connect to Elasticsearch")

            logger.info("Successfully connected to Elasticsearch")
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch: {e}")
            raise

    def _create_index_for_node_type(self, node_type: str) -> None:
        """Create Elasticsearch index for a specific node type with dynamic mapping."""
        index_name = node_type.lower()
        if not self.es.indices.exists(index=index_name):
            mapping = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "analysis": {
                        "analyzer": {
                            "simple_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": ["lowercase", "asciifolding"],
                            }
                        }
                    },
                },
                "mappings": {
                    "dynamic": True,  # Allow dynamic fields
                    "properties": {
                        "name": {"type": "keyword"},
                        "neo4j_id": {"type": "keyword"},
                        "node_type": {"type": "keyword"},
                        "name_suggest": {
                            "type": "completion",
                            "analyzer": "simple_analyzer",
                            "preserve_separators": True,
                            "preserve_position_increments": True,
                            "max_input_length": 100,
                        },
                    }
                },
            }
            try:
                self.es.indices.create(index=index_name, body=mapping)
                logger.info(f"Created index {index_name}")
            except Exception as e:
                logger.error(f"Failed to create index {index_name}: {e}")
                raise RuntimeError(f"Index creation failed: {e}")

    def reindex_from_neo4j(
        self,
        neo4j_driver: GraphDatabase.driver,
        batch_size: int = 1000,
        incremental: bool = True,
        labels: Optional[List[str]] = None,
    ) -> None:
        """Reindex Neo4j nodes to Elasticsearch with dynamic properties and deletion handling."""
        try:
            # Test Neo4j connection
            with neo4j_driver.session() as session:
                session.run("RETURN 1").single()

            # Use delta indexing if incremental
            last_indexed_time = self.last_indexed_time if incremental else 0
            self.last_indexed_time = int(datetime.utcnow().timestamp())

            # Fetch labels if not provided
            if not labels:
                with neo4j_driver.session() as session:
                    result = session.run("CALL db.labels() YIELD label RETURN label")
                    labels = [record["label"] for record in result]
                    logger.info(f"Found node labels: {labels}")

            if not labels:
                logger.warning("No labels found in Neo4j database")
                return

            for label in labels:
                nodes_by_type: Dict[str, List[Dict]] = {}
                skip = 0
                limit = 1000

                while True:
                    query = f"""
                    MATCH (n:{label})
                    WHERE any(prop IN keys(n) WHERE prop ENDS WITH '_name')
                    AND (n.last_updated IS NULL OR n.last_updated > $last_indexed_time)
                    RETURN n, labels(n) as labels, 
                           [prop IN keys(n) WHERE prop ENDS WITH '_name' | prop][0] as name_field,
                           n[[prop IN keys(n) WHERE prop ENDS WITH '_name' | prop][0]] as name,
                           properties(n) as props
                    SKIP $skip LIMIT $limit
                    """
                    with neo4j_driver.session() as session:
                        result = session.run(
                            query, last_indexed_time=last_indexed_time, skip=skip, limit=limit
                        )

                        batch_empty = True
                        for record in result:
                            batch_empty = False
                            if not record["name"] or not record["labels"]:
                                continue
                            node_type = record["labels"][0].lower()
                            props = record["props"]

                            # Build node document with only the name for suggestions
                            node = {
                                "name": record["name"],
                                "neo4j_id": str(record["n"].id),
                                "node_type": node_type,
                                "name_suggest": {"input": [record["name"]]}
                            }

                            # Add other string/list properties dynamically
                            for key, value in props.items():
                                if key in ["last_updated", "start", "end"]:
                                    continue
                                if isinstance(value, (str, list)) and value:
                                    node[key] = value

                            # Use 'id' as domain_id if present
                            if "id" in props and isinstance(props["id"], str):
                                node["domain_id"] = props["id"]

                            if node_type not in nodes_by_type:
                                nodes_by_type[node_type] = []
                            nodes_by_type[node_type].append(node)
                            self.node_types.add(node_type)

                        if batch_empty:
                            break
                        skip += limit

                if not nodes_by_type:
                    logger.info(f"No nodes found for label {label}")
                    continue

                for node_type, nodes in nodes_by_type.items():
                    index_name = node_type
                    logger.info(f"Indexing {len(nodes)} nodes for type {node_type}")

                    if not incremental:
                        if self.es.indices.exists(index=index_name):
                            self.es.indices.delete(index=index_name)
                        self._create_index_for_node_type(node_type)
                    else:
                        # Handle deletions
                        neo4j_ids = set(
                            node.get("domain_id", node["neo4j_id"]) for node in nodes
                        )
                        es_response = self.es.search(
                            index=index_name,
                            body={"query": {"match_all": {}}, "_source": ["domain_id", "neo4j_id"]},
                            size=10000
                        )
                        es_ids = set(
                            hit["_source"].get("domain_id", hit["_source"]["neo4j_id"])
                            for hit in es_response["hits"]["hits"]
                        )
                        stale_ids = es_ids - neo4j_ids
                        if stale_ids:
                            delete_actions = [
                                {"_op_type": "delete", "_index": index_name, "_id": _id}
                                for _id in stale_ids
                            ]
                            bulk(self.es, delete_actions, chunk_size=batch_size)
                            logger.info(f"Deleted {len(stale_ids)} stale documents from {index_name}")

                    self._create_index_for_node_type(node_type)

                    # Optimize indexing
                    self.es.indices.put_settings(index=index_name, body={"refresh_interval": "30s"})
                    actions = [
                        {
                            "_op_type": "index" if not incremental else "update",
                            "_index": index_name,
                            "_id": node.get("domain_id", node["neo4j_id"]),
                            "_source": (
                                node
                                if not incremental
                                else {"doc": node, "doc_as_upsert": True}
                            ),
                        }
                        for node in nodes
                    ]
                    try:
                        success, failed = bulk(
                            self.es, actions, chunk_size=batch_size, request_timeout=60
                        )
                        if failed:
                            logger.error(f"Failed to index {len(failed)} nodes for {node_type}: {failed}")
                        logger.info(f"Indexed {success} nodes for {node_type}")
                    except Exception as e:
                        logger.error(f"Bulk indexing failed for {node_type}: {str(e)}")
                        raise RuntimeError(f"Bulk indexing failed: {str(e)}")
                    finally:
                        self.es.indices.put_settings(index=index_name, body={"refresh_interval": "1s"})

                    count = self.es.count(index=index_name)["count"]
                    logger.info(f"Total indexed documents for {node_type}: {count}")

        except Exception as e:
            logger.error(f"Failed to reindex from Neo4j: {e}")
            raise RuntimeError(f"Reindexing failed: {e}")

    def search_suggestions(
        self, node_type: str, query: str, size: int = 10
    ) -> List[Dict[str, Any]]:
        """Search for autocomplete suggestions for a specific node type."""
        try:
            if not node_type or not isinstance(node_type, str):
                raise ValueError("Node type must be a non-empty string")
            if not query or not isinstance(query, str) or len(query) > 100:
                raise ValueError("Query must be a non-empty string with max length 100")
            if size < 1 or size > 100:
                raise ValueError("Size must be between 1 and 100")

            index_name = node_type.lower()
            if not self.es.indices.exists(index=index_name):
                raise ValueError(f"Index for node type {node_type} does not exist")

            suggest_query = {
                "suggest": {
                    "name-suggest": {
                        "prefix": query.lower(),
                        "completion": {
                            "field": "name_suggest",
                            "size": size,
                            "skip_duplicates": True,
                            "fuzzy": False,
                        },
                    }
                }
            }

            response = self.es.search(index=index_name, body=suggest_query)
            suggestions = []
            if "suggest" in response and "name-suggest" in response["suggest"]:
                for suggestion in response["suggest"]["name-suggest"]:
                    for option in suggestion["options"]:
                        suggestion_dict = {
                            "name": option["_source"]["name"],
                            "id": option["_source"].get("domain_id", option["_source"]["neo4j_id"]),
                            "score": option["_score"],
                            "node_type": option["_source"]["node_type"]
                        }
                        # Include other properties dynamically
                        for key, value in option["_source"].items():
                            if key not in ["name", "neo4j_id", "node_type", "name_suggest", "domain_id"]:
                                suggestion_dict[key] = value
                        suggestions.append(suggestion_dict)

            return suggestions
        except Exception as e:
            logger.error(f"Failed to search suggestions for query '{query}' in {node_type}: {e}")
            raise RuntimeError(f"Search failed: {e}")

    def search_all_suggestions(
        self, query: str, size: int = 10
    ) -> List[Dict[str, Any]]:
        """Search for autocomplete suggestions across all node types."""
        try:
            if not query or not isinstance(query, str) or len(query) > 100:
                raise ValueError("Query must be a non-empty string with max length 100")
            if size < 1 or size > 100:
                raise ValueError("Size must be between 1 and 100")

            if not self.node_types:
                raise ValueError("No node types indexed")

            indices = ",".join(self.node_types)
            suggest_query = {
                "suggest": {
                    "name-suggest": {
                        "prefix": query.lower(),
                        "completion": {
                            "field": "name_suggest",
                            "size": size,
                            "skip_duplicates": True,
                            "fuzzy": False,
                        },
                    }
                }
            }

            response = self.es.search(index=indices, body=suggest_query)
            suggestions = []
            if "suggest" in response and "name-suggest" in response["suggest"]:
                for suggestion in response["suggest"]["name-suggest"]:
                    for option in suggestion["options"]:
                        suggestion_dict = {
                            "name": option["_source"]["name"],
                            "id": option["_source"].get("domain_id", option["_source"]["neo4j_id"]),
                            "score": option["_score"],
                            "node_type": option["_source"]["node_type"]
                        }
                        # Include other properties dynamically
                        for key, value in option["_source"].items():
                            if key not in ["name", "neo4j_id", "node_type", "name_suggest", "domain_id"]:
                                suggestion_dict[key] = value
                        suggestions.append(suggestion_dict)

            return suggestions
        except Exception as e:
            logger.error(f"Failed to search all suggestions for query '{query}': {e}")
            raise RuntimeError(f"Search failed: {e}")