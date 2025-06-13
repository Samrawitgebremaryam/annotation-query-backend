import logging
from typing import List, Dict, Optional, Any
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from neo4j import GraphDatabase
import yaml
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AutocompleteService:
    def __init__(self, config_path: str = "config/elasticsearch_config.yaml"):
        """Initialize AutocompleteService with Elasticsearch configuration.

        Args:
            config_path (str): Path to YAML configuration file.
        """
        load_dotenv()
        self._load_config(config_path)
        self._init_elasticsearch()
        self.node_types = set()  # Track known node types

    def _load_config(self, config_path: str) -> None:
        """Load Elasticsearch configuration from YAML file.

        Args:
            config_path (str): Path to YAML file.

        Raises:
            ValueError: If configuration file cannot be loaded.
        """
        try:
            with open(config_path, "r") as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise ValueError(f"Invalid configuration: {e}")

    def _init_elasticsearch(self) -> None:
        """Initialize Elasticsearch client with authentication.

        Uses verify_certs=False for development; production requires SSL certificates.

        Raises:
            ValueError: If credentials are missing.
            ConnectionError: If Elasticsearch connection fails.
        """
        try:
            es_username = os.getenv("ES_USERNAME")
            es_password = os.getenv("ES_PASSWORD")
            if not es_username or not es_password:
                raise ValueError("ES_USERNAME and ES_PASSWORD must be set")

            # For development only; in production, set verify_certs=True with certificates
            self.es = Elasticsearch(
                hosts=[self.config["elasticsearch"]["host"]],
                basic_auth=(es_username, es_password),
                verify_certs=False,
                request_timeout=30,
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
        """Create Elasticsearch index for a specific node type.

        Args:
            node_type (str): Node type (e.g., 'gene', 'transcript').

        Raises:
            RuntimeError: If index creation fails.
        """
        index_name = node_type.lower()
        if not self.es.indices.exists(index=index_name):
            mapping = {
                "settings": {
                    "number_of_shards": 2,  # Adjust based on dataset size
                    "number_of_replicas": 1,
                    "index": {
                        "query": {"default_field": "name_suggest"},
                        "requests.cache.enable": True  # Enable caching
                    }
                },
                "mappings": {
                    "properties": {
                        "name": {"type": "keyword"},  # Exact name for retrieval
                        "id": {"type": "keyword"},    # Neo4j node ID
                        "name_suggest": {
                            "type": "completion",
                            "analyzer": "simple",
                            "max_input_length": 50
                        }
                    }
                }
            }
            try:
                self.es.indices.create(index=index_name, body=mapping)
                logger.info(f"Created index {index_name}")
            except Exception as e:
                logger.error(f"Failed to create index {index_name}: {e}")
                raise RuntimeError(f"Index creation failed: {e}")

    def reindex_from_neo4j(self, neo4j_driver: GraphDatabase.driver, batch_size: int = 500, incremental: bool = True) -> None:
        """Reindex Neo4j nodes to Elasticsearch, creating separate indices per node type.

        Indexes the first '_name' property and node ID for each node.

        Args:
            neo4j_driver: Neo4j driver instance.
            batch_size (int): Number of nodes to index per batch.
            incremental (bool): If True, updates existing nodes instead of deleting indices.

        Raises:
            RuntimeError: If reindexing fails.
        """
        try:
            with neo4j_driver.session() as session:
                query = """
                MATCH (n)
                WHERE any(prop IN keys(n) WHERE prop ENDS WITH '_name')
                RETURN n, labels(n) as labels, 
                       [prop IN keys(n) WHERE prop ENDS WITH '_name' | prop][0] as name_field,
                       n[[prop IN keys(n) WHERE prop ENDS WITH '_name' | prop][0]] as name,
                       n.popularity as popularity
                """
                result = session.run(query)

                nodes_by_type: Dict[str, List[Dict]] = {}
                for record in result:
                    if not record["name"] or not record["labels"]:
                        continue
                    node_type = record["labels"][0].lower()  # Primary label as node type
                    node = {
                        "_id": str(record["n"].id),
                        "name": record["name"],
                        "id": str(record["n"].id),
                        "name_suggest": {
                            "input": record["name"],
                            "weight": record["popularity"] or 1  # Dynamic weight
                        }
                    }
                    if node_type not in nodes_by_type:
                        nodes_by_type[node_type] = []
                    nodes_by_type[node_type].append(node)
                    self.node_types.add(node_type)

                if not nodes_by_type:
                    logger.warning("No nodes found to index")
                    return

                logger.info(f"Found nodes for {len(nodes_by_type)} node types")

                for node_type, nodes in nodes_by_type.items():
                    index_name = node_type
                    logger.info(f"Indexing {len(nodes)} nodes for type {node_type}")

                    if not incremental:
                        if self.es.indices.exists(index=index_name):
                            logger.info(f"Deleting existing index {index_name}")
                            self.es.indices.delete(index=index_name)
                        self._create_index_for_node_type(node_type)

                    self._create_index_for_node_type(node_type)

                    actions = [
                        {
                            "_op_type": "index" if not incremental else "update",
                            "_index": index_name,
                            "_id": node["_id"],
                            "_source": node if not incremental else {"doc": node, "doc_as_upsert": True}
                        }
                        for node in nodes
                    ]
                    success, failed = bulk(self.es, actions, chunk_size=batch_size)
                    if failed:
                        logger.error(f"Failed to index {len(failed)} nodes for {node_type}: {failed}")
                    logger.info(f"Indexed {success} nodes for {node_type}")

                    count = self.es.count(index=index_name)["count"]
                    logger.info(f"Total indexed documents for {node_type}: {count}")
        except Exception as e:
            logger.error(f"Failed to reindex from Neo4j: {e}")
            raise RuntimeError(f"Reindexing failed: {e}")

    def search_suggestions(
        self, node_type: str, query: str, size: int = 10
    ) -> List[Dict[str, Any]]:
        """Search for autocomplete suggestions for a specific node type.

        Args:
            node_type (str): Node type (e.g., 'gene', 'transcript').
            query (str): Search query for suggestions.
            size (int): Maximum number of suggestions (max 20).

        Returns:
            List[Dict[str, Any]]: List of suggestion dictionaries with name, id, and score.

        Raises:
            ValueError: If node_type, query, or size is invalid.
            RuntimeError: If search fails.
        """
        try:
            if not node_type or not isinstance(node_type, str):
                raise ValueError("Node type must be a non-empty string")
            if not query or not isinstance(query, str) or len(query) > 50:
                raise ValueError("Query must be a non-empty string with max length 50")
            if size < 1 or size > 20:
                raise ValueError("Size must be between 1 and 20")

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
                            "skip_duplicates": True
                        }
                    }
                }
            }

            response = self.es.search(index=index_name, body=suggest_query)
            suggestions = [
                {
                    "name": option["_source"]["name"],
                    "id": option["_source"]["id"],
                    "score": option["_score"]
                }
                for option in response["suggest"]["name-suggest"][0]["options"]
            ]

            return suggestions
        except Exception as e:
            logger.error(f"Failed to search suggestions for query '{query}' in {node_type}: {e}")
            raise RuntimeError(f"Search failed: {e}")


# if __name__ == "__main__":
#     neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
#     service = AutocompleteService()
#     service.reindex_from_neo4j(neo4j_driver)
#     suggestions = service.search_suggestions(node_type="gene", query="BRC", size=5)
#     for suggestion in suggestions:
#         print(suggestion)
#     neo4j_driver.close()