import logging
from typing import List, Dict, Optional, Any
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from neo4j import GraphDatabase
import yaml
from pathlib import Path
import os
from dotenv import load_dotenv
import urllib3

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Disable SSL warnings for development
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AutocompleteService:
    def __init__(self, config_path: str = "config/elasticsearch_config.yaml"):
        """Initialize the AutocompleteService with Elasticsearch configuration."""
        load_dotenv()  # Load environment variables
        self._load_config(config_path)
        self._init_elasticsearch()
        self.index_name = "nodes"
        self._create_index_if_not_exists()

    def _load_config(self, config_path: str) -> None:
        """Load Elasticsearch configuration from YAML file."""
        try:
            with open(config_path, "r") as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {str(e)}")
            raise

    def _init_elasticsearch(self) -> None:
        """Initialize Elasticsearch client with SSL configuration."""
        try:
            # Get credentials from environment variables
            es_username = os.getenv("ES_USERNAME")
            es_password = os.getenv("ES_PASSWORD")

            if not es_username or not es_password:
                raise ValueError(
                    "ES_USERNAME and ES_PASSWORD environment variables must be set"
                )

            # Configure Elasticsearch client
            self.es = Elasticsearch(
                hosts=[self.config["elasticsearch"]["host"]],
                basic_auth=(es_username, es_password),
                verify_certs=False,  # For development only
                ssl_show_warn=False,  # For development only
                request_timeout=30,  # Increase timeout
                retry_on_timeout=True,
                max_retries=3,
            )

            # Test connection
            if not self.es.ping():
                raise ConnectionError("Failed to connect to Elasticsearch")

            logger.info("Successfully connected to Elasticsearch")

            # Get cluster info for debugging
            cluster_info = self.es.info()
            logger.info(
                f"Connected to Elasticsearch cluster: {cluster_info.get('cluster_name', 'unknown')}"
            )

        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch client: {str(e)}")
            logger.error(
                "Please ensure Elasticsearch is running and credentials are correct"
            )
            logger.error(f"ES_USERNAME: {os.getenv('ES_USERNAME', 'not set')}")
            logger.error(
                f"ES_PASSWORD: {'set' if os.getenv('ES_PASSWORD') else 'not set'}"
            )
            raise

    def _create_index_if_not_exists(self) -> None:
        """Create Elasticsearch index with completion suggester mapping if it doesn't exist."""
        if not self.es.indices.exists(index=self.index_name):
            mapping = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "analysis": {
                        "analyzer": {
                            "autocomplete": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": ["lowercase", "autocomplete_filter"],
                            }
                        },
                        "filter": {
                            "autocomplete_filter": {
                                "type": "edge_ngram",
                                "min_gram": 1,
                                "max_gram": 20,
                            }
                        },
                    },
                },
                "mappings": {
                    "properties": {
                        "name": {
                            "type": "text",
                            "analyzer": "autocomplete",
                            "search_analyzer": "standard",
                        },
                        "name_suggest": {
                            "type": "completion",
                            "analyzer": "simple",
                            "preserve_separators": True,
                            "preserve_position_increments": True,
                            "max_input_length": 50,
                            "contexts": {
                                "label": {"type": "category", "path": "labels"}
                            },
                        },
                        "labels": {"type": "keyword"},
                        "name_field": {"type": "keyword"},
                    }
                },
            }
            try:
                self.es.indices.create(index=self.index_name, body=mapping)
                logger.info(f"Created index {self.index_name}")
            except Exception as e:
                logger.error(f"Failed to create index {self.index_name}: {str(e)}")
                raise

    def index_node(self, name: str, labels: List[str], name_field: str) -> None:
        """Index a single node in Elasticsearch."""
        try:
            doc = {
                "name": name,
                "name_suggest": {
                    "input": [name.lower()],
                    "weight": 10,
                    "contexts": {"label": labels},
                },
                "labels": labels,
                "name_field": name_field,
            }
            self.es.index(index=self.index_name, document=doc)
            logger.info(f"Indexed node: {name}")
        except Exception as e:
            logger.error(f"Failed to index node {name}: {str(e)}")
            raise

    def bulk_index_nodes(self, nodes: List[Dict[str, Any]]) -> None:
        """Bulk index multiple nodes in Elasticsearch."""
        try:
            actions = []
            for node in nodes:
                action = {
                    "_index": self.index_name,
                    "_source": {
                        "name": node["name"],
                        "name_suggest": {
                            "input": [node["name"].lower()],
                            "weight": 10,
                            "contexts": {"label": node["labels"]},
                        },
                        "labels": node["labels"],
                        "name_field": node["name_field"],
                    },
                }
                actions.append(action)

            success, failed = bulk(self.es, actions)
            logger.info(f"Bulk indexed {success} nodes, {failed} failed")
        except Exception as e:
            logger.error(f"Failed to bulk index nodes: {str(e)}")
            raise

    def reindex_from_neo4j(self, neo4j_driver: GraphDatabase.driver) -> None:
        """Reindex all nodes from Neo4j to Elasticsearch."""
        try:
            with neo4j_driver.session() as session:
                # Query to get all nodes with name properties
                query = """
                MATCH (n)
                WHERE any(prop IN keys(n) WHERE prop ENDS WITH '_name')
                RETURN n, labels(n) as labels, 
                       [prop IN keys(n) WHERE prop ENDS WITH '_name' | prop][0] as name_field,
                       n[[prop IN keys(n) WHERE prop ENDS WITH '_name' | prop][0]] as name
                """
                result = session.run(query)

                nodes_to_index = []
                for record in result:
                    node = {
                        "name": record["name"],
                        "labels": record["labels"],
                        "name_field": record["name_field"],
                    }
                    nodes_to_index.append(node)

                if nodes_to_index:
                    self.bulk_index_nodes(nodes_to_index)
                    logger.info(f"Successfully reindexed {len(nodes_to_index)} nodes")
                else:
                    logger.warning("No nodes found to index")
        except Exception as e:
            logger.error(f"Failed to reindex from Neo4j: {str(e)}")
            raise

    def search_suggestions(
        self, query: str, label: Optional[str] = None, size: int = 10
    ) -> List[Dict[str, Any]]:
        """Search for suggestions based on the query and optional label filter."""
        try:
            suggest_query = {
                "suggest": {
                    "name_suggest": {
                        "prefix": query.lower(),
                        "completion": {
                            "field": "name_suggest",
                            "size": size,
                            "skip_duplicates": True,
                            "fuzzy": {"fuzziness": "AUTO"},
                        },
                    }
                }
            }

            if label:
                suggest_query["suggest"]["name_suggest"]["completion"]["contexts"] = {
                    "label": [label]
                }

            response = self.es.search(index=self.index_name, body=suggest_query)

            suggestions = []
            for option in response["suggest"]["name_suggest"][0]["options"]:
                source = option["_source"]
                suggestions.append(
                    {
                        "name": source["name"],
                        "labels": source["labels"],
                        "score": option["_score"],
                        "name_field": source["name_field"],
                    }
                )

            return suggestions
        except Exception as e:
            logger.error(f"Failed to search suggestions for query '{query}': {str(e)}")
            raise
