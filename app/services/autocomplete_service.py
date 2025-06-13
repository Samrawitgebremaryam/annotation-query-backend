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
        self.index_name = "nodes"
        self._create_index_if_not_exists()

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

    def _create_index_if_not_exists(self) -> None:
        """Create Elasticsearch index with completion suggester mapping.

        Raises:
            RuntimeError: If index creation fails.
        """
        if not self.es.indices.exists(index=self.index_name):
            mapping = {
                "settings": {
                    "number_of_shards": 2,
                    "number_of_replicas": 1,
                    "index": {
                        "query": {"default_field": "name_suggest"},
                        "requests.cache.enable": True  # Enable query caching
                    }
                },
                "mappings": {
                    "properties": {
                        "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "labels": {"type": "keyword"},
                        "name_field": {"type": "keyword"},
                        "name_suggest": {
                            "type": "completion",
                            "analyzer": "simple",
                            "max_input_length": 50,
                            "contexts": [
                                {"name": "labels", "type": "category", "path": "labels"}
                            ]
                        }
                    }
                }
            }
            try:
                self.es.indices.create(index=self.index_name, body=mapping)
                logger.info(f"Created index {self.index_name}")
            except Exception as e:
                logger.error(f"Failed to create index {self.index_name}: {e}")
                raise RuntimeError(f"Index creation failed: {e}")

    def reindex_from_neo4j(self, neo4j_driver: GraphDatabase.driver, batch_size: int = 500, incremental: bool = True) -> None:
        """Reindex Neo4j nodes to Elasticsearch with completion field.

        Indexes the first property ending in '_name' for each node into the 'nodes' index.

        Args:
            neo4j_driver: Neo4j driver instance.
            batch_size (int): Number of nodes to index per batch (default: 500).
            incremental (bool): If True, updates existing nodes instead of deleting index.

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

                nodes_to_index = []
                for record in result:
                    if not record["name"]:
                        continue
                    node = {
                        "_id": str(record["n"].id),
                        "name": record["name"],
                        "labels": record["labels"],
                        "name_field": record["name_field"],
                        "name_suggest": {
                            "input": record["name"],
                            "weight": record["popularity"] or 1,  # Dynamic weight
                            "contexts": {"labels": record["labels"]}
                        }
                    }
                    nodes_to_index.append(node)

                if not nodes_to_index:
                    logger.warning("No nodes found to index")
                    return

                logger.info(f"Found {len(nodes_to_index)} nodes to index")

                if not incremental:
                    if self.es.indices.exists(index=self.index_name):
                        logger.info(f"Deleting existing index {self.index_name}")
                        self.es.indices.delete(index=self.index_name)
                    self._create_index_if_not_exists()

                actions = [
                    {
                        "_op_type": "index" if not incremental else "update",
                        "_index": self.index_name,
                        "_id": node["_id"],
                        "_source": node if not incremental else {"doc": node, "doc_as_upsert": True}
                    }
                    for node in nodes_to_index
                ]
                success, failed = bulk(self.es, actions, chunk_size=batch_size)
                if failed:
                    logger.error(f"Failed to index {len(failed)} nodes: {failed}")
                logger.info(f"Successfully indexed {success} nodes")

                count = self.es.count(index=self.index_name)["count"]
                logger.info(f"Total indexed documents: {count}")
        except Exception as e:
            logger.error(f"Failed to reindex from Neo4j: {e}")
            raise RuntimeError(f"Reindexing failed: {e}")

    def search_suggestions(
        self, query: str, label: Optional[str] = None, size: int = 10, include_counts: bool = False
    ) -> List[Dict[str, Any]]:
        """Search for autocomplete suggestions using Completion Suggester.

        Args:
            query (str): Search query for suggestions.
            label (Optional[str]): Filter suggestions by Neo4j label.
            size (int): Maximum number of suggestions (max 20).
            include_counts (bool): If True, include document counts.

        Returns:
            List[Dict[str, Any]]: List of suggestion dictionaries with name, labels, score, name_field, and optional doc_count.

        Raises:
            ValueError: If query or size is invalid.
            RuntimeError: If search fails.
        """
        try:
            if not query or not isinstance(query, str) or len(query) > 50:
                raise ValueError("Query must be a non-empty string with max length 50")
            if size < 1 or size > 20:
                raise ValueError("Size must be between 1 and 20")

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

            if label:
                suggest_query["suggest"]["name-suggest"]["completion"]["contexts"] = {
                    "labels": [label]
                }

            response = self.es.search(index=self.index_name, body=suggest_query)
            suggestions = [
                {
                    "name": option["_source"]["name"],
                    "labels": option["_source"]["labels"],
                    "score": option["_score"],
                    "name_field": option["_source"]["name_field"]
                }
                for option in response["suggest"]["name-suggest"][0]["options"]
            ]

            if include_counts:
                agg_query = {
                    "query": {
                        "bool": {
                            "filter": [{"term": {"labels": label}}] if label else []
                        }
                    },
                    "aggs": {
                        "by_name": {
                            "terms": {
                                "field": "name.keyword",
                                "size": size
                            }
                        }
                    },
                    "size": 0
                }
                agg_response = self.es.search(index=self.index_name, body=agg_query)
                counts = {
                    bucket["key"]: bucket["doc_count"]
                    for bucket in agg_response["aggregations"]["by_name"]["buckets"]
                }
                for suggestion in suggestions:
                    suggestion["doc_count"] = counts.get(suggestion["name"], 0)

            return suggestions
        except Exception as e:
            logger.error(f"Failed to search suggestions for query '{query}': {e}")
            raise RuntimeError(f"Search failed: {e}")

