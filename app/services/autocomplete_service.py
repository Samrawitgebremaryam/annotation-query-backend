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
            es_username = os.getenv("ES_USERNAME")
            es_password = os.getenv("ES_PASSWORD")
            if not es_username or not es_password:
                raise ValueError("ES_USERNAME and ES_PASSWORD environment variables must be set")

            self.es = Elasticsearch(
                hosts=[self.config["elasticsearch"]["host"]],
                basic_auth=(es_username, es_password),
                verify_certs=False,  # For development only
                ssl_show_warn=False,  # For development only
                request_timeout=30,
                retry_on_timeout=True,
                max_retries=3,
            )

            if not self.es.ping():
                raise ConnectionError("Failed to connect to Elasticsearch")

            logger.info("Successfully connected to Elasticsearch")
            cluster_info = self.es.info()
            logger.info(f"Connected to Elasticsearch cluster: {cluster_info.get('cluster_name', 'unknown')}")

        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch client: {str(e)}")
            logger.error("Please ensure Elasticsearch is running and credentials are correct")
            logger.error(f"ES_USERNAME: {os.getenv('ES_USERNAME', 'not set')}")
            logger.error(f"ES_PASSWORD: {'set' if os.getenv('ES_PASSWORD') else 'not set'}")
            raise

    def _create_index_if_not_exists(self) -> None:
        """Create Elasticsearch index with completion suggester mapping if it doesn't exist."""
        if not self.es.indices.exists(index=self.index_name):
            mapping = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "name": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "labels": {"type": "keyword"},
                        "name_field": {"type": "keyword"},
                        "name_suggest": {
                            "type": "completion",
                            "analyzer": "simple",  # Simple analyzer for case-insensitive suggestions
                            "max_input_length": 50  # Limit input length for performance
                        }
                    }
                }
            }
            try:
                self.es.indices.create(index=self.index_name, body=mapping)
                logger.info(f"Created index {self.index_name}")
            except Exception as e:
                logger.error(f"Failed to create index {self.index_name}: {str(e)}")
                raise

    def reindex_from_neo4j(self, neo4j_driver: GraphDatabase.driver) -> None:
        """Reindex all nodes from Neo4j to Elasticsearch with completion field."""
        try:
            with neo4j_driver.session() as session:
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
                    if not record["name"]:  # Skip nodes with empty names
                        continue
                    node = {
                        "name": record["name"],
                        "labels": record["labels"],
                        "name_field": record["name_field"],
                        "name_suggest": {
                            "input": record["name"],  # Completion field input
                            "weight": 1  # Default weight; adjust based on popularity if needed
                        }
                    }
                    nodes_to_index.append(node)

                if nodes_to_index:
                    logger.info(f"Found {len(nodes_to_index)} nodes to index")

                    # Delete existing index if it exists
                    if self.es.indices.exists(index=self.index_name):
                        logger.info(f"Deleting existing index {self.index_name}")
                        self.es.indices.delete(index=self.index_name)

                    # Create new index
                    self._create_index_if_not_exists()

                    # Index the nodes
                    actions = [
                        {"_index": self.index_name, "_source": node}
                        for node in nodes_to_index
                    ]
                    success, failed = bulk(self.es, actions)
                    if failed:
                        logger.error(f"Failed to index {len(failed)} nodes: {failed}")
                    logger.info(f"Successfully indexed {success} nodes")

                    # Verify indexing
                    count = self.es.count(index=self.index_name)
                    if count["count"] != success:
                        logger.warning(
                            f"Index count ({count['count']}) doesn't match success count ({success})"
                        )
                else:
                    logger.warning("No nodes found to index")
        except Exception as e:
            logger.error(f"Failed to reindex from Neo4j: {str(e)}")
            raise

    def search_suggestions(
        self, query: str, label: Optional[str] = None, size: int = 10
    ) -> List[Dict[str, Any]]:
        """Search for suggestions using Elasticsearch Completion Suggester."""
        try:
            if not query:
                return []

            # Build the suggest query
            suggest_query = {
                "suggest": {
                    "name-suggest": {
                        "prefix": query.lower(),  # Case-insensitive prefix
                        "completion": {
                            "field": "name_suggest",
                            "size": size,
                            "skip_duplicates": True
                        }
                    }
                }
            }

            # Add context filtering for labels if specified
            if label:
                suggest_query["suggest"]["name-suggest"]["completion"]["contexts"] = {
                    "labels": [label]
                }

            # Execute the suggest query
            response = self.es.search(index=self.index_name, body=suggest_query)

            # Process suggestions
            suggestions = []
            for option in response["suggest"]["name-suggest"][0]["options"]:
                source = option["_source"]
                suggestions.append(
                    {
                        "name": source["name"],
                        "labels": source["labels"],
                        "score": option["_score"],
                        "name_field": source["name_field"]
                    }
                )

            # Optional: Fetch counts with a terms aggregation if needed
            if label:
                agg_query = {
                    "query": {
                        "bool": {
                            "filter": [{"term": {"labels": label}}]
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
            else:
                agg_query = {
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

            # Enrich suggestions with counts
            for suggestion in suggestions:
                suggestion["doc_count"] = counts.get(suggestion["name"], 0)

            return suggestions
        except Exception as e:
            logger.error(f"Failed to search suggestions for query '{query}': {str(e)}")
            raise

