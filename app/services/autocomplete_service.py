from elasticsearch import Elasticsearch
from typing import List, Dict, Optional
import logging
from app.services.schema_manager import DynamicSchemaManager
import os
import yaml
import time
from elasticsearch.exceptions import (
    ConnectionError,
    ConnectionTimeout,
    AuthenticationException,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AutocompleteService:
    def __init__(self, config_path: str = "config/elasticsearch_config.yaml"):
        self.index_name = "node_properties"
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        self.config_path = config_path

        # Initialize Elasticsearch client with retry logic
        self.es = None
        self._initialize_elasticsearch()

        # Set up the index only if we have a valid connection
        if self.es is not None:
            self._setup_index()
            logger.info("AutocompleteService initialized successfully")
        else:
            logger.error("Failed to initialize Elasticsearch client")

    def _initialize_elasticsearch(self):
        """Initialize Elasticsearch client with retry logic"""
        for attempt in range(self.max_retries):
            try:
                # Try to load configuration from file first
                try:
                    with open(self.config_path, "r") as f:
                        config = yaml.safe_load(f)
                        es_config = config.get("elasticsearch", {})
                except FileNotFoundError:
                    logger.warning(
                        f"Config file {self.config_path} not found, using environment variables"
                    )
                    es_config = {}

                # Get configuration from environment variables or use defaults
                es_host = os.getenv(
                    "ELASTICSEARCH_HOST", es_config.get("host", "localhost")
                )
                es_port = os.getenv("ELASTICSEARCH_PORT", es_config.get("port", "9200"))
                es_username = os.getenv(
                    "ELASTICSEARCH_USERNAME", es_config.get("username", "elastic")
                )
                es_password = os.getenv(
                    "ELASTICSEARCH_PASSWORD", es_config.get("password", "changeme")
                )

                # Initialize client with HTTPS and authentication
                client = Elasticsearch(
                    [f"https://{es_host}:{es_port}"],
                    basic_auth=(es_username, es_password),
                    verify_certs=False,  # Set to True in production
                    ssl_show_warn=False,  # Set to True in production
                    request_timeout=30,
                    retry_on_timeout=True,
                    max_retries=3,
                )

                # Test connection
                if client.ping():
                    logger.info(
                        f"Successfully connected to Elasticsearch at {es_host}:{es_port}"
                    )
                    self.es = client
                    return

            except AuthenticationException as e:
                logger.error(f"Authentication failed: {e}")
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"Retrying authentication (attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(self.retry_delay)
                else:
                    logger.error(
                        "Failed to authenticate with Elasticsearch after all attempts"
                    )
                    return
            except (ConnectionError, ConnectionTimeout) as e:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"Failed to connect to Elasticsearch (attempt {attempt + 1}/{self.max_retries}): {e}"
                    )
                    time.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"Failed to connect to Elasticsearch after {self.max_retries} attempts: {e}"
                    )
                    return
            except Exception as e:
                logger.error(f"Unexpected error initializing Elasticsearch: {e}")
                return

    def _setup_index(self):
        """Setup the Elasticsearch index with search-as-you-type mapping"""
        if self.es is None:
            logger.error("Cannot setup index: Elasticsearch client is not initialized")
            return

        try:
            if not self.es.indices.exists(index=self.index_name):
                mapping = {
                    "mappings": {
                        "properties": {
                            "node_type": {"type": "keyword"},
                            "property_name": {"type": "keyword"},
                            "property_value": {
                                "type": "search_as_you_type",
                                "analyzer": "autocomplete",
                                "search_analyzer": "autocomplete_search",
                            },
                        }
                    },
                    "settings": {
                        "analysis": {
                            "analyzer": {
                                "autocomplete": {
                                    "tokenizer": "autocomplete",
                                    "filter": ["lowercase"],
                                },
                                "autocomplete_search": {"tokenizer": "lowercase"},
                            },
                            "tokenizer": {
                                "autocomplete": {
                                    "type": "edge_ngram",
                                    "min_gram": 1,
                                    "max_gram": 20,
                                    "token_chars": ["letter"],
                                }
                            },
                        }
                    },
                }
                self.es.indices.create(index=self.index_name, body=mapping)
                logger.info(
                    f"Created index {self.index_name} with search-as-you-type mapping"
                )
        except Exception as e:
            logger.error(f"Error setting up index: {e}")
            raise

    def index_node_property(
        self, node_type: str, property_name: str, property_value: str
    ):
        """Index a node property for autocomplete"""
        if self.es is None:
            logger.error(
                "Cannot index property: Elasticsearch client is not initialized"
            )
            return

        try:
            doc = {
                "node_type": node_type,
                "property_name": property_name,
                "property_value": property_value,
            }
            self.es.index(index=self.index_name, body=doc)
        except Exception as e:
            logger.error(f"Error indexing property: {e}")

    def bulk_index_properties(self, properties: List[Dict]):
        """Bulk index multiple properties for better performance"""
        if self.es is None:
            logger.error("Cannot bulk index: Elasticsearch client is not initialized")
            return

        try:
            actions = []
            for prop in properties:
                action = {
                    "_index": self.index_name,
                    "_source": {
                        "node_type": prop["node_type"],
                        "property_name": prop["property_name"],
                        "property_value": prop["property_value"],
                    },
                }
                actions.append(action)

            from elasticsearch.helpers import bulk

            success, failed = bulk(self.es, actions)
            logger.info(f"Bulk indexed {success} documents, {failed} failed")
        except Exception as e:
            logger.error(f"Error in bulk indexing: {e}")

    def search_suggestions(
        self,
        query: str,
        node_type: Optional[str] = None,
        property_name: Optional[str] = None,
        size: int = 10,
    ) -> List[Dict]:
        """Search for autocomplete suggestions using search-as-you-type"""
        if self.es is None:
            logger.error(
                "Cannot search suggestions: Elasticsearch client is not initialized"
            )
            return []

        try:
            search_query = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "type": "bool_prefix",
                        "fields": [
                            "property_value",
                            "property_value._2gram",
                            "property_value._3gram",
                            "property_value._4gram",
                        ],
                        "analyzer": "autocomplete_search",
                    }
                }
            }

            if node_type or property_name:
                search_query["query"] = {
                    "bool": {"must": [search_query["query"]], "filter": []}
                }
                if node_type:
                    search_query["query"]["bool"]["filter"].append(
                        {"term": {"node_type": node_type}}
                    )
                if property_name:
                    search_query["query"]["bool"]["filter"].append(
                        {"term": {"property_name": property_name}}
                    )

            response = self.es.search(
                index=self.index_name, body=search_query, size=size
            )

            return [
                {
                    "node_type": hit["_source"]["node_type"],
                    "property_name": hit["_source"]["property_name"],
                    "property_value": hit["_source"]["property_value"],
                    "score": hit["_score"],
                }
                for hit in response["hits"]["hits"]
            ]
        except Exception as e:
            logger.error(f"Error searching suggestions: {e}")
            return []

    def reindex_from_schema(self, schema_manager: DynamicSchemaManager):
        """Reindex all properties from the schema manager"""
        if self.es is None:
            logger.error("Cannot reindex: Elasticsearch client is not initialized")
            return

        try:
            # Get all node types and their properties
            for node_type, node_schema in schema_manager.schema.items():
                if node_schema.get("represented_as") == "node":
                    properties = node_schema.get("properties", {})
                    for prop_name, prop_config in properties.items():
                        # Query Neo4j to get all values for this property
                        with schema_manager.driver.session() as session:
                            query = f"""
                            MATCH (n:{node_type})
                            WHERE n.{prop_name} IS NOT NULL
                            RETURN DISTINCT n.{prop_name} as value
                            """
                            result = session.run(query)

                            # Bulk index the values
                            bulk_props = []
                            for record in result:
                                bulk_props.append(
                                    {
                                        "node_type": node_type,
                                        "property_name": prop_name,
                                        "property_value": str(record["value"]),
                                    }
                                )

                            if bulk_props:
                                self.bulk_index_properties(bulk_props)

            logger.info("Completed reindexing from schema")
        except Exception as e:
            logger.error(f"Error reindexing from schema: {e}")
