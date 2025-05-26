import os
import time
import yaml
import logging
from typing import List, Dict, Optional
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import (
    ConnectionError,
    ConnectionTimeout,
    AuthenticationException,
)
import ssl

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AutocompleteService:
    def __init__(self, config_path: str = "config/elasticsearch_config.yaml"):
        self.index_name = "node_properties"
        self.max_retries = 3
        self.retry_delay = 2
        self.config_path = config_path
        self.es = None

        self.config = self._load_config()
        if not self.config:
            logger.error("Elasticsearch config failed to load.")
            return

        self._initialize_elasticsearch()

        if self.es:
            self._setup_index()
            logger.info("AutocompleteService initialized successfully.")
        else:
            logger.error("Failed to initialize Elasticsearch client.")

    def _load_config(self):
        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)

            es_conf = config.get("elasticsearch", {})
            es_conf["username"] = os.getenv("ELASTICSEARCH_USERNAME", "elastic")
            es_conf["password"] = os.getenv("ELASTICSEARCH_PASSWORD")

            if not es_conf["password"]:
                raise ValueError("Missing ELASTICSEARCH_PASSWORD environment variable.")

            # Set default hosts if missing
            if not es_conf.get("hosts"):
                es_conf["hosts"] = ["https://localhost:9200"]

            # SSL settings
            es_conf["verify_certs"] = False
            es_conf["ssl_show_warn"] = False
            es_conf["use_ssl"] = True

            # Connection settings
            es_conf.setdefault("timeout", 30)
            es_conf.setdefault("retry_on_timeout", True)
            es_conf.setdefault("max_retries", 3)

            return es_conf
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return None

    def _initialize_elasticsearch(self):
        for attempt in range(self.max_retries):
            try:
                # Create an insecure SSL context
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE

                # Configure Elasticsearch client for version 8.11.1
                self.es = Elasticsearch(
                    hosts=self.config["hosts"],
                    basic_auth=(self.config["username"], self.config["password"]),
                    verify_certs=False,
                    ssl_show_warn=False,
                    ssl_context=context,
                    request_timeout=self.config["timeout"],
                    retry_on_timeout=self.config["retry_on_timeout"],
                    max_retries=self.config["max_retries"],
                )

                # Try to get cluster info first to verify connection
                try:
                    cluster_info = self.es.info()
                    logger.info(
                        f"Connected to Elasticsearch cluster: {cluster_info.get('version', {}).get('number', 'unknown')}"
                    )
                    return
                except Exception as e:
                    logger.warning(f"Failed to get cluster info: {str(e)}")
                    if self.es.ping():
                        logger.info("Connected to Elasticsearch (ping successful).")
                        return
                    else:
                        logger.warning("Elasticsearch ping failed.")

            except Exception as e:
                logger.warning(
                    f"Attempt {attempt + 1}: Elasticsearch connection failed - {str(e)}"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"Final connection attempt failed with error: {str(e)}"
                    )

        self.es = None
        logger.error("Elasticsearch connection attempts exceeded.")

    def _setup_index(self):
        if not self.es:
            return

        try:
            if not self.es.indices.exists(index=self.index_name):
                logger.info(f"Creating index: {self.index_name}")
                mappings = {
                    "mappings": {
                        "properties": {
                            "node_type": {"type": "keyword"},
                            "property_name": {"type": "keyword"},
                            "property_value": {
                                "type": "text",
                                "fields": {
                                    "keyword": {"type": "keyword", "ignore_above": 256}
                                },
                            },
                        }
                    },
                    "settings": self.config.get(
                        "settings",
                        {
                            "number_of_shards": 1,
                            "number_of_replicas": 0,
                            "refresh_interval": "1s",
                        },
                    ),
                }
                self.es.indices.create(index=self.index_name, body=mappings)
            else:
                logger.info(f"Index '{self.index_name}' already exists.")
        except Exception as e:
            logger.error(f"Index setup error: {e}")

    def index_node_property(
        self, node_type: str, property_name: str, property_value: str
    ):
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
        if self.es is None:
            logger.error("Cannot search: Elasticsearch client is not initialized")
            return []

        try:
            # Define property name mappings for different node types
            property_mappings = {
                "Gene": {"name": "gene_name"},
                "Transcript": {"name": "transcript_name"},
                "Protein": {"name": "protein_name"},
                # Add more mappings as needed
            }

            must_conditions = []

            # Add query condition
            must_conditions.append(
                {
                    "bool": {
                        "should": [
                            {
                                "prefix": {
                                    "property_value": {
                                        "value": query.lower(),
                                        "case_insensitive": True,
                                    }
                                }
                            },
                            {
                                "prefix": {
                                    "property_value.keyword": {
                                        "value": query.upper(),
                                        "case_insensitive": True,
                                    }
                                }
                            },
                        ]
                    }
                }
            )

            # If node_type is specified, add it to conditions
            if node_type:
                must_conditions.append({"term": {"node_type": node_type}})

                # If property_name is specified, map it to the correct property name
                if property_name and node_type in property_mappings:
                    mapped_property = property_mappings[node_type].get(
                        property_name, property_name
                    )
                    must_conditions.append({"term": {"property_name": mapped_property}})

            # If only property_name is specified (general search)
            elif property_name:
                # Check if property_name is a mapped property
                for node_type, mappings in property_mappings.items():
                    if property_name in mappings.values():
                        must_conditions.append(
                            {"term": {"property_name": property_name}}
                        )
                        break

            search_query = {
                "query": {"bool": {"must": must_conditions}},
                "size": size,
                "sort": [{"_score": "desc"}, {"property_value.keyword": "asc"}],
            }

            logger.info(f"Executing search query: {search_query}")

            response = self.es.search(index=self.index_name, body=search_query)

            suggestions = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                suggestions.append(
                    {
                        "node_type": source["node_type"],
                        "property_name": source["property_name"],
                        "property_value": source["property_value"],
                    }
                )

            logger.info(f"Found {len(suggestions)} suggestions for query: {query}")
            return suggestions

        except Exception as e:
            logger.error(f"Error searching suggestions: {e}")
            return []

    def reindex_from_schema(self, schema_manager):
        if self.es is None:
            logger.error("Cannot reindex: Elasticsearch client is not initialized")
            return

        try:
            if self.es.indices.exists(index=self.index_name):
                self.es.indices.delete(index=self.index_name)
                logger.info(f"Deleted existing index {self.index_name}")

            self._setup_index()
            logger.info("Created new index with mappings")

            total_indexed = 0
            for node_type, node_schema in schema_manager.schema.items():
                if node_schema.get("represented_as") == "node":
                    properties = node_schema.get("properties", {})
                    logger.info(
                        f"Processing node type: {node_type} with properties: {list(properties.keys())}"
                    )

                    for prop_name in properties.keys():
                        with schema_manager.driver.session() as session:
                            query = f"""
                            MATCH (n:{node_type})
                            WHERE n.{prop_name} IS NOT NULL
                            RETURN DISTINCT n.{prop_name} as value
                            """
                            result = session.run(query)
                            logger.info(
                                f"Found {len(result.data())} distinct values for {node_type}.{prop_name}"
                            )

                            bulk_props = []
                            for record in result:
                                value = str(record["value"])
                                if value:
                                    bulk_props.append(
                                        {
                                            "node_type": node_type,
                                            "property_name": prop_name,
                                            "property_value": value,
                                        }
                                    )

                            if bulk_props:
                                self.bulk_index_properties(bulk_props)
                                total_indexed += len(bulk_props)
                                logger.info(
                                    f"Indexed {len(bulk_props)} values for {node_type}.{prop_name}"
                                )

            logger.info(
                f"Completed reindexing. Total properties indexed: {total_indexed}"
            )

            count = self.es.count(index=self.index_name)
            logger.info(f"Total documents in index: {count['count']}")

        except Exception as e:
            logger.error(f"Error reindexing from schema: {e}")
            raise

    def force_reindex(self, schema_manager):
        """Force reindexing of all data"""
        try:
            logger.info("Starting forced reindexing...")
            self.reindex_from_schema(schema_manager)
            logger.info("Forced reindexing completed successfully")
            return True
        except Exception as e:
            logger.error(f"Error during forced reindexing: {e}")
            return False
