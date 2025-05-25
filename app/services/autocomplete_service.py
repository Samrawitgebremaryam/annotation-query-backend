from elasticsearch import Elasticsearch
from typing import List, Dict, Optional
import logging
from app.services.schema_manager import DynamicSchemaManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AutocompleteService:
    def __init__(self, es_host: str = "localhost", es_port: int = 9200):
        self.es = Elasticsearch([{"host": es_host, "port": es_port}])
        self.index_name = "node_properties"
        self._setup_index()

    def _setup_index(self):
        """Setup the Elasticsearch index with search-as-you-type mapping"""
        if not self.es.indices.exists(index=self.index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "node_type": {"type": "keyword"},
                        "property_name": {"type": "keyword"},
                        "property_value": {"type": "search_as_you_type"},
                        "property_value._2gram": {"type": "search_as_you_type"},
                        "property_value._3gram": {"type": "search_as_you_type"},
                        "property_value._4gram": {"type": "search_as_you_type"},
                    }
                }
            }
            self.es.indices.create(index=self.index_name, body=mapping)
            logger.info(
                f"Created index {self.index_name} with search-as-you-type mapping"
            )

    def index_node_property(
        self, node_type: str, property_name: str, property_value: str
    ):
        """Index a node property for autocomplete"""
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
