from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any


class QueryGeneratorInterface(ABC):
    """Abstract base class for query generators that support any data model."""

    @abstractmethod
    def __init__(self, dataset_path: str):
        """Initialize the query generator with a dataset path."""
        pass

    @abstractmethod
    def load_dataset(self, path: str) -> None:
        """
        Load a dataset from the specified path.

        Args:
            path (str): Path to the dataset
        """
        pass

    @abstractmethod
    def generate_query(
        self,
        nodes: List[Dict],
        relationships: List[Dict],
        filters: Optional[Dict] = None,
        limit: Optional[int] = None,
        node_only: bool = False,
    ) -> str:
        """
        Generate a query based on nodes and relationships.

        Args:
            nodes (List[Dict]): List of node definitions
            relationships (List[Dict]): List of relationship definitions
            filters (Optional[Dict]): Additional query filters
            limit (Optional[int]): Maximum number of results
            node_only (bool): Whether to only include nodes in the query

        Returns:
            str: The generated query string
        """
        pass

    @abstractmethod
    def execute_query(self, query: str) -> List[Dict]:
        """
        Execute a query and return results.

        Args:
            query (str): The query to execute

        Returns:
            List[Dict]: Query results
        """
        pass

    @abstractmethod
    def parse_results(
        self,
        results: List[Dict],
        schema: Dict,
        components: Dict,
        result_type: str = "graph",
    ) -> Dict:
        """
        Parse and format query results.

        Args:
            results (List[Dict]): Raw query results
            schema (Dict): Schema configuration
            components (Dict): Graph components configuration
            result_type (str): Type of results to return ("graph" or "count")

        Returns:
            Dict: Formatted results
        """
        pass

    @abstractmethod
    def validate_query(self, query: str) -> bool:
        """
        Validate a query string.

        Args:
            query (str): Query to validate

        Returns:
            bool: Whether the query is valid
        """
        pass

    @abstractmethod
    def get_query_metadata(self, query: str) -> Dict:
        """
        Get metadata about a query.

        Args:
            query (str): Query to analyze

        Returns:
            Dict: Query metadata
        """
        pass
