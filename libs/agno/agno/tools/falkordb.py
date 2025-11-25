import os
from typing import Any, List, Optional

try:
    from falkordb import FalkorDB
except ImportError:
    raise ImportError("`FalkorDB` not installed. Please install using `pip install FalkorDB`")

from agno.tools import Toolkit
from agno.utils.log import log_debug, logger


class FalkorDBTools(Toolkit):
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        graph_name: Optional[str] = None,
        # Enable flags for functions
        enable_list_labels: bool = True,
        enable_list_relationships: bool = True,
        enable_get_schema: bool = True,
        enable_run_cypher: bool = True,
        all: bool = False,
        **kwargs,
    ):
        """
        Initialize the FalkorDBTools toolkit.
        Connection parameters (host/port/username/password) can be provided.
        If not provided, falls back to FALKORDB_HOST, FALKORDB_PORT, FALKORDB_USERNAME, FALKORDB_PASSWORD env vars.

        Args:
            host (Optional[str]): The FalkorDB host.
            port (Optional[int]): The FalkorDB port.
            username (Optional[str]): The FalkorDB username.
            password (Optional[str]): The FalkorDB password.
            graph_name (Optional[str]): The FalkorDB graph name.
            enable_list_labels (bool): Whether to list node labels.
            enable_list_relationships (bool): Whether to list relationship types.
            enable_get_schema (bool): Whether to get the schema.
            enable_run_cypher (bool): Whether to run Cypher queries.
            **kwargs: Additional keyword arguments.
        """
        # Determine the connection parameters
        host = host or os.getenv("FALKORDB_HOST", "localhost")
        port = port or int(os.getenv("FALKORDB_PORT", "6379"))
        username = username or os.getenv("FALKORDB_USERNAME")
        password = password or os.getenv("FALKORDB_PASSWORD")

        # Create the FalkorDB connection
        try:
            self.db = FalkorDB(host=host, port=port, username=username, password=password)
            log_debug("Connected to FalkorDB database")
        except Exception as e:
            logger.error(f"Failed to connect to FalkorDB: {e}")
            raise

        self.graph_name = graph_name or os.getenv("FALKORDB_GRAPH", "agno")

        # Register toolkit methods as tools
        tools: List[Any] = []
        if all or enable_list_labels:
            tools.append(self.list_labels)
        if all or enable_list_relationships:
            tools.append(self.list_relationship_types)
        if all or enable_get_schema:
            tools.append(self.get_schema)
        if all or enable_run_cypher:
            tools.append(self.run_cypher_query)
        super().__init__(name="falkordb_tools", tools=tools, **kwargs)

    def list_labels(self) -> list:
        """
        Retrieve all node labels present in the connected FalkorDB database.
        """
        try:
            log_debug("Listing node labels in FalkorDB database")
            graph = self.db.select_graph(self.graph_name)
            result = graph.query("CALL db.labels()")
            labels = [record[0] for record in result.result_set]
            return labels
        except Exception as e:
            logger.error(f"Error listing labels: {e}")
            return []

    def list_relationship_types(self) -> list:
        """
        Retrieve all relationship types present in the connected FalkorDB database.
        """
        try:
            log_debug("Listing relationship types in FalkorDB database")
            graph = self.db.select_graph(self.graph_name)
            result = graph.query("CALL db.relationshipTypes()")
            types = [record[0] for record in result.result_set]
            return types
        except Exception as e:
            logger.error(f"Error listing relationship types: {e}")
            return []

    def get_schema(self) -> list:
        """
        Retrieve a visualization of the database schema, including nodes and relationships.
        """
        try:
            log_debug("Retrieving FalkorDB schema")
            graph = self.db.select_graph(self.graph_name)
            # FalkorDB doesn't have db.schema.visualization(), so we get labels and relationships
            # and construct a schema representation
            labels_result = graph.query("CALL db.labels()")
            labels = [record[0] for record in labels_result.result_set]

            rel_types_result = graph.query("CALL db.relationshipTypes()")
            rel_types = [record[0] for record in rel_types_result.result_set]

            schema_data = [{"labels": labels, "relationshipTypes": rel_types}]
            return schema_data
        except Exception as e:
            logger.error(f"Error getting FalkorDB schema: {e}")
            return []

    def run_cypher_query(self, query: str) -> list:
        """
        Execute an arbitrary Cypher query against the connected FalkorDB database.

        Args:
            query (str): The Cypher query string to execute.
        """
        try:
            log_debug(f"Running Cypher query: {query}")
            graph = self.db.select_graph(self.graph_name)
            result = graph.query(query)
            # Convert result_set to list of dicts if result has header
            if result.header:
                data = []
                for record in result.result_set:
                    row_dict = {}
                    for i, column_name in enumerate(result.header):
                        row_dict[column_name] = record[i]
                    data.append(row_dict)
                return data
            else:
                # If no header, return result_set as-is (already in list format)
                return list(result.result_set)
        except Exception as e:
            logger.error(f"Error running Cypher query: {e}")
            return []
