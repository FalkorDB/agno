import os
from unittest.mock import MagicMock, patch

import pytest

from agno.tools.falkordb import FalkorDBTools


def test_list_labels():
    with patch("agno.tools.falkordb.FalkorDB") as mock_falkordb:
        mock_db = mock_falkordb.return_value
        mock_graph = mock_db.select_graph.return_value
        mock_result = MagicMock()
        mock_result.result_set = [["Person"], ["Movie"]]
        mock_graph.query.return_value = mock_result

        tools = FalkorDBTools(host="localhost", port=6379)
        labels = tools.list_labels()
        assert labels == ["Person", "Movie"]
        mock_graph.query.assert_called_with("CALL db.labels()")


def test_list_labels_connection_error():
    with patch("agno.tools.falkordb.FalkorDB") as mock_falkordb:
        mock_falkordb.side_effect = Exception("Connection error")

        with pytest.raises(Exception, match="Connection error"):
            FalkorDBTools(host="localhost", port=6379)


def test_list_labels_runtime_error():
    with patch("agno.tools.falkordb.FalkorDB") as mock_falkordb:
        mock_db = mock_falkordb.return_value
        mock_graph = mock_db.select_graph.return_value
        mock_graph.query.side_effect = Exception("Query failed")

        tools = FalkorDBTools(host="localhost", port=6379)
        labels = tools.list_labels()
        assert labels == []


def test_list_relationship_types():
    with patch("agno.tools.falkordb.FalkorDB") as mock_falkordb:
        mock_db = mock_falkordb.return_value
        mock_graph = mock_db.select_graph.return_value
        mock_result = MagicMock()
        mock_result.result_set = [["ACTED_IN"], ["DIRECTED"]]
        mock_graph.query.return_value = mock_result

        tools = FalkorDBTools(host="localhost", port=6379)
        rel_types = tools.list_relationship_types()
        assert rel_types == ["ACTED_IN", "DIRECTED"]
        mock_graph.query.assert_called_with("CALL db.relationshipTypes()")


def test_list_relationship_types_error():
    with patch("agno.tools.falkordb.FalkorDB") as mock_falkordb:
        mock_db = mock_falkordb.return_value
        mock_graph = mock_db.select_graph.return_value
        mock_graph.query.side_effect = Exception("Query failed")

        tools = FalkorDBTools(host="localhost", port=6379)
        rel_types = tools.list_relationship_types()
        assert rel_types == []


def test_get_schema():
    with patch("agno.tools.falkordb.FalkorDB") as mock_falkordb:
        mock_db = mock_falkordb.return_value
        mock_graph = mock_db.select_graph.return_value

        # Mock labels result
        mock_labels_result = MagicMock()
        mock_labels_result.result_set = [["Person"], ["Movie"]]

        # Mock relationship types result
        mock_rel_result = MagicMock()
        mock_rel_result.result_set = [["ACTED_IN"], ["DIRECTED"]]

        # Configure query to return different results based on the query string
        def query_side_effect(query_string):
            if "db.labels()" in query_string:
                return mock_labels_result
            elif "db.relationshipTypes()" in query_string:
                return mock_rel_result

        mock_graph.query.side_effect = query_side_effect

        tools = FalkorDBTools(host="localhost", port=6379)
        schema = tools.get_schema()
        assert len(schema) == 1
        assert "labels" in schema[0]
        assert "relationshipTypes" in schema[0]
        assert schema[0]["labels"] == ["Person", "Movie"]
        assert schema[0]["relationshipTypes"] == ["ACTED_IN", "DIRECTED"]


def test_get_schema_error():
    with patch("agno.tools.falkordb.FalkorDB") as mock_falkordb:
        mock_db = mock_falkordb.return_value
        mock_graph = mock_db.select_graph.return_value
        mock_graph.query.side_effect = Exception("Schema query failed")

        tools = FalkorDBTools(host="localhost", port=6379)
        schema = tools.get_schema()
        assert schema == []


def test_run_cypher_query_with_header():
    with patch("agno.tools.falkordb.FalkorDB") as mock_falkordb:
        mock_db = mock_falkordb.return_value
        mock_graph = mock_db.select_graph.return_value
        mock_result = MagicMock()
        mock_result.header = ["name", "age"]
        mock_result.result_set = [["John", 30], ["Jane", 25]]
        mock_graph.query.return_value = mock_result

        tools = FalkorDBTools(host="localhost", port=6379)
        query = "MATCH (p:Person) RETURN p.name as name, p.age as age"
        result = tools.run_cypher_query(query)

        assert len(result) == 2
        assert result[0]["name"] == "John"
        assert result[0]["age"] == 30
        assert result[1]["name"] == "Jane"
        assert result[1]["age"] == 25
        mock_graph.query.assert_called_with(query)


def test_run_cypher_query_without_header():
    with patch("agno.tools.falkordb.FalkorDB") as mock_falkordb:
        mock_db = mock_falkordb.return_value
        mock_graph = mock_db.select_graph.return_value
        mock_result = MagicMock()
        mock_result.header = None
        mock_result.result_set = [[1], [2], [3]]
        mock_graph.query.return_value = mock_result

        tools = FalkorDBTools(host="localhost", port=6379)
        query = "MATCH (n) RETURN count(n)"
        result = tools.run_cypher_query(query)

        assert len(result) == 3
        assert result[0] == [1]
        assert result[1] == [2]
        assert result[2] == [3]


def test_run_cypher_query_error():
    with patch("agno.tools.falkordb.FalkorDB") as mock_falkordb:
        mock_db = mock_falkordb.return_value
        mock_graph = mock_db.select_graph.return_value
        mock_graph.query.side_effect = Exception("Cypher query failed")

        tools = FalkorDBTools(host="localhost", port=6379)
        result = tools.run_cypher_query("INVALID QUERY")
        assert result == []


def test_initialization_with_env_vars():
    with (
        patch("agno.tools.falkordb.FalkorDB") as mock_falkordb,
        patch.dict(
            os.environ,
            {
                "FALKORDB_HOST": "test-host",
                "FALKORDB_PORT": "6379",
                "FALKORDB_USERNAME": "test_user",
                "FALKORDB_PASSWORD": "test_pass",
            },
        ),
    ):
        FalkorDBTools()
        mock_falkordb.assert_called_with(host="test-host", port=6379, username="test_user", password="test_pass")


def test_initialization_default_values():
    with patch("agno.tools.falkordb.FalkorDB") as mock_falkordb, patch.dict(os.environ, {}, clear=True):
        FalkorDBTools()
        mock_falkordb.assert_called_with(host="localhost", port=6379, username=None, password=None)


def test_initialization_custom_graph():
    with patch("agno.tools.falkordb.FalkorDB") as _:
        tools = FalkorDBTools(host="localhost", port=6379, graph_name="custom_graph")
        assert tools.graph_name == "custom_graph"


def test_initialization_default_graph():
    with patch("agno.tools.falkordb.FalkorDB") as _:
        tools = FalkorDBTools(host="localhost", port=6379)
        assert tools.graph_name == "agno"


def test_initialization_selective_tools():
    with patch("agno.tools.falkordb.FalkorDB") as _:
        tools = FalkorDBTools(
            host="localhost",
            port=6379,
            enable_list_labels=True,
            enable_list_relationships=False,
            enable_get_schema=False,
            enable_run_cypher=True,
        )

        # Check that only selected tools are registered
        tool_names = [tool.__name__ for tool in tools.tools]
        assert "list_labels" in tool_names
        assert "run_cypher_query" in tool_names
        assert "list_relationship_types" not in tool_names
        assert "get_schema" not in tool_names


def test_initialization_all_tools():
    with patch("agno.tools.falkordb.FalkorDB") as _:
        tools = FalkorDBTools(
            host="localhost",
            port=6379,
            all=True,
        )

        # Check that all tools are registered
        tool_names = [tool.__name__ for tool in tools.tools]
        assert "list_labels" in tool_names
        assert "run_cypher_query" in tool_names
        assert "list_relationship_types" in tool_names
        assert "get_schema" in tool_names
