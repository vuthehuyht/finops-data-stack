"""Unit tests for Redshift DDL Executor."""

from unittest.mock import MagicMock, mock_open, patch

import jinja2
import pytest

from src.redshift.ddl_executor import (
    _make_concatenated_ddl_query_string,
    _render_query,
    confirm_execution,
    execute_ddl_queries,
)


def test_render_query() -> None:
    """Test Jinja2 template rendering logic with variables."""
    query_template = "CREATE SCHEMA IF NOT EXISTS {{ schema_name }};"
    parameters = {"schema_name": "TEST_SCHEMA"}
    rendered = _render_query(query_template, parameters)
    assert rendered == "CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA;"


def test_render_query_missing_param_raises_error() -> None:
    """Test rendering raises an error if a template parameter is missing."""
    query_template = "CREATE SCHEMA IF NOT EXISTS {{ schema_name }};"
    with pytest.raises(jinja2.exceptions.UndefinedError):
        _render_query(query_template, {})


def test_make_concatenated_ddl_query_string() -> None:
    """Test reading multiple templates and joining them into one query."""
    file_contents = [
        "CREATE SCHEMA {{ s1 }};\n",
        "CREATE TABLE {{ s2 }}.t1 (id INT);",
    ]
    parameters = {"s1": "S1", "s2": "S2"}

    # Mock open function to return sequential mock files
    m_open = mock_open()
    m_open.side_effect = [
        mock_open(read_data=content).return_value for content in file_contents
    ]

    with patch("builtins.open", m_open):
        concatenated = _make_concatenated_ddl_query_string(
            ["file1.sql", "file2.sql"], parameters
        )

    expected = "CREATE SCHEMA S1\n;\nCREATE TABLE S2.t1 (id INT)"
    assert concatenated == expected


@pytest.mark.parametrize(
    "user_input,expected_outcome",
    [
        ("y", True),
        ("yes", True),
        ("YES", True),
        ("n", False),
        ("no", False),
        ("", False),
        ("random", False),
    ],
)
def test_confirm_execution(user_input: str, expected_outcome: bool) -> None:
    """Test prompt user confirmation handling."""
    with patch("builtins.input", return_value=user_input):
        assert confirm_execution("Test prompt") is expected_outcome


def test_confirm_execution_eof_error() -> None:
    """Test prompt confirmation defaults to False on EOFError."""
    with patch("builtins.input", side_effect=EOFError):
        assert confirm_execution("Test prompt") is False


@patch("src.redshift.ddl_executor.get_redshift_connection")
def test_execute_ddl_queries_success(
    mock_get_conn: MagicMock,
) -> None:
    """Test successful DDL batch execution commits transaction."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_get_conn.return_value.__enter__.return_value = mock_conn

    queries = "CREATE SCHEMA S1; CREATE TABLE S1.t1 (id INT);"
    execute_ddl_queries(
        queries=queries,
        skip_confirmation=True,
        total_files=2,
        parameters={},
    )

    # Verify that cursor execute was called and transaction committed
    mock_cursor.execute.assert_called_once_with(queries)
    mock_conn.commit.assert_called_once()
    mock_conn.rollback.assert_not_called()


@patch("src.redshift.ddl_executor.get_redshift_connection")
def test_execute_ddl_queries_failure_rolls_back(
    mock_get_conn: MagicMock,
) -> None:
    """Test failing DDL batch execution triggers rollback and raises error."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    # Mock execute raising exception
    mock_cursor.execute.side_effect = Exception("Redshift syntax error")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_get_conn.return_value.__enter__.return_value = mock_conn

    queries = "BAD SQL STATEMENT;"

    with pytest.raises(Exception, match="Redshift syntax error"):
        execute_ddl_queries(
            queries=queries,
            skip_confirmation=True,
            total_files=1,
            parameters={},
        )

    # Verify cursor executed, commit was not called, and rollback was called
    mock_cursor.execute.assert_called_once_with(queries)
    mock_conn.commit.assert_not_called()
    mock_conn.rollback.assert_called_once()


@patch("src.redshift.ddl_executor.confirm_execution", return_value=False)
@patch("src.redshift.ddl_executor.get_redshift_connection")
def test_execute_ddl_queries_user_cancelled(
    mock_get_conn: MagicMock,
    mock_confirm: MagicMock,
) -> None:
    """Test interactive cancellation exits program immediately."""
    with pytest.raises(SystemExit) as excinfo:
        execute_ddl_queries(
            queries="CREATE SCHEMA S1;",
            skip_confirmation=False,
            total_files=1,
            parameters={},
        )

    assert excinfo.value.code == 1
    mock_get_conn.assert_not_called()
