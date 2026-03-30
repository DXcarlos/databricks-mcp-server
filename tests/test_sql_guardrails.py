from databricks_mcp_server.services.sql import _validate_read_only_query


def test_validate_read_only_query_allows_select_show_describe():
    assert _validate_read_only_query("SELECT 1") == "SELECT 1"
    assert _validate_read_only_query("show catalogs") == "show catalogs"
    assert _validate_read_only_query("DESCRIBE TABLE main.default.t") == "DESCRIBE TABLE main.default.t"


def test_validate_read_only_query_blocks_forbidden_keywords():
    try:
        _validate_read_only_query("SELECT 1; DROP TABLE x")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "Forbidden keyword detected: drop" in str(exc)
