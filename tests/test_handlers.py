from databricks_mcp_server.handlers import commands
from databricks_mcp_server.handlers.commands import run_command


def test_unknown_command_returns_standard_error_envelope():
    result = run_command("does_not_exist", {"profile": "DEFAULT"})
    assert result["status"] == "error"
    assert "Unsupported command" in result["message"]


def test_execute_query_blocks_non_read_only_sql():
    result = run_command(
        "execute_query",
        {
            "profile": "DEFAULT",
            "warehouse_id": "dummy",
            "query": "DROP TABLE main.default.t",
        },
    )
    assert result["status"] == "error"
    assert "Only read queries are allowed" in result["message"]


def test_list_catalogs_success_envelope(monkeypatch):
    monkeypatch.setattr(commands.catalogs, "list_catalogs", lambda profile=None: {"catalogs": [{"name": "main"}]})
    result = run_command("list_catalogs", {"profile": "DEFAULT"})
    assert result == {"status": "success", "data": {"catalogs": [{"name": "main"}]}}


def test_run_command_returns_error_when_handler_raises(monkeypatch):
    def _boom(profile=None):
        raise RuntimeError("backend unavailable")

    monkeypatch.setattr(commands.catalogs, "list_catalogs", _boom)
    result = run_command("list_catalogs", {"profile": "DEFAULT"})
    assert result["status"] == "error"
    assert "backend unavailable" in result["message"]
