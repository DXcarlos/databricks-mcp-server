import json
import sys
from typing import Any

from mcp.server.fastmcp import FastMCP

from databricks_mcp_server.handlers.commands import run_command

mcp = FastMCP("Databricks Unity Catalog Explorer", log_level="WARNING")


def _normalize_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    result = dict(payload or {})
    if not result.get("profile"):
        result["profile"] = "DEFAULT"
    return result


@mcp.tool()
def list_catalogs(profile: str = "DEFAULT") -> dict[str, Any]:
    """List all Unity Catalog catalogs visible for the selected Databricks profile."""
    return run_command("list_catalogs", {"profile": profile})


@mcp.tool()
def list_schemas(catalog: str, profile: str = "DEFAULT") -> dict[str, Any]:
    """List schemas inside a Unity Catalog catalog."""
    return run_command("list_schemas", _normalize_payload({"catalog": catalog, "profile": profile}))


@mcp.tool()
def list_tables(catalog: str, schema: str, profile: str = "DEFAULT") -> dict[str, Any]:
    """List tables and views inside a catalog/schema namespace."""
    return run_command(
        "list_tables",
        _normalize_payload(
            {
                "catalog": catalog,
                "schema": schema,
                "profile": profile,
            }
        ),
    )


@mcp.tool()
def get_table_metadata(catalog: str, schema: str, table: str, profile: str = "DEFAULT") -> dict[str, Any]:
    """Get table metadata including column names, types, nullability, and comments."""
    return run_command(
        "get_table_metadata",
        _normalize_payload(
            {
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "profile": profile,
            }
        ),
    )


@mcp.tool()
def describe_table_full(catalog: str, schema: str, table: str, profile: str = "DEFAULT") -> dict[str, Any]:
    """Get extended governance metadata for a table (owner, type, storage, and columns)."""
    return run_command(
        "describe_table_full",
        _normalize_payload(
            {
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "profile": profile,
            }
        ),
    )


@mcp.tool()
def preview_table(
    catalog: str,
    schema: str,
    table: str,
    warehouse_id: str,
    limit: int = 10,
    profile: str = "DEFAULT",
) -> dict[str, Any]:
    """Return sample rows from a table using Databricks SQL Statement Execution."""
    return run_command(
        "preview_table",
        _normalize_payload(
            {
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "warehouse_id": warehouse_id,
                "limit": limit,
                "profile": profile,
            }
        ),
    )


@mcp.tool()
def search_tables(query: str, profile: str = "DEFAULT") -> dict[str, Any]:
    """Search tables by matching table name or table comment across catalogs and schemas."""
    return run_command("search_tables", _normalize_payload({"query": query, "profile": profile}))


@mcp.tool()
def execute_query(query: str, warehouse_id: str, profile: str = "DEFAULT") -> dict[str, Any]:
    """Execute a read-only SQL query (SELECT, SHOW, DESCRIBE) on a SQL warehouse."""
    return run_command(
        "execute_query",
        _normalize_payload(
            {
                "query": query,
                "warehouse_id": warehouse_id,
                "profile": profile,
            }
        ),
    )


@mcp.tool()
def check_missing_descriptions(catalog: str, schema: str, table: str, profile: str = "DEFAULT") -> dict[str, Any]:
    """List columns in a table that do not have a column description/comment."""
    return run_command(
        "check_missing_descriptions",
        _normalize_payload(
            {
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "profile": profile,
            }
        ),
    )


@mcp.tool()
def get_table_columns_only(catalog: str, schema: str, table: str, profile: str = "DEFAULT") -> dict[str, Any]:
    """Return only the column schema for a table."""
    return run_command(
        "get_table_columns_only",
        _normalize_payload(
            {
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "profile": profile,
            }
        ),
    )


def run_stdio_command_loop() -> None:
    """
    Optional CLI mode that reads one JSON payload from stdin:
    {"command":"list_catalogs","profile":"DEFAULT", ...}
    """
    raw = sys.stdin.read().strip()
    if not raw:
        print(json.dumps({"status": "error", "message": "stdin payload is required"}))
        return

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(json.dumps({"status": "error", "message": f"invalid JSON: {exc}"}))
        return

    command = payload.pop("command", None)
    if not command:
        print(json.dumps({"status": "error", "message": "command is required"}))
        return

    normalized = _normalize_payload(payload)
    print(json.dumps(run_command(command, normalized)))


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
