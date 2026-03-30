from typing import Any, Callable

from databricks_mcp_server.services import catalogs, schemas, sql, tables


def _success(data: Any) -> dict[str, Any]:
    return {"status": "success", "data": data}


def _error(message: str) -> dict[str, str]:
    return {"status": "error", "message": message}


def list_catalogs(payload: dict[str, Any]) -> dict[str, Any]:
    return _success(catalogs.list_catalogs(profile=payload.get("profile")))


def list_schemas(payload: dict[str, Any]) -> dict[str, Any]:
    return _success(
        schemas.list_schemas(
            catalog=payload.get("catalog"),
            profile=payload.get("profile"),
        )
    )


def list_tables(payload: dict[str, Any]) -> dict[str, Any]:
    return _success(
        tables.list_tables(
            catalog=payload.get("catalog"),
            schema=payload.get("schema"),
            profile=payload.get("profile"),
        )
    )


def get_table_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    return _success(
        tables.get_table_metadata(
            catalog=payload.get("catalog"),
            schema=payload.get("schema"),
            table=payload.get("table"),
            profile=payload.get("profile"),
        )
    )


def describe_table_full(payload: dict[str, Any]) -> dict[str, Any]:
    return _success(
        tables.describe_table_full(
            catalog=payload.get("catalog"),
            schema=payload.get("schema"),
            table=payload.get("table"),
            profile=payload.get("profile"),
        )
    )


def preview_table(payload: dict[str, Any]) -> dict[str, Any]:
    limit = payload.get("limit", 10)
    return _success(
        sql.preview_table(
            catalog=payload.get("catalog"),
            schema=payload.get("schema"),
            table=payload.get("table"),
            warehouse_id=payload.get("warehouse_id"),
            limit=int(limit),
            profile=payload.get("profile"),
        )
    )


def search_tables(payload: dict[str, Any]) -> dict[str, Any]:
    return _success(
        tables.search_tables(
            query=payload.get("query"),
            profile=payload.get("profile"),
        )
    )


def execute_query(payload: dict[str, Any]) -> dict[str, Any]:
    return _success(
        sql.execute_query(
            query=payload.get("query"),
            warehouse_id=payload.get("warehouse_id"),
            profile=payload.get("profile"),
        )
    )


def check_missing_descriptions(payload: dict[str, Any]) -> dict[str, Any]:
    return _success(
        tables.check_missing_descriptions(
            catalog=payload.get("catalog"),
            schema=payload.get("schema"),
            table=payload.get("table"),
            profile=payload.get("profile"),
        )
    )


def get_table_columns_only(payload: dict[str, Any]) -> dict[str, Any]:
    return _success(
        tables.get_table_columns_only(
            catalog=payload.get("catalog"),
            schema=payload.get("schema"),
            table=payload.get("table"),
            profile=payload.get("profile"),
        )
    )


_COMMANDS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "list_catalogs": list_catalogs,
    "list_schemas": list_schemas,
    "list_tables": list_tables,
    "get_table_metadata": get_table_metadata,
    "describe_table_full": describe_table_full,
    "preview_table": preview_table,
    "search_tables": search_tables,
    "execute_query": execute_query,
    "check_missing_descriptions": check_missing_descriptions,
    "get_table_columns_only": get_table_columns_only,
}


def run_command(command: str, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        handler = _COMMANDS.get(command)
        if handler is None:
            return _error(f"Unsupported command: {command}")
        return handler(payload or {})
    except Exception as exc:  # noqa: BLE001
        return _error(str(exc))
