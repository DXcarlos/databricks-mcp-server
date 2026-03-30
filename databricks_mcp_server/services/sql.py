import re
import time

from databricks_mcp_server.services.client import get_workspace_client
from databricks.sdk.service import sql

_ALLOWED_PREFIXES = ("select", "show", "describe")
_FORBIDDEN_KEYWORDS = ("drop", "delete", "update", "insert", "alter", "truncate", "create", "merge")


def _validate_read_only_query(query: str) -> str:
    if not query or not query.strip():
        raise ValueError("query is required")

    normalized = query.strip().lower()
    if not normalized.startswith(_ALLOWED_PREFIXES):
        raise ValueError("Only read queries are allowed (SELECT, SHOW, DESCRIBE)")

    # Guardrail against mixed statements and dangerous keywords.
    for keyword in _FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", normalized):
            raise ValueError(f"Forbidden keyword detected: {keyword}")

    return query.strip()


def _wait_for_statement_completion(w, statement_id: str, initial_response, timeout_seconds: int = 120):
    response = initial_response
    start = time.time()
    terminal_states = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}

    while response.status:
        state = response.status.state.value if hasattr(response.status.state, "value") else str(response.status.state)
        if state in terminal_states:
            break
        if time.time() - start > timeout_seconds:
            w.statement_execution.cancel_execution(statement_id=statement_id)
            raise TimeoutError(f"Statement {statement_id} timed out after {timeout_seconds} seconds")
        time.sleep(1)
        response = w.statement_execution.get_statement(statement_id=statement_id)

    return response


def _extract_rows(w, statement_id: str, response) -> list[list]:
    rows = []
    result = response.result
    if result and result.data_array:
        rows.extend(result.data_array)

    next_chunk = result.next_chunk_index if result else None
    while next_chunk is not None:
        chunk = w.statement_execution.get_statement_result_chunk_n(statement_id=statement_id, chunk_index=next_chunk)
        if chunk.data_array:
            rows.extend(chunk.data_array)
        next_chunk = chunk.next_chunk_index

    return rows


def execute_query(query: str, warehouse_id: str, profile: str | None = None, timeout_seconds: int = 120) -> dict:
    if not warehouse_id:
        raise ValueError("warehouse_id is required")

    safe_query = _validate_read_only_query(query)
    w = get_workspace_client(profile)
    initial = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=safe_query,
    )
    if not initial.statement_id:
        raise RuntimeError("Databricks did not return a statement_id")

    final_response = _wait_for_statement_completion(w, initial.statement_id, initial, timeout_seconds=timeout_seconds)
    state = None
    if final_response.status and final_response.status.state is not None:
        state = final_response.status.state.value if hasattr(final_response.status.state, "value") else str(final_response.status.state)
    if state != "SUCCEEDED":
        error_message = None
        if final_response.status and final_response.status.error:
            error_message = final_response.status.error.message
        raise RuntimeError(error_message or f"Query failed with state: {state}")

    manifest = final_response.manifest
    columns = []
    for column in (manifest.schema.columns if manifest and manifest.schema and manifest.schema.columns else []):
        columns.append(column.name)

    rows = _extract_rows(w, initial.statement_id, final_response)
    return {"columns": columns, "rows": rows}


def preview_table(
    catalog: str,
    schema: str,
    table: str,
    warehouse_id: str,
    limit: int = 10,
    profile: str | None = None,
    timeout_seconds: int = 120,
) -> dict:
    if not catalog:
        raise ValueError("catalog is required")
    if not schema:
        raise ValueError("schema is required")
    if not table:
        raise ValueError("table is required")
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if limit > 1000:
        raise ValueError("limit must be <= 1000")

    query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}"
    return execute_query(query=query, warehouse_id=warehouse_id, profile=profile, timeout_seconds=timeout_seconds)
