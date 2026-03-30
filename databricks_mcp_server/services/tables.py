from databricks_mcp_server.services.client import get_workspace_client


def _enum_to_value(value):
    if value is None:
        return None
    return value.value if hasattr(value, "value") else str(value)


def _full_name(catalog: str, schema: str, table: str) -> str:
    if not catalog:
        raise ValueError("catalog is required")
    if not schema:
        raise ValueError("schema is required")
    if not table:
        raise ValueError("table is required")
    return f"{catalog}.{schema}.{table}"


def _serialize_columns(columns: list | None) -> list[dict]:
    result = []
    for column in columns or []:
        result.append(
            {
                "name": column.name,
                "type": column.type_text or (str(column.type_name) if column.type_name else None),
                "nullable": column.nullable,
                "comment": column.comment,
            }
        )
    return result


def list_tables(catalog: str, schema: str, profile: str | None = None) -> dict:
    if not catalog:
        raise ValueError("catalog is required")
    if not schema:
        raise ValueError("schema is required")

    w = get_workspace_client(profile)
    tables = []
    for table in w.tables.list(catalog_name=catalog, schema_name=schema):
        tables.append(
            {
                "name": table.name,
                "type": _enum_to_value(table.table_type),
                "comment": table.comment,
            }
        )
    return {"tables": tables}


def get_table_metadata(catalog: str, schema: str, table: str, profile: str | None = None) -> dict:
    w = get_workspace_client(profile)
    table_info = w.tables.get(full_name=_full_name(catalog, schema, table))
    return {
        "table_name": table_info.name,
        "catalog": table_info.catalog_name,
        "schema": table_info.schema_name,
        "comment": table_info.comment,
        "columns": _serialize_columns(table_info.columns),
    }


def describe_table_full(catalog: str, schema: str, table: str, profile: str | None = None) -> dict:
    w = get_workspace_client(profile)
    table_info = w.tables.get(full_name=_full_name(catalog, schema, table))
    return {
        "table_name": table_info.name,
        "owner": table_info.owner,
        "created_at": table_info.created_at,
        "table_type": _enum_to_value(table_info.table_type),
        "data_source_format": _enum_to_value(table_info.data_source_format),
        "storage_location": table_info.storage_location,
        "columns": _serialize_columns(table_info.columns),
    }


def search_tables(query: str, profile: str | None = None) -> dict:
    if not query:
        raise ValueError("query is required")

    query_lower = query.lower()
    w = get_workspace_client(profile)
    results = []

    for catalog in w.catalogs.list():
        catalog_name = catalog.name
        if not catalog_name:
            continue
        for schema in w.schemas.list(catalog_name=catalog_name):
            schema_name = schema.name
            if not schema_name:
                continue
            for table in w.tables.list(catalog_name=catalog_name, schema_name=schema_name):
                name = table.name or ""
                comment = table.comment or ""
                match = None
                if query_lower in name.lower():
                    match = "name"
                elif query_lower in comment.lower():
                    match = "comment"
                if match:
                    results.append(
                        {
                            "catalog": catalog_name,
                            "schema": schema_name,
                            "table": name,
                            "match": match,
                        }
                    )
    return {"results": results}


def check_missing_descriptions(catalog: str, schema: str, table: str, profile: str | None = None) -> dict:
    w = get_workspace_client(profile)
    table_info = w.tables.get(full_name=_full_name(catalog, schema, table))
    missing = []
    for column in table_info.columns or []:
        if not column.comment or not column.comment.strip():
            missing.append(column.name)
    return {"missing_columns": missing}


def get_table_columns_only(catalog: str, schema: str, table: str, profile: str | None = None) -> dict:
    w = get_workspace_client(profile)
    table_info = w.tables.get(full_name=_full_name(catalog, schema, table))
    return {"columns": _serialize_columns(table_info.columns)}
