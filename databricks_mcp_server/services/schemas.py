from databricks_mcp_server.services.client import get_workspace_client


def list_schemas(catalog: str, profile: str | None = None) -> dict:
    if not catalog:
        raise ValueError("catalog is required")

    w = get_workspace_client(profile)
    schemas = []
    for schema in w.schemas.list(catalog_name=catalog):
        schemas.append(
            {
                "name": schema.name,
                "comment": schema.comment,
            }
        )
    return {"schemas": schemas}
