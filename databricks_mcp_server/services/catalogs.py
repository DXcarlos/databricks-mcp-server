from databricks_mcp_server.services.client import get_workspace_client


def list_catalogs(profile: str | None = None) -> dict:
    w = get_workspace_client(profile)
    catalogs = []
    for catalog in w.catalogs.list():
        catalogs.append(
            {
                "name": catalog.name,
                "comment": catalog.comment,
            }
        )
    return {"catalogs": catalogs}
