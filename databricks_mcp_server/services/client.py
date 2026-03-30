import os

from databricks.sdk import WorkspaceClient


def get_workspace_client(profile: str | None = None) -> WorkspaceClient:
    selected_profile = profile or os.getenv("DATABRICKS_CONFIG_PROFILE") or "DEFAULT"
    return WorkspaceClient(profile=selected_profile, auth_type="databricks-cli")
