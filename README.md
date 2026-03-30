# databricks-mcp-server

MCP server to explore Unity Catalog metadata and run read-only SQL via Databricks SQL Statement Execution.

## Prerequisites

- Python 3.10+
- Databricks CLI v0.205.0+

Install and login with Databricks CLI:

```bash
databricks auth login --host https://<your-workspace-host>
```

Optional: use a non-default profile:

```bash
databricks auth login --host https://<your-workspace-host> --profile my-profile
```

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Run MCP server

```bash
databricks-mcp-server
```

The server uses Databricks CLI auth from `~/.databrickscfg` with multi-profile support (`DEFAULT` fallback).

## Available MCP tools

- `list_catalogs`
- `list_schemas`
- `list_tables`
- `get_table_metadata`
- `describe_table_full`
- `preview_table`
- `search_tables`
- `execute_query`

Optional high-value tools implemented:

- `check_missing_descriptions`
- `get_table_columns_only`

## Notes

- `execute_query` only allows queries starting with `SELECT`, `SHOW`, or `DESCRIBE`.
- Mutating keywords like `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `TRUNCATE`, `CREATE`, `MERGE` are blocked.
