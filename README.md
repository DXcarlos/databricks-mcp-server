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

## Just Commands

- `just install`: install deps with Poetry.
- `just run`: run MCP server over stdio.
- `just lint`: compile-time sanity check.
- `just test`: run unit tests.
- `just list-warehouses`: list SQL warehouses for `DEFAULT` profile.
- `just list-warehouses my-profile`: list SQL warehouses for custom profile.
- `just live-smoke`: live Databricks check (auto-selects warehouse only if exactly one exists).
- `just live-smoke DEFAULT <warehouse_id>`: live check with explicit warehouse.

## Use With GitHub Copilot

You can connect this MCP server to GitHub Copilot Chat as a local `stdio` server.

### 1. Ensure prerequisites

- GitHub Copilot with MCP support enabled in your IDE.
- Databricks CLI login completed:

```bash
databricks auth login --host https://<your-workspace-host> --profile DEFAULT
```

### 2. Configure MCP in your IDE

In VS Code, create `.vscode/mcp.json` (or use your global MCP settings) with:

```json
{
  "servers": {
    "databricks-unity-catalog": {
      "type": "stdio",
      "command": "poetry",
      "args": ["run", "databricks-mcp-server"],
      "env": {
        "DATABRICKS_CONFIG_PROFILE": "DEFAULT"
      }
    }
  }
}
```

For JetBrains IDEs with Copilot MCP support, use equivalent `mcp.json` server settings (`stdio` + `poetry run databricks-mcp-server`).

### 3. Start and verify tools

- Start the MCP server from the IDE MCP config UI.
- Open Copilot Chat in `Agent` mode.
- Open the tools list and confirm you can see:
  - `list_catalogs`
  - `list_schemas`
  - `list_tables`
  - `get_table_metadata`
  - `describe_table_full`
  - `preview_table`
  - `search_tables`
  - `execute_query`

### 4. Example prompts in Copilot Chat

- `List my Unity Catalog catalogs using the databricks-unity-catalog MCP server.`
- `List schemas in catalog main.`
- `Find tables related to transactions.`
- `Describe table main.finance.transactions.`

If you are on Copilot Business/Enterprise, your org policy must allow MCP servers.
