# databricks-mcp-server

MCP server to explore Unity Catalog metadata and run read-only SQL via Databricks SQL Statement Execution.

## ✅ Prerequisites

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

## 📦 Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## ▶️ Run MCP Server

```bash
databricks-mcp-server
```

The server uses Databricks CLI auth from `~/.databrickscfg` with multi-profile support (`DEFAULT` fallback).

## 🧰 Available MCP Tools

- `list_catalogs`: List Unity Catalog catalogs available to the current user/profile.
- `list_schemas`: List schemas inside a given catalog.
- `list_tables`: List tables/views inside a given catalog and schema.
- `get_table_metadata`: Return table metadata with full column details (name, type, nullable, comment).
- `describe_table_full`: Return extended governance metadata (owner, created_at, table type, storage, columns).
- `preview_table`: Fetch sample rows from a table using a SQL warehouse (read-only).
- `search_tables`: Search tables by name or comment across catalogs and schemas.
- `execute_query`: Execute read-only SQL (`SELECT`, `SHOW`, `DESCRIBE`) via SQL warehouse.

Optional high-value tools implemented:

- `check_missing_descriptions`: Detect columns without descriptions/comments.
- `get_table_columns_only`: Return only the column schema for a table.

## 📝 Notes

- `execute_query` only allows queries starting with `SELECT`, `SHOW`, or `DESCRIBE`.
- Mutating keywords like `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `TRUNCATE`, `CREATE`, `MERGE` are blocked.

## ⚙️ Just Commands

- `just install`: install deps with Poetry.
- `just run`: run MCP server over stdio.
- `just lint`: compile-time sanity check.
- `just format`: format code with Ruff.
- `just test`: run unit tests.
- `just coverage`: run unit tests with coverage report.
- `just list-warehouses`: list SQL warehouses for `DEFAULT` profile.
- `just list-warehouses my-profile`: list SQL warehouses for custom profile.
- `just live-smoke`: live Databricks check (auto-selects warehouse only if exactly one exists).
- `just live-smoke DEFAULT <warehouse_id>`: live check with explicit warehouse.

## 🤖 Use With GitHub Copilot

You can connect this MCP server to GitHub Copilot Chat as a local `stdio` server.

### 1. ✅ Ensure Prerequisites

- GitHub Copilot with MCP support enabled in your IDE.
- Databricks CLI login completed:

```bash
databricks auth login --host https://<your-workspace-host> --profile DEFAULT
```

### 2. 🛠️ Configure MCP In Your IDE

In VS Code, create `.vscode/mcp.json` (or use your global MCP settings) with:

```json
{
  "servers": {
    "databricks-mcp": {
      "command": "poetry",
      "args": ["run", "databricks-mcp-server"],
      "cwd": "/Users/carloslopez/PycharmProjects/databricks-mcp-server"
    }
  }
}
```

For JetBrains IDEs with Copilot MCP support, use equivalent `mcp.json` server settings (`stdio` + `poetry run databricks-mcp-server`).

### 3. 🔍 Start And Verify Tools

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

### 4. 💬 Example Prompts In Copilot Chat

- `List my Unity Catalog catalogs using the databricks-mcp MCP server.`
- `List schemas in catalog main.`
- `Find tables related to transactions.`
- `Describe table main.finance.transactions.`

If you are on Copilot Business/Enterprise, your org policy must allow MCP servers.

## 🚀 CI/CD

This repo includes GitHub Actions workflows:

- `CI` (`.github/workflows/ci.yml`)
  - Runs on pushes and pull requests to `main`
  - Python matrix: `3.10`, `3.11`, `3.12`
  - Installs with Poetry, then runs:
    - compile check
    - unit tests

- `Release` (`.github/workflows/release.yml`)
  - Runs on tags like `v0.1.0` (and manual dispatch)
  - Builds distribution artifacts with Poetry
  - Publishes to PyPI using GitHub OIDC trusted publishing

### 📤 Release Setup

1. In PyPI, configure a Trusted Publisher for this GitHub repository and workflow.
2. In GitHub, keep the `pypi` environment (used by release job) with required protections.
3. Create and push a version tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```
