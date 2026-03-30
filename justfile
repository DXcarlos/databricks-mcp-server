set shell := ["bash", "-cu"]

# Show all available recipes.
default:
  @just --list

# Install project and dev dependencies with Poetry.
install:
  poetry install

# Run the MCP server over stdio transport.
run:
  poetry run databricks-mcp-server

# Compile Python sources as a fast syntax sanity check.
lint:
  poetry run python -m compileall databricks_mcp_server scripts tests

# Run unit tests.
test:
  poetry run pytest -q

# List available SQL warehouses for a Databricks profile.
list-warehouses profile="DEFAULT":
  poetry run python scripts/live_checks.py list-warehouses --profile {{profile}}

# Run live integration smoke checks against Databricks.
live-smoke profile="DEFAULT" warehouse_id="":
  if [ -n "{{warehouse_id}}" ]; then \
    poetry run python scripts/live_checks.py smoke --profile {{profile}} --warehouse-id {{warehouse_id}}; \
  else \
    poetry run python scripts/live_checks.py smoke --profile {{profile}}; \
  fi
