import argparse
import json
import sys

from databricks_mcp_server.services import catalogs, schemas, sql, tables
from databricks_mcp_server.services.client import get_workspace_client


def list_warehouses(profile: str) -> list[dict]:
    w = get_workspace_client(profile)
    items = []
    for wh in w.warehouses.list():
        items.append(
            {
                "id": wh.id,
                "name": wh.name,
                "state": wh.state.value if hasattr(wh.state, "value") else str(wh.state),
            }
        )
    return items


def resolve_warehouse_id(profile: str, explicit_warehouse_id: str | None) -> str:
    if explicit_warehouse_id:
        return explicit_warehouse_id

    warehouses = list_warehouses(profile)
    if not warehouses:
        raise RuntimeError("No SQL warehouses available for this profile")
    if len(warehouses) == 1:
        return warehouses[0]["id"]

    if sys.stdin.isatty():
        print("Multiple warehouses found. Select one:")
        for idx, wh in enumerate(warehouses, start=1):
            print(f"{idx}. {wh['name']} ({wh['id']}) [{wh['state']}]")

        while True:
            choice = input("Enter number: ").strip()
            if not choice.isdigit():
                print("Please enter a valid number.")
                continue
            choice_idx = int(choice)
            if 1 <= choice_idx <= len(warehouses):
                return warehouses[choice_idx - 1]["id"]
            print(f"Please choose a number between 1 and {len(warehouses)}.")

    msg = {
        "status": "error",
        "message": "Multiple warehouses found. Pick one and pass --warehouse-id, or run interactively to choose.",
        "warehouses": warehouses,
    }
    print(json.dumps(msg, indent=2))
    raise SystemExit(2)


def run_smoke(profile: str, warehouse_id: str | None) -> None:
    selected_warehouse = resolve_warehouse_id(profile, warehouse_id)
    report: dict = {
        "status": "success",
        "profile": profile,
        "warehouse_id": selected_warehouse,
        "checks": [],
        "summary": {"passed": 0, "failed": 0, "skipped": 0},
    }

    def check(name: str, fn):
        try:
            data = fn()
            report["checks"].append({"name": name, "status": "passed", "data": data})
            report["summary"]["passed"] += 1
            return data
        except RuntimeError as exc:
            report["checks"].append({"name": name, "status": "skipped", "message": str(exc)})
            report["summary"]["skipped"] += 1
            return None
        except Exception as exc:  # noqa: BLE001
            report["checks"].append({"name": name, "status": "failed", "message": str(exc)})
            report["summary"]["failed"] += 1
            return None

    cats = check("list_catalogs", lambda: catalogs.list_catalogs(profile))
    check(
        "execute_query_show_catalogs",
        lambda: sql.execute_query(
            query="SHOW CATALOGS",
            warehouse_id=selected_warehouse,
            profile=profile,
            timeout_seconds=120,
        ),
    )

    catalog_name = None
    if cats and cats.get("catalogs"):
        # Prefer a user-facing catalog for functional checks.
        for item in cats["catalogs"]:
            name = item.get("name")
            if name and name not in {"system", "samples"}:
                catalog_name = name
                break
        if not catalog_name:
            catalog_name = cats["catalogs"][0].get("name")

    if not catalog_name:
        raise RuntimeError("No catalogs available for further smoke checks")

    schema_payload = check("list_schemas", lambda: schemas.list_schemas(catalog_name, profile))
    check(
        "execute_query_show_schemas",
        lambda: sql.execute_query(
            query=f"SHOW SCHEMAS IN {catalog_name}",
            warehouse_id=selected_warehouse,
            profile=profile,
            timeout_seconds=120,
        ),
    )

    schema_name = None
    if schema_payload and schema_payload.get("schemas"):
        for item in schema_payload["schemas"]:
            name = item.get("name")
            if name and name not in {"information_schema"}:
                schema_name = name
                break
        if not schema_name:
            schema_name = schema_payload["schemas"][0].get("name")
    if not schema_name:
        raise RuntimeError(f"No schemas found in catalog {catalog_name}")

    tables_payload = check("list_tables", lambda: tables.list_tables(catalog_name, schema_name, profile))
    check(
        "execute_query_show_tables",
        lambda: sql.execute_query(
            query=f"SHOW TABLES IN {catalog_name}.{schema_name}",
            warehouse_id=selected_warehouse,
            profile=profile,
            timeout_seconds=120,
        ),
    )

    table_name = None
    if tables_payload and tables_payload.get("tables"):
        table_name = tables_payload["tables"][0].get("name")

    if table_name:
        check(
            "get_table_metadata",
            lambda: tables.get_table_metadata(catalog_name, schema_name, table_name, profile),
        )
        check(
            "describe_table_full",
            lambda: tables.describe_table_full(catalog_name, schema_name, table_name, profile),
        )
        check(
            "get_table_columns_only",
            lambda: tables.get_table_columns_only(catalog_name, schema_name, table_name, profile),
        )
        check(
            "check_missing_descriptions",
            lambda: tables.check_missing_descriptions(catalog_name, schema_name, table_name, profile),
        )
        check(
            "preview_table",
            lambda: sql.preview_table(
                catalog=catalog_name,
                schema=schema_name,
                table=table_name,
                warehouse_id=selected_warehouse,
                limit=5,
                profile=profile,
                timeout_seconds=120,
            ),
        )
        check(
            "execute_query_describe_table",
            lambda: sql.execute_query(
                query=f"DESCRIBE TABLE {catalog_name}.{schema_name}.{table_name}",
                warehouse_id=selected_warehouse,
                profile=profile,
                timeout_seconds=120,
            ),
        )
    else:
        report["checks"].append(
            {
                "name": "table_level_checks",
                "status": "skipped",
                "message": f"No tables found in {catalog_name}.{schema_name}",
            }
        )
        report["summary"]["skipped"] += 1

    check("search_tables_smoke", lambda: tables.search_tables(query="transaction", profile=profile))

    if report["summary"]["failed"] > 0:
        report["status"] = "error"
        print(json.dumps(report, indent=2))
        sys.exit(1)

    print(
        json.dumps(report, indent=2)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Live checks for Databricks MCP server")
    parser.add_argument("command", choices=["list-warehouses", "smoke"])
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--warehouse-id", default=None)
    args = parser.parse_args()

    try:
        if args.command == "list-warehouses":
            print(json.dumps({"status": "success", "warehouses": list_warehouses(args.profile)}, indent=2))
            return
        run_smoke(profile=args.profile, warehouse_id=args.warehouse_id)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"status": "error", "message": str(exc)}, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
