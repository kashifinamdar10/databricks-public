# Databricks notebook source
# MAGIC %md
# MAGIC # Dispatcher (for_each + Power BI task variant)
# MAGIC Parses the table-update trigger's `updated_tables`, looks up `report_mapping`,
# MAGIC and emits a **JSON array of report objects** as task value `reports_to_refresh`.
# MAGIC A downstream `for_each` task iterates over it, and its nested **native Power BI
# MAGIC task** binds each object's fields via `{{input.<field>}}`.
# MAGIC
# MAGIC Unlike the notebook-based for_each variant (which passed the whole object as
# MAGIC `{{input}}` and parsed it in-notebook), the Power BI task has typed fields, so
# MAGIC each element must expose the exact keys the task references:
# MAGIC `workspace`, `model`, `catalog`, `schema`, `table`.

# COMMAND ----------
import json

CONFIG_TABLE = "mohdkashif_inamdar.pbi_trigger_poc.report_mapping"

dbutils.widgets.text("updated_tables", "")
raw = dbutils.widgets.get("updated_tables")

updated = []
try:
    if raw and raw.strip().startswith("["):
        updated = [t.strip().lower() for t in json.loads(raw)]
except Exception as e:  # noqa
    print(f"WARN: could not parse updated_tables={raw!r}: {e}")

print(f"Trigger payload (updated_tables): {updated or '<empty / manual run>'}")

# COMMAND ----------
mapping = (
    spark.table(CONFIG_TABLE)
    .filter("enabled = true")
    .select(
        "source_table",
        "report_id",
        "report_name",
        "powerbi_dataset_id",
        "powerbi_workspace",
        "powerbi_model",
    )
    .collect()
)

reports_to_refresh = []
for row in mapping:
    if row["source_table"].lower() in updated:
        # source_table is fully-qualified catalog.schema.table -> split for the
        # Power BI task's tables[] entry (catalog / schema / name).
        parts = row["source_table"].split(".")
        catalog, schema, table = (parts + [None, None, None])[:3] if len(parts) == 3 else (None, None, row["source_table"])
        reports_to_refresh.append({
            # --- consumed by the native Power BI task via {{input.<field>}} ---
            "workspace": row["powerbi_workspace"],
            "model": row["powerbi_model"],
            "catalog": catalog,
            "schema": schema,
            "table": table,
            # --- carried for logging / auditing (not required by the task) ---
            "report_id": row["report_id"],
            "report_name": row["report_name"],
            "powerbi_dataset_id": row["powerbi_dataset_id"],
            "triggering_tables": json.dumps(updated),
        })

# for_each iterates over this list; empty list = zero iterations (task still succeeds).
dbutils.jobs.taskValues.set(key="reports_to_refresh", value=reports_to_refresh)

print(f"Reports to refresh ({len(reports_to_refresh)}):")
for r in reports_to_refresh:
    print(f"  - {r['report_id']:10s} ws={r['workspace']!r} model={r['model']!r} table={r['catalog']}.{r['schema']}.{r['table']}")

