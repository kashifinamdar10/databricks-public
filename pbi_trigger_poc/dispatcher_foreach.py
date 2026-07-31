# Databricks notebook source
# MAGIC %md
# MAGIC # Dispatcher (for_each variant)
# MAGIC Parses the table-update trigger's `updated_tables`, looks up the mapping, and
# MAGIC emits a **JSON array of the reports that need refreshing** as task value
# MAGIC `reports_to_refresh`. A downstream `for_each` task iterates over it — one
# MAGIC refresh run per changed report. The job DAG stays 2 tasks regardless of scale.

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
    .select("source_table", "report_id", "report_name", "powerbi_dataset_id")
    .collect()
)

reports_to_refresh = []
for row in mapping:
    if row["source_table"].lower() in updated:
        reports_to_refresh.append({
            "report_id": row["report_id"],
            "report_name": row["report_name"],
            "powerbi_dataset_id": row["powerbi_dataset_id"],
            "triggering_tables": json.dumps(updated),
        })

# for_each iterates over this list; empty list = zero iterations (task still succeeds).
dbutils.jobs.taskValues.set(key="reports_to_refresh", value=reports_to_refresh)

print(f"Reports to refresh ({len(reports_to_refresh)}):")
for r in reports_to_refresh:
    print(f"  - {r['report_id']:10s} {r['report_name']}")

