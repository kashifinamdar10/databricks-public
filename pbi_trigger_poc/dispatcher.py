# Databricks notebook source
# MAGIC %md
# MAGIC # Dispatcher / Router
# MAGIC Parses the table-update trigger's `updated_tables` list, looks up the report
# MAGIC mapping, and sets a task value `refresh_<report_id>` = "true"/"false" for each
# MAGIC report. Downstream condition tasks read these to decide which report refreshes.

# COMMAND ----------
import json

CONFIG_TABLE = "mohdkashif_inamdar.pbi_trigger_poc.report_mapping"

# `updated_tables` is populated by the job param {{job.trigger.table_update.updated_tables}}.
# On manual runs you can override it with a JSON list to simulate a trigger.
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
    .select("source_table", "report_id", "report_name")
    .collect()
)

decisions = {}
for row in mapping:
    src = row["source_table"].lower()
    rid = row["report_id"]
    should = src in updated
    decisions[rid] = should
    # Task value key MUST match what the condition tasks reference in the job DAG.
    dbutils.jobs.taskValues.set(key=f"refresh_{rid}", value="true" if should else "false")
    print(f"  report={rid:10s} source={src:55s} -> refresh={should}")

# Expose the raw triggering list for the audit trail in refresh tasks.
dbutils.jobs.taskValues.set(key="updated_tables", value=json.dumps(updated))

to_refresh = [r for r, v in decisions.items() if v]
print(f"\nReports to refresh: {to_refresh or '<none>'}")

