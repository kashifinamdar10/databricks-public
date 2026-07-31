# Databricks notebook source
# MAGIC %md
# MAGIC # Reusable "Power BI report refresh" task (dummy)
# MAGIC One notebook reused by every report task. Each task passes its own
# MAGIC `report_id` / `report_name`. Logs a row to the audit table. Replace the
# MAGIC marked section with a real Power BI dataset-refresh REST call to go live.

# COMMAND ----------
import datetime
from pyspark.sql import Row

AUDIT_TABLE = "mohdkashif_inamdar.pbi_trigger_poc.refresh_audit"

dbutils.widgets.text("report_id", "")
dbutils.widgets.text("report_name", "")
dbutils.widgets.text("run_id", "")

report_id = dbutils.widgets.get("report_id")
report_name = dbutils.widgets.get("report_name")
run_id = dbutils.widgets.get("run_id")

# Which tables triggered this run (set by the dispatcher task).
try:
    triggering = dbutils.jobs.taskValues.get(
        taskKey="dispatcher", key="updated_tables", debugValue="[]"
    )
except Exception:
    triggering = "[]"

# COMMAND ----------
print(f"=== Refreshing Power BI report: '{report_name}' (id={report_id}) ===")
print(f"    Triggered by tables: {triggering}")

# --- REAL POWER BI HOOK (dummy for POC) -------------------------------------
# import requests
# token = <AAD token>
# requests.post(
#     f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes",
#     headers={"Authorization": f"Bearer {token}"},
# )
# ---------------------------------------------------------------------------

audit = spark.createDataFrame([Row(
    run_id=run_id,
    report_id=report_id,
    report_name=report_name,
    triggering_tables=triggering,
    refreshed_at=datetime.datetime.now(),
    status="SUCCESS",
)])
audit.write.mode("append").saveAsTable(AUDIT_TABLE)
print("Audit row written. Report refresh complete.")

