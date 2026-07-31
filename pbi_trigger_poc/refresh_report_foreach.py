# Databricks notebook source
# MAGIC %md
# MAGIC # Reusable refresh task (for_each variant)
# MAGIC Runs once per `for_each` iteration. Receives the current report as a JSON
# MAGIC object in `report_spec` (`{{input}}`), parses it, and logs to the audit table.

# COMMAND ----------
import json
import datetime
from pyspark.sql import Row

AUDIT_TABLE = "mohdkashif_inamdar.pbi_trigger_poc.refresh_audit"

dbutils.widgets.text("report_spec", "{}")
dbutils.widgets.text("run_id", "")

spec = json.loads(dbutils.widgets.get("report_spec") or "{}")
run_id = dbutils.widgets.get("run_id")

report_id = spec.get("report_id", "unknown")
report_name = spec.get("report_name", "unknown")
triggering = spec.get("triggering_tables", "[]")

# COMMAND ----------
print(f"=== Refreshing Power BI report: '{report_name}' (id={report_id}) ===")
print(f"    Dataset: {spec.get('powerbi_dataset_id')} | triggered by: {triggering}")

# --- REAL POWER BI HOOK (dummy for POC) -------------------------------------
# import requests
# requests.post(
#     f"https://api.powerbi.com/v1.0/myorg/datasets/{spec['powerbi_dataset_id']}/refreshes",
#     headers={"Authorization": f"Bearer {token}"},
# )
# ---------------------------------------------------------------------------

audit = spark.createDataFrame([Row(
    run_id=run_id,
    report_id=report_id,
    report_name=report_name,
    triggering_tables=triggering,
    refreshed_at=datetime.datetime.now(),
    status="SUCCESS (for_each)",
)])
audit.write.mode("append").saveAsTable(AUDIT_TABLE)
print("Audit row written. Report refresh complete.")

