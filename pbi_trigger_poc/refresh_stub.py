# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh stub (placeholder for the native Power BI task)
# MAGIC Stands in for a `power_bi_task` until a UC Power BI (FABRIC) connection
# MAGIC exists. Logs what WOULD be refreshed. Swap this task for a power_bi_task
# MAGIC with literal workspace/model/tables per report; the commit_<id> gate/DAG
# MAGIC wiring stays identical.

# COMMAND ----------
CAT, SCH = "mohdkashif_inamdar", "pbi_trigger_poc"
dbutils.widgets.text("report_id", "")
dbutils.widgets.text("workspace", "")
dbutils.widgets.text("model", "")

rid = dbutils.widgets.get("report_id")
ws = dbutils.widgets.get("workspace")
model = dbutils.widgets.get("model")
print(f"[STUB] Would refresh Power BI model {model!r} in workspace {ws!r} for report {rid!r}")
# Real task: power_bi_task { connection_resource_name, warehouse_id, power_bi_model{workspace_name, model_name}, tables[...] }

