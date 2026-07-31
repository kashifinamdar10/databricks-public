# Databricks notebook source
# MAGIC %md
# MAGIC # Commit report watermark (at-least-once)
# MAGIC Runs AFTER a report's refresh task succeeds. Advances report_state to the
# MAGIC dispatcher's candidate_ts, marking every update <= that ts as handled.
# MAGIC If the refresh failed, this task never runs, so the report stays "due" and
# MAGIC retries on the next trigger.

# COMMAND ----------
CAT, SCH = "mohdkashif_inamdar", "pbi_trigger_poc"
T_RSTATE = f"{CAT}.{SCH}.report_state"

dbutils.widgets.text("report_id", "")
dbutils.widgets.text("candidate_ts", "")   # {{tasks.dispatcher.values.candidate_ts_<id>}}
dbutils.widgets.text("run_id", "")

report_id = dbutils.widgets.get("report_id").strip()
candidate_ts = dbutils.widgets.get("candidate_ts").strip()
run_id = dbutils.widgets.get("run_id") or "manual"
assert report_id, "report_id required"
assert candidate_ts, "candidate_ts empty — nothing to commit (report was not due?)"

spark.createDataFrame(
    [(report_id, candidate_ts, run_id)],
    "report_id string, ts_str string, last_refresh_run_id string",
).createOrReplaceTempView("_commit")

spark.sql(f"""
    MERGE INTO {T_RSTATE} AS s
    USING (SELECT report_id, to_timestamp(ts_str) AS last_refresh_ts, last_refresh_run_id FROM _commit) AS u
      ON s.report_id = u.report_id
    WHEN MATCHED THEN UPDATE SET
      s.last_refresh_ts = u.last_refresh_ts, s.last_refresh_run_id = u.last_refresh_run_id
    WHEN NOT MATCHED THEN INSERT (report_id, last_refresh_ts, last_refresh_run_id)
      VALUES (u.report_id, u.last_refresh_ts, u.last_refresh_run_id)
""")
print(f"Committed {report_id} watermark -> {candidate_ts}")

