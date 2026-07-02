# Databricks notebook source
# MAGIC %md
# MAGIC # Self-Healing Pipelines — Job Failure Taxonomy + Error Enrichment
# MAGIC
# MAGIC Quantifies production job failures from `system.lakeflow.job_run_timeline`, classifies each
# MAGIC failure into a root-cause category, and — the new step — enriches the "needs triage" long tail
# MAGIC with the actual **error message and stack trace** pulled from the **Jobs API**, so a
# MAGIC self-healing / RCA agent has real error text to reason over.
# MAGIC
# MAGIC **Flow**
# MAGIC 1. Daily failure matrix  (workspace × day × category)
# MAGIC 2. Executive rollup       (% solved by best practice vs. agent long-tail)
# MAGIC 3. Long-tail decomposition (chronic vs. sporadic `RUN_EXECUTION_ERROR`)
# MAGIC 4. **Error enrichment via Jobs API** (state_message + task error + stack trace)
# MAGIC
# MAGIC Portable: no hard-coded workspace_id / job_id. Tune with the `lookback_days` widget.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("lookback_days", "30", "Lookback window (days)")
dbutils.widgets.text("enrich_limit", "50", "Max failed runs to enrich via Jobs API")
dbutils.widgets.dropdown(
    "enrich_scope",
    "sporadic_triage",
    ["sporadic_triage", "all_exec_errors", "all_failures"],
    "Which failures to enrich",
)
# Leave blank for ALL workspaces; set a single workspace_id to scope everything below to it.
dbutils.widgets.text("workspace_id", "", "Workspace ID (blank = all)")

LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
ENRICH_LIMIT = int(dbutils.widgets.get("enrich_limit"))
ENRICH_SCOPE = dbutils.widgets.get("enrich_scope")

# workspace_id is a STRING in the system tables — quote it and filter as text.
WS_FILTER = dbutils.widgets.get("workspace_id").strip()
WS_CLAUSE = f"AND workspace_id = '{WS_FILTER}'" if WS_FILTER else ""
# Reusable name lookup (system.access.workspaces_latest maps id -> name).
WS_NAMES = "(SELECT workspace_id, workspace_name FROM system.access.workspaces_latest)"

print(f"lookback_days={LOOKBACK_DAYS}  enrich_limit={ENRICH_LIMIT}  "
      f"enrich_scope={ENRICH_SCOPE}  workspace_id={WS_FILTER or 'ALL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 1 — Daily failure matrix: workspace × day × category
# MAGIC The "30+ days of job failures by workspace and day" deliverable. `job_run_timeline` emits one
# MAGIC row per run *state period*, so we keep only the terminal row per run (latest `period_end_time`,
# MAGIC non-null `result_state`) to count each run once.

# COMMAND ----------

daily_matrix = spark.sql(f"""
WITH terminal_runs AS (
  SELECT
    workspace_id, job_id, run_id,
    DATE(period_end_time)                       AS run_date,
    result_state, termination_code, termination_type, trigger_type
  FROM system.lakeflow.job_run_timeline
  WHERE period_end_time >= dateadd(DAY, -{LOOKBACK_DAYS}, current_timestamp())
    AND run_type = 'JOB_RUN'
    AND result_state IS NOT NULL
    {WS_CLAUSE}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, run_id ORDER BY period_end_time DESC) = 1
),
categorized AS (
  SELECT
    workspace_id, run_date, run_id, result_state, termination_code,
    CASE
      WHEN result_state = 'SUCCEEDED'                                 THEN '0. Succeeded'
      WHEN result_state = 'CANCELLED' OR termination_code = 'USER_CANCELLED'
                                                                      THEN '0. Cancelled (expected)'
      WHEN termination_code IN ('INTERNAL_ERROR','CLOUD_FAILURE','CLUSTER_ERROR','DRIVER_ERROR')
                                                                      THEN '1. Transient / Infra'
      WHEN termination_code IN ('INVALID_RUN_CONFIGURATION','INVALID_CLUSTER_REQUEST',
                                'RESOURCE_NOT_FOUND','REPOSITORY_CHECKOUT_FAILED',
                                'LIBRARY_INSTALLATION_ERROR','FEATURE_DISABLED')
                                                                      THEN '2. Config / Definition (tech debt)'
      WHEN termination_code IN ('UNAUTHORIZED_ERROR','STORAGE_ACCESS_ERROR')
                                                                      THEN '3. Permissions / Governance'
      WHEN termination_code IN ('MAX_CONCURRENT_RUNS_EXCEEDED','MAX_JOB_QUEUE_SIZE_EXCEEDED',
                                'WORKSPACE_RUN_LIMIT_EXCEEDED','CLUSTER_REQUEST_LIMIT_EXCEEDED',
                                'MAX_SPARK_CONTEXTS_EXCEEDED')
                                                                      THEN '4. Concurrency / Limits'
      WHEN termination_code = 'RUN_EXECUTION_ERROR'                   THEN '5. Code / Data logic (needs triage)'
      ELSE                                                                 '6. Other / Uncategorized'
    END AS failure_category,
    CASE
      WHEN result_state = 'SUCCEEDED' OR termination_code = 'USER_CANCELLED' THEN 'n/a'
      WHEN termination_code IN ('INTERNAL_ERROR','CLOUD_FAILURE','CLUSTER_ERROR','DRIVER_ERROR')
        THEN 'Job retries + serverless (auto-recover)'
      WHEN termination_code IN ('INVALID_RUN_CONFIGURATION','INVALID_CLUSTER_REQUEST',
                                'RESOURCE_NOT_FOUND','REPOSITORY_CHECKOUT_FAILED',
                                'LIBRARY_INSTALLATION_ERROR','FEATURE_DISABLED')
        THEN 'DABs + CI/CD + serverless (eliminate config drift)'
      WHEN termination_code IN ('UNAUTHORIZED_ERROR','STORAGE_ACCESS_ERROR')
        THEN 'Unity Catalog governance + service principals'
      WHEN termination_code IN ('MAX_CONCURRENT_RUNS_EXCEEDED','MAX_JOB_QUEUE_SIZE_EXCEEDED',
                                'WORKSPACE_RUN_LIMIT_EXCEEDED','CLUSTER_REQUEST_LIMIT_EXCEEDED',
                                'MAX_SPARK_CONTEXTS_EXCEEDED')
        THEN 'Orchestration design + serverless elasticity'
      WHEN termination_code = 'RUN_EXECUTION_ERROR'
        THEN 'Lakeflow/DLT expectations + Auto Loader; triage agent for residual'
      ELSE 'Investigate'
    END AS resolution_lever,
    CASE
      WHEN result_state = 'SUCCEEDED' OR termination_code = 'USER_CANCELLED'           THEN 'Not a failure'
      WHEN termination_code = 'RUN_EXECUTION_ERROR'                                    THEN 'Agent candidate (long tail)'
      WHEN result_state IN ('FAILED','ERROR','SKIPPED','TIMEDOUT')                     THEN 'Solved by best practices'
      ELSE 'Review'
    END AS disposition
  FROM terminal_runs
)
SELECT c.workspace_id, w.workspace_name, c.run_date, c.failure_category,
       c.resolution_lever, c.disposition,
       COUNT(*) AS runs, COUNT(DISTINCT c.run_id) AS distinct_runs
FROM categorized c
LEFT JOIN {WS_NAMES} w ON c.workspace_id = w.workspace_id
WHERE c.failure_category NOT LIKE '0.%'
GROUP BY c.workspace_id, w.workspace_name, c.run_date, c.failure_category,
         c.resolution_lever, c.disposition
ORDER BY c.workspace_id, c.run_date, runs DESC
""")
display(daily_matrix)

# COMMAND ----------

# MAGIC %md ## Query 2 — Executive rollup: % solved by best practice vs. agent long-tail

# COMMAND ----------

exec_rollup = spark.sql(f"""
WITH terminal_runs AS (
  SELECT workspace_id, run_id, result_state, termination_code,
         DATE(period_end_time) AS run_date
  FROM system.lakeflow.job_run_timeline
  WHERE period_end_time >= dateadd(DAY, -{LOOKBACK_DAYS}, current_timestamp())
    AND run_type = 'JOB_RUN'
    AND result_state IS NOT NULL
    {WS_CLAUSE}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, run_id ORDER BY period_end_time DESC) = 1
),
failures AS (
  SELECT
    workspace_id,
    CASE WHEN termination_code = 'RUN_EXECUTION_ERROR' THEN 'Agent candidate (long tail)'
         ELSE 'Solved by best practices' END AS disposition,
    CASE
      WHEN termination_code IN ('INTERNAL_ERROR','CLOUD_FAILURE','CLUSTER_ERROR','DRIVER_ERROR')                       THEN '1. Transient / Infra'
      WHEN termination_code IN ('INVALID_RUN_CONFIGURATION','INVALID_CLUSTER_REQUEST','RESOURCE_NOT_FOUND',
                                'REPOSITORY_CHECKOUT_FAILED','LIBRARY_INSTALLATION_ERROR','FEATURE_DISABLED')          THEN '2. Config / Definition (tech debt)'
      WHEN termination_code IN ('UNAUTHORIZED_ERROR','STORAGE_ACCESS_ERROR')                                          THEN '3. Permissions / Governance'
      WHEN termination_code IN ('MAX_CONCURRENT_RUNS_EXCEEDED','MAX_JOB_QUEUE_SIZE_EXCEEDED','WORKSPACE_RUN_LIMIT_EXCEEDED',
                                'CLUSTER_REQUEST_LIMIT_EXCEEDED','MAX_SPARK_CONTEXTS_EXCEEDED')                        THEN '4. Concurrency / Limits'
      WHEN termination_code = 'RUN_EXECUTION_ERROR'                                                                   THEN '5. Code / Data logic (needs triage)'
      ELSE '6. Other / Uncategorized'
    END AS failure_category
  FROM terminal_runs
  WHERE result_state IN ('FAILED','ERROR','SKIPPED','TIMEDOUT')
)
SELECT f.workspace_id, w.workspace_name, f.failure_category, f.disposition,
       COUNT(*) AS failed_runs,
       -- % within each workspace (partitioned), so per-workspace views still sum to 100
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY f.workspace_id), 1) AS pct_of_ws_failures
FROM failures f
LEFT JOIN {WS_NAMES} w ON f.workspace_id = w.workspace_id
GROUP BY f.workspace_id, w.workspace_name, f.failure_category, f.disposition
ORDER BY f.workspace_id, failed_runs DESC
""")
display(exec_rollup)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 3 — Decompose the "needs triage" long tail (`RUN_EXECUTION_ERROR`)
# MAGIC Chronic repeat-offenders are remediation candidates (fix once, don't heal repeatedly); only the
# MAGIC sporadic 1–4× failures are a genuine triage-agent surface.

# COMMAND ----------

# Query 3A — concentration bands
concentration = spark.sql(f"""
WITH exec_errors AS (
  SELECT workspace_id, job_id, run_id
  FROM system.lakeflow.job_run_timeline
  WHERE period_end_time >= dateadd(DAY, -{LOOKBACK_DAYS}, current_timestamp())
    AND run_type = 'JOB_RUN' AND result_state IS NOT NULL
    AND termination_code = 'RUN_EXECUTION_ERROR'
    {WS_CLAUSE}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, run_id ORDER BY period_end_time DESC) = 1
),
per_job AS (SELECT workspace_id, job_id, COUNT(*) AS fails FROM exec_errors GROUP BY workspace_id, job_id)
SELECT
  CASE WHEN fails >= 20 THEN 'chronic (20+ fails) -> remediate pipeline'
       WHEN fails >= 5  THEN 'recurring (5-19)    -> remediate pipeline'
       ELSE                  'sporadic (1-4)      -> genuine triage-agent surface' END AS job_failure_band,
  COUNT(*) AS num_jobs, SUM(fails) AS failed_runs,
  ROUND(100.0 * SUM(fails) / SUM(SUM(fails)) OVER (), 1) AS pct_of_exec_errors
FROM per_job GROUP BY 1 ORDER BY failed_runs DESC
""")
display(concentration)

# COMMAND ----------

# Query 3B — named repeat-offender worklist (top chronic jobs)
repeat_offenders = spark.sql(f"""
WITH exec_errors AS (
  SELECT workspace_id, job_id, run_id
  FROM system.lakeflow.job_run_timeline
  WHERE period_end_time >= dateadd(DAY, -{LOOKBACK_DAYS}, current_timestamp())
    AND run_type = 'JOB_RUN' AND result_state IS NOT NULL
    AND termination_code = 'RUN_EXECUTION_ERROR'
    {WS_CLAUSE}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, run_id ORDER BY period_end_time DESC) = 1
),
per_job AS (SELECT workspace_id, job_id, COUNT(*) AS failed_runs FROM exec_errors GROUP BY workspace_id, job_id)
SELECT e.workspace_id, w.workspace_name, e.job_id, j.name AS job_name, e.failed_runs,
       ROUND(100.0 * e.failed_runs / SUM(e.failed_runs) OVER (), 1) AS pct_of_exec_errors,
       ROUND(100.0 * SUM(e.failed_runs) OVER (ORDER BY e.failed_runs DESC, e.job_id
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
             / SUM(e.failed_runs) OVER (), 1) AS running_pct_cumulative
FROM per_job e
LEFT JOIN (
  SELECT workspace_id, job_id, name FROM system.lakeflow.jobs
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
) j ON e.workspace_id = j.workspace_id AND e.job_id = j.job_id
LEFT JOIN {WS_NAMES} w ON e.workspace_id = w.workspace_id
ORDER BY e.failed_runs DESC, e.job_id
LIMIT 25
""")
display(repeat_offenders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 4 (NEW) — Error enrichment via the Jobs API
# MAGIC `job_run_timeline` tells us a run threw `RUN_EXECUTION_ERROR`, but **not the actual error text**.
# MAGIC To give a self-healing / RCA agent something to reason over, we resolve each failed job run to
# MAGIC its task-level error message and stack trace via the Jobs API:
# MAGIC
# MAGIC - `jobs/runs/get`        → enumerate tasks in the run + each task's `state.state_message`
# MAGIC - `jobs/runs/get-output` → per-task `error` + `error_trace` (the real stack trace)
# MAGIC
# MAGIC The Jobs API only sees jobs in the **current** workspace, so we restrict enrichment to failed
# MAGIC runs whose `workspace_id` matches this workspace.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from pyspark.sql import Row

w = WorkspaceClient()
CURRENT_WS_ID = int(w.get_workspace_id())
print(f"Current workspace_id = {CURRENT_WS_ID}")

# The Jobs API only sees jobs in the workspace this notebook runs in, so enrichment is always
# scoped to CURRENT_WS_ID. If the user filtered to a *different* workspace, the taxonomy queries
# above reflect that workspace but the enrichment below cannot reach its runs — warn and skip.
if WS_FILTER and WS_FILTER != str(CURRENT_WS_ID):
    print(f"NOTE: workspace_id filter '{WS_FILTER}' != current workspace {CURRENT_WS_ID}. "
          f"Jobs API enrichment can only reach the current workspace, so no runs will be enriched. "
          f"Run this notebook inside workspace {WS_FILTER} to enrich its failures.")

# Resolve the current workspace name once, to stamp onto every enriched row.
_wsn = spark.sql(
    f"SELECT workspace_name FROM system.access.workspaces_latest "
    f"WHERE workspace_id = '{CURRENT_WS_ID}' LIMIT 1"
).collect()
CURRENT_WS_NAME = _wsn[0]["workspace_name"] if _wsn else None

# Pick the set of failed runs to enrich, per the enrich_scope widget.
scope_filter = {
    "sporadic_triage": """
        AND termination_code = 'RUN_EXECUTION_ERROR'
        AND job_id IN (
          SELECT job_id FROM (
            SELECT job_id, COUNT(*) AS c
            FROM system.lakeflow.job_run_timeline
            WHERE period_end_time >= dateadd(DAY, -{lb}, current_timestamp())
              AND run_type='JOB_RUN' AND result_state IS NOT NULL
              AND termination_code='RUN_EXECUTION_ERROR' AND workspace_id={ws}
            GROUP BY job_id HAVING COUNT(*) BETWEEN 1 AND 4))""",
    "all_exec_errors": "AND termination_code = 'RUN_EXECUTION_ERROR'",
    "all_failures":    "AND result_state IN ('FAILED','ERROR','TIMEDOUT')",
}[ENRICH_SCOPE].format(lb=LOOKBACK_DAYS, ws=CURRENT_WS_ID)

runs_to_enrich = spark.sql(f"""
  SELECT run_id, job_id, termination_code, result_state,
         DATE(period_end_time) AS run_date
  FROM system.lakeflow.job_run_timeline
  WHERE period_end_time >= dateadd(DAY, -{LOOKBACK_DAYS}, current_timestamp())
    AND run_type = 'JOB_RUN' AND result_state IS NOT NULL
    AND workspace_id = {CURRENT_WS_ID}
    {scope_filter}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY period_end_time DESC) = 1
  ORDER BY period_end_time DESC
  LIMIT {ENRICH_LIMIT}
""").collect()

print(f"{len(runs_to_enrich)} run(s) selected for enrichment.")

# COMMAND ----------

def enrich_run(run_meta):
    """Resolve one job run_id to its task-level error text via the Jobs API."""
    rows = []
    try:
        run = w.jobs.get_run(run_id=int(run_meta["run_id"]))
    except Exception as e:  # run may be aged out of Jobs API history, or not in this workspace
        return [Row(workspace_id=CURRENT_WS_ID, workspace_name=CURRENT_WS_NAME,
                    run_id=int(run_meta["run_id"]), job_id=int(run_meta["job_id"]),
                    run_date=str(run_meta["run_date"]), termination_code=run_meta["termination_code"],
                    task_key=None, task_run_id=None,
                    state_message=f"[Jobs API unavailable: {type(e).__name__}: {e}]",
                    error=None, error_trace=None, run_page_url=None)]

    tasks = run.tasks or []
    if not tasks:  # single-task / no task breakdown
        tasks = [None]
    for t in tasks:
        state_message = None
        error = None
        error_trace = None
        task_key = getattr(t, "task_key", None) if t else None
        task_run_id = getattr(t, "run_id", None) if t else int(run_meta["run_id"])
        if t is not None and getattr(t, "state", None) is not None:
            state_message = getattr(t.state, "state_message", None)
        # get-output carries the real error + stack trace for the failed task
        try:
            out = w.jobs.get_run_output(run_id=task_run_id)
            error = getattr(out, "error", None)
            error_trace = getattr(out, "error_trace", None)
            if state_message is None and getattr(out, "metadata", None) is not None:
                st = getattr(out.metadata, "state", None)
                state_message = getattr(st, "state_message", None) if st else None
        except Exception as e:
            error = f"[get-output unavailable: {type(e).__name__}: {e}]"
        rows.append(Row(
            workspace_id=CURRENT_WS_ID, workspace_name=CURRENT_WS_NAME,
            run_id=int(run_meta["run_id"]), job_id=int(run_meta["job_id"]),
            run_date=str(run_meta["run_date"]), termination_code=run_meta["termination_code"],
            task_key=task_key, task_run_id=task_run_id,
            state_message=state_message, error=error, error_trace=error_trace,
            run_page_url=getattr(run, "run_page_url", None)))
    return rows

enriched_rows = []
for rm in runs_to_enrich:
    enriched_rows.extend(enrich_run(rm))

if enriched_rows:
    enriched = spark.createDataFrame(enriched_rows)
    display(enriched)
else:
    print("No runs to enrich in the selected window/scope for this workspace.")

# COMMAND ----------

# MAGIC %md ### Run summary (machine-readable exit value)

# COMMAND ----------

import json

def _has_real_error(r):
    e = r["error"]
    return bool(e) and not str(e).startswith("[")

got_error_text = [r for r in enriched_rows if _has_real_error(r)]
sample = None
if got_error_text:
    s = got_error_text[0]
    sample = {
        "run_id": s["run_id"], "job_id": s["job_id"], "task_key": s["task_key"],
        "state_message": (s["state_message"] or "")[:300],
        "error": (s["error"] or "")[:300],
        "error_trace_present": bool(s["error_trace"]),
    }

summary = {
    "lookback_days": LOOKBACK_DAYS,
    "current_workspace_id": CURRENT_WS_ID,
    "daily_matrix_rows": daily_matrix.count(),
    "exec_rollup_rows": exec_rollup.count(),
    "exec_error_bands": concentration.count(),
    "repeat_offenders": repeat_offenders.count(),
    "runs_selected_for_enrichment": len(runs_to_enrich),
    "enriched_task_rows": len(enriched_rows),
    "rows_with_real_error_text": len(got_error_text),
    "sample_enriched_error": sample,
}
print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist enriched errors for the agent (optional)
# MAGIC Uncomment and set a target table to hand a clean error corpus to the RCA/self-healing agent.

# COMMAND ----------

# TARGET_TABLE = "main.default.self_healing_error_corpus"
# if enriched_rows:
#     (enriched
#        .write.mode("overwrite")
#        .option("mergeSchema", "true")
#        .saveAsTable(TARGET_TABLE))
#     print(f"Wrote {enriched.count()} rows to {TARGET_TABLE}")

