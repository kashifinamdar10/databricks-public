-- ============================================================================
-- Self-Healing Pipelines — Job Failure Taxonomy
-- ----------------------------------------------------------------------------
-- Purpose : Quantify production job failures by workspace x day and classify
--           each failure into a root-cause category, tagged with the
--           Databricks "self-healing" lever that already addresses it vs. the
--           residual long-tail that warrants a triage agent.
--
-- Source  : system.lakeflow.job_run_timeline  (enabled in every UC metastore)
-- Portable: No hard-coded workspace_id / job_id. Runs as-is in any workspace.
--           Tune the lookback with the :lookback_days parameter (default 30).
--
-- Note on the table: job_run_timeline emits one row per run *state period*, so
-- a single run_id can appear multiple times. We keep only the terminal row
-- (latest period_end_time, non-null result_state) to count each run once.
-- ============================================================================


-- ============================================================================
-- QUERY 1 — Daily failure matrix:  workspace x day x category
--   This is the "30+ days of job failures by workspace and day" deliverable.
-- ============================================================================
WITH terminal_runs AS (
  SELECT
    workspace_id,
    job_id,
    run_id,
    DATE(period_end_time)                       AS run_date,
    result_state,
    termination_code,
    termination_type,
    trigger_type
  FROM system.lakeflow.job_run_timeline
  WHERE period_end_time >= dateadd(DAY, -:lookback_days, current_timestamp())
    AND run_type = 'JOB_RUN'          -- exclude sub-task / workflow rows
    AND result_state IS NOT NULL      -- keep terminal periods only
  QUALIFY ROW_NUMBER() OVER (
            PARTITION BY workspace_id, run_id
            ORDER BY period_end_time DESC) = 1
),
categorized AS (
  SELECT
    workspace_id,
    run_date,
    run_id,
    result_state,
    termination_code,
    -- ---- Root-cause category -------------------------------------------
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
    -- ---- Self-healing lever -------------------------------------------
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
    -- ---- Coarse disposition for the exec slide -------------------------
    CASE
      WHEN result_state = 'SUCCEEDED' OR termination_code = 'USER_CANCELLED'           THEN 'Not a failure'
      WHEN termination_code = 'RUN_EXECUTION_ERROR'                                    THEN 'Agent candidate (long tail)'
      WHEN result_state IN ('FAILED','ERROR','SKIPPED','TIMEDOUT')                     THEN 'Solved by best practices'
      ELSE 'Review'
    END AS disposition
  FROM terminal_runs
)
SELECT
  workspace_id,
  run_date,
  failure_category,
  resolution_lever,
  disposition,
  COUNT(*)                                            AS runs,
  COUNT(DISTINCT run_id)                              AS distinct_runs
FROM categorized
WHERE failure_category NOT LIKE '0.%'                 -- failures only
GROUP BY workspace_id, run_date, failure_category, resolution_lever, disposition
ORDER BY workspace_id, run_date, runs DESC;


-- ============================================================================
-- QUERY 2 — Executive rollup: % of failures solved today vs. agent long-tail
--   Drop the workspace_id GROUP BY for an account-wide view; keep it for the
--   per-workspace breakdown.
-- ============================================================================
WITH terminal_runs AS (
  SELECT workspace_id, run_id, result_state, termination_code,
         DATE(period_end_time) AS run_date
  FROM system.lakeflow.job_run_timeline
  WHERE period_end_time >= dateadd(DAY, -:lookback_days, current_timestamp())
    AND run_type = 'JOB_RUN'
    AND result_state IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, run_id ORDER BY period_end_time DESC) = 1
),
failures AS (
  SELECT
    CASE
      WHEN termination_code = 'RUN_EXECUTION_ERROR'                   THEN 'Agent candidate (long tail)'
      ELSE 'Solved by best practices'
    END AS disposition,
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
SELECT
  failure_category,
  disposition,
  COUNT(*)                                                          AS failed_runs,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)                AS pct_of_failures
FROM failures
GROUP BY failure_category, disposition
ORDER BY failed_runs DESC;


-- ============================================================================
-- QUERY 3 — Decompose the "needs triage" long tail (RUN_EXECUTION_ERROR)
-- ----------------------------------------------------------------------------
-- The 39% "agent candidate" bucket is a coarse catch-all ("the task threw").
-- Key question for the roadmap: how much of it is *novel* (one-off incidents a
-- triage agent should investigate) vs. *chronic* (the same pipeline failing
-- over and over = tech debt to fix once, not heal repeatedly)?
--
-- We band each job by how many times it failed with RUN_EXECUTION_ERROR in the
-- window. Chronic repeat-offenders are remediation candidates; only the
-- sporadic 1-4x failures are a genuine triage-agent surface.
--
-- Part A: concentration bands.  Part B: the named repeat-offender worklist.
-- ============================================================================

-- ---- Query 3A: concentration of execution errors by job ----
WITH exec_errors AS (
  SELECT workspace_id, job_id, run_id
  FROM system.lakeflow.job_run_timeline
  WHERE period_end_time >= dateadd(DAY, -:lookback_days, current_timestamp())
    AND run_type = 'JOB_RUN'
    AND result_state IS NOT NULL
    AND termination_code = 'RUN_EXECUTION_ERROR'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, run_id ORDER BY period_end_time DESC) = 1
),
per_job AS (
  SELECT workspace_id, job_id, COUNT(*) AS fails
  FROM exec_errors
  GROUP BY workspace_id, job_id
)
SELECT
  CASE
    WHEN fails >= 20 THEN 'chronic (20+ fails) -> remediate pipeline'
    WHEN fails >= 5  THEN 'recurring (5-19)    -> remediate pipeline'
    ELSE                  'sporadic (1-4)      -> genuine triage-agent surface'
  END                                                          AS job_failure_band,
  COUNT(*)                                                     AS num_jobs,
  SUM(fails)                                                   AS failed_runs,
  ROUND(100.0 * SUM(fails) / SUM(SUM(fails)) OVER (), 1)       AS pct_of_exec_errors
FROM per_job
GROUP BY 1
ORDER BY failed_runs DESC;


-- ---- Query 3B: named repeat-offender worklist (top chronic jobs) ----
--   Hand this list straight to the platform/DEPS team: fixing the top N jobs
--   eliminates the bulk of the "needs triage" volume before any agent is built.
WITH exec_errors AS (
  SELECT workspace_id, job_id, run_id
  FROM system.lakeflow.job_run_timeline
  WHERE period_end_time >= dateadd(DAY, -:lookback_days, current_timestamp())
    AND run_type = 'JOB_RUN'
    AND result_state IS NOT NULL
    AND termination_code = 'RUN_EXECUTION_ERROR'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, run_id ORDER BY period_end_time DESC) = 1
),
per_job AS (
  SELECT workspace_id, job_id, COUNT(*) AS failed_runs
  FROM exec_errors
  GROUP BY workspace_id, job_id
)
SELECT
  e.workspace_id,
  e.job_id,
  j.name                                                       AS job_name,
  e.failed_runs,
  ROUND(100.0 * e.failed_runs
        / SUM(e.failed_runs) OVER (), 1)                       AS pct_of_exec_errors,
  ROUND(100.0 * SUM(e.failed_runs) OVER (ORDER BY e.failed_runs DESC, e.job_id
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        / SUM(e.failed_runs) OVER (), 1)                       AS running_pct_cumulative
FROM per_job e
-- system.lakeflow.jobs is SCD2; take the latest name per job
LEFT JOIN (
  SELECT workspace_id, job_id, name
  FROM system.lakeflow.jobs
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
) j
  ON e.workspace_id = j.workspace_id AND e.job_id = j.job_id
ORDER BY e.failed_runs DESC, e.job_id
LIMIT 25;
