# Databricks notebook source
# MAGIC %md
# MAGIC # Cluster Utilization Advisor
# MAGIC
# MAGIC Identifies **over-utilized**, **under-utilized**, and **right-sized** classic compute (all-purpose and jobs clusters) from your own Unity Catalog system tables, then attaches a $-impact estimate and a concrete recommendation per cluster.
# MAGIC
# MAGIC ## What it does
# MAGIC 1. Aggregates per-minute node-level CPU/memory metrics from `system.compute.node_timeline` over a configurable lookback window.
# MAGIC 2. Joins the latest cluster config from `system.compute.clusters` (driver/worker type, autoscale bounds, auto-termination).
# MAGIC 3. Joins DBU spend and list-price cost from `system.billing.usage` + `system.billing.list_prices`.
# MAGIC 4. Scores each cluster against published thresholds.
# MAGIC 5. Returns ranked recommendations + writes summary tables for the companion Lakeview dashboard.
# MAGIC
# MAGIC ## Verdict thresholds
# MAGIC | Verdict | CPU | Memory |
# MAGIC | --- | --- | --- |
# MAGIC | **OVER_UTILIZED** | p95 CPU > 90% | OR p95 mem > 90% OR mem swap > 1% |
# MAGIC | **UNDER_UTILIZED** | avg CPU < 20% AND p95 CPU < 40% | AND avg mem < 50% |
# MAGIC | **RIGHT_SIZED** | avg CPU 20-80% AND p95 CPU < 95% | n/a |
# MAGIC | **REVIEW** | falls outside the bands above | manual triage |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC The notebook user (or service principal) needs:
# MAGIC - `USE CATALOG` on `system`
# MAGIC - `USE SCHEMA` + `SELECT` on `system.compute`, `system.billing`, `system.lakeflow`
# MAGIC
# MAGIC If you hit `INSUFFICIENT_PERMISSIONS`, ask an account admin to run the grants in `setup_grants.sql`.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("lookback_days", "30", "Lookback window (days)")
dbutils.widgets.text("output_catalog", "main", "Output catalog")
dbutils.widgets.text("output_schema", "cluster_utilization_advisor", "Output schema")
dbutils.widgets.text("workspace_id_filter", "", "Workspace ID filter (blank = all)")
dbutils.widgets.text("min_samples", "10", "Min node_timeline samples per cluster")

LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
OUTPUT_CATALOG = dbutils.widgets.get("output_catalog")
OUTPUT_SCHEMA = dbutils.widgets.get("output_schema")
WORKSPACE_FILTER = dbutils.widgets.get("workspace_id_filter").strip()
MIN_SAMPLES = int(dbutils.widgets.get("min_samples"))

print(f"Lookback: {LOOKBACK_DAYS} days")
print(f"Output: {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}")
print(f"Workspace filter: {WORKSPACE_FILTER or '(all)'}")

# COMMAND ----------

# MAGIC %md ## Create output schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}")

# COMMAND ----------

# MAGIC %md ## Build per-cluster utilization view

# COMMAND ----------

workspace_predicate_nt = f"AND workspace_id = '{WORKSPACE_FILTER}'" if WORKSPACE_FILTER else ""
workspace_predicate_cl = f"AND workspace_id = '{WORKSPACE_FILTER}'" if WORKSPACE_FILTER else ""
workspace_predicate_bu = f"AND u.workspace_id = '{WORKSPACE_FILTER}'" if WORKSPACE_FILTER else ""

per_cluster_sql = f"""
CREATE OR REPLACE TABLE {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.per_cluster_utilization AS
WITH nt AS (
  SELECT
    workspace_id, cluster_id, instance_id, driver, start_time,
    cpu_user_percent + cpu_system_percent AS cpu_busy_percent,
    cpu_wait_percent, mem_used_percent, mem_swap_percent
  FROM system.compute.node_timeline
  WHERE start_time >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
    {workspace_predicate_nt}
),
nt_agg AS (
  SELECT
    workspace_id, cluster_id,
    COUNT(*) AS sample_count,
    COUNT(DISTINCT instance_id) AS distinct_instances,
    AVG(cpu_busy_percent) AS avg_cpu_busy_pct,
    PERCENTILE(cpu_busy_percent, 0.5) AS p50_cpu_busy_pct,
    PERCENTILE(cpu_busy_percent, 0.95) AS p95_cpu_busy_pct,
    MAX(cpu_busy_percent) AS max_cpu_busy_pct,
    AVG(cpu_wait_percent) AS avg_cpu_wait_pct,
    AVG(mem_used_percent) AS avg_mem_used_pct,
    PERCENTILE(mem_used_percent, 0.95) AS p95_mem_used_pct,
    MAX(mem_used_percent) AS max_mem_used_pct,
    AVG(mem_swap_percent) AS avg_mem_swap_pct,
    MAX(mem_swap_percent) AS max_mem_swap_pct,
    MIN(start_time) AS first_sample,
    MAX(start_time) AS last_sample
  FROM nt
  GROUP BY workspace_id, cluster_id
  HAVING COUNT(*) >= {MIN_SAMPLES}
),
cluster_latest AS (
  SELECT
    workspace_id, cluster_id, cluster_name, owned_by,
    driver_node_type, worker_node_type, worker_count,
    min_autoscale_workers, max_autoscale_workers, auto_termination_minutes,
    dbr_version, cluster_source, data_security_mode, policy_id, tags,
    ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
  FROM system.compute.clusters
  WHERE 1=1 {workspace_predicate_cl}
),
cluster_cfg AS (SELECT * FROM cluster_latest WHERE rn = 1),
spend AS (
  SELECT
    u.usage_metadata.cluster_id AS cluster_id,
    u.workspace_id,
    SUM(u.usage_quantity) AS dbus,
    SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)) AS list_cost_usd,
    SUM(CASE WHEN u.sku_name ILIKE '%JOBS%' THEN u.usage_quantity ELSE 0 END) AS jobs_dbus,
    SUM(CASE WHEN u.sku_name ILIKE '%ALL_PURPOSE%' THEN u.usage_quantity ELSE 0 END) AS allpurpose_dbus
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices lp
    ON u.cloud = lp.cloud
   AND u.sku_name = lp.sku_name
   AND lp.currency_code = 'USD'
   AND u.usage_start_time >= lp.price_start_time
   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
  WHERE u.usage_metadata.cluster_id IS NOT NULL
    AND u.usage_start_time >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
    {workspace_predicate_bu}
  GROUP BY u.usage_metadata.cluster_id, u.workspace_id
)
SELECT
  nt_agg.workspace_id,
  nt_agg.cluster_id,
  cluster_cfg.cluster_name,
  cluster_cfg.owned_by,
  cluster_cfg.cluster_source,
  cluster_cfg.dbr_version,
  cluster_cfg.driver_node_type,
  cluster_cfg.worker_node_type,
  cluster_cfg.worker_count,
  cluster_cfg.min_autoscale_workers,
  cluster_cfg.max_autoscale_workers,
  cluster_cfg.auto_termination_minutes,
  cluster_cfg.policy_id,
  cluster_cfg.tags,
  nt_agg.sample_count,
  nt_agg.distinct_instances,
  ROUND(nt_agg.avg_cpu_busy_pct, 1) AS avg_cpu_busy_pct,
  ROUND(nt_agg.p50_cpu_busy_pct, 1) AS p50_cpu_busy_pct,
  ROUND(nt_agg.p95_cpu_busy_pct, 1) AS p95_cpu_busy_pct,
  ROUND(nt_agg.max_cpu_busy_pct, 1) AS max_cpu_busy_pct,
  ROUND(nt_agg.avg_mem_used_pct, 1) AS avg_mem_used_pct,
  ROUND(nt_agg.p95_mem_used_pct, 1) AS p95_mem_used_pct,
  ROUND(nt_agg.max_mem_used_pct, 1) AS max_mem_used_pct,
  ROUND(nt_agg.max_mem_swap_pct, 2) AS max_mem_swap_pct,
  COALESCE(spend.dbus, 0)            AS dbus_consumed,
  ROUND(COALESCE(spend.list_cost_usd, 0), 2) AS list_cost_usd,
  COALESCE(spend.jobs_dbus, 0)       AS jobs_dbus,
  COALESCE(spend.allpurpose_dbus, 0) AS allpurpose_dbus,
  CASE
    WHEN nt_agg.max_mem_swap_pct > 1
      OR nt_agg.p95_mem_used_pct > 90
      OR nt_agg.p95_cpu_busy_pct > 90 THEN 'OVER_UTILIZED'
    WHEN nt_agg.avg_cpu_busy_pct < 20
      AND nt_agg.p95_cpu_busy_pct < 40
      AND nt_agg.avg_mem_used_pct < 50 THEN 'UNDER_UTILIZED'
    WHEN nt_agg.avg_cpu_busy_pct BETWEEN 20 AND 80
      AND nt_agg.p95_cpu_busy_pct < 95 THEN 'RIGHT_SIZED'
    ELSE 'REVIEW'
  END AS utilization_verdict,
  CASE
    WHEN nt_agg.avg_cpu_busy_pct < 20
      AND nt_agg.p95_cpu_busy_pct < 40
      AND nt_agg.avg_mem_used_pct < 50
        THEN ROUND(COALESCE(spend.list_cost_usd, 0) * (1 - nt_agg.avg_cpu_busy_pct / 100.0) * 0.6, 2)
    ELSE 0
  END AS estimated_waste_usd,
  CASE
    WHEN nt_agg.max_mem_swap_pct > 1
        THEN 'Memory pressure (swap > 1%). Move to memory-optimized worker type or add workers.'
    WHEN nt_agg.p95_mem_used_pct > 90
        THEN 'p95 memory > 90%. Move to a larger memory-optimized instance type.'
    WHEN nt_agg.p95_cpu_busy_pct > 90
        THEN 'p95 CPU > 90%. Raise max workers or move to a larger compute-optimized instance type.'
    WHEN nt_agg.avg_cpu_busy_pct < 20 AND nt_agg.p95_cpu_busy_pct < 40
        THEN 'Low CPU. Consider Serverless, smaller node type, lower max_autoscale_workers, or shorter auto_termination.'
    WHEN cluster_cfg.auto_termination_minutes IS NULL OR cluster_cfg.auto_termination_minutes > 60
        THEN 'auto_termination_minutes high or unset. Reduce to <= 60 for interactive clusters.'
    ELSE 'Healthy. Continue monitoring.'
  END AS recommendation,
  nt_agg.first_sample,
  nt_agg.last_sample
FROM nt_agg
LEFT JOIN cluster_cfg
  ON nt_agg.workspace_id = cluster_cfg.workspace_id
 AND nt_agg.cluster_id = cluster_cfg.cluster_id
LEFT JOIN spend
  ON nt_agg.workspace_id = spend.workspace_id
 AND nt_agg.cluster_id = spend.cluster_id
"""

spark.sql(per_cluster_sql)
print(f"Built {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.per_cluster_utilization")

# COMMAND ----------

# MAGIC %md ## Build right-sizing detail view
# MAGIC
# MAGIC One row per cluster with:
# MAGIC - **Reasons** — which specific thresholds fired (so a customer knows *why* the verdict was assigned)
# MAGIC - **Current vs recommended worker count** — sized to target ~60% average CPU
# MAGIC - **Recommended action** — concrete sentence with the next step
# MAGIC - **Node specs** — vCPU and memory of the current worker type for context

# COMMAND ----------

rightsizing_sql = f"""
CREATE OR REPLACE TABLE {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.cluster_rightsizing_details AS
WITH base AS (
  SELECT
    p.*,
    nt.core_count        AS worker_cores,
    nt.memory_mb         AS worker_memory_mb,
    ROUND(nt.memory_mb / 1024.0, 1) AS worker_memory_gb,
    COALESCE(p.max_autoscale_workers, p.worker_count) AS effective_max_workers
  FROM {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.per_cluster_utilization p
  LEFT JOIN system.compute.node_types nt
    ON p.worker_node_type = nt.node_type
),
scored AS (
  SELECT
    *,
    -- Right-size workers to target ~60% avg CPU. Floor at 1.
    GREATEST(1,
      CEIL(COALESCE(effective_max_workers, 1) * (avg_cpu_busy_pct / 60.0))
    ) AS recommended_workers,
    -- Reasons array — comma-separated string for easy display in a table
    CONCAT_WS(' | ',
      CASE WHEN avg_cpu_busy_pct < 20         THEN 'avg CPU < 20% (idle)' END,
      CASE WHEN p95_cpu_busy_pct < 40         THEN 'p95 CPU < 40% (no bursts)' END,
      CASE WHEN avg_mem_used_pct < 50         THEN 'avg memory < 50% (RAM headroom)' END,
      CASE WHEN p95_cpu_busy_pct > 90         THEN 'p95 CPU > 90% (CPU-bound)' END,
      CASE WHEN p95_mem_used_pct > 90         THEN 'p95 memory > 90% (memory-bound)' END,
      CASE WHEN max_mem_swap_pct > 1          THEN 'swap > 1% (memory pressure)' END,
      CASE WHEN auto_termination_minutes IS NULL
                 OR auto_termination_minutes > 60
                                              THEN CONCAT('auto_termination = ',
                                                      COALESCE(CAST(auto_termination_minutes AS STRING), 'unset'),
                                                      ' min (high)') END
    ) AS reasons
  FROM base
)
SELECT
  workspace_id,
  cluster_id,
  cluster_name,
  owned_by,
  cluster_source,
  utilization_verdict,
  reasons,
  -- Current config
  worker_node_type            AS current_worker_type,
  worker_cores                AS current_worker_cores,
  worker_memory_gb            AS current_worker_memory_gb,
  worker_count                AS current_worker_count,
  min_autoscale_workers       AS current_min_workers,
  max_autoscale_workers       AS current_max_workers,
  auto_termination_minutes    AS current_auto_term_minutes,
  dbr_version,
  -- Utilization signals
  avg_cpu_busy_pct,
  p95_cpu_busy_pct,
  avg_mem_used_pct,
  p95_mem_used_pct,
  max_mem_swap_pct,
  -- Cost
  ROUND(list_cost_usd, 2)     AS list_cost_usd,
  ROUND(estimated_waste_usd, 2) AS estimated_waste_usd,
  -- Right-sizing recommendation
  CASE
    WHEN utilization_verdict = 'UNDER_UTILIZED'
      THEN recommended_workers
    ELSE NULL
  END                         AS recommended_workers,
  CASE
    -- Memory-bound: keep workers, change node type
    WHEN max_mem_swap_pct > 1 OR p95_mem_used_pct > 90
      THEN CONCAT(
        'Memory-bound. Move to a memory-optimized worker type (e.g., r-family) ',
        'or increase worker count from ', COALESCE(CAST(effective_max_workers AS STRING), '?'),
        ' to ', COALESCE(CAST(effective_max_workers + GREATEST(1, CEIL(effective_max_workers * 0.5)) AS STRING), '?'),
        '. Current: ', COALESCE(worker_node_type, 'unknown'),
        ' (', COALESCE(CAST(worker_cores AS STRING), '?'), ' vCPU / ',
        COALESCE(CAST(worker_memory_gb AS STRING), '?'), ' GB).'
      )
    -- CPU-bound: scale out or larger compute-optimized
    WHEN p95_cpu_busy_pct > 90
      THEN CONCAT(
        'CPU-bound. Raise max_autoscale_workers from ',
        COALESCE(CAST(effective_max_workers AS STRING), '?'),
        ' to ', COALESCE(CAST(effective_max_workers * 2 AS STRING), '?'),
        ', or move to a compute-optimized worker (e.g., c-family). Current: ',
        COALESCE(worker_node_type, 'unknown'),
        ' (', COALESCE(CAST(worker_cores AS STRING), '?'), ' vCPU).'
      )
    -- Very idle: serverless or stop
    WHEN avg_cpu_busy_pct < 10 AND p95_cpu_busy_pct < 30
      THEN CONCAT(
        'Mostly idle (avg ', CAST(ROUND(avg_cpu_busy_pct,1) AS STRING),
        '%, p95 ', CAST(ROUND(p95_cpu_busy_pct,1) AS STRING),
        '%). Strongly consider Serverless or shutting down. ',
        'If kept, reduce max workers from ',
        COALESCE(CAST(effective_max_workers AS STRING), '?'),
        ' to ', CAST(recommended_workers AS STRING), '.'
      )
    -- Under-utilized: scale in
    WHEN utilization_verdict = 'UNDER_UTILIZED'
      THEN CONCAT(
        'Reduce max_autoscale_workers from ',
        COALESCE(CAST(effective_max_workers AS STRING), '?'),
        ' to ', CAST(recommended_workers AS STRING),
        ' (targets ~60% avg CPU). ',
        CASE
          WHEN avg_mem_used_pct < 30 AND worker_cores >= 8
            THEN CONCAT('Both CPU and memory are low; consider downsizing node type from ',
                       COALESCE(worker_node_type, 'current'), ' to a smaller tier.')
          ELSE 'Keep worker node type the same.'
        END,
        CASE
          WHEN auto_termination_minutes IS NULL OR auto_termination_minutes > 60
            THEN CONCAT(' Also reduce auto_termination from ',
                       COALESCE(CAST(auto_termination_minutes AS STRING), 'unset'),
                       ' min to 60.')
          ELSE ''
        END
      )
    WHEN auto_termination_minutes IS NULL OR auto_termination_minutes > 60
      THEN CONCAT('Healthy CPU/memory. Reduce auto_termination from ',
                  COALESCE(CAST(auto_termination_minutes AS STRING), 'unset'),
                  ' min to 60 to free idle time.')
    ELSE 'Within healthy utilization band. No action needed.'
  END                         AS recommended_action,
  -- Helpful link template (customer fills in their workspace URL)
  CONCAT('#setting/clusters/', cluster_id, '/configuration') AS cluster_config_path
FROM scored
"""

spark.sql(rightsizing_sql)
print(f"Built {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.cluster_rightsizing_details")

# COMMAND ----------

# MAGIC %md ## Build hourly time-series view (for dashboard drill-down)

# COMMAND ----------

hourly_sql = f"""
CREATE OR REPLACE TABLE {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.hourly_cluster_utilization AS
SELECT
  workspace_id,
  cluster_id,
  date_trunc('HOUR', start_time) AS hour_bucket,
  AVG(cpu_user_percent + cpu_system_percent) AS avg_cpu_busy_pct,
  PERCENTILE(cpu_user_percent + cpu_system_percent, 0.95) AS p95_cpu_busy_pct,
  AVG(mem_used_percent) AS avg_mem_used_pct,
  PERCENTILE(mem_used_percent, 0.95) AS p95_mem_used_pct,
  MAX(mem_swap_percent) AS max_mem_swap_pct,
  COUNT(DISTINCT instance_id) AS active_instances
FROM system.compute.node_timeline
WHERE start_time >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
  {workspace_predicate_nt}
GROUP BY workspace_id, cluster_id, date_trunc('HOUR', start_time)
"""

spark.sql(hourly_sql)
print(f"Built {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.hourly_cluster_utilization")

# COMMAND ----------

# MAGIC %md ## Build per-job cluster utilization view

# COMMAND ----------

per_job_sql = f"""
CREATE OR REPLACE TABLE {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.per_job_utilization AS
WITH job_spend AS (
  SELECT
    u.usage_metadata.job_id AS job_id,
    u.usage_metadata.cluster_id AS cluster_id,
    u.workspace_id,
    SUM(u.usage_quantity) AS dbus,
    SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)) AS list_cost_usd
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices lp
    ON u.cloud = lp.cloud
   AND u.sku_name = lp.sku_name
   AND lp.currency_code = 'USD'
   AND u.usage_start_time >= lp.price_start_time
   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
  WHERE u.usage_metadata.job_id IS NOT NULL
    AND u.usage_start_time >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
    AND u.sku_name ILIKE '%JOBS%'
    {workspace_predicate_bu}
  GROUP BY u.usage_metadata.job_id, u.usage_metadata.cluster_id, u.workspace_id
),
job_meta AS (
  SELECT workspace_id, job_id, name AS job_name, creator_id,
         ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
  FROM system.lakeflow.jobs
),
job_meta_latest AS (SELECT * FROM job_meta WHERE rn = 1)
SELECT
  js.workspace_id,
  js.job_id,
  jm.job_name,
  js.cluster_id,
  ROUND(js.dbus, 2) AS dbus,
  ROUND(js.list_cost_usd, 2) AS list_cost_usd,
  cu.utilization_verdict,
  cu.recommendation,
  cu.avg_cpu_busy_pct,
  cu.p95_cpu_busy_pct,
  cu.avg_mem_used_pct,
  cu.p95_mem_used_pct,
  cu.estimated_waste_usd
FROM job_spend js
LEFT JOIN job_meta_latest jm
  ON js.workspace_id = jm.workspace_id AND js.job_id = jm.job_id
LEFT JOIN {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.per_cluster_utilization cu
  ON js.workspace_id = cu.workspace_id AND js.cluster_id = cu.cluster_id
"""

spark.sql(per_job_sql)
print(f"Built {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.per_job_utilization")

# COMMAND ----------

# MAGIC %md ## Top under-utilized clusters by $ waste

# COMMAND ----------

display(spark.sql(f"""
  SELECT cluster_id, cluster_name, cluster_source, worker_node_type, worker_count,
         avg_cpu_busy_pct, p95_cpu_busy_pct, avg_mem_used_pct,
         list_cost_usd, estimated_waste_usd, recommendation
  FROM {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.per_cluster_utilization
  WHERE utilization_verdict = 'UNDER_UTILIZED'
  ORDER BY estimated_waste_usd DESC
  LIMIT 25
"""))

# COMMAND ----------

# MAGIC %md ## Over-utilized clusters (memory or CPU pressure)

# COMMAND ----------

display(spark.sql(f"""
  SELECT cluster_id, cluster_name, cluster_source, worker_node_type, worker_count,
         p95_cpu_busy_pct, p95_mem_used_pct, max_mem_swap_pct,
         list_cost_usd, recommendation
  FROM {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.per_cluster_utilization
  WHERE utilization_verdict = 'OVER_UTILIZED'
  ORDER BY list_cost_usd DESC
  LIMIT 25
"""))

# COMMAND ----------

# MAGIC %md ## Verdict summary

# COMMAND ----------

display(spark.sql(f"""
  SELECT utilization_verdict,
         COUNT(*) AS cluster_count,
         ROUND(SUM(list_cost_usd), 2) AS list_cost_usd,
         ROUND(SUM(estimated_waste_usd), 2) AS estimated_waste_usd
  FROM {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.per_cluster_utilization
  GROUP BY utilization_verdict
  ORDER BY list_cost_usd DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC - Open the **Cluster Utilization Advisor** Lakeview dashboard for an interactive ranked view + drill-down.
# MAGIC - Re-run weekly (schedule this notebook as a Job) to keep recommendations fresh.
# MAGIC - For under-utilized clusters with the highest `estimated_waste_usd`, evaluate Serverless migration or smaller node types.
