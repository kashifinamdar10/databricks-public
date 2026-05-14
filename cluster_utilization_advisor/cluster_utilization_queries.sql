-- =============================================================================
-- Databricks Cluster Utilization Dashboard — SQL Queries
-- Scope: All-purpose (interactive) clusters
-- Window: Trailing 90 days (parameterized via :days_back, default 90)
-- Data Sources:
--   system.compute.clusters           -- cluster config & lifecycle
--   system.compute.node_timeline      -- per-minute CPU/mem utilization
--   system.compute.node_types         -- node sizing reference
--   system.billing.usage              -- DBU consumption / cost
-- =============================================================================


-- =============================================================================
-- QUERY 1: cluster_base
-- Latest config snapshot per cluster, filtered to all-purpose clusters only.
-- Used as the foundation for every downstream query.
-- =============================================================================
CREATE OR REPLACE TEMP VIEW cluster_base AS
WITH ranked AS (
  SELECT
    c.workspace_id,
    c.cluster_id,
    c.cluster_name,
    c.owned_by,
    c.create_time,
    c.delete_time,
    c.driver_node_type,
    c.worker_node_type,
    c.worker_count,
    c.min_autoscale_workers,
    c.max_autoscale_workers,
    c.auto_termination_minutes,
    c.enable_elastic_disk,
    c.tags,
    c.cluster_source,
    c.dbr_version,
    c.data_security_mode,
    c.policy_id,
    c.change_time,
    ROW_NUMBER() OVER (PARTITION BY c.cluster_id ORDER BY c.change_time DESC) AS rn
  FROM system.compute.clusters c
  WHERE c.cluster_source IN ('UI', 'API')          -- exclude JOB / PIPELINE clusters
    AND (c.delete_time IS NULL
         OR c.delete_time >= CURRENT_TIMESTAMP() - INTERVAL :days_back DAYS)
)
SELECT * EXCEPT (rn) FROM ranked WHERE rn = 1;


-- =============================================================================
-- QUERY 2: utilization_raw
-- Joins minute-level node timeline with node specs to compute weighted
-- CPU & memory utilization per cluster over the window.
-- =============================================================================
CREATE OR REPLACE TEMP VIEW utilization_raw AS
SELECT
  nt.workspace_id,
  nt.cluster_id,
  nt.instance_id,
  nt.start_time,
  nt.end_time,
  nt.driver,
  nt.cpu_user_percent,
  nt.cpu_system_percent,
  nt.cpu_wait_percent,
  nt.mem_used_percent,
  nt.mem_swap_percent,
  nt.network_received_bytes,
  nt.network_sent_bytes,
  nt.disk_free_bytes_per_mount_point,
  -- combined active CPU usage (user + system)
  COALESCE(nt.cpu_user_percent, 0) + COALESCE(nt.cpu_system_percent, 0) AS cpu_active_percent
FROM system.compute.node_timeline nt
WHERE nt.start_time >= CURRENT_TIMESTAMP() - INTERVAL :days_back DAYS
  AND nt.cluster_id IN (SELECT cluster_id FROM cluster_base);


-- =============================================================================
-- QUERY 3: cluster_utilization_summary
-- Aggregates utilization, runtime, scaling activity, and idle time per cluster.
-- =============================================================================
CREATE OR REPLACE TEMP VIEW cluster_utilization_summary AS
WITH per_cluster AS (
  SELECT
    cluster_id,
    -- runtime metrics
    COUNT(DISTINCT DATE_TRUNC('MINUTE', start_time)) AS active_minutes,
    COUNT(DISTINCT DATE_TRUNC('DAY', start_time))    AS active_days,
    COUNT(DISTINCT instance_id)                       AS distinct_nodes_seen,
    -- CPU
    AVG(cpu_active_percent)                           AS avg_cpu_percent,
    PERCENTILE_APPROX(cpu_active_percent, 0.50)       AS p50_cpu_percent,
    PERCENTILE_APPROX(cpu_active_percent, 0.95)       AS p95_cpu_percent,
    MAX(cpu_active_percent)                           AS max_cpu_percent,
    STDDEV(cpu_active_percent)                        AS stddev_cpu_percent,
    -- Memory
    AVG(mem_used_percent)                             AS avg_mem_percent,
    PERCENTILE_APPROX(mem_used_percent, 0.95)         AS p95_mem_percent,
    MAX(mem_used_percent)                             AS max_mem_percent,
    AVG(mem_swap_percent)                             AS avg_swap_percent,
    -- Idle minutes: both driver and workers under 10% CPU
    SUM(CASE WHEN cpu_active_percent < 10 THEN 1 ELSE 0 END) AS idle_minute_samples,
    COUNT(*)                                          AS total_minute_samples
  FROM utilization_raw
  GROUP BY cluster_id
),
scaling AS (
  -- Approximates autoscale activity by counting distinct worker counts seen in node timeline.
  -- (node_timeline samples every minute; distinct concurrent non-driver nodes ≈ active worker count)
  SELECT
    cluster_id,
    DATE_TRUNC('MINUTE', start_time) AS minute_bucket,
    SUM(CASE WHEN driver = false THEN 1 ELSE 0 END)  AS workers_active
  FROM utilization_raw
  GROUP BY cluster_id, DATE_TRUNC('MINUTE', start_time)
),
scaling_agg AS (
  SELECT
    cluster_id,
    AVG(workers_active)              AS avg_active_workers,
    MIN(workers_active)              AS min_active_workers,
    MAX(workers_active)              AS max_active_workers,
    PERCENTILE_APPROX(workers_active, 0.50) AS p50_active_workers,
    PERCENTILE_APPROX(workers_active, 0.95) AS p95_active_workers,
    COUNT(DISTINCT workers_active)   AS distinct_worker_counts  -- scale events proxy
  FROM scaling
  GROUP BY cluster_id
)
SELECT
  pc.cluster_id,
  pc.active_minutes,
  pc.active_days,
  pc.distinct_nodes_seen,
  ROUND(pc.avg_cpu_percent, 2)      AS avg_cpu_percent,
  ROUND(pc.p50_cpu_percent, 2)      AS p50_cpu_percent,
  ROUND(pc.p95_cpu_percent, 2)      AS p95_cpu_percent,
  ROUND(pc.max_cpu_percent, 2)      AS max_cpu_percent,
  ROUND(pc.stddev_cpu_percent, 2)   AS stddev_cpu_percent,
  ROUND(pc.avg_mem_percent, 2)      AS avg_mem_percent,
  ROUND(pc.p95_mem_percent, 2)      AS p95_mem_percent,
  ROUND(pc.max_mem_percent, 2)      AS max_mem_percent,
  ROUND(pc.avg_swap_percent, 2)     AS avg_swap_percent,
  pc.idle_minute_samples,
  pc.total_minute_samples,
  ROUND(100.0 * pc.idle_minute_samples / NULLIF(pc.total_minute_samples, 0), 2) AS idle_percent,
  sa.avg_active_workers,
  sa.min_active_workers,
  sa.max_active_workers,
  sa.p50_active_workers,
  sa.p95_active_workers,
  sa.distinct_worker_counts
FROM per_cluster pc
LEFT JOIN scaling_agg sa USING (cluster_id);


-- =============================================================================
-- QUERY 4: cluster_cost
-- DBU consumption & dollar cost per cluster from billing system table.
-- =============================================================================
CREATE OR REPLACE TEMP VIEW cluster_cost AS
SELECT
  usage_metadata.cluster_id AS cluster_id,
  SUM(usage_quantity)                             AS total_dbus,
  SUM(usage_quantity * COALESCE(lp.pricing.default, 0)) AS estimated_cost_usd,
  COUNT(DISTINCT usage_date)                      AS billed_days
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name
 AND u.usage_unit = lp.usage_unit
 AND u.usage_end_time BETWEEN lp.price_start_time AND COALESCE(lp.price_end_time, CURRENT_TIMESTAMP())
WHERE usage_metadata.cluster_id IS NOT NULL
  AND usage_date >= CURRENT_DATE() - INTERVAL :days_back DAYS
  AND usage_metadata.cluster_id IN (SELECT cluster_id FROM cluster_base)
GROUP BY usage_metadata.cluster_id;


-- =============================================================================
-- QUERY 5: cluster_assessment  *** CORE CATEGORIZATION QUERY ***
-- Joins config + utilization + cost and applies sizing logic.
-- Produces: category, reasons, and concrete recommendation per cluster.
--
-- Categorization logic
-- --------------------
--   OVER_UTILIZED   : p95 CPU >= 85% OR p95 mem >= 90% OR avg swap > 5%
--                     OR autoscale ceiling pinned (p95 workers == max_autoscale_workers
--                                                  AND max_autoscale_workers > min_autoscale_workers)
--   UNDER_UTILIZED  : avg CPU < 20% AND p95 CPU < 40% AND p95 mem < 50%
--                     OR idle_percent > 60% with auto_termination_minutes >= 120 or NULL
--   IDLE_OR_UNUSED  : active_minutes < 60 over the whole window  (essentially abandoned)
--   RIGHT_SIZED     : everything else
-- =============================================================================
CREATE OR REPLACE TEMP VIEW cluster_assessment AS
WITH joined AS (
  SELECT
    b.workspace_id,
    b.cluster_id,
    b.cluster_name,
    b.owned_by,
    b.driver_node_type,
    b.worker_node_type,
    b.worker_count,
    b.min_autoscale_workers,
    b.max_autoscale_workers,
    CASE WHEN b.max_autoscale_workers IS NOT NULL THEN TRUE ELSE FALSE END AS autoscale_enabled,
    b.auto_termination_minutes,
    b.enable_elastic_disk,
    b.dbr_version,
    b.data_security_mode,
    b.policy_id,
    b.create_time,
    b.delete_time,
    u.active_minutes,
    u.active_days,
    u.avg_cpu_percent,
    u.p50_cpu_percent,
    u.p95_cpu_percent,
    u.max_cpu_percent,
    u.stddev_cpu_percent,
    u.avg_mem_percent,
    u.p95_mem_percent,
    u.max_mem_percent,
    u.avg_swap_percent,
    u.idle_percent,
    u.avg_active_workers,
    u.min_active_workers,
    u.max_active_workers,
    u.p50_active_workers,
    u.p95_active_workers,
    u.distinct_worker_counts,
    c.total_dbus,
    c.estimated_cost_usd
  FROM cluster_base b
  LEFT JOIN cluster_utilization_summary u USING (cluster_id)
  LEFT JOIN cluster_cost c USING (cluster_id)
),
flagged AS (
  SELECT
    j.*,
    -- individual flags (used to construct human-readable reasons)
    (p95_cpu_percent >= 85)                                                              AS flag_cpu_pressure,
    (p95_mem_percent >= 90)                                                              AS flag_mem_pressure,
    (avg_swap_percent > 5)                                                               AS flag_swap_pressure,
    (autoscale_enabled
       AND p95_active_workers >= max_autoscale_workers
       AND max_autoscale_workers > min_autoscale_workers)                                AS flag_autoscale_pinned,
    (avg_cpu_percent < 20 AND p95_cpu_percent < 40 AND p95_mem_percent < 50)             AS flag_low_util,
    (idle_percent > 60)                                                                  AS flag_high_idle,
    (auto_termination_minutes IS NULL OR auto_termination_minutes >= 120)                AS flag_loose_termination,
    (autoscale_enabled
       AND p95_active_workers <= min_autoscale_workers
       AND min_autoscale_workers < max_autoscale_workers
       AND avg_cpu_percent < 30)                                                         AS flag_autoscale_floor_only,
    (active_minutes IS NULL OR active_minutes < 60)                                      AS flag_essentially_unused,
    (NOT autoscale_enabled AND worker_count IS NOT NULL AND worker_count >= 4
       AND avg_cpu_percent < 25)                                                         AS flag_fixed_oversized
  FROM joined j
),
categorized AS (
  SELECT
    f.*,
    CASE
      WHEN flag_essentially_unused                                            THEN 'IDLE_OR_UNUSED'
      WHEN flag_cpu_pressure OR flag_mem_pressure OR flag_swap_pressure
           OR flag_autoscale_pinned                                           THEN 'OVER_UTILIZED'
      WHEN flag_low_util OR flag_fixed_oversized
           OR (flag_high_idle AND flag_loose_termination)
           OR flag_autoscale_floor_only                                       THEN 'UNDER_UTILIZED'
      ELSE 'RIGHT_SIZED'
    END AS utilization_category
  FROM flagged f
)
SELECT
  c.*,
  -- Build a human-readable reason string from the flags
  CONCAT_WS(' | ',
    CASE WHEN flag_cpu_pressure         THEN CONCAT('CPU p95 ',         CAST(p95_cpu_percent AS STRING), '% >= 85% (sustained CPU pressure)') END,
    CASE WHEN flag_mem_pressure         THEN CONCAT('Memory p95 ',      CAST(p95_mem_percent AS STRING), '% >= 90% (memory pressure)') END,
    CASE WHEN flag_swap_pressure        THEN CONCAT('Avg swap ',        CAST(avg_swap_percent AS STRING), '% > 5% (memory thrashing)') END,
    CASE WHEN flag_autoscale_pinned     THEN CONCAT('Autoscale pinned at max (', CAST(max_autoscale_workers AS STRING), ' workers) at p95') END,
    CASE WHEN flag_low_util             THEN CONCAT('Low utilization: avg CPU ', CAST(avg_cpu_percent AS STRING), '%, p95 CPU ', CAST(p95_cpu_percent AS STRING), '%, p95 mem ', CAST(p95_mem_percent AS STRING), '%') END,
    CASE WHEN flag_high_idle AND flag_loose_termination
                                        THEN CONCAT('Idle ', CAST(idle_percent AS STRING), '% of runtime with auto-termination = ', COALESCE(CAST(auto_termination_minutes AS STRING), 'DISABLED'), ' min') END,
    CASE WHEN flag_autoscale_floor_only THEN CONCAT('Autoscale never exceeded floor (', CAST(min_autoscale_workers AS STRING), ' workers); ceiling of ', CAST(max_autoscale_workers AS STRING), ' unused') END,
    CASE WHEN flag_fixed_oversized      THEN CONCAT('Fixed-size cluster with ', CAST(worker_count AS STRING), ' workers but avg CPU only ', CAST(avg_cpu_percent AS STRING), '%') END,
    CASE WHEN flag_essentially_unused   THEN CONCAT('Cluster ran <60 minutes total in window (', COALESCE(CAST(active_minutes AS STRING), '0'), ' min) — likely abandoned') END
  ) AS reason,

  -- Concrete, actionable recommendation
  CASE
    WHEN flag_essentially_unused THEN
      'DELETE: cluster has been essentially unused. If retained for ad-hoc use, ensure auto-termination is set to 30 minutes or less.'

    WHEN flag_autoscale_pinned AND flag_cpu_pressure THEN
      CONCAT('SCALE UP: raise max_autoscale_workers above ', CAST(max_autoscale_workers AS STRING),
             ' (workload routinely hits the ceiling). Also consider a larger worker node type since CPU p95 = ',
             CAST(p95_cpu_percent AS STRING), '%.')

    WHEN flag_autoscale_pinned THEN
      CONCAT('RAISE CEILING: increase max_autoscale_workers from ', CAST(max_autoscale_workers AS STRING),
             ' (workload is pinned at the ceiling at p95). Suggested new max: ',
             CAST(CEIL(max_autoscale_workers * 1.5) AS STRING), '.')

    WHEN flag_mem_pressure OR flag_swap_pressure THEN
      'UPGRADE NODE TYPE: move to a memory-optimized worker (e.g., r-series) — sustained memory pressure / swap detected. Also enable elastic disk if not already on.'

    WHEN flag_cpu_pressure THEN
      'UPGRADE NODE TYPE: move to a compute-optimized worker (e.g., c-series) or one with more vCPUs — sustained CPU pressure detected.'

    WHEN flag_fixed_oversized THEN
      CONCAT('CONVERT TO AUTOSCALING: replace fixed worker_count=', CAST(worker_count AS STRING),
             ' with autoscaling min=', CAST(GREATEST(1, CAST(worker_count/4 AS INT)) AS STRING),
             ', max=', CAST(worker_count AS STRING),
             '. Average CPU is only ', CAST(avg_cpu_percent AS STRING), '%.')

    WHEN flag_autoscale_floor_only THEN
      CONCAT('LOWER FLOOR: reduce min_autoscale_workers from ', CAST(min_autoscale_workers AS STRING),
             ' to 1 — workload never scaled above the floor. Keep max at ', CAST(max_autoscale_workers AS STRING), '.')

    WHEN flag_high_idle AND flag_loose_termination THEN
      CONCAT('TIGHTEN AUTO-TERMINATION: set auto_termination_minutes to 30 (currently ',
             COALESCE(CAST(auto_termination_minutes AS STRING), 'DISABLED'),
             '). Cluster sits idle ', CAST(idle_percent AS STRING), '% of runtime.')

    WHEN flag_low_util THEN
      CONCAT('DOWNSIZE: shift to a smaller worker node type and/or reduce max workers. Avg CPU = ',
             CAST(avg_cpu_percent AS STRING), '%, p95 mem = ', CAST(p95_mem_percent AS STRING),
             '%. If usage is bursty and intermittent, consider migrating workloads to a serverless or job cluster.')

    ELSE
      'NO CHANGE: cluster is appropriately sized for its current workload. Continue to monitor.'
  END AS recommendation
FROM categorized c;


-- =============================================================================
-- DASHBOARD QUERIES (these are the ones the visuals read from)
-- =============================================================================

-- ---- D1: KPI tiles ----------------------------------------------------------
-- Total clusters, totals by category, total cost, potential savings (from
-- under-utilized + idle clusters).
SELECT
  COUNT(*)                                                                       AS total_clusters,
  COUNT_IF(utilization_category = 'OVER_UTILIZED')                               AS over_utilized,
  COUNT_IF(utilization_category = 'UNDER_UTILIZED')                              AS under_utilized,
  COUNT_IF(utilization_category = 'RIGHT_SIZED')                                 AS right_sized,
  COUNT_IF(utilization_category = 'IDLE_OR_UNUSED')                              AS idle_or_unused,
  ROUND(SUM(estimated_cost_usd), 2)                                              AS total_cost_usd,
  ROUND(SUM(CASE WHEN utilization_category IN ('UNDER_UTILIZED','IDLE_OR_UNUSED')
                 THEN estimated_cost_usd * 0.4 ELSE 0 END), 2)                   AS estimated_savings_usd,
  ROUND(SUM(total_dbus), 2)                                                      AS total_dbus
FROM cluster_assessment;


-- ---- D2: Category distribution (pie / donut) --------------------------------
SELECT
  utilization_category,
  COUNT(*)                            AS cluster_count,
  ROUND(SUM(estimated_cost_usd), 2)   AS cost_usd
FROM cluster_assessment
GROUP BY utilization_category
ORDER BY cluster_count DESC;


-- ---- D3: Top-cost clusters (bar chart) --------------------------------------
SELECT
  cluster_name,
  utilization_category,
  ROUND(estimated_cost_usd, 2) AS cost_usd,
  avg_cpu_percent,
  p95_cpu_percent
FROM cluster_assessment
WHERE estimated_cost_usd IS NOT NULL
ORDER BY estimated_cost_usd DESC
LIMIT 20;


-- ---- D4: Cost-vs-utilization scatter ---------------------------------------
-- Visual: scatter — x=avg_cpu_percent, y=estimated_cost_usd, color=category, size=total_dbus.
-- Helps customer instantly spot expensive low-utilization clusters.
SELECT
  cluster_id,
  cluster_name,
  utilization_category,
  COALESCE(avg_cpu_percent, 0)        AS avg_cpu_percent,
  COALESCE(p95_cpu_percent, 0)        AS p95_cpu_percent,
  COALESCE(estimated_cost_usd, 0)     AS estimated_cost_usd,
  COALESCE(total_dbus, 0)             AS total_dbus
FROM cluster_assessment;


-- ---- D5: Owner breakdown ----------------------------------------------------
SELECT
  owned_by,
  COUNT(*)                                                AS cluster_count,
  COUNT_IF(utilization_category = 'UNDER_UTILIZED')       AS under_utilized,
  COUNT_IF(utilization_category = 'OVER_UTILIZED')        AS over_utilized,
  COUNT_IF(utilization_category = 'IDLE_OR_UNUSED')       AS idle_or_unused,
  ROUND(SUM(estimated_cost_usd), 2)                       AS total_cost_usd
FROM cluster_assessment
WHERE owned_by IS NOT NULL
GROUP BY owned_by
ORDER BY total_cost_usd DESC NULLS LAST
LIMIT 25;


-- ---- D6: Detail table (the main customer-facing list) ----------------------
SELECT
  cluster_name,
  utilization_category                              AS category,
  owned_by,
  driver_node_type,
  worker_node_type,
  CASE WHEN autoscale_enabled
       THEN CONCAT(CAST(min_autoscale_workers AS STRING), ' – ', CAST(max_autoscale_workers AS STRING), ' (autoscale)')
       ELSE CONCAT(CAST(worker_count AS STRING), ' (fixed)')
  END                                               AS worker_config,
  auto_termination_minutes                          AS auto_term_min,
  active_days,
  avg_cpu_percent,
  p95_cpu_percent,
  p95_mem_percent,
  idle_percent,
  p95_active_workers,
  ROUND(estimated_cost_usd, 2)                      AS cost_usd,
  reason,
  recommendation
FROM cluster_assessment
ORDER BY
  CASE utilization_category
    WHEN 'OVER_UTILIZED'  THEN 1
    WHEN 'UNDER_UTILIZED' THEN 2
    WHEN 'IDLE_OR_UNUSED' THEN 3
    ELSE 4
  END,
  estimated_cost_usd DESC NULLS LAST;


-- ---- D7: Autoscaling effectiveness ------------------------------------------
SELECT
  CASE
    WHEN NOT autoscale_enabled                                                       THEN 'No autoscale (fixed)'
    WHEN p95_active_workers >= max_autoscale_workers                                 THEN 'Pinned at ceiling'
    WHEN p95_active_workers <= min_autoscale_workers
         AND min_autoscale_workers < max_autoscale_workers                           THEN 'Stuck at floor'
    ELSE 'Scaling normally'
  END                                                AS autoscale_status,
  COUNT(*)                                           AS cluster_count,
  ROUND(AVG(avg_cpu_percent), 2)                     AS avg_cpu,
  ROUND(SUM(estimated_cost_usd), 2)                  AS total_cost_usd
FROM cluster_assessment
GROUP BY 1
ORDER BY cluster_count DESC;


-- ---- D8: Auto-termination compliance ---------------------------------------
SELECT
  CASE
    WHEN auto_termination_minutes IS NULL                  THEN 'DISABLED (risky)'
    WHEN auto_termination_minutes <= 30                    THEN '<= 30 min (good)'
    WHEN auto_termination_minutes <= 60                    THEN '31 – 60 min (ok)'
    WHEN auto_termination_minutes <= 120                   THEN '61 – 120 min (loose)'
    ELSE '> 120 min (too loose)'
  END                                                AS termination_band,
  COUNT(*)                                           AS cluster_count,
  ROUND(AVG(idle_percent), 2)                        AS avg_idle_percent,
  ROUND(SUM(estimated_cost_usd), 2)                  AS total_cost_usd
FROM cluster_assessment
GROUP BY 1
ORDER BY cluster_count DESC;


-- ---- D9: Daily utilization trend (line chart) -------------------------------
-- Note: this hits the raw utilization view, not the per-cluster summary.
SELECT
  DATE_TRUNC('DAY', start_time)         AS day,
  ROUND(AVG(cpu_active_percent), 2)     AS avg_cpu_percent,
  ROUND(AVG(mem_used_percent), 2)       AS avg_mem_percent,
  COUNT(DISTINCT cluster_id)            AS active_clusters
FROM utilization_raw
GROUP BY 1
ORDER BY 1;
