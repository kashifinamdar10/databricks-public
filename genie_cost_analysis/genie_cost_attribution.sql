-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Genie query cost attribution on a shared SQL warehouse
-- MAGIC
-- MAGIC **Method: duration-proportional allocation.** SQL warehouses bill DBUs for *uptime* (wall-clock), not per query — so there is no exact "cost per query." For each **warehouse-hour** this notebook splits the full billed warehouse cost across every query by its share of active time:
-- MAGIC
-- MAGIC ```
-- MAGIC est_genie_cost = warehouse_cost_that_hour × (genie_active_ms / all_active_ms_that_hour)
-- MAGIC ```
-- MAGIC
-- MAGIC - **Denominator = every query on that warehouse that hour** (Genie + non-Genie + all users) → Genie never absorbs more than its fair share, and idle / keep-alive time (e.g. a lone user until timeout, or a query fired just before timeout) is spread proportionally across active work.
-- MAGIC - **Grain:** `day × genie_space × user × warehouse`.
-- MAGIC - **Price:** `system.billing.list_prices` effective list. Swap for negotiated rates if needed.
-- MAGIC
-- MAGIC **Caveats:** (1) It's an *estimate*, not a bill. (2) `system.billing.usage` lags a few hours; the join is inner, so run windows ending ~1 day back for stable numbers. (3) On some workspaces Genie queries have NULL `executed_by` (collapses into a NULL user row); space/warehouse attribution stays reliable. (4) For exact numbers, put the space on a dedicated tagged serverless warehouse and read `system.billing.usage` directly.
-- MAGIC
-- MAGIC Set the widgets above (comma-separate multiple space IDs), then run.

-- COMMAND ----------

CREATE WIDGET TEXT space_ids DEFAULT '01f164d0ba841bbbaf8b9c3ef43503cf';
CREATE WIDGET TEXT lookback_days DEFAULT '30';

-- COMMAND ----------

WITH params AS (
  SELECT
    split('${space_ids}', ',')                                              AS space_ids,
    current_timestamp() - make_interval(0, 0, 0, cast('${lookback_days}' AS INT)) AS start_ts,
    current_timestamp()                                                     AS end_ts
),

-- Denominator: ALL activity per warehouse-hour (Genie + non-Genie + all users)
all_activity AS (
  SELECT
    compute.warehouse_id                     AS warehouse_id,
    date_trunc('hour', start_time)           AS hour_ts,
    SUM(total_duration_ms)                   AS total_ms
  FROM system.query.history, params
  WHERE compute.warehouse_id IS NOT NULL
    AND start_time >= params.start_ts AND start_time < params.end_ts
  GROUP BY 1, 2
),

-- Numerator: target-space activity at day x space x user x warehouse x hour
genie_activity AS (
  SELECT
    compute.warehouse_id                     AS warehouse_id,
    date_trunc('hour', start_time)           AS hour_ts,
    to_date(start_time)                      AS usage_date,
    query_source.genie_space_id              AS genie_space_id,
    executed_by                              AS user_email,
    SUM(total_duration_ms)                   AS genie_ms,
    COUNT(*)                                 AS genie_query_count
  FROM system.query.history, params
  WHERE query_source.genie_space_id IS NOT NULL
    AND array_contains(params.space_ids, query_source.genie_space_id)
    AND compute.warehouse_id IS NOT NULL
    AND start_time >= params.start_ts AND start_time < params.end_ts
  GROUP BY 1, 2, 3, 4, 5
),

-- Warehouse billed DBUs and $ per warehouse-hour (effective list price)
warehouse_billing AS (
  SELECT
    u.usage_metadata.warehouse_id                                 AS warehouse_id,
    date_trunc('hour', u.usage_start_time)                        AS hour_ts,
    SUM(u.usage_quantity)                                         AS warehouse_dbus,
    SUM(u.usage_quantity * lp.pricing.effective_list.default)     AS warehouse_cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp
    ON u.cloud = lp.cloud
   AND u.sku_name = lp.sku_name
   AND u.usage_start_time >= lp.price_start_time
   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
  , params
  WHERE u.usage_metadata.warehouse_id IS NOT NULL
    AND u.usage_start_time >= params.start_ts AND u.usage_start_time < params.end_ts
  GROUP BY 1, 2
)

SELECT
  g.usage_date,
  g.genie_space_id,
  g.user_email,
  g.warehouse_id,
  SUM(g.genie_query_count)                                       AS genie_query_count,
  ROUND(SUM(g.genie_ms)/1000.0, 1)                              AS genie_active_sec,
  ROUND(SUM(b.warehouse_dbus * g.genie_ms / a.total_ms), 4)    AS est_genie_dbus,
  ROUND(SUM(b.warehouse_cost_usd * g.genie_ms / a.total_ms), 4) AS est_genie_cost_usd
FROM genie_activity g
JOIN all_activity   a ON g.warehouse_id = a.warehouse_id AND g.hour_ts = a.hour_ts
JOIN warehouse_billing b ON g.warehouse_id = b.warehouse_id AND g.hour_ts = b.hour_ts
WHERE a.total_ms > 0
GROUP BY 1, 2, 3, 4
ORDER BY g.usage_date DESC, est_genie_cost_usd DESC

