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
-- MAGIC - **Denominator = every query on that warehouse that hour** (Genie + non-Genie + all users) → Genie never absorbs more than its fair share, and idle / keep-alive time (a lone user until timeout, or a query fired just before timeout) is spread proportionally across active work.
-- MAGIC - **Grain:** `day × genie_space × user × warehouse`.
-- MAGIC - **Price:** `system.billing.list_prices` effective list. Swap for negotiated rates if needed.
-- MAGIC
-- MAGIC **This notebook produces two views:**
-- MAGIC 1. **Warehouse summary** — size, cluster config, auto-stop, total cost, avg $/running-hour, and the Genie space's estimated cost + % share of the whole warehouse bill.
-- MAGIC 2. **Per-user daily detail** — estimated DBUs/$ per user, the warehouse's total cost that day, and each user's % share of it.
-- MAGIC
-- MAGIC **Parameters** are bound via named markers (`:space_ids`, `:lookback_days`). Set them in the widget bar (comma-separate multiple space IDs), then run.
-- MAGIC
-- MAGIC **Caveats:** (1) It's an *estimate*, not a bill. (2) `system.billing.usage` lags a few hours; joins are inner, so run windows ending ~1 day back for stable numbers. (3) On some workspaces Genie queries have NULL `executed_by` (collapses into a NULL user row); space/warehouse attribution stays reliable. (4) Warehouse size can change over time — the summary shows the **latest** config; `avg_cost_per_running_hour` is the true realized rate from billing regardless of resizes. (5) For exact numbers, put the space on a dedicated tagged serverless warehouse and read `system.billing.usage` directly.

-- COMMAND ----------

CREATE WIDGET TEXT space_ids DEFAULT '01f164d0ba841bbbaf8b9c3ef43503cf';
CREATE WIDGET TEXT lookback_days DEFAULT '30';

-- COMMAND ----------

-- MAGIC %md ## 1. Warehouse summary — size, config, avg hourly cost, Genie share

-- COMMAND ----------

WITH params AS (
  SELECT
    split(:space_ids, ',')                                          AS space_ids,
    current_timestamp() - make_interval(0, 0, 0, cast(:lookback_days AS INT)) AS start_ts,
    current_timestamp()                                             AS end_ts
),
all_activity AS (
  SELECT compute.warehouse_id AS warehouse_id, date_trunc('hour', start_time) AS hour_ts,
         SUM(total_duration_ms) AS total_ms
  FROM system.query.history, params
  WHERE compute.warehouse_id IS NOT NULL AND start_time >= start_ts AND start_time < end_ts
  GROUP BY 1, 2
),
genie_activity AS (
  SELECT compute.warehouse_id AS warehouse_id, date_trunc('hour', start_time) AS hour_ts,
         SUM(total_duration_ms) AS genie_ms, COUNT(*) AS genie_query_count
  FROM system.query.history, params
  WHERE query_source.genie_space_id IS NOT NULL
    AND array_contains(space_ids, query_source.genie_space_id)
    AND compute.warehouse_id IS NOT NULL AND start_time >= start_ts AND start_time < end_ts
  GROUP BY 1, 2
),
warehouse_billing AS (
  SELECT u.usage_metadata.warehouse_id AS warehouse_id, date_trunc('hour', u.usage_start_time) AS hour_ts,
         SUM(u.usage_quantity) AS dbus,
         SUM(u.usage_quantity * lp.pricing.effective_list.default) AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
    AND u.usage_start_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time), params
  WHERE u.usage_metadata.warehouse_id IS NOT NULL
    AND u.usage_start_time >= start_ts AND u.usage_start_time < end_ts
  GROUP BY 1, 2
),
wh_totals AS (
  -- cost_usd is already per warehouse-hour, so MIN/MEDIAN/MAX over rows = hourly-rate
  -- distribution. The spread reflects serverless autoscaling (1 -> max_clusters).
  SELECT warehouse_id,
         COUNT(DISTINCT hour_ts)                                    AS running_hours,
         ROUND(SUM(dbus), 2)                                        AS wh_total_dbus,
         ROUND(SUM(cost_usd), 2)                                    AS wh_total_cost_usd,
         ROUND(AVG(cost_usd), 2)                                    AS avg_cost_per_running_hour,
         ROUND(MIN(cost_usd), 2)                                    AS min_cost_per_hour,
         ROUND(percentile(cost_usd, 0.5), 2)                        AS median_cost_per_hour,
         ROUND(percentile(cost_usd, 0.9), 2)                        AS p90_cost_per_hour,
         ROUND(MAX(cost_usd), 2)                                    AS max_cost_per_hour
  FROM warehouse_billing GROUP BY 1
),
genie_cost AS (
  SELECT g.warehouse_id,
         SUM(g.genie_query_count)                          AS genie_query_count,
         ROUND(SUM(b.dbus * g.genie_ms / a.total_ms), 4)   AS est_genie_dbus,
         ROUND(SUM(b.cost_usd * g.genie_ms / a.total_ms), 4) AS est_genie_cost_usd
  FROM genie_activity g
  JOIN all_activity a ON g.warehouse_id = a.warehouse_id AND g.hour_ts = a.hour_ts
  JOIN warehouse_billing b ON g.warehouse_id = b.warehouse_id AND g.hour_ts = b.hour_ts
  WHERE a.total_ms > 0 GROUP BY 1
),
wh_cfg AS (
  SELECT warehouse_id, warehouse_name, warehouse_type, warehouse_size,
         min_clusters, max_clusters, auto_stop_minutes, tags,
         ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) AS rn
  FROM system.compute.warehouses
)
SELECT c.warehouse_name, c.warehouse_size, c.warehouse_type,
       c.min_clusters, c.max_clusters, c.auto_stop_minutes,
       t.running_hours, t.wh_total_dbus, t.wh_total_cost_usd,
       t.avg_cost_per_running_hour, t.min_cost_per_hour, t.median_cost_per_hour,
       t.p90_cost_per_hour, t.max_cost_per_hour,
       gc.genie_query_count, gc.est_genie_dbus, gc.est_genie_cost_usd,
       ROUND(100.0 * gc.est_genie_cost_usd / NULLIF(t.wh_total_cost_usd, 0), 4) AS genie_pct_of_wh_cost
FROM genie_cost gc
JOIN wh_totals t ON gc.warehouse_id = t.warehouse_id
LEFT JOIN wh_cfg c ON gc.warehouse_id = c.warehouse_id AND c.rn = 1
ORDER BY gc.est_genie_cost_usd DESC;

-- COMMAND ----------

-- MAGIC %md ## 2. Per-user daily detail — estimated cost and share of warehouse

-- COMMAND ----------

WITH params AS (
  SELECT
    split(:space_ids, ',')                                          AS space_ids,
    current_timestamp() - make_interval(0, 0, 0, cast(:lookback_days AS INT)) AS start_ts,
    current_timestamp()                                             AS end_ts
),
all_activity AS (
  SELECT compute.warehouse_id AS warehouse_id, date_trunc('hour', start_time) AS hour_ts,
         SUM(total_duration_ms) AS total_ms
  FROM system.query.history, params
  WHERE compute.warehouse_id IS NOT NULL AND start_time >= start_ts AND start_time < end_ts GROUP BY 1, 2
),
genie_activity AS (
  SELECT compute.warehouse_id AS warehouse_id, date_trunc('hour', start_time) AS hour_ts,
         to_date(start_time) AS usage_date, query_source.genie_space_id AS genie_space_id,
         executed_by AS user_email, SUM(total_duration_ms) AS genie_ms, COUNT(*) AS genie_query_count
  FROM system.query.history, params
  WHERE query_source.genie_space_id IS NOT NULL AND array_contains(space_ids, query_source.genie_space_id)
    AND compute.warehouse_id IS NOT NULL AND start_time >= start_ts AND start_time < end_ts GROUP BY 1, 2, 3, 4, 5
),
warehouse_billing AS (
  SELECT u.usage_metadata.warehouse_id AS warehouse_id, date_trunc('hour', u.usage_start_time) AS hour_ts,
         SUM(u.usage_quantity) AS dbus, SUM(u.usage_quantity * lp.pricing.effective_list.default) AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
    AND u.usage_start_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time), params
  WHERE u.usage_metadata.warehouse_id IS NOT NULL AND u.usage_start_time >= start_ts AND u.usage_start_time < end_ts GROUP BY 1, 2
),
wh_daily AS (
  SELECT warehouse_id, to_date(hour_ts) AS usage_date, SUM(cost_usd) AS wh_day_cost
  FROM warehouse_billing GROUP BY 1, 2
),
user_daily AS (
  SELECT g.usage_date, g.genie_space_id, g.user_email, g.warehouse_id,
         SUM(g.genie_query_count) AS genie_query_count, ROUND(SUM(g.genie_ms)/1000.0, 1) AS genie_active_sec,
         ROUND(SUM(b.dbus * g.genie_ms / a.total_ms), 4) AS est_user_dbus,
         ROUND(SUM(b.cost_usd * g.genie_ms / a.total_ms), 4) AS est_user_cost_usd
  FROM genie_activity g
  JOIN all_activity a ON g.warehouse_id = a.warehouse_id AND g.hour_ts = a.hour_ts
  JOIN warehouse_billing b ON g.warehouse_id = b.warehouse_id AND g.hour_ts = b.hour_ts
  WHERE a.total_ms > 0 GROUP BY 1, 2, 3, 4
)
SELECT u.usage_date, u.genie_space_id, u.user_email, u.warehouse_id,
       u.genie_query_count, u.genie_active_sec, u.est_user_dbus, u.est_user_cost_usd,
       ROUND(d.wh_day_cost, 2)                                             AS warehouse_day_cost_usd,
       ROUND(100.0 * u.est_user_cost_usd / NULLIF(d.wh_day_cost, 0), 4)   AS user_pct_of_wh_day_cost
FROM user_daily u
LEFT JOIN wh_daily d ON u.warehouse_id = d.warehouse_id AND u.usage_date = d.usage_date
ORDER BY u.usage_date DESC, u.est_user_cost_usd DESC;

-- COMMAND ----------

-- MAGIC %md ## 3. Combined single view — per user: warehouse config + hourly-rate distribution + cost & share
-- MAGIC
-- MAGIC One flat row per `genie_space × user × warehouse` over the whole window. Warehouse config and the hourly-rate distribution (avg/min/median/p90/max $/hr) repeat on each user row so everything is visible together. The rate columns are a **warehouse-level** property (the warehouse costs the same per hour regardless of who queries); `est_user_cost_usd` and `user_pct_of_wh_cost` are the user-specific attribution.

-- COMMAND ----------

WITH params AS (
  SELECT
    split(:space_ids, ',')                                          AS space_ids,
    current_timestamp() - make_interval(0, 0, 0, cast(:lookback_days AS INT)) AS start_ts,
    current_timestamp()                                             AS end_ts
),
all_activity AS (
  SELECT compute.warehouse_id AS warehouse_id, date_trunc('hour', start_time) AS hour_ts, SUM(total_duration_ms) AS total_ms
  FROM system.query.history, params
  WHERE compute.warehouse_id IS NOT NULL AND start_time >= start_ts AND start_time < end_ts GROUP BY 1, 2
),
genie_activity AS (
  SELECT compute.warehouse_id AS warehouse_id, date_trunc('hour', start_time) AS hour_ts,
         query_source.genie_space_id AS genie_space_id, executed_by AS user_email,
         SUM(total_duration_ms) AS genie_ms, COUNT(*) AS genie_query_count
  FROM system.query.history, params
  WHERE query_source.genie_space_id IS NOT NULL AND array_contains(space_ids, query_source.genie_space_id)
    AND compute.warehouse_id IS NOT NULL AND start_time >= start_ts AND start_time < end_ts GROUP BY 1, 2, 3, 4
),
warehouse_billing AS (
  SELECT u.usage_metadata.warehouse_id AS warehouse_id, date_trunc('hour', u.usage_start_time) AS hour_ts,
         SUM(u.usage_quantity) AS dbus, SUM(u.usage_quantity * lp.pricing.effective_list.default) AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
    AND u.usage_start_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time), params
  WHERE u.usage_metadata.warehouse_id IS NOT NULL AND u.usage_start_time >= start_ts AND u.usage_start_time < end_ts GROUP BY 1, 2
),
wh_totals AS (
  SELECT warehouse_id, COUNT(DISTINCT hour_ts) AS running_hours, ROUND(SUM(dbus), 2) AS wh_total_dbus,
         ROUND(SUM(cost_usd), 2) AS wh_total_cost_usd, ROUND(AVG(cost_usd), 2) AS avg_cost_per_hour,
         ROUND(MIN(cost_usd), 2) AS min_cost_per_hour, ROUND(percentile(cost_usd, 0.5), 2) AS median_cost_per_hour,
         ROUND(percentile(cost_usd, 0.9), 2) AS p90_cost_per_hour, ROUND(MAX(cost_usd), 2) AS max_cost_per_hour
  FROM warehouse_billing GROUP BY 1
),
user_totals AS (
  SELECT g.genie_space_id, g.user_email, g.warehouse_id,
         SUM(g.genie_query_count) AS genie_query_count, ROUND(SUM(g.genie_ms)/1000.0, 1) AS genie_active_sec,
         ROUND(SUM(b.dbus * g.genie_ms / a.total_ms), 4) AS est_user_dbus,
         ROUND(SUM(b.cost_usd * g.genie_ms / a.total_ms), 4) AS est_user_cost_usd
  FROM genie_activity g
  JOIN all_activity a ON g.warehouse_id = a.warehouse_id AND g.hour_ts = a.hour_ts
  JOIN warehouse_billing b ON g.warehouse_id = b.warehouse_id AND g.hour_ts = b.hour_ts
  WHERE a.total_ms > 0 GROUP BY 1, 2, 3
),
wh_cfg AS (
  SELECT warehouse_id, warehouse_name, warehouse_type, warehouse_size, min_clusters, max_clusters,
         auto_stop_minutes, tags, ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) AS rn
  FROM system.compute.warehouses
)
SELECT
  u.genie_space_id, u.user_email,
  c.warehouse_name, c.warehouse_size, c.warehouse_type, c.min_clusters, c.max_clusters, c.auto_stop_minutes,
  t.running_hours, t.wh_total_cost_usd,
  t.avg_cost_per_hour, t.min_cost_per_hour, t.median_cost_per_hour, t.p90_cost_per_hour, t.max_cost_per_hour,
  u.genie_query_count, u.genie_active_sec, u.est_user_dbus, u.est_user_cost_usd,
  ROUND(100.0 * u.est_user_cost_usd / NULLIF(t.wh_total_cost_usd, 0), 4) AS user_pct_of_wh_cost
FROM user_totals u
JOIN wh_totals t ON u.warehouse_id = t.warehouse_id
LEFT JOIN wh_cfg c ON u.warehouse_id = c.warehouse_id AND c.rn = 1
ORDER BY u.est_user_cost_usd DESC;

-- COMMAND ----------

-- MAGIC %md ## 4. Hourly-window breakdown — one row per hour of Genie activity
-- MAGIC
-- MAGIC The finest grain: one row per **hour** in which the target space ran queries. Each row shows the warehouse's *actual* billed cost for that specific hour (`wh_hour_cost_usd` — this is the real hourly rate, which varies as serverless autoscales), how busy the warehouse was overall (`all_active_sec`), who was active (`active_users`, `user_list`), the space's queries/active time, its allocated DBUs/$, and its share of that hour (`genie_pct_of_hour`).
-- MAGIC
-- MAGIC `warehouse_size` / cluster config is resolved **point-in-time** (the config effective during that hour), so historical resizes show correctly rather than only the latest setting.

-- COMMAND ----------

WITH params AS (
  SELECT
    split(:space_ids, ',')                                          AS space_ids,
    current_timestamp() - make_interval(0, 0, 0, cast(:lookback_days AS INT)) AS start_ts,
    current_timestamp()                                             AS end_ts
),
all_activity AS (
  SELECT compute.warehouse_id AS warehouse_id, date_trunc('hour', start_time) AS hour_ts, SUM(total_duration_ms) AS total_ms
  FROM system.query.history, params
  WHERE compute.warehouse_id IS NOT NULL AND start_time >= start_ts AND start_time < end_ts GROUP BY 1, 2
),
genie_activity AS (
  SELECT compute.warehouse_id AS warehouse_id, date_trunc('hour', start_time) AS hour_ts,
         query_source.genie_space_id AS genie_space_id,
         SUM(total_duration_ms) AS genie_ms, COUNT(*) AS genie_query_count,
         COUNT(DISTINCT executed_by) AS active_users,
         array_join(array_sort(collect_set(executed_by)), ', ') AS user_list
  FROM system.query.history, params
  WHERE query_source.genie_space_id IS NOT NULL AND array_contains(space_ids, query_source.genie_space_id)
    AND compute.warehouse_id IS NOT NULL AND start_time >= start_ts AND start_time < end_ts GROUP BY 1, 2, 3
),
warehouse_billing AS (
  SELECT u.usage_metadata.warehouse_id AS warehouse_id, date_trunc('hour', u.usage_start_time) AS hour_ts,
         SUM(u.usage_quantity) AS dbus, SUM(u.usage_quantity * lp.pricing.effective_list.default) AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
    AND u.usage_start_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time), params
  WHERE u.usage_metadata.warehouse_id IS NOT NULL AND u.usage_start_time >= start_ts AND u.usage_start_time < end_ts GROUP BY 1, 2
),
wh_cfg_scd AS (
  SELECT warehouse_id, warehouse_name, warehouse_size, min_clusters, max_clusters, auto_stop_minutes,
         change_time,
         LEAD(change_time) OVER (PARTITION BY warehouse_id ORDER BY change_time) AS next_change
  FROM system.compute.warehouses
)
SELECT
  g.hour_ts,
  g.genie_space_id,
  b.warehouse_id,
  cfg.warehouse_name, cfg.warehouse_size, cfg.min_clusters, cfg.max_clusters, cfg.auto_stop_minutes,
  ROUND(b.dbus, 2)                                AS wh_hour_dbus,
  ROUND(b.cost_usd, 2)                            AS wh_hour_cost_usd,
  ROUND(a.total_ms/1000.0, 1)                     AS all_active_sec,
  g.active_users, g.user_list,
  g.genie_query_count,
  ROUND(g.genie_ms/1000.0, 1)                     AS genie_active_sec,
  ROUND(b.dbus * g.genie_ms / a.total_ms, 4)      AS est_genie_dbus,
  ROUND(b.cost_usd * g.genie_ms / a.total_ms, 4)  AS est_genie_cost_usd,
  ROUND(100.0 * g.genie_ms / a.total_ms, 2)       AS genie_pct_of_hour
FROM genie_activity g
JOIN all_activity a ON g.warehouse_id = a.warehouse_id AND g.hour_ts = a.hour_ts
JOIN warehouse_billing b ON g.warehouse_id = b.warehouse_id AND g.hour_ts = b.hour_ts
LEFT JOIN wh_cfg_scd cfg ON b.warehouse_id = cfg.warehouse_id
  AND cfg.change_time <= g.hour_ts AND (cfg.next_change IS NULL OR g.hour_ts < cfg.next_change)
WHERE a.total_ms > 0
ORDER BY g.hour_ts DESC;
