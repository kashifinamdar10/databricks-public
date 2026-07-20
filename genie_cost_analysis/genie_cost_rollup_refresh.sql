-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Genie cost — hourly rollup refresh
-- MAGIC
-- MAGIC Rebuilds the three materialized tables the **Genie Cost on Shared SQL Warehouse** dashboard reads from, so the dashboard is fast and needs **no `system.billing.list_prices`** access (it stores DBUs; the dashboard applies the `dbu_price` parameter).
-- MAGIC
-- MAGIC | Table | Grain | Purpose |
-- MAGIC |---|---|---|
-- MAGIC | `main.mki_genie_cost.wh_hourly` | warehouse × hour | Denominator (all active ms) + billed DBUs |
-- MAGIC | `main.mki_genie_cost.space_user_hourly` | space × warehouse × hour × user | Numerator (Genie active ms, queries) |
-- MAGIC | `main.mki_genie_cost.wh_config` | warehouse | Latest size / clusters / auto-stop |
-- MAGIC
-- MAGIC Window = last **30 days** (edit the `make_interval` below to change). **Schedule this as a daily job** to keep the dashboard current.

-- COMMAND ----------

CREATE OR REPLACE TABLE main.mki_genie_cost.wh_hourly AS
WITH win AS (SELECT current_timestamp()-make_interval(0,0,0,30) AS s, current_timestamp() AS e),
gw AS (SELECT DISTINCT compute.warehouse_id AS wid FROM system.query.history, win
       WHERE query_source.genie_space_id IS NOT NULL AND compute.warehouse_id IS NOT NULL
         AND start_time>=s AND start_time<e),
act AS (SELECT compute.warehouse_id AS wid, date_trunc('hour',start_time) AS hour_ts, SUM(total_duration_ms) AS total_ms
        FROM system.query.history, win
        WHERE compute.warehouse_id IN (SELECT wid FROM gw) AND start_time>=s AND start_time<e GROUP BY 1,2),
bil AS (SELECT usage_metadata.warehouse_id AS wid, date_trunc('hour',usage_start_time) AS hour_ts, SUM(usage_quantity) AS dbus
        FROM system.billing.usage, win
        WHERE usage_metadata.warehouse_id IN (SELECT wid FROM gw) AND usage_start_time>=s AND usage_start_time<e GROUP BY 1,2)
SELECT COALESCE(a.wid,b.wid) AS warehouse_id, COALESCE(a.hour_ts,b.hour_ts) AS hour_ts, a.total_ms, b.dbus
FROM act a FULL OUTER JOIN bil b ON a.wid=b.wid AND a.hour_ts=b.hour_ts;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.mki_genie_cost.space_user_hourly AS
WITH win AS (SELECT current_timestamp()-make_interval(0,0,0,30) AS s, current_timestamp() AS e)
SELECT query_source.genie_space_id AS genie_space_id, compute.warehouse_id AS warehouse_id,
       date_trunc('hour',start_time) AS hour_ts, executed_by AS user_email,
       SUM(total_duration_ms) AS genie_ms, COUNT(*) AS genie_q
FROM system.query.history, win
WHERE query_source.genie_space_id IS NOT NULL AND compute.warehouse_id IS NOT NULL
  AND start_time>=s AND start_time<e GROUP BY 1,2,3,4;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.mki_genie_cost.wh_config AS
SELECT warehouse_id, warehouse_name, warehouse_size, min_clusters, max_clusters, auto_stop_minutes
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) rn
      FROM system.compute.warehouses) WHERE rn=1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ✅ Rollup refreshed. Dashboard: **Genie Cost on Shared SQL Warehouse** → applies `dbu_price` (NFCU net = 0.393, list = 0.70).

