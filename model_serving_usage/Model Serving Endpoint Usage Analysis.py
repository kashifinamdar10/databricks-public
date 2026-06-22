# Databricks notebook source
# DBTITLE 1,Model Serving Endpoint Usage Analysis
# MAGIC %md
# MAGIC # Model Serving Endpoint Usage Analysis
# MAGIC Queries against `system.serving.endpoint_usage` and `system.serving.served_entities` system tables.

# COMMAND ----------

# DBTITLE 1,Total Requests & Token Usage per Endpoint (Last 30 Days)
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   se.endpoint_name,
# MAGIC   se.entity_type,
# MAGIC   se.entity_name,
# MAGIC   COUNT(*) AS total_requests,
# MAGIC   SUM(eu.input_token_count) AS total_input_tokens,
# MAGIC   SUM(eu.output_token_count) AS total_output_tokens,
# MAGIC   SUM(eu.input_token_count + eu.output_token_count) AS total_tokens,
# MAGIC   ROUND(AVG(eu.input_token_count), 0) AS avg_input_tokens_per_req,
# MAGIC   ROUND(AVG(eu.output_token_count), 0) AS avg_output_tokens_per_req
# MAGIC FROM system.serving.endpoint_usage eu
# MAGIC JOIN system.serving.served_entities se
# MAGIC   ON eu.served_entity_id = se.served_entity_id
# MAGIC WHERE eu.request_time >= current_date() - 30
# MAGIC GROUP BY se.endpoint_name, se.entity_type, se.entity_name
# MAGIC ORDER BY total_requests DESC

# COMMAND ----------

# DBTITLE 1,Daily Request Volume Trend (Last 30 Days)
# MAGIC %sql
# MAGIC SELECT DATE(eu.request_time) AS request_date,
# MAGIC        se.endpoint_name,
# MAGIC        COUNT(*) AS requests,
# MAGIC        SUM(eu.input_token_count + eu.output_token_count) AS total_tokens
# MAGIC FROM system.serving.endpoint_usage eu
# MAGIC JOIN system.serving.served_entities se ON eu.served_entity_id = se.served_entity_id
# MAGIC WHERE eu.request_time >= current_date() - 30
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1 DESC, 3 DESC

# COMMAND ----------

# DBTITLE 1,Usage by Requester (Last 7 Days)
# MAGIC %sql
# MAGIC SELECT eu.requester,
# MAGIC        COUNT(*) AS total_requests,
# MAGIC        SUM(eu.input_token_count) AS total_input_tokens,
# MAGIC        SUM(eu.output_token_count) AS total_output_tokens,
# MAGIC        SUM(eu.input_token_count + eu.output_token_count) AS total_tokens
# MAGIC FROM system.serving.endpoint_usage eu
# MAGIC WHERE eu.request_time >= current_date() - 7
# MAGIC GROUP BY eu.requester
# MAGIC ORDER BY total_tokens DESC

# COMMAND ----------

# DBTITLE 1,Error Rate by Endpoint (Last 7 Days)
# MAGIC %sql
# MAGIC SELECT se.endpoint_name,
# MAGIC        COUNT(*) AS total_requests,
# MAGIC        SUM(CASE WHEN eu.status_code >= 400 THEN 1 ELSE 0 END) AS error_count,
# MAGIC        ROUND(100.0 * SUM(CASE WHEN eu.status_code >= 400 THEN 1 ELSE 0 END) / COUNT(*), 2) AS error_pct,
# MAGIC        SUM(CASE WHEN eu.status_code = 429 THEN 1 ELSE 0 END) AS rate_limit_errors,
# MAGIC        SUM(CASE WHEN eu.status_code >= 500 THEN 1 ELSE 0 END) AS server_errors
# MAGIC FROM system.serving.endpoint_usage eu
# MAGIC JOIN system.serving.served_entities se ON eu.served_entity_id = se.served_entity_id
# MAGIC WHERE eu.request_time >= current_date() - 7
# MAGIC GROUP BY se.endpoint_name
# MAGIC ORDER BY error_pct DESC

# COMMAND ----------

# DBTITLE 1,Hourly Request Pattern (Last 7 Days)
# MAGIC %sql
# MAGIC SELECT HOUR(eu.request_time) AS hour_of_day,
# MAGIC        COUNT(*) AS total_requests,
# MAGIC        SUM(eu.input_token_count + eu.output_token_count) AS total_tokens
# MAGIC FROM system.serving.endpoint_usage eu
# MAGIC WHERE eu.request_time >= current_date() - 7
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# DBTITLE 1,Streaming vs Non-Streaming Requests (Last 30 Days)
# MAGIC %sql
# MAGIC SELECT se.endpoint_name,
# MAGIC        eu.request_streaming,
# MAGIC        COUNT(*) AS total_requests,
# MAGIC        SUM(eu.input_token_count + eu.output_token_count) AS total_tokens
# MAGIC FROM system.serving.endpoint_usage eu
# MAGIC JOIN system.serving.served_entities se ON eu.served_entity_id = se.served_entity_id
# MAGIC WHERE eu.request_time >= current_date() - 30
# MAGIC GROUP BY se.endpoint_name, eu.request_streaming
# MAGIC ORDER BY total_requests DESC

# COMMAND ----------

# DBTITLE 1,Model Serving Cost with List Prices (Last 30 Days)
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   u.usage_date,
# MAGIC   u.sku_name,
# MAGIC   SUM(u.usage_quantity) AS total_dbus,
# MAGIC   ROUND(SUM(u.usage_quantity * COALESCE(lp.pricing.effective_list.default, lp.pricing.default)), 2) AS estimated_cost_usd,
# MAGIC   lp.currency_code
# MAGIC FROM system.billing.usage u
# MAGIC LEFT JOIN system.billing.list_prices lp
# MAGIC   ON u.sku_name = lp.sku_name
# MAGIC   AND u.cloud = lp.cloud
# MAGIC   AND u.usage_start_time >= lp.price_start_time
# MAGIC   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
# MAGIC WHERE (u.sku_name LIKE '%SERVING%' OR u.sku_name LIKE '%MODEL_INFERENCE%')
# MAGIC   AND u.usage_date >= current_date() - 30
# MAGIC GROUP BY u.usage_date, u.sku_name, lp.currency_code
# MAGIC ORDER BY u.usage_date DESC, estimated_cost_usd DESC

# COMMAND ----------

# DBTITLE 1,Cost per Endpoint with Pricing (Last 30 Days)
# MAGIC %sql
# MAGIC -- Use CTEs to avoid fan-out join between billing and endpoint_usage
# MAGIC WITH cost_agg AS (
# MAGIC   SELECT 
# MAGIC     se.endpoint_name,
# MAGIC     se.entity_type,
# MAGIC     u.sku_name,
# MAGIC     SUM(u.usage_quantity) AS total_dbus,
# MAGIC     ROUND(SUM(u.usage_quantity * COALESCE(lp.pricing.effective_list.default, lp.pricing.default)), 2) AS estimated_cost_usd,
# MAGIC     MAX(lp.currency_code) AS currency_code
# MAGIC   FROM system.billing.usage u
# MAGIC   LEFT JOIN system.billing.list_prices lp
# MAGIC     ON u.sku_name = lp.sku_name
# MAGIC     AND u.cloud = lp.cloud
# MAGIC     AND u.usage_start_time >= lp.price_start_time
# MAGIC     AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
# MAGIC   INNER JOIN system.serving.served_entities se
# MAGIC     ON u.usage_metadata.endpoint_id = se.endpoint_id
# MAGIC   WHERE (u.sku_name LIKE '%SERVING%' OR u.sku_name LIKE '%MODEL_INFERENCE%')
# MAGIC     AND u.usage_date >= current_date() - 30
# MAGIC   GROUP BY se.endpoint_name, se.entity_type, u.sku_name
# MAGIC ),
# MAGIC request_agg AS (
# MAGIC   SELECT 
# MAGIC     se.endpoint_name,
# MAGIC     COUNT(*) AS total_requests,
# MAGIC     SUM(eu.input_token_count) AS total_input_tokens,
# MAGIC     SUM(eu.output_token_count) AS total_output_tokens
# MAGIC   FROM system.serving.endpoint_usage eu
# MAGIC   JOIN system.serving.served_entities se
# MAGIC     ON eu.served_entity_id = se.served_entity_id
# MAGIC   WHERE eu.request_time >= current_date() - 30
# MAGIC   GROUP BY se.endpoint_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   c.endpoint_name,
# MAGIC   c.entity_type,
# MAGIC   c.sku_name,
# MAGIC   r.total_requests,
# MAGIC   r.total_input_tokens,
# MAGIC   r.total_output_tokens,
# MAGIC   c.total_dbus,
# MAGIC   c.estimated_cost_usd,
# MAGIC   c.currency_code
# MAGIC FROM cost_agg c
# MAGIC LEFT JOIN request_agg r
# MAGIC   ON c.endpoint_name = r.endpoint_name
# MAGIC ORDER BY c.estimated_cost_usd DESC

# COMMAND ----------

# DBTITLE 1,Daily Cost Trend by Endpoint (Last 30 Days)
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   u.usage_date,
# MAGIC   se.endpoint_name,
# MAGIC   SUM(u.usage_quantity) AS total_dbus,
# MAGIC   ROUND(SUM(u.usage_quantity * COALESCE(lp.pricing.effective_list.default, lp.pricing.default)), 2) AS estimated_cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC LEFT JOIN system.billing.list_prices lp
# MAGIC   ON u.sku_name = lp.sku_name
# MAGIC   AND u.cloud = lp.cloud
# MAGIC   AND u.usage_start_time >= lp.price_start_time
# MAGIC   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
# MAGIC INNER JOIN system.serving.served_entities se
# MAGIC   ON u.usage_metadata.endpoint_id = se.endpoint_id
# MAGIC WHERE (u.sku_name LIKE '%SERVING%' OR u.sku_name LIKE '%MODEL_INFERENCE%')
# MAGIC   AND u.usage_date >= current_date() - 30
# MAGIC GROUP BY u.usage_date, se.endpoint_name
# MAGIC ORDER BY u.usage_date DESC, estimated_cost_usd DESC
