# Cluster Utilization & Right-Sizing Dashboard

A Databricks AI/BI Lakeview dashboard that analyzes **all-purpose (interactive) cluster** utilization over the trailing 90 days using Unity Catalog system tables, categorizes each cluster as Over-Utilized / Under-Utilized / Right-Sized / Idle-or-Unused, and produces an actionable recommendation per cluster.

## Files in this package

| File | Purpose |
|---|---|
| `cluster_utilization_dashboard.lvdash.json` | The dashboard definition. Import into Databricks AI/BI. |
| `cluster_utilization_queries.sql` | Standalone, fully commented SQL with all views and dashboard queries. Use this for ad-hoc analysis or to embed into other dashboards. |
| `README.md` | This file — logic and deployment guide. |

---

## Prerequisites

1. **System tables enabled** on the workspace's metastore. The dashboard reads:
   - `system.compute.clusters`
   - `system.compute.node_timeline`
   - `system.billing.usage`
   - `system.billing.list_prices`

   To enable, run the system schemas enablement command (account admin):
   ```sql
   -- One-time per metastore; see Databricks docs "Monitor account usage with system tables"
   ```
2. **SQL warehouse** (serverless or pro). Any size works; serverless small is enough.
3. **USE CATALOG** privilege on `system` for the dashboard runner / viewers.

---

## How to deploy

### Option A — Import the `.lvdash.json` (recommended)

1. In Databricks: **Dashboards → Create dashboard → Import dashboard from file**.
2. Select `cluster_utilization_dashboard.lvdash.json`.
3. Pick a **target catalog/schema** for the dashboard's underlying queries (any catalog works — the queries reference `system.*` explicitly).
4. Attach a SQL warehouse and click **Publish**.
5. To share with the customer: **Share → Add people** with **Can View** permission. They will need at least USE CATALOG on `system`.

### Option B — Use the SQL file directly

Open `cluster_utilization_queries.sql` in the SQL editor. The file is organized as:
- 5 `CREATE OR REPLACE TEMP VIEW` statements that build the analysis layer.
- 9 dashboard-ready `SELECT` queries (D1 – D9), each labeled with the visualization it powers.

Run the temp views first, then any of the D* queries. The `:days_back` parameter defaults to 90.

---

## Categorization logic

Each cluster gets a single category derived from these flags. Flags evaluate to TRUE/FALSE, and the category is assigned by the first matching rule in this priority order:

| Priority | Category | Trigger |
|:-:|---|---|
| 1 | **IDLE_OR_UNUSED** | Cluster has fewer than 60 minutes of active runtime over the whole window (essentially abandoned). |
| 2 | **OVER_UTILIZED** | Any of: CPU p95 ≥ 85%; memory p95 ≥ 90%; average swap > 5%; autoscale pinned at the ceiling at p95 (i.e. workload regularly maxes out `max_autoscale_workers`). |
| 3 | **UNDER_UTILIZED** | Any of: average CPU < 20% AND p95 CPU < 40% AND p95 memory < 50%; fixed-size cluster (≥4 workers) with avg CPU < 25%; idle > 60% of runtime combined with auto-termination ≥ 120 min or disabled; autoscale never exceeded the floor and avg CPU < 30%. |
| 4 | **RIGHT_SIZED** | None of the above flags fired. |

### Why these thresholds?

- **p95 CPU ≥ 85% → over-utilized:** at p95, CPU pressure is sustained, not bursty. Below this, brief spikes are normal.
- **p95 memory ≥ 90%:** memory at p95 above 90% means OOM risk; combined with swap activity it almost always causes job failures.
- **Avg CPU < 20% AND p95 CPU < 40%:** the cluster is paying for cores it never uses, even under peak workload.
- **Autoscale pinned at ceiling:** the workload wants more parallelism than the policy allows — raising the ceiling will reduce wall-clock time.
- **Autoscale stuck at floor:** the cluster is paying for `min_autoscale_workers` continuously that the workload doesn't need.
- **Idle > 60% with loose auto-termination:** the cluster sits paying for the driver (and floor workers, if autoscaling) when no work is happening.

### How autoscale and auto-termination factor in

The categorization explicitly inspects:
- `min_autoscale_workers` / `max_autoscale_workers` — vs the observed `p95_active_workers` to detect ceiling-pinning and floor-stuck patterns.
- `auto_termination_minutes` — combined with observed idle time to detect terminate-policy looseness.
- `worker_count` (fixed clusters) — flagged as oversized when ≥4 workers and avg CPU < 25%.

This is why two clusters with identical CPU graphs can land in different categories: a fixed-size cluster pinning workers at high CPU is "over-utilized" with a "scale up node type" recommendation, but an autoscaling cluster pinning at its max workers is "over-utilized" with a "raise the ceiling" recommendation.

---

## Recommendations

Each cluster gets exactly one recommendation. The mapping (priority order):

| Flag pattern | Recommendation |
|---|---|
| Essentially unused | **DELETE.** If kept for ad-hoc use, set auto-termination ≤ 30 min. |
| Pinned at ceiling AND CPU pressure | **SCALE UP:** raise `max_autoscale_workers` AND consider larger worker node type. |
| Pinned at ceiling (only) | **RAISE CEILING:** increase `max_autoscale_workers` to ~1.5× current. |
| Memory pressure or swap | **UPGRADE NODE TYPE:** switch to memory-optimized (r-series) workers; enable elastic disk. |
| CPU pressure (not pinned) | **UPGRADE NODE TYPE:** compute-optimized (c-series) or more vCPUs per node. |
| Fixed-size oversized | **CONVERT TO AUTOSCALING:** suggested `min = max/4`, `max = current`. |
| Autoscale stuck at floor | **LOWER FLOOR:** reduce `min_autoscale_workers` to 1, keep current ceiling. |
| Idle + loose auto-termination | **TIGHTEN AUTO-TERMINATION:** set to 30 min. |
| Low utilization, no other flag | **DOWNSIZE:** smaller worker node type and/or reduce max workers. Consider serverless / job clusters if bursty. |
| Right-sized | **NO CHANGE:** continue to monitor. |

---

## Dashboard widgets

The dashboard has 15 widgets organized as:

1. **Header & filter** — markdown title + `days_back` parameter selector (default 90).
2. **KPI row** — six counters: Total / Over / Under / Right / Idle / Estimated Savings (USD).
3. **Category distribution pie** — cluster counts per category.
4. **Top clusters by cost bar** — color-coded by category.
5. **Cost-vs-utilization scatter** — bubble size = DBUs. Top-left bubbles = expensive & under-used → highest savings targets.
6. **Autoscaling effectiveness bar** — Pinned / Stuck at floor / Scaling normally / No autoscale.
7. **Auto-termination compliance bar** — distribution of `auto_termination_minutes` bands.
8. **Daily utilization trend line** — CPU and memory averages across all clusters.
9. **Detail table** — every cluster with its category, config, metrics, reason, and recommendation. This is the customer-facing list.

The "Estimated Savings" KPI uses a conservative 40% cost-reduction assumption applied to under-utilized + idle clusters; tune the multiplier in dataset `ds_cluster_assessment` if your customer wants a different model.

---

## Tuning the thresholds

All thresholds live in the `flagged` CTE inside both files. To adjust:

- `flag_cpu_pressure`: change `85` (p95 CPU %)
- `flag_mem_pressure`: change `90` (p95 memory %)
- `flag_low_util`: change `20` / `40` / `50` (avg CPU, p95 CPU, p95 mem)
- `flag_high_idle`: change `60` (idle %)
- `flag_loose_termination`: change `120` (auto-term minutes)
- `flag_fixed_oversized`: change `4` workers / `25` % CPU
- `flag_essentially_unused`: change `60` active minutes

Edit those numbers in **both** `cluster_utilization_queries.sql` (the `flagged` CTE) **and** `cluster_utilization_dashboard.lvdash.json` (the `flagged` CTE inside `ds_cluster_assessment.queryLines`) to keep them in sync.

---

## Known caveats

- **Cost estimates** use `system.billing.list_prices.pricing.default` as a USD list-price proxy and exclude cloud infrastructure (VM, storage, network) costs. They reflect DBU spend only. For true total cost, join cloud provider billing exports.
- **Worker count over time** is approximated by counting distinct non-driver instances per minute in `node_timeline`. This is accurate for live clusters but may under-count during very fast scale events (sub-minute).
- The dashboard filters to `cluster_source IN ('UI','API')` — i.e. interactive all-purpose clusters only. Job clusters and DLT clusters are excluded by design.
- `system.compute.node_timeline` retention is typically 90 days. Going beyond that with `days_back` returns partial data.

---

## Customer sharing checklist

- [ ] Dashboard imported and published.
- [ ] Customer's identity (group or user) granted **Can View** on the dashboard.
- [ ] Customer granted **USE CATALOG** on `system` and **SELECT** on `system.compute.*` + `system.billing.*` (or share via a dedicated service principal if the customer should not query system tables directly).
- [ ] Initial walkthrough: explain the 4 categories, the priority order, and the "Estimated Savings" multiplier assumption.
- [ ] Agreed cadence for reviewing the recommendations (suggested: monthly).
