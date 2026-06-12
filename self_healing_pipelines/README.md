# Self-Healing Pipelines — Job Failure Taxonomy

A portable SQL toolkit that quantifies production **Databricks Jobs** failures from Unity Catalog system tables and classifies each one into a root-cause category — separating failures that are **already solved by platform best practices** from the genuinely novel long tail that a triage/"self-healing" agent should own.

It exists to answer a specific question that comes up when customers ask about a "self-healing pipeline agent" (often prompted by competitor marketing): *how much of your failure volume actually needs an agent, versus how much is tech debt that best practices already eliminate?*

## Files in this package

| File | Purpose |
|---|---|
| `self_healing_job_failures.sql` | Three standalone, fully commented queries (Q1–Q3). Run in any SQL editor / warehouse. |
| `README.md` | This file — logic, categories, and how to read the output. |

---

## Prerequisites

1. **System tables enabled** on the workspace's metastore. The queries read:
   - `system.lakeflow.job_run_timeline`
   - `system.lakeflow.jobs` (Q3B only, for job names)
2. **SQL warehouse** (serverless small is plenty).
3. **USE CATALOG** on `system` and **SELECT** on `system.lakeflow.*` for the runner.
4. A `:lookback_days` parameter (defaults to 30 in the examples). Set it in the SQL editor's parameter panel or pass it via the SQL Statement Execution API.

No workspace IDs or job IDs are hard-coded — the queries run unchanged in any workspace, and automatically span every workspace present in the metastore.

---

## The three queries

### Q1 — Daily failure matrix (`workspace_id × day × category`)
The raw deliverable: failed runs grouped by workspace, day, root-cause category, the Databricks lever that addresses it, and a coarse disposition (`Solved by best practices` vs `Agent candidate`). This is the "30+ days of job failures by workspace and day" extract.

### Q2 — Executive rollup
Collapses Q1 into the headline: **% of failures solved by best practices today vs. the `RUN_EXECUTION_ERROR` long tail**. Drop the implicit per-workspace grouping for an account-wide number; the categories make the split self-evident.

### Q3 — Decompose the long tail
`RUN_EXECUTION_ERROR` is a coarse catch-all ("the task threw"). Q3 answers whether that bucket is *novel* (one-off incidents worth triaging) or *chronic* (the same pipeline failing repeatedly = tech debt to fix once, not heal nightly).

- **Q3A** bands each job by how many times it failed in the window: `chronic (20+)` / `recurring (5–19)` / `sporadic (1–4)`. Only the sporadic band is a genuine triage-agent surface.
- **Q3B** emits a named repeat-offender worklist (top 25 jobs with cumulative Pareto) you can hand straight to the platform team.

---

## Categorization logic

Each terminal run is assigned one category from its `termination_code`:

| Category | Termination codes | Disposition | Self-healing lever |
|---|---|---|---|
| **1. Transient / Infra** | `INTERNAL_ERROR`, `CLOUD_FAILURE`, `CLUSTER_ERROR`, `DRIVER_ERROR` | Solved by best practices | Job retries + serverless (auto-recover) |
| **2. Config / Definition (tech debt)** | `INVALID_RUN_CONFIGURATION`, `INVALID_CLUSTER_REQUEST`, `RESOURCE_NOT_FOUND`, `REPOSITORY_CHECKOUT_FAILED`, `LIBRARY_INSTALLATION_ERROR`, `FEATURE_DISABLED` | Solved by best practices | DABs + CI/CD + serverless (eliminate config drift) |
| **3. Permissions / Governance** | `UNAUTHORIZED_ERROR`, `STORAGE_ACCESS_ERROR` | Solved by best practices | Unity Catalog governance + service principals |
| **4. Concurrency / Limits** | `MAX_CONCURRENT_RUNS_EXCEEDED`, `MAX_JOB_QUEUE_SIZE_EXCEEDED`, `WORKSPACE_RUN_LIMIT_EXCEEDED`, `CLUSTER_REQUEST_LIMIT_EXCEEDED`, `MAX_SPARK_CONTEXTS_EXCEEDED` | Solved by best practices | Orchestration design + serverless elasticity |
| **5. Code / Data logic (needs triage)** | `RUN_EXECUTION_ERROR` | Agent candidate (long tail) | Lakeflow/DLT expectations + Auto Loader; triage agent for residual |
| **6. Other / Uncategorized** | anything else | Review | Investigate |

`SUCCEEDED` and user-`CANCELLED` runs are excluded from failure counts.

### Why this split

The thesis is that a "self-healing agent" that masks the same broken pipeline run after run is an anti-pattern. Categories 1–4 are failure classes that disciplined platform practices (retries, serverless, Declarative Asset Bundles + CI/CD, Unity Catalog governance, sound orchestration) engineer out entirely. Only category 5 — code/data logic errors — is a candidate for agent-assisted triage, and Q3 shows that most of *that* is chronic repeat-offenders, not novel incidents.

---

## How to read the output

A representative run on a busy multi-tenant workspace (30-day window) produced roughly:

- **~61%** of failures in categories 1–4 → already solvable with best practices.
- **~39%** in category 5 (`RUN_EXECUTION_ERROR`).
- Within that 39%, **~90%** came from chronic jobs failing 20+ times (remediation candidates), and only **~4.6%** were sporadic 1–4× failures.

Net: the genuinely novel, agent-addressable surface is roughly `39% × 4.6% ≈ under 2%` of all failures. Your numbers will differ by environment — the framework, not the specific percentages, is the point.

---

## Important notes & caveats

- **`job_run_timeline` is period-based.** It emits one row per run *state period*, so a `run_id` can appear several times. Every query dedupes to the terminal period via `QUALIFY ROW_NUMBER() ... ORDER BY period_end_time DESC = 1` to count each run once.
- **`termination_code` populated from late Aug 2024 onward.** Older rows have null codes and fall into "Other / Uncategorized." Keep windows recent.
- **No error-message text in system tables.** `RUN_EXECUTION_ERROR` cannot be sub-classified by error semantics (schema drift vs. data quality vs. code bug) from system tables alone — that requires the Jobs API per-run output. Q3 therefore classifies the long tail by failure *pattern/frequency*, which is enough to size the agent's real scope.
- **`run_type = 'JOB_RUN'`** filters out sub-task / workflow rows so counts reflect job runs, not task fan-out.
- The `:lookback_days` filter uses `dateadd(DAY, -:lookback_days, current_timestamp())` rather than `INTERVAL`, because parameter markers aren't valid inside an `INTERVAL` literal.
