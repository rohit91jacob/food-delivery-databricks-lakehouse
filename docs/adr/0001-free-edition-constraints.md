# ADR 0001: Design around Databricks Free Edition limits

**Status:** accepted

## Context
Free Edition comes with these limits:
- compute is **serverless only** (no custom clusters), with daily quotas
- **one SQL warehouse** (2X-Small)
- **max 5 concurrent job tasks**
- **one active pipeline per pipeline type**
- **restricted outbound internet** from serverless
- no custom storage locations
- no account-level APIs
- **100 tables per schema** (found on the first real deploy; hidden `__materialization_*` tables count towards it)

## Decision
- **Data is produced outside Databricks.** Airflow downloads the Kaggle seed and runs the generator, then pushes
  files into a managed Unity Catalog **volume** through the Files API (`VolumeSync`). Nothing in the workspace
  needs internet egress.
- Everything in the workspace is serverless and declared in one bundle:
  - a Lakeflow Declarative Pipeline (`serverless: true`)
  - a serverless job (`environment_version: 5`, `performance_target: STANDARD`)
  - an AI/BI dashboard on the single warehouse (`lookup: warehouse: Serverless Starter Warehouse`)
- Managed schemas and volume (no `storage_location`). The `workspace` catalog is the default. Gold publishes to
  `<schema>_gold` and `dq_results` lives in `<schema>_ops`. Quarantine tables exist only for the four feeds where
  rejects are expected (orders, order_events, payments, refunds). That keeps every schema well under 100 tables.
- **Serialized execution.** The job runs with `max_concurrent_runs: 1` and a queue, and the DAG with
  `max_active_runs: 1`. The job has two tasks.
- dev and prod both live in the single workspace as bundle targets. Dev mode prefixes the schema
  (`dev_<user>_fooddelivery`) and resource names (`[dev <user>] ...`).

## Consequences
- Backfills run date by date, so they are slower but stay within limits.
- The orchestrator is the only component that needs secrets for both Kaggle and Databricks.
- Moving to a paid workspace needs no code changes. Point the bundle at it, and optionally turn on
  continuous/file-arrival triggers.
