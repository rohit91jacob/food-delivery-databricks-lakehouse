# Runbook

## Daily operation

- **Hosted (what runs today):** the GitHub Actions workflow `refresh.yml` processes one business date per
  night on the dev target ([Scheduled refresh](#scheduled-refresh)). It does the same steps as the DAG.
  Run either it or Airflow, not both against the same target.
- **Self-hosted alternative:** the Airflow DAG `food_delivery_daily` runs at 00:00 Asia/Kolkata for the business date that just ended
  (interval `[D 00:00, D+1 00:00)` IST). Its steps are prepare_seed → generate_batch → upload_batch →
  run_lakehouse_job → reconcile_gold_with_manifest → publish_summary.
- The Databricks job `fooddelivery_daily` runs the pipeline refresh first, then `data_quality` (`fd-dq`). Its own
  schedule ships **paused**, because Airflow is the scheduler. To run without Airflow, unpause it in
  `resources/job.yml` (`pause_status: UNPAUSED`) and redeploy.
- Failures email the deploying user (job + pipeline notifications) and, if `FD_ALERT_WEBHOOK_URL` is set,
  post to that webhook (Airflow `on_failure_callback`).

## Backfill / reprocessing a date range

Every step is idempotent per business date. The generator is deterministic, the upload skips unchanged files by
sha256, Auto Loader never re-ingests a file at the same path, and silver upserts by key.

```bash
airflow backfill create --dag-id food_delivery_daily --from-date 2026-09-01 --to-date 2026-09-07
```

`max_active_runs=1` and `depends_on_past` on `generate_batch` keep dates in order. That ordering matters
because each day's dimension CDC feed builds on the previous day's. Free Edition allows **one active pipeline
update at a time**, so backfills run one date after another by design.

Without Airflow (CLI):

```bash
fd generate --date 2026-09-01 --days 7
fd upload   --date 2026-09-01 --days 7
databricks bundle run fooddelivery_daily -t dev --params run_date=2026-09-07
```

The dq gate checks the `run_date` partition. Run it once per date, or rerun only the `data_quality` task with
each date.

## Changing generator semantics (GENERATOR_VERSION bump)

1. Bump `GENERATOR_VERSION` in `src/fooddelivery/__init__.py`. New files land as `part-00000-v<N>.json.gz`, and
   the upload deletes the old-version files from each touched partition.
2. Regenerate and re-upload every affected date (backfill).
3. **Full-refresh the pipeline.** Bronze is append-only and still holds rows from the old files:
   `databricks bundle run fooddelivery_lakehouse -t <target> --full-refresh-all`
   The landing volume is the system of record, so bronze is fully rebuildable from it.
4. Rerun the dq gate for the affected dates. `duplicates_removed_as_expected` fails if stale bronze rows remain.

## Data-quality failures

| Check (dq_results.check_name) | Likely cause | Action |
|---|---|---|
| `gold_partition_present` | Upload didn't happen or the pipeline didn't pick up files | Check `upload_batch` logs and the pipeline event log; rerun the job |
| `gold_reconciles_with_manifest` | Logic regression in gold or silver, or stale bronze after a generator change | `SELECT detail FROM dq_results ...` lists the mismatched city/metrics; full refresh if stale |
| `duplicates_removed_as_expected` / `quarantine_matches_injected_invalid` | Dedupe or drop rules changed, or stale bronze | Compare bronze vs silver vs quarantine counts for the date |
| `order_lines_priced_from_menu_scd2`, `orders_join_restaurant_version`, `*_scd2_*` | SCD2 history broken (e.g. a manual table edit, or a missed CDC file) | Full refresh of the affected silver table |
| `delivered_lifecycle_monotonic`, `one_payment_per_order` | Bad source events | Inspect `fct_orders` rows listed in `detail` |
| `on_time_rate_floor` (warn) | Business signal (rain, rider shortage) | No pipeline action; see `gold_delivery_sla_hourly` |

Quarantined rows: `SELECT _failed_rules, count(*) FROM quarantine_orders GROUP BY 1`.

## Scheduled refresh

`.github/workflows/refresh.yml` runs nightly at 01:00 UTC (06:30 IST). It processes the **next
unprocessed business date** on the dev target: the newest date in `gold_daily_kpis` plus one. It
runs as one `concurrency` group, so runs never overlap. Each run writes a summary (job state, minutes,
reconciliation result) to the run page.

| Symptom | Cause | Action |
|---|---|---|
| Issue **"Databricks credential needs attention"**; `credential health` failed | The credential is rejected, missing, or expires within 14 days | Renew it with [databricks_auth.md](databricks_auth.md), then **Run workflow**. No data was touched. |
| Issue **"Scheduled refresh failed"** at *Run the Lakeflow job* with `exceeded quota` / compute unavailable | Daily fair-use quota spent | Nothing to fix. The next night processes the same date again (gold is the source of truth). See below. |
| Fails at *Run the Lakeflow job* with a pipeline error | A code or data problem | Open the job run link from the log. Fix it, push, then **Run workflow** with the same `business_date`. |
| Fails at *Reconcile gold with the manifest* | Gold ≠ ground truth | Treat it as a data-quality incident ([below](#data-quality-failures)). The `raise_error` message lists the city and metric. |
| SQL step times out | Warehouse cold start or quota | Re-run. Statements are bounded at 15 minutes, the job at 90. |
| "Gold is caught up with today; nothing to process." | The simulated calendar reached today (IST) | Expected. Runs resume as the days pass. |
| Scheduled runs stopped appearing | GitHub disabled schedules after 60 days without repository activity | `keepalive.yml` re-enables them monthly. Run it manually, or click **Enable workflow** in the Actions tab. |

**Re-running a date:** Actions → **Scheduled refresh** → **Run workflow** → `business_date: 2026-09-04`. Every
step is idempotent: generation is byte-identical, the upload skips unchanged files, the pipeline dedupes, and
the reconciliation re-checks. Re-running a reconciled date is a safe no-op apart from the compute it uses.

**Pausing:** disable the workflow in the Actions tab, or edit the cron. Nothing in the workspace needs
changing.

## Free Edition quota exhausted

If compute shuts down for the day ("exceeded quota"), the job and the Airflow or GitHub refresh tasks fail.
For Airflow, let the DAG run fail, wait for the daily reset, then clear the failed task instances (or rerun the
backfill). The GitHub refresh needs nothing: the next nightly run retries the same date. Nothing is lost,
because the landing files are already uploaded and every step is idempotent.

One business date costs about 16 minutes of serverless compute, about 9 of them the pipeline refresh. A nightly
refresh therefore spends a meaningful share of the daily quota. To spend less, run less often (for example
`cron: "0 1 * * 1,4"`), lower `FD_BASE_ORDERS_PER_CITY`, reduce `FD_CITIES`, or raise `FD_GPS_PING_SECONDS`.

## Removing a dataset from the pipeline

Lakeflow does not drop a table when its definition is removed. The table, and its hidden `__materialization_*`
table, keep counting towards the 100-table limit. After deleting a dataset from the code, drop the orphaned table
in each target with `DROP TABLE <catalog>.<schema>.<name>`. In dev, `databricks bundle destroy -t dev` followed by
`deploy` also works.

## Credentials

| Credential | Used by | Lifetime | Renew |
|---|---|---|---|
| Service principal OAuth secret (`DATABRICKS_CLIENT_ID` variable + `DATABRICKS_CLIENT_SECRET` secret) | `refresh.yml` | ≤ 730 days | Generate a second secret, swap it in, delete the old one ([guide](databricks_auth.md#6-renewal-every-two-years)) |
| PAT (`DATABRICKS_TOKEN` secret) | `deploy.yml`; `refresh.yml` until the service principal exists | ≤ 730 days in this workspace | Generate a new token, update the secret, revoke the old one ([guide](databricks_auth.md#c-a-personal-access-token-with-the-maximum-lifetime-for-deploys-or-as-a-fallback)) |
| PAT in the Airflow connection `databricks_default` | the local / compose Airflow | as created | Rotate in the workspace, then update `AIRFLOW_CONN_DATABRICKS_DEFAULT` |
| `GITHUB_TOKEN` | issues, keep-alive | per run | Automatic |
| Kaggle `KGAT_` token | only `FD_SEED_SOURCE=kaggle` | as created | Not needed by default: the CC0 seed is committed (`seed/`) |

The refresh's `credential health` job fails 14 days before a PAT or recorded secret expiry and opens an issue.
Set the variable `DATABRICKS_TOKEN_ID` to pin the exact PAT being checked.

## Resetting a dev environment

```bash
databricks bundle destroy -t dev     # drops the dev schema (and its tables/volume), pipeline, job, dashboard
databricks bundle deploy  -t dev
```
