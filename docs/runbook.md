# Runbook

## Daily operation

- Airflow DAG `food_delivery_daily` runs at 00:00 Asia/Kolkata for the business date that just ended
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

## Free Edition quota exhausted

If compute shuts down for the day ("exceeded quota"), the job and Airflow tasks fail and retry. Let the DAG run
fail, wait for the daily reset, then clear the failed task instances (or rerun the backfill). Nothing is lost,
because the landing files are already uploaded and every step is idempotent. To spend less quota, lower
`FD_BASE_ORDERS_PER_CITY`, reduce `FD_CITIES`, or raise `FD_GPS_PING_SECONDS`.

## Credentials

- Databricks: a PAT in the Airflow connection `databricks_default` (`AIRFLOW_CONN_DATABRICKS_DEFAULT`), and repo
  secrets `DATABRICKS_HOST` / `DATABRICKS_TOKEN` for CI deploys. Rotate the PAT in the workspace, then update both.
- Kaggle: a `KGAT_` token in `~/.kaggle/access_token` (local) or the compose secret `kaggle_token`. It is only
  needed the first time the seed is built.

## Resetting a dev environment

```bash
databricks bundle destroy -t dev     # drops the dev schema (and its tables/volume), pipeline, job, dashboard
databricks bundle deploy  -t dev
```
