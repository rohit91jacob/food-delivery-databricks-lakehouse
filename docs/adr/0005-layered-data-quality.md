# ADR 0005: Three layers of data quality

**Status:** accepted

1. **Row level, in the pipeline.** Expectations on every silver view: `warn` (metrics only), `drop` (row goes to
   `quarantine_<feed>` with `_failed_rules`), `fail` (stops the update; reserved for "impossible" states).
   Gold MVs add a few `expect_or_fail` invariants. Rules are null-safe and shared by the pipeline and the emulator.
2. **Dataset level, in the job.** The `data_quality` task (`fd-dq`) runs the check catalog for `run_date`. It
   covers manifest reconciliation, uniqueness, SCD2 integrity, lifecycle order, payment coverage, exact
   dedupe/quarantine counts and freshness. It appends results to `dq_results` and fails the job on any `error`.
3. **Independent reconciliation, in Airflow.** The DAG inlines the manifest it generated into one SQL
   statement, runs it on the warehouse (`DatabricksSqlOperator`) and fails via `raise_error()` on any mismatch.
   This catches failure modes where the in-lakehouse copy of the manifest (`silver_manifests`) is itself wrong.

The data generator deliberately injects duplicates and corrupt records. That way the DQ machinery is exercised
every day, not only when something breaks.
