# ADR 0004: Verify the real pipeline sources offline with an emulator

**Status:** accepted

## Context
Lakeflow-specific APIs aren't available in open-source PySpark 4.0:
- `create_auto_cdc_flow`
- expectations
- Auto Loader

CI has no workspace (forks and PRs never get secrets), and Free Edition quotas make "test in the workspace on
every push" impractical.

## Decision
`fooddelivery.local.lakehouse.PipelineEmulator` executes the **unchanged** files in
`src/fooddelivery_pipeline/transformations/` with a shim `pyspark.pipelines` module and a shim `spark`:
- Auto Loader becomes a batch JSON read with the same explicit schema and `_metadata`
- expectations are enforced with warn/drop/fail semantics
- AUTO CDC is applied by `fooddelivery.local.scd` (SCD1/SCD2, deletes, in-place updates of untracked columns)

The end-to-end test runs the generator through the emulator and then the full DQ catalog.

## Consequences
- Wiring mistakes surface on every push in CI, without credentials. That includes undefined datasets,
  expectations on missing columns, broken joins and reconciliation drift.
- Databricks-only behaviour (incremental streaming state, checkpoints, MV incremental refresh) is covered only
  by deploying the bundle. The emulator is not a substitute for that.
