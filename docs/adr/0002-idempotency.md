# ADR 0002: Idempotency by construction

**Status:** accepted

## Decision
Every layer is safe to rerun for a business date:

1. **Deterministic generator.** All randomness comes from `sha256(seed, scope, business_date)`. A date
   regenerates byte-identically (gzip `mtime=0`) without depending on other dates.
2. **Business-date partitions with versioned file names:** `<feed>/dt=D/part-00000-v<GENERATOR_VERSION>.json.gz`.
   Every event of an order belongs to the order's placement date, even past midnight, so a date is self-contained.
3. **Checksum-indexed upload.** Unchanged files are skipped, stale versions are pruned, and the checksum index is
   written last as a commit marker.
4. **Auto Loader** never re-ingests a path it has already processed (`cloudFiles.allowOverwrites=false`).
5. **Silver upserts by natural key** (`create_auto_cdc_flow`), so exact duplicates (injected on purpose)
   collapse to one row.
6. **Gold materialized views** are deterministic functions of silver.

A generator change bumps `GENERATOR_VERSION` and needs a pipeline full refresh (see the runbook). The landing
volume is the system of record, so bronze is allowed to be rebuilt.

## Consequences
The DQ gate can assert *exact* invariants:
- `bronze - silver - quarantine = injected duplicates`
- `quarantine = injected invalid`
- gold equals the manifest

These assertions catch both logic regressions and broken idempotency.
