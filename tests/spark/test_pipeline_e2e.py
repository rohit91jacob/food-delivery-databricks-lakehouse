"""End to end: generator -> landing -> the real Lakeflow pipeline sources (emulated) -> DQ catalog."""

from __future__ import annotations

import pytest

from fdtest import DAYS, SMALL
from fooddelivery.generator.writer import read_manifest
from fooddelivery.local.lakehouse import PipelineEmulator
from fooddelivery.quality.reconcile import build_reconciliation_sql
from fooddelivery.quality.runner import qualified_tables, run_checks

pytestmark = [pytest.mark.spark, pytest.mark.slow]


def tables(emulator):
    return {t: emulator.table_name(t) for t in qualified_tables("x", "y")}


def test_pipeline_defines_the_expected_datasets(emulated):
    ds = emulated.registry.datasets
    for entity in ("restaurants", "menu_items", "orders", "order_events", "rider_locations", "manifests"):
        assert ds[f"bronze_{entity}"].kind == "table"
        assert ds[f"silver_{entity}"].kind == "streaming_table"
    assert {n for n in ds if n.startswith("quarantine_")} == {
        "quarantine_orders",
        "quarantine_order_events",
        "quarantine_payments",
        "quarantine_refunds",
    }
    assert len(ds) - sum(d.kind == "temporary_view" for d in ds.values()) <= 50  # Free Edition: 100 tables/schema
    assert ds["fct_orders"].kind == "materialized_view"
    assert sum(1 for n in ds if n.startswith("gold_")) == 10
    flows = emulated.registry.flows
    assert flows["silver_restaurants"][0].scd_type == 2 and flows["silver_menu_items"][0].scd_type == 2
    assert flows["silver_orders"][0].keys == ["order_id"]


@pytest.mark.parametrize("d", DAYS)
def test_every_error_check_passes(spark, emulated, d):
    failed = [r for r in run_checks(spark, tables(emulated), d) if r.severity == "error" and not r.passed]
    assert not failed, [(r.check_name, r.detail) for r in failed]


def test_dedupe_and_quarantine_are_exact(emulated, batches):
    rows = emulated.report.rows
    dups = sum(m["duplicate_order_records"] for b in batches.values() for m in b.manifests)
    bad = sum(m["invalid_order_records"] for b in batches.values() for m in b.manifests)
    assert dups > 0 and bad > 0
    assert rows["quarantine_orders"] == bad
    assert rows["bronze_orders"] - rows["silver_orders"] - rows["quarantine_orders"] == dups


def test_scd2_point_in_time_pricing(spark, emulated):
    stats = spark.sql("""
        SELECT count(*) AS lines, count_if(price_matches_menu) AS matched,
               (SELECT count(*) FROM silver_menu_items WHERE __END_AT IS NOT NULL) AS closed_versions
        FROM fct_order_items""").collect()[0]
    assert stats.lines == stats.matched > 0
    assert stats.closed_versions > 0  # price changes / removals actually produced history


def test_airflow_reconciliation_sql_passes_then_catches_tampering(spark, emulated, landing):
    from pyspark.errors import PySparkException

    bd = DAYS[1].isoformat()
    manifests = read_manifest(landing, bd)
    ok = spark.sql(build_reconciliation_sql(manifests, "gold_daily_kpis", bd)).collect()[0]["result"]
    assert ok.startswith(f"reconciled {len(SMALL.cities)} cities")
    tampered = [dict(m) for m in manifests]
    tampered[0]["gmv"] = tampered[0]["gmv"] + 0.01
    with pytest.raises(PySparkException, match="does not reconcile"):
        spark.sql(build_reconciliation_sql(tampered, "gold_daily_kpis", bd)).collect()


def test_rerun_is_idempotent(spark, emulated, landing):
    before = spark.sql("SELECT * FROM gold_daily_kpis ORDER BY business_date, city").collect()
    again = PipelineEmulator(spark, landing)  # same landing files -> same gold, row for row
    again.run()
    after = spark.sql("SELECT * FROM gold_daily_kpis ORDER BY business_date, city").collect()
    assert before == after
    assert again.report.rows == emulated.report.rows
