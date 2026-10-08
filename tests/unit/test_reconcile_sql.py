from __future__ import annotations

import pytest

from fooddelivery.quality.checks import CHECKS, RECONCILED_METRICS, render
from fooddelivery.quality.reconcile import build_reconciliation_sql
from fooddelivery.quality.runner import qualified_tables


def test_reconciliation_sql_inlines_every_city_and_metric(batches):
    batch = next(iter(batches.values()))
    sql = build_reconciliation_sql(batch.manifests, "`w`.`s`.`gold_daily_kpis`", batch.business_date.isoformat())
    assert sql.count("CAST(") >= len(RECONCILED_METRICS) * len(batch.manifests)
    assert "raise_error" in sql and f"DATE'{batch.business_date.isoformat()}'" in sql
    for m in batch.manifests:
        assert f"'{m['city']}'" in sql


def test_reconciliation_sql_rejects_bad_input():
    with pytest.raises(ValueError):
        build_reconciliation_sql([], "t", "2026-09-01")
    with pytest.raises(ValueError):
        build_reconciliation_sql([{"city": "x"}], "t", "2026-09-01'; DROP")


def test_check_catalog_renders_with_qualified_tables():
    tables = qualified_tables("workspace", "fooddelivery")
    names = [c.name for c in CHECKS]
    assert len(names) == len(set(names))
    for check in CHECKS:
        sql = render(check, tables)
        assert "{" not in sql.replace("{}", "")
        assert "failing_rows" in sql and check.severity in ("error", "warn")
