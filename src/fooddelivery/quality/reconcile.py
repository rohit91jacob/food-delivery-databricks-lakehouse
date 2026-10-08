"""Orchestrator-side reconciliation: the gold mart against the manifest Airflow generated itself.

The Databricks job also reconciles gold with ``silver_manifests``. That copy of the manifest came
through the same pipeline, though. This check is independent: Airflow inlines the ground truth it
produced into a single SQL statement and runs it on the SQL warehouse. Any mismatch fails through
``raise_error()``, so a plain ``DatabricksSqlOperator`` fails the task.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from fooddelivery.generator.simulator import MONEY_FIELDS
from fooddelivery.quality.checks import RECONCILED_METRICS


def _literal(metric: str, value) -> str:
    if metric in MONEY_FIELDS:
        return f"CAST('{Decimal(repr(float(value))):.2f}' AS DECIMAL(18,2))"
    return f"CAST({int(value)} AS BIGINT)"


def _city(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def build_reconciliation_sql(manifests: list[dict], kpis_table: str, run_date: str) -> str:
    date_lit = date.fromisoformat(run_date).isoformat()  # raises on anything but YYYY-MM-DD
    if not manifests:
        raise ValueError("no manifest rows to reconcile")
    cols = ("city", *RECONCILED_METRICS)
    rows = ",\n    ".join(
        "(" + ", ".join([_city(m["city"]), *[_literal(k, m[k]) for k in RECONCILED_METRICS]]) + ")"
        for m in sorted(manifests, key=lambda m: m["city"])
    )
    mismatch = ",\n      ".join(f"IF(a.{k} <=> e.{k}, NULL, '{k}')" for k in RECONCILED_METRICS)
    return f"""
WITH expected AS (
  SELECT * FROM VALUES
    {rows}
  AS t({", ".join(cols)})
),
actual AS (SELECT * FROM {kpis_table} WHERE business_date = DATE'{date_lit}'),
diff AS (
  SELECT coalesce(e.city, a.city) AS city,
    filter(array(
      {mismatch}
    ), x -> x IS NOT NULL) AS mismatched
  FROM expected e FULL OUTER JOIN actual a ON e.city = a.city
)
SELECT CASE
  WHEN sum(size(mismatched)) > 0 THEN raise_error(concat(
    'gold_daily_kpis does not reconcile with the generator manifest for {date_lit}: ',
    to_json(collect_list(named_struct('city', city, 'metrics', mismatched)) FILTER (WHERE size(mismatched) > 0))))
  ELSE concat('reconciled ', count(*), ' cities x {len(RECONCILED_METRICS)} metrics for {date_lit}')
END AS result
FROM diff""".strip()
