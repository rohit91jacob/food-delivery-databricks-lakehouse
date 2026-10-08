"""Post-pipeline data-quality checks, as one catalog shared by every runtime.

Each check is a SQL query that returns exactly one row ``(failing_rows BIGINT, detail STRING)``.
The check passes when ``failing_rows = 0``. Queries take the business date as the named parameter
marker ``:run_date`` and use ``{placeholders}`` for fully-qualified table names, which are built
from validated identifiers (see ``config.validate_identifier``).

The same catalog runs:
  * in the Databricks job (``fd-dq`` task, after the pipeline), with results stored in ``dq_results``
  * in the local lakehouse (tests and ``fd local-run``)
"""

from __future__ import annotations

from dataclasses import dataclass

from fooddelivery.generator.simulator import COUNT_FIELDS, MONEY_FIELDS

TABLES = (
    "bronze_orders",
    "silver_orders",
    "quarantine_orders",
    "silver_restaurants",
    "silver_menu_items",
    "silver_manifests",
    "silver_payments",
    "fct_orders",
    "fct_order_items",
    "gold_daily_kpis",
)

# metrics carried by both the manifest and gold_daily_kpis (dedupe/quarantine counts are checked apart)
RECONCILED_METRICS = (
    tuple(m for m in COUNT_FIELDS if m not in ("duplicate_order_records", "invalid_order_records")) + MONEY_FIELDS
)


@dataclass(frozen=True)
class Check:
    name: str
    severity: str  # error | warn
    description: str
    sql: str


def _reconciliation_sql() -> str:
    mismatch = ",\n      ".join(f"IF(k.{m} <=> m.{m}, NULL, '{m}')" for m in RECONCILED_METRICS)
    return f"""
WITH k AS (SELECT * FROM {{gold_daily_kpis}} WHERE business_date = :run_date),
     m AS (SELECT * FROM {{silver_manifests}} WHERE business_date = :run_date),
     j AS (
  SELECT coalesce(k.city, m.city) AS city,
    filter(array(
      {mismatch}
    ), x -> x IS NOT NULL) AS mismatched
  FROM k FULL OUTER JOIN m ON k.city = m.city
)
SELECT CAST(coalesce(sum(size(mismatched)), 0) AS BIGINT) AS failing_rows,
       to_json(collect_list(named_struct('city', city, 'metrics', mismatched))
               FILTER (WHERE size(mismatched) > 0)) AS detail
FROM j"""


CHECKS: tuple[Check, ...] = (
    Check(
        "gold_partition_present",
        "error",
        "gold_daily_kpis has one row per city listed in the manifest for the run date.",
        """
SELECT CAST(abs(coalesce(m.n, 0) - coalesce(k.n, 0)) + IF(coalesce(m.n, 0) = 0, 1, 0) AS BIGINT) AS failing_rows,
       concat('manifest cities=', coalesce(m.n, 0), ', gold cities=', coalesce(k.n, 0)) AS detail
FROM (SELECT count(*) AS n FROM {silver_manifests} WHERE business_date = :run_date) m
CROSS JOIN (SELECT count(*) AS n FROM {gold_daily_kpis} WHERE business_date = :run_date) k""",
    ),
    Check(
        "gold_reconciles_with_manifest",
        "error",
        "Every count and money metric in gold_daily_kpis equals the generator ground truth.",
        _reconciliation_sql(),
    ),
    Check(
        "silver_orders_unique",
        "error",
        "order_id is unique in silver_orders.",
        """
SELECT CAST(count(*) - count(DISTINCT order_id) AS BIGINT) AS failing_rows, '' AS detail
FROM {silver_orders} WHERE business_date = :run_date""",
    ),
    Check(
        "fct_orders_complete",
        "error",
        "Every valid silver order reaches fct_orders exactly once.",
        """
SELECT CAST(abs(s.n - f.n) AS BIGINT) AS failing_rows, concat('silver=', s.n, ', fct=', f.n) AS detail
FROM (SELECT count(*) AS n FROM {silver_orders} WHERE business_date = :run_date) s
CROSS JOIN (SELECT count(*) AS n FROM {fct_orders} WHERE business_date = :run_date) f""",
    ),
    Check(
        "duplicates_removed_as_expected",
        "error",
        "bronze - silver - quarantine order rows equals the duplicates the generator injected.",
        """
SELECT CAST(abs((b.n - s.n - q.n) - m.dups) AS BIGINT) AS failing_rows,
       concat('bronze=', b.n, ', silver=', s.n, ', quarantined=', q.n, ', expected_duplicates=', m.dups) AS detail
FROM (SELECT count(*) AS n FROM {bronze_orders} WHERE business_date = cast(:run_date AS STRING)) b
CROSS JOIN (SELECT count(*) AS n FROM {silver_orders} WHERE business_date = :run_date) s
CROSS JOIN (SELECT count(*) AS n FROM {quarantine_orders} WHERE business_date = :run_date) q
CROSS JOIN (SELECT coalesce(sum(duplicate_order_records), 0) AS dups FROM {silver_manifests}
            WHERE business_date = :run_date) m""",
    ),
    Check(
        "quarantine_matches_injected_invalid",
        "error",
        "Exactly the invalid orders the generator injected were quarantined.",
        """
SELECT CAST(abs(q.n - m.bad) AS BIGINT) AS failing_rows, concat('quarantined=', q.n, ', injected=', m.bad) AS detail
FROM (SELECT count(*) AS n FROM {quarantine_orders} WHERE business_date = :run_date) q
CROSS JOIN (SELECT coalesce(sum(invalid_order_records), 0) AS bad FROM {silver_manifests}
            WHERE business_date = :run_date) m""",
    ),
    Check(
        "restaurants_single_current_version",
        "error",
        "SCD2: at most one open (__END_AT IS NULL) version per restaurant.",
        """
SELECT CAST(count(*) AS BIGINT) AS failing_rows, to_json(slice(collect_list(restaurant_id), 1, 10)) AS detail
FROM (SELECT restaurant_id FROM {silver_restaurants} WHERE __END_AT IS NULL
      GROUP BY restaurant_id HAVING count(*) > 1)""",
    ),
    Check(
        "menu_scd2_versions_do_not_overlap",
        "error",
        "SCD2: menu item versions never overlap in time.",
        """
SELECT CAST(count(*) AS BIGINT) AS failing_rows, to_json(slice(collect_list(item_id), 1, 10)) AS detail
FROM (SELECT item_id, __START_AT,
             lag(__START_AT) OVER w AS prev_start,
             lag(__END_AT) OVER w AS prev_end
      FROM {silver_menu_items}
      WINDOW w AS (PARTITION BY item_id ORDER BY __START_AT))
WHERE prev_start IS NOT NULL AND (prev_end IS NULL OR prev_end > __START_AT)""",
    ),
    Check(
        "order_lines_priced_from_menu_scd2",
        "error",
        "Each order line's unit price equals the menu price in force when the order was placed.",
        """
SELECT CAST(count(*) AS BIGINT) AS failing_rows,
       to_json(slice(collect_list(concat(order_id, '#', line_no)), 1, 10)) AS detail
FROM {fct_order_items}
WHERE business_date = :run_date AND NOT coalesce(price_matches_menu, false)""",
    ),
    Check(
        "orders_join_restaurant_version",
        "error",
        "Every order finds the restaurant version in force at placement (point-in-time join).",
        """
SELECT CAST(count(*) AS BIGINT) AS failing_rows, to_json(slice(collect_list(order_id), 1, 10)) AS detail
FROM {fct_orders} WHERE business_date = :run_date AND commission_rate IS NULL""",
    ),
    Check(
        "delivered_lifecycle_monotonic",
        "error",
        "placed <= accepted <= preparing <= ready <= picked_up <= delivered for delivered orders.",
        """
SELECT CAST(count(*) AS BIGINT) AS failing_rows, to_json(slice(collect_list(order_id), 1, 10)) AS detail
FROM {fct_orders}
WHERE business_date = :run_date AND order_status = 'delivered'
  AND NOT (placed_at <= accepted_at AND accepted_at <= preparing_at AND preparing_at <= ready_at
           AND ready_at <= picked_up_at AND picked_up_at <= delivered_at)""",
    ),
    Check(
        "one_payment_per_order",
        "error",
        "Every order has exactly one payment record.",
        """
SELECT CAST(count(*) AS BIGINT) AS failing_rows, to_json(slice(collect_list(order_id), 1, 10)) AS detail
FROM {fct_orders} WHERE business_date = :run_date AND payment_count <> 1""",
    ),
    Check(
        "on_time_rate_floor",
        "warn",
        "Business guardrail: on-time rate per city stays above 70%.",
        """
SELECT CAST(count(*) AS BIGINT) AS failing_rows,
       to_json(collect_list(named_struct('city', city, 'on_time_rate', on_time_rate))) AS detail
FROM {gold_daily_kpis} WHERE business_date = :run_date AND on_time_rate < 0.70""",
    ),
)


def render(check: Check, tables: dict[str, str]) -> str:
    return check.sql.format(**tables).strip()
