"""Runs the DQ check catalog with Spark and records the results.

On Databricks this is the ``fd-dq`` wheel entry point of the job's ``data_quality`` task
(serverless). It writes every result to ``<catalog>.<schema>.dq_results`` and exits non-zero
when an ``error`` check fails, which fails the job run (and so the Airflow task that triggered it).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime

from fooddelivery.config import validate_identifier
from fooddelivery.quality.checks import CHECKS, TABLES, Check, render

log = logging.getLogger(__name__)

RESULTS_DDL = (
    "run_date DATE, check_name STRING, severity STRING, failing_rows BIGINT, passed BOOLEAN, "
    "detail STRING, description STRING, checked_at TIMESTAMP, job_run_id STRING"
)


@dataclass(frozen=True)
class CheckResult:
    run_date: date
    check_name: str
    severity: str
    failing_rows: int
    passed: bool
    detail: str
    description: str
    checked_at: datetime
    job_run_id: str


GOLD_TABLES = ("fct_orders", "fct_order_items", "gold_daily_kpis")


def qualified_tables(
    catalog: str, schema: str, gold_schema: str | None = None, *, quote: bool = True
) -> dict[str, str]:
    validate_identifier(catalog, "catalog")
    validate_identifier(schema, "schema")
    gold_schema = validate_identifier(gold_schema or schema, "gold schema")
    q = (lambda x: f"`{x}`") if quote else (lambda x: x)
    return {t: f"{q(catalog)}.{q(gold_schema if t in GOLD_TABLES else schema)}.{q(t)}" for t in TABLES}


def run_checks(
    spark, tables: dict[str, str], run_date: date, *, job_run_id: str = "", checks: tuple[Check, ...] = CHECKS
) -> list[CheckResult]:
    results = []
    for check in checks:
        row = spark.sql(render(check, tables), args={"run_date": run_date}).collect()[0]
        failing = int(row["failing_rows"] or 0)
        results.append(
            CheckResult(
                run_date=run_date,
                check_name=check.name,
                severity=check.severity,
                failing_rows=failing,
                passed=failing == 0,
                detail=(row["detail"] or "")[:4000],
                description=check.description,
                checked_at=datetime.now(UTC).replace(tzinfo=None),
                job_run_id=job_run_id,
            )
        )
        level = logging.INFO if failing == 0 else (logging.ERROR if check.severity == "error" else logging.WARNING)
        log.log(
            level,
            "dq check",
            extra={"check": check.name, "failing_rows": failing, "severity": check.severity, "detail": row["detail"]},
        )
    return results


def store_results(spark, results_table: str, results: list[CheckResult]) -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {results_table} ({RESULTS_DDL})")
    rows = [tuple(asdict(r).values()) for r in results]
    spark.createDataFrame(rows, RESULTS_DDL).write.mode("append").saveAsTable(results_table)


def failed_errors(results: list[CheckResult]) -> list[CheckResult]:
    return [r for r in results if r.severity == "error" and not r.passed]


def main(argv: list[str] | None = None) -> int:
    from fooddelivery import logs

    logs.configure()
    p = argparse.ArgumentParser(prog="fd-dq", description=__doc__)
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--run-date", required=True, type=date.fromisoformat)
    p.add_argument("--job-run-id", default="")
    p.add_argument("--results-table", default="dq_results")
    p.add_argument("--gold-schema", default=None, help="schema of fct_*/gold_* (default: --schema)")
    p.add_argument("--results-schema", default=None, help="schema for dq_results (default: --schema)")
    args = p.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    tables = qualified_tables(args.catalog, args.schema, args.gold_schema)
    results = run_checks(spark, tables, args.run_date, job_run_id=args.job_run_id)
    results_schema = validate_identifier(args.results_schema or args.schema, "results schema")
    validate_identifier(args.results_table, "results table")
    store_results(spark, f"`{args.catalog}`.`{results_schema}`.`{args.results_table}`", results)
    failures = failed_errors(results)
    summary = {
        "run_date": args.run_date.isoformat(),
        "checks": len(results),
        "failed_errors": len(failures),
        "failed": [f.check_name for f in failures],
    }
    print(json.dumps(summary))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
