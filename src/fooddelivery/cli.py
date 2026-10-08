"""``fd``, the operator CLI. Each subcommand prints one JSON document on stdout; logs go to stderr."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta

from fooddelivery import logs
from fooddelivery.config import GeneratorConfig, LakehouseTarget, Paths


def _dates(start: date, days: int) -> list[date]:
    if days < 1:
        raise SystemExit("--days must be >= 1")
    return [start + timedelta(days=i) for i in range(days)]


def cmd_seed(args) -> dict:
    from fooddelivery.seeding import ensure_seed

    return ensure_seed(Paths.from_env(), GeneratorConfig.from_env(), args.source, force=args.force)


def _simulator():
    from fooddelivery.generator.seed import read_seed
    from fooddelivery.generator.simulator import Simulator

    paths = Paths.from_env()
    if not paths.seed_file.exists():
        raise SystemExit(f"seed file {paths.seed_file} missing: run `fd seed` first")
    return Simulator(GeneratorConfig.from_env(), read_seed(paths.seed_file)), paths


def cmd_generate(args) -> dict:
    from fooddelivery.generator.writer import write_batch

    sim, paths = _simulator()
    out = {}
    for d in _dates(args.date, args.days):
        batch = sim.generate_day(d)
        index = write_batch(batch, paths.landing)
        out[d.isoformat()] = {
            "files": len(index),
            "rows": {e: len(r) for e, r in batch.records.items()},
            "orders_placed": sum(m["orders_placed"] for m in batch.manifests),
        }
    return out


def cmd_upload(args) -> dict:
    from fooddelivery.landing.uploader import VolumeSync, workspace_client

    paths = Paths.from_env()
    client = workspace_client(profile=args.profile)
    sync = VolumeSync(client.files, LakehouseTarget.from_env())
    return {d.isoformat(): sync.sync(paths.landing, d.isoformat()).as_dict() for d in _dates(args.date, args.days)}


def cmd_local_run(args) -> dict:
    from pathlib import Path

    from fooddelivery.generator.writer import MANIFEST_DIR
    from fooddelivery.local.lakehouse import PipelineEmulator, local_spark
    from fooddelivery.quality.runner import failed_errors, qualified_tables, run_checks

    paths = Paths.from_env()
    spark = local_spark(Path(args.warehouse).expanduser())
    emulator = PipelineEmulator(spark, paths.landing, database=args.database, persist=args.persist)
    report = emulator.run()
    tables = {t: emulator.table_name(t) for t in qualified_tables("x", "y")}
    dates = sorted(p.name.removeprefix("dt=") for p in (paths.landing / MANIFEST_DIR).glob("dt=*"))
    dq, failures = {}, 0
    for d in dates:
        results = run_checks(spark, tables, date.fromisoformat(d))
        failures += len(failed_errors(results))
        dq[d] = {
            r.check_name: ("ok" if r.passed else f"{r.severity.upper()} ({r.failing_rows}): {r.detail}")
            for r in results
        }
    out = {
        "rows": report.rows,
        "dropped_by_expectations": {k: v for k, v in report.dropped.items() if v},
        "warnings": {k: v for k, v in report.warnings.items() if v},
        "dq": dq,
        "dq_failed_errors": failures,
    }
    if failures:
        print(json.dumps(out, indent=2, default=str))
        raise SystemExit(1)
    return out


def cmd_reconcile_sql(args) -> dict:
    from fooddelivery.generator.writer import read_manifest
    from fooddelivery.quality.reconcile import build_reconciliation_sql

    target = LakehouseTarget.from_env()
    manifests = read_manifest(Paths.from_env().landing, args.date.isoformat())
    return {"sql": build_reconciliation_sql(manifests, target.table("gold_daily_kpis"), args.date.isoformat())}


def main(argv: list[str] | None = None) -> int:
    logs.configure()
    p = argparse.ArgumentParser(prog="fd", description="Food-delivery lakehouse operator CLI")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("seed", help="build the restaurant seed (Kaggle or synthetic)")
    s.add_argument("--source", choices=("kaggle", "synthetic"), default=None)
    s.add_argument("--force", action="store_true", help="rebuild even if a seed exists")
    s.set_defaults(func=cmd_seed)

    for name, func, help_ in (
        ("generate", cmd_generate, "generate landing files for business dates"),
        ("upload", cmd_upload, "sync landing files to the Unity Catalog volume"),
    ):
        g = sub.add_parser(name, help=help_)
        g.add_argument("--date", type=date.fromisoformat, required=True)
        g.add_argument("--days", type=int, default=1)
        if name == "upload":
            g.add_argument("--profile", default=None, help="~/.databrickscfg profile (default: unified auth)")
        g.set_defaults(func=func)

    r = sub.add_parser("local-run", help="run the Lakeflow pipeline sources + DQ checks on local Spark")
    r.add_argument("--warehouse", default="data/spark-warehouse")
    r.add_argument("--database", default="fooddelivery_local")
    r.add_argument("--persist", action="store_true", help="write Delta tables instead of temp views")
    r.set_defaults(func=cmd_local_run)

    q = sub.add_parser("reconcile-sql", help="print the Airflow reconciliation SQL for a date")
    q.add_argument("--date", type=date.fromisoformat, required=True)
    q.set_defaults(func=cmd_reconcile_sql)

    args = p.parse_args(argv)
    result = args.func(args)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
