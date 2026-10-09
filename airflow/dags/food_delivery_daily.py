"""
### food_delivery_daily

One run per **business date** (Asia/Kolkata), covering the interval `[D 00:00, D+1 00:00)` IST:

1. **prepare_seed**: download the CC0 Kaggle Swiggy dataset once and normalise it into the
   restaurant seed (reused afterwards so the simulation stays reproducible).
2. **generate_batch**: produce the day's raw feeds and ground-truth manifest. The output is
   deterministic, so a rerun is byte-identical. `depends_on_past` keeps the dimension CDC feeds in order.
3. **upload_batch**: idempotent sync to the Unity Catalog landing volume through the Files API. Free
   Edition serverless has no open internet egress, so data is pushed in from here.
4. **run_lakehouse_job**: trigger the bundle's Lakeflow Job (pipeline refresh + `fd-dq` checks).
5. **reconcile_gold_with_manifest**: an independent check on the SQL warehouse. Gold must equal
   the manifest this DAG produced, metric by metric, or the task fails via `raise_error`.
6. **publish_summary**: log a run summary and post it to `FD_ALERT_WEBHOOK_URL` if one is set.

Backfill: `airflow backfill create --dag-id food_delivery_daily --from-date 2026-09-01 --to-date 2026-09-07`.
Every task is idempotent per business date.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import timedelta

import pendulum
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from fooddelivery.config import GeneratorConfig, LakehouseTarget, Paths

log = logging.getLogger(__name__)

TZ = "Asia/Kolkata"
CONN_ID = os.environ.get("FD_DATABRICKS_CONN_ID", "databricks_default")
TARGET = LakehouseTarget.from_env()
START = pendulum.datetime(*map(int, os.environ.get("FD_START_DATE", "2026-09-01").split("-")), tz=TZ)
BUSINESS_DATE = "{{ data_interval_start.in_timezone('Asia/Kolkata').strftime('%Y-%m-%d') }}"


def _business_date() -> str:
    return get_current_context()["data_interval_start"].in_timezone(TZ).strftime("%Y-%m-%d")


def _post_webhook(text: str) -> None:
    url = os.environ.get("FD_ALERT_WEBHOOK_URL", "").strip()
    if not url:
        return
    req = urllib.request.Request(
        url, data=json.dumps({"text": text}).encode(), headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def notify_failure(context) -> None:
    ti = context["task_instance"]
    text = f":rotating_light: food_delivery_daily failed: task={ti.task_id} run={context['run_id']} try={ti.try_number}"
    log.error(text)
    try:
        _post_webhook(text)
    except Exception:  # alerting must never mask the original failure
        log.exception("failure webhook could not be delivered")


def _workspace_client():
    from airflow.sdk import Connection

    from fooddelivery.landing.uploader import workspace_client

    conn = Connection.get(CONN_ID)
    host = conn.host if conn.host.startswith("http") else f"https://{conn.host}"
    return workspace_client(host=host, token=conn.password)


@dag(
    dag_id="food_delivery_daily",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=TZ),
    start_date=START,
    catchup=True,
    max_active_runs=1,  # Free Edition allows one active pipeline update at a time
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": notify_failure,
    },
    tags=["fooddelivery", "databricks", "lakeflow"],
    doc_md=__doc__,
)
def food_delivery_daily():
    @task
    def prepare_seed() -> dict:
        from fooddelivery.seeding import ensure_seed

        return ensure_seed(Paths.from_env(), GeneratorConfig.from_env())

    @task(depends_on_past=True)
    def generate_batch(seed: dict) -> dict:
        from datetime import date

        from fooddelivery.generator.seed import read_seed
        from fooddelivery.generator.simulator import Simulator
        from fooddelivery.generator.writer import write_batch

        paths = Paths.from_env()
        business_date = _business_date()
        batch = Simulator(GeneratorConfig.from_env(), read_seed(paths.seed_file)).generate_day(
            date.fromisoformat(business_date)
        )
        index = write_batch(batch, paths.landing)
        return {
            "business_date": business_date,
            "files": len(index),
            "seed_restaurants": (seed or {}).get("restaurants"),
            "manifests": batch.manifests,
        }

    @task
    def upload_batch(batch: dict) -> dict:
        from fooddelivery.landing.uploader import VolumeSync

        sync = VolumeSync(_workspace_client().files, TARGET)
        return sync.sync(Paths.from_env().landing, batch["business_date"]).as_dict()

    @task
    def build_reconciliation_sql(batch: dict) -> str:
        from fooddelivery.quality.reconcile import build_reconciliation_sql as build

        return build(batch["manifests"], TARGET.gold_table("gold_daily_kpis"), batch["business_date"])

    @task
    def publish_summary(batch: dict, upload: dict) -> dict:
        totals = {k: sum(m[k] for m in batch["manifests"]) for k in ("orders_placed", "orders_delivered")}
        summary = {"business_date": batch["business_date"], **totals, "upload": upload, "reconciled": True}
        log.info("food_delivery_daily summary %s", json.dumps(summary, default=str))
        _post_webhook(
            f":white_check_mark: food delivery {batch['business_date']}: "
            f"{totals['orders_placed']} orders placed, {totals['orders_delivered']} delivered, "
            "gold reconciled with the manifest"
        )
        return summary

    seed = prepare_seed()
    batch = generate_batch(seed)
    upload = upload_batch(batch)

    run_job = DatabricksRunNowOperator(
        task_id="run_lakehouse_job",
        databricks_conn_id=CONN_ID,
        job_name=TARGET.job_name,
        job_parameters={"run_date": BUSINESS_DATE},
        wait_for_termination=True,
        deferrable=True,
        polling_period_seconds=30,
    )
    recon_sql = build_reconciliation_sql(batch)
    reconcile = DatabricksSqlOperator(
        task_id="reconcile_gold_with_manifest",
        databricks_conn_id=CONN_ID,
        sql_endpoint_name=TARGET.warehouse_name,
        sql=recon_sql,
        do_xcom_push=False,
    )
    upload >> run_job >> reconcile
    reconcile >> publish_summary(batch, upload)


food_delivery_daily()
