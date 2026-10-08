from __future__ import annotations

import pytest

from conftest import REPO

pytestmark = pytest.mark.airflow
airflow = pytest.importorskip("airflow", reason="Airflow not installed (uv sync --group airflow)")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(REPO / "airflow" / "dags"))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_food_delivery_daily_shape(dagbag):
    dag = dagbag.dags["food_delivery_daily"]
    assert dag is not None
    assert dag.catchup is True and dag.max_active_runs == 1
    assert dag.timetable.summary.startswith("0 0 * * *")
    deps = {t.task_id: sorted(t.downstream_task_ids) for t in dag.tasks}
    assert deps == {
        "prepare_seed": ["generate_batch"],
        "generate_batch": ["build_reconciliation_sql", "publish_summary", "upload_batch"],
        "upload_batch": ["publish_summary", "run_lakehouse_job"],
        "run_lakehouse_job": ["reconcile_gold_with_manifest"],
        "build_reconciliation_sql": ["reconcile_gold_with_manifest"],
        "reconcile_gold_with_manifest": ["publish_summary"],
        "publish_summary": [],
    }


def test_retries_and_ordering_guards(dagbag):
    dag = dagbag.dags["food_delivery_daily"]
    for t in dag.tasks:
        assert t.retries == 2, t.task_id
        assert t.on_failure_callback, t.task_id
    assert dag.get_task("generate_batch").depends_on_past is True
    run = dag.get_task("run_lakehouse_job")
    assert run.deferrable is True
    assert run.job_parameters["run_date"].startswith("{{ data_interval_start")  # templated field
