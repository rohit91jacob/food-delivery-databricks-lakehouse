"""Operate the Genie agent: tracing destination, ask, evaluate, monitor, deploy.

Environment (all optional except where noted):

| Variable | Default | Meaning |
|---|---|---|
| ``FD_GENIE_SPACE_ID`` | (required for ask/eval/deploy) | printed by ``fd agent space`` |
| ``FD_AGENT_EXPERIMENT`` | ``/Users/<me>/fooddelivery-genie-agent`` | MLflow experiment for traces, evals, monitoring |
| ``FD_AGENT_TRACE_SCHEMA`` | empty = experiment storage | ``catalog.schema`` to store traces in Unity Catalog |
| ``FD_AGENT_MODEL`` | ``<catalog>.<schema>_ops.genie_agent`` | UC model name used by ``fd agent deploy`` |
"""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

from fooddelivery.config import LakehouseTarget

log = logging.getLogger(__name__)

AGENT_FILE = Path(__file__).with_name("genie_agent.py")


def experiment_path(w) -> str:
    return os.environ.get("FD_AGENT_EXPERIMENT") or f"/Users/{w.current_user.me().user_name}/fooddelivery-genie-agent"


def configure_tracking(w, warehouse_id: str | None = None) -> str:
    """Point MLflow at the workspace and the agent's experiment. Returns the experiment id."""
    import mlflow

    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    name = experiment_path(w)
    trace_schema = os.environ.get("FD_AGENT_TRACE_SCHEMA", "").strip()
    if trace_schema:
        from mlflow.entities.trace_location import UnityCatalog

        catalog, schema = trace_schema.split(".", 1)
        if warehouse_id:
            os.environ.setdefault("MLFLOW_TRACING_SQL_WAREHOUSE_ID", warehouse_id)
        exp = mlflow.set_experiment(
            experiment_name=name, trace_location=UnityCatalog(catalog_name=catalog, schema_name=schema)
        )
    else:
        exp = mlflow.set_experiment(experiment_name=name)
    log.info("MLflow experiment %s (%s)", name, exp.experiment_id)
    return exp.experiment_id


def trace_url(host: str, experiment_id: str, trace_id: str) -> str:
    return f"{host.rstrip('/')}/ml/experiments/{experiment_id}/traces/{trace_id.rsplit('/', 1)[-1]}"


def ask(w, question: str, conversation_id: str | None = None) -> dict:
    import mlflow

    from fooddelivery.agent.genie_agent import GenieAgent

    exp_id = configure_tracking(w)
    agent = GenieAgent(workspace_client=w)
    request = {"input": [{"role": "user", "content": question}]}
    if conversation_id:
        request["custom_inputs"] = {"conversation_id": conversation_id}
    response = agent.predict(request)
    trace_id = mlflow.get_last_active_trace_id()
    mlflow.flush_trace_async_logging()
    out = response.model_dump()
    return {
        "answer": out["output"][0]["content"][0]["text"],
        **(out.get("custom_outputs") or {}),
        "trace_id": trace_id,
        "trace_url": trace_url(w.config.host, exp_id, trace_id) if trace_id else None,
    }


def evaluate(w, start: date, end: date, *, per_metric: int = 3) -> dict:
    import mlflow

    from fooddelivery.agent.evalset import build_evalset, manifests_from_simulator
    from fooddelivery.agent.genie_agent import GenieAgent
    from fooddelivery.agent.scorers import evaluation_scorers

    exp_id = configure_tracking(w)
    # Genie answers a handful of questions per minute per workspace; more workers only queue on 429s.
    os.environ.setdefault("MLFLOW_GENAI_EVAL_MAX_WORKERS", "2")
    data = build_evalset(manifests_from_simulator(start, end), per_metric=per_metric)
    agent = GenieAgent(workspace_client=w)

    def predict_fn(request):
        return agent.predict(request).model_dump()

    with mlflow.start_run(run_name=f"genie-eval-{start.isoformat()}-{end.isoformat()}") as run:
        mlflow.log_params({"eval_start": start.isoformat(), "eval_end": end.isoformat(), "questions": len(data)})
        result = mlflow.genai.evaluate(data=data, predict_fn=predict_fn, scorers=evaluation_scorers())
    metrics = {k: float(v) for k, v in sorted(result.metrics.items())}
    return {"experiment_id": exp_id, "run_id": run.info.run_id, "questions": len(data), "metrics": metrics}


def start_monitoring(w, *, sample_scale: float = 1.0) -> dict:
    """Register the monitoring scorers on the experiment and start them (idempotent)."""
    from mlflow.genai.scorers import ScorerSamplingConfig, get_scorer

    from fooddelivery.agent.scorers import monitoring_scorers

    exp_id = configure_tracking(w)
    out = {}
    for name, (scorer, rate) in monitoring_scorers().items():
        rate = max(0.0, min(1.0, rate * sample_scale))
        try:
            registered = get_scorer(name=name, experiment_id=exp_id)
        except Exception:
            registered = scorer.register(name=name, experiment_id=exp_id)
        registered.start(name=name, experiment_id=exp_id, sampling_config=ScorerSamplingConfig(sample_rate=rate))
        out[name] = rate
    return {"experiment_id": exp_id, "scorers": out}


def stop_monitoring(w) -> dict:
    from mlflow.genai.scorers import get_scorer

    from fooddelivery.agent.scorers import monitoring_scorers

    exp_id = configure_tracking(w)
    stopped = []
    for name in monitoring_scorers():
        try:
            get_scorer(name=name, experiment_id=exp_id).stop(name=name, experiment_id=exp_id)
            stopped.append(name)
        except Exception as e:  # not registered
            log.info("scorer %s not stopped: %s", name, e)
    return {"experiment_id": exp_id, "stopped": stopped}


def log_and_register(w, target: LakehouseTarget, space_id: str, warehouse_id: str) -> dict:
    """Log the agent as models-from-code and register it in Unity Catalog."""
    import mlflow
    from mlflow.models.resources import DatabricksGenieSpace, DatabricksSQLWarehouse, DatabricksTable

    from fooddelivery.agent.space import GOLD_TABLES

    exp_id = configure_tracking(w)
    model_name = os.environ.get("FD_AGENT_MODEL") or f"{target.catalog}.{target.schema}_ops.genie_agent"
    resources = [
        DatabricksGenieSpace(genie_space_id=space_id),
        DatabricksSQLWarehouse(warehouse_id=warehouse_id),
        *(DatabricksTable(table_name=f"{target.catalog}.{target.gold_schema}.{t}") for t in GOLD_TABLES),
    ]
    example = {"input": [{"role": "user", "content": "What was GMV by city on the latest business date?"}]}
    kwargs = {
        "python_model": str(AGENT_FILE),
        "model_config": {"FD_GENIE_SPACE_ID": space_id},
        "resources": resources,
        "input_example": example,
        "pip_requirements": [f"mlflow=={mlflow.__version__}", "databricks-sdk==0.148.0"],
    }
    with mlflow.start_run(run_name="genie-agent-log") as run:
        if os.name != "nt":
            info = mlflow.pyfunc.log_model(name="genie_agent", registered_model_name=model_name, **kwargs)
            model_uri, version = info.model_uri, info.registered_model_version
        else:
            model_uri, version = _log_from_windows(mlflow, run.info.run_id, model_name, kwargs)
    return {"experiment_id": exp_id, "model_uri": model_uri, "registered_model": model_name, "version": version}


def _log_from_windows(mlflow, run_id: str, model_name: str, kwargs: dict) -> tuple[str, str]:
    """Work around an MLflow models-from-code bug for models logged on Windows.

    MLmodel records ``model_code_path`` as the absolute ``C:\\...`` path. The Linux serving
    container rebuilds it with ``os.path.join(model_dir, os.path.basename(path))``, and POSIX
    ``basename`` doesn't split on backslashes, so it looks for ``/model/C:\\...`` and the endpoint
    fails with "An error occurred in model loading code". The file itself is bundled as
    ``genie_agent.py``, so saving locally and recording the bare filename fixes it.
    """
    import tempfile

    import yaml

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "genie_agent"
        mlflow.pyfunc.save_model(path=str(path), **kwargs)
        mlmodel = path / "MLmodel"
        meta = yaml.safe_load(mlmodel.read_text(encoding="utf-8"))
        meta["flavors"]["python_function"]["model_code_path"] = AGENT_FILE.name
        mlmodel.write_text(yaml.safe_dump(meta, sort_keys=False), encoding="utf-8")
        mlflow.log_artifacts(str(path), artifact_path="genie_agent")
    mv = mlflow.register_model(f"runs:/{run_id}/genie_agent", model_name)
    return f"models:/{model_name}/{mv.version}", str(mv.version)


def deploy(w, target: LakehouseTarget, space_id: str, warehouse_id: str) -> dict:
    """Register, then serve with Mosaic AI Agent Framework (traces + monitoring go to the experiment)."""
    from databricks import agents

    reg = log_and_register(w, target, space_id, warehouse_id)
    dep = agents.deploy(
        reg["registered_model"],
        reg["version"],
        scale_to_zero=True,
        tags={"project": "fooddelivery"},
        # belt and braces: the logged model_config also carries it
        environment_vars={"FD_GENIE_SPACE_ID": space_id},
    )
    return {**reg, "endpoint": dep.endpoint_name, "query_endpoint": getattr(dep, "query_endpoint", None)}
