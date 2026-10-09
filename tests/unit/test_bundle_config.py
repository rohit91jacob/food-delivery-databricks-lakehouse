"""Static checks of the Databricks bundle: schema-valid YAML, resolvable references, dashboard sanity."""

from __future__ import annotations

import json
import re
import shutil
import subprocess

import pytest
import yaml

from fdtest import REPO

RESOURCES = sorted((REPO / "resources").glob("*.yml"))


def load(path):
    return yaml.safe_load(path.read_text())


def merged() -> dict:
    root = load(REPO / "databricks.yml")
    for path in RESOURCES:
        for kind, items in load(path)["resources"].items():
            root.setdefault("resources", {}).setdefault(kind, {}).update(items)
    return root


def test_bundle_references_resolve():
    cfg = merged()
    res = cfg["resources"]
    text = "\n".join(p.read_text() for p in [REPO / "databricks.yml", *RESOURCES])
    for kind, name in re.findall(r"\$\{resources\.(\w+)\.(\w+)\.", text):
        assert name in res.get(kind, {}), f"${{resources.{kind}.{name}}} is not defined"
    for var in re.findall(r"\$\{var\.(\w+)\}", text):
        assert var in cfg["variables"], var
    assert set(cfg["targets"]) == {"dev", "prod"}
    assert cfg["targets"]["dev"]["mode"] == "development" and cfg["targets"]["prod"]["mode"] == "production"


def test_bundle_paths_exist():
    res = merged()["resources"]
    pipeline = res["pipelines"]["fooddelivery_lakehouse"]
    assert pipeline["serverless"] is True
    assert (REPO / "resources" / pipeline["root_path"] / "fooddelivery").resolve().is_dir()
    assert list((REPO / "src/fooddelivery_pipeline/transformations").glob("*.py"))
    dash = res["dashboards"]["fooddelivery_ops"]
    assert (REPO / "resources" / dash["file_path"]).resolve().is_file()


def test_job_runs_pipeline_then_dq_gate():
    job = merged()["resources"]["jobs"]["fooddelivery_daily"]
    tasks = {t["task_key"]: t for t in job["tasks"]}
    assert tasks["data_quality"]["depends_on"] == [{"task_key": "refresh_pipeline"}]
    assert tasks["data_quality"]["python_wheel_task"]["entry_point"] == "fd-dq"
    assert job["schedule"]["pause_status"] == "PAUSED" and job["max_concurrent_runs"] == 1


def test_dashboard_reads_only_existing_gold_tables():
    dash = json.loads((REPO / "dashboards/food_delivery_ops.lvdash.json").read_text())
    defined = set(
        re.findall(
            r'name=G\("(gold_\w+|fct_\w+)"\)',
            (REPO / "src/fooddelivery_pipeline/transformations/gold_marts.py").read_text(),
        )
    )
    names = {d["name"] for d in dash["datasets"]}
    for ds in dash["datasets"]:
        for table in re.findall(r"FROM\s+(\w+)", "".join(ds["queryLines"])):
            assert table in defined, table
    for item in dash["pages"][0]["layout"]:
        for q in item["widget"].get("queries", []):
            assert q["query"]["datasetName"] in names


@pytest.mark.skipif(shutil.which("databricks") is None, reason="Databricks CLI not on PATH")
def test_bundle_matches_cli_json_schema():
    jsonschema = pytest.importorskip("jsonschema")
    regex = pytest.importorskip("regex")

    def pattern(validator, patrn, instance, schema):
        # the CLI schema uses unicode property classes (\p{L}) that only the `regex` module understands
        if validator.is_type(instance, "string") and not regex.search(patrn, instance):
            yield jsonschema.ValidationError(f"{instance!r} does not match {patrn!r}")

    out = subprocess.run(["databricks", "bundle", "schema"], check=True, capture_output=True, text=True).stdout
    validator_cls = jsonschema.validators.extend(jsonschema.Draft202012Validator, {"pattern": pattern})
    validator = validator_cls(json.loads(out))
    for path in [REPO / "databricks.yml", *RESOURCES]:
        errors = [f"{list(e.absolute_path)}: {e.message}" for e in validator.iter_errors(load(path))]
        assert not errors, (path.name, errors[:5])
