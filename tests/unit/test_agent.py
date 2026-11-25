"""Genie agent: space serialisation and the evaluation set (no MLflow needed)."""

from __future__ import annotations

import json
import re
from datetime import date
from types import SimpleNamespace as NS

from fooddelivery.config import LakehouseTarget

HEX32 = re.compile(r"^[0-9a-f]{32}$")


# --- space ------------------------------------------------------------------


def test_serialized_space_follows_the_api_rules():
    from fooddelivery.agent.space import GOLD_TABLES, build_serialized_space

    target = LakehouseTarget(catalog="workspace", schema="dev_me_fooddelivery")
    space = build_serialized_space(target)
    json.dumps(space)  # serialisable

    assert space["version"] == 2
    tables = [t["identifier"] for t in space["data_sources"]["tables"]]
    assert tables == sorted(tables)
    assert len(tables) == len(GOLD_TABLES)
    assert all(t.startswith("workspace.dev_me_fooddelivery_gold.") for t in tables)

    samples = space["config"]["sample_questions"]
    ins = space["instructions"]
    assert len(ins["text_instructions"]) == 1  # the API allows at most one
    for coll in (samples, ins["example_question_sqls"]):
        ids = [x["id"] for x in coll]
        assert ids == sorted(ids)
        assert all(HEX32.match(i) for i in ids)
    all_ids = [x["id"] for x in samples] + [x["id"] for x in ins["example_question_sqls"]]
    all_ids.append(ins["text_instructions"][0]["id"])
    assert len(set(all_ids)) == len(all_ids)
    for e in ins["example_question_sqls"]:
        assert "workspace.dev_me_fooddelivery_gold." in "".join(e["sql"])


def test_serialized_space_is_deterministic():
    from fooddelivery.agent.space import build_serialized_space

    t = LakehouseTarget()
    assert build_serialized_space(t) == build_serialized_space(t)


def test_ensure_space_updates_existing_by_title():
    from fooddelivery.agent.space import SPACE_TITLE, ensure_space

    calls = {}

    class Genie:
        def list_spaces(self, page_token=None):
            if page_token is None:
                return NS(spaces=[NS(title="other", space_id="a")], next_page_token="p2")
            return NS(spaces=[NS(title=SPACE_TITLE, space_id="b")], next_page_token=None)

        def update_space(self, space_id, **kw):
            calls["update"] = (space_id, kw)

        def create_space(self, *a, **kw):
            raise AssertionError("must not create when the title exists")

    out = ensure_space(NS(genie=Genie()), LakehouseTarget(), "wh1")
    assert out == {"space_id": "b", "action": "updated"}
    assert calls["update"][1]["warehouse_id"] == "wh1"


# --- eval set ---------------------------------------------------------------


def _manifests():
    cities = ("Bangalore", "Mumbai", "Delhi")
    out = {}
    for i, d in enumerate(date(2026, 9, n) for n in range(1, 9)):
        out[d] = [
            {
                "city": c,
                "orders_placed": 100 + 10 * i + j,
                "orders_delivered": 90 + i + j,
                "orders_cancelled": 5 + j,
                "late_deliveries": 3 + j,
                "cancelled_no_rider_available": 1 + j,
                "gmv": 1000.5 + 100 * j + i,
                "refunds_amount": 12.25 + j,
            }
            for j, c in enumerate(cities)
        ]
    return out


def test_evalset_is_deterministic_and_grounded():
    from fooddelivery.agent.evalset import METRIC_QUESTIONS, build_evalset

    rows = build_evalset(_manifests(), per_metric=2)
    assert rows == build_evalset(_manifests(), per_metric=2)
    assert len(rows) == 2 * len(METRIC_QUESTIONS) + 2 + 2 + 1 + 1
    for r in rows:
        assert r["inputs"]["request"]["input"][0]["role"] == "user"
        assert r["expectations"]["expected_facts"]
    ranking = [r for r in rows if r["tags"]["kind"] == "ranking"]
    assert all(r["expectations"]["expected_text"] == "Delhi" for r in ranking)  # j=2 has the highest gmv
    totals = [r for r in rows if r["tags"]["kind"] == "aggregate"]
    m = _manifests()
    for r in totals:
        d = date.fromisoformat(re.search(r"\d{4}-\d{2}-\d{2}", r["inputs"]["request"]["input"][0]["content"])[0])
        assert r["expectations"]["expected_value"] == sum(x["orders_placed"] for x in m[d])
