"""Evaluation questions with ground truth taken from the generator, not from gold.

The simulator is deterministic per business date and writes a manifest of the exact counts and money
totals per city. The lakehouse already proves gold == manifest every day, so a Genie answer that
matches the manifest is correct end to end, and the evaluation does not trust the system under test
for its own answer key.
"""

from __future__ import annotations

import random
from datetime import date, timedelta

# metric -> (question template, unit for expected_facts)
METRIC_QUESTIONS: dict[str, tuple[str, str]] = {
    "orders_placed": ("How many orders were placed in {city} on {day}?", "orders"),
    "orders_cancelled": ("How many orders were cancelled in {city} on {day}?", "cancelled orders"),
    "late_deliveries": ("How many deliveries were late in {city} on {day}?", "late deliveries"),
    "cancelled_no_rider_available": (
        "How many orders in {city} were cancelled on {day} because no rider was available?",
        "orders",
    ),
    "gmv": ("What was the GMV in {city} on {day}?", "INR"),
    "refunds_amount": ("What was the total refund amount for {city} orders placed on {day}?", "INR"),
}


def _row(question: str, expectations: dict, tags: dict) -> dict:
    return {
        "inputs": {"request": {"input": [{"role": "user", "content": question}]}},
        "expectations": expectations,
        "tags": tags,
    }


def build_evalset(manifests_by_day: dict[date, list[dict]], *, per_metric: int = 3, seed: int = 7) -> list[dict]:
    """``manifests_by_day``: business date -> the generator's manifest rows (one per city)."""
    rng = random.Random(seed)
    days = sorted(manifests_by_day)
    if not days:
        raise ValueError("no manifests to build an evaluation set from")
    rows: list[dict] = []

    for metric, (template, unit) in METRIC_QUESTIONS.items():
        for _ in range(per_metric):
            d = rng.choice(days)
            m = rng.choice(manifests_by_day[d])
            value = m[metric]
            q = template.format(city=m["city"], day=d.isoformat())
            rows.append(
                _row(
                    q,
                    {"expected_value": value, "expected_facts": [f"The answer is {value} {unit}."]},
                    {"kind": "point_lookup", "metric": metric},
                )
            )

    # aggregation across cities
    for d in rng.sample(days, min(per_metric, len(days))):
        total = sum(m["orders_placed"] for m in manifests_by_day[d])
        rows.append(
            _row(
                f"How many orders were placed across all cities on {d.isoformat()}?",
                {"expected_value": total, "expected_facts": [f"{total} orders were placed in total."]},
                {"kind": "aggregate", "metric": "orders_placed"},
            )
        )

    # ranking
    for d in rng.sample(days, min(per_metric, len(days))):
        top = max(manifests_by_day[d], key=lambda m: m["gmv"])
        rows.append(
            _row(
                f"Which city had the highest GMV on {d.isoformat()}?",
                {"expected_text": top["city"], "expected_facts": [f"{top['city']} had the highest GMV."]},
                {"kind": "ranking", "metric": "gmv"},
            )
        )

    # a multi-day window
    if len(days) >= 7:
        end = days[-1]
        window = [d for d in days if d > end - timedelta(days=7)]
        city = rng.choice(manifests_by_day[end])["city"]
        total = sum(m["orders_delivered"] for d in window for m in manifests_by_day[d] if m["city"] == city)
        rows.append(
            _row(
                f"How many orders were delivered in {city} between {window[0].isoformat()} and {end.isoformat()}?",
                {"expected_value": total, "expected_facts": [f"{total} orders were delivered."]},
                {"kind": "window", "metric": "orders_delivered"},
            )
        )

    # out of scope: no numeric ground truth, judged by the guidelines/relevance scorers
    rows.append(
        _row(
            "What will the weather be in Mumbai next Friday?",
            {
                "expected_facts": ["The data cannot answer weather forecasts; the response says so."],
                "expect_refusal": True,
            },
            {"kind": "out_of_scope"},
        )
    )
    return rows


def manifests_from_simulator(start: date, end: date) -> dict[date, list[dict]]:
    """Regenerate the manifests for ``start..end`` with the configured seed (no files written)."""
    from fooddelivery.config import GeneratorConfig, Paths
    from fooddelivery.generator.seed import read_seed
    from fooddelivery.generator.simulator import Simulator
    from fooddelivery.seeding import ensure_seed

    paths, cfg = Paths.from_env(), GeneratorConfig.from_env()
    ensure_seed(paths, cfg)
    sim = Simulator(cfg, read_seed(paths.seed_file))
    out = {}
    d = start
    while d <= end:
        out[d] = sim.generate_day(d).manifests
        d += timedelta(days=1)
    return out
