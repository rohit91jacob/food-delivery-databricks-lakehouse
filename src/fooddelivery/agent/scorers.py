"""Scorers for the Genie agent.

Deterministic checks first (cheap, exact), LLM judges only where semantics matter:

| Scorer | Needs | Used in |
|---|---|---|
| ``numeric_match`` | ``expectations.expected_value`` / ``expected_text`` (generator ground truth) | evaluation |
| ``genie_answered`` | outputs | evaluation + monitoring |
| ``sql_generated`` | trace (``genie.ask`` span) | evaluation + monitoring |
| ``genie_latency_s`` | trace | evaluation + monitoring |
| ``Correctness`` | ``expectations.expected_facts`` | evaluation |
| ``RelevanceToQuery``, ``Safety``, ``Guidelines`` | inputs + outputs | evaluation + monitoring |

Registered (monitoring) scorers are serialised by source, so every custom scorer here is
self-contained: imports live inside the function and it calls no module-level helpers.
"""

from __future__ import annotations

from mlflow.entities import Feedback, Trace
from mlflow.genai.scorers import Guidelines, RelevanceToQuery, Safety, scorer

LATENCY_BUDGET_S = 60.0

ANSWER_GUIDELINES = [
    "If the response contains figures, it says which city and which date or period they refer to.",
    "If the question cannot be answered from the data, the response says so plainly instead of guessing.",
]


@scorer
def numeric_match(outputs, expectations) -> Feedback:
    """The answer contains the ground-truth value (exact for counts, to the paisa for money)."""
    import re

    def answer_text(o) -> str:
        if isinstance(o, str):
            return o
        parts = []
        for item in (o or {}).get("output", []) if isinstance(o, dict) else []:
            for c in item.get("content", []) or []:
                if isinstance(c, dict) and c.get("text"):
                    parts.append(c["text"])
        return "\n".join(parts)

    text = answer_text(outputs)
    expectations = expectations or {}
    if "expected_text" in expectations:
        want = str(expectations["expected_text"])
        ok = want.lower() in text.lower()
        return Feedback(value=ok, rationale=f"expected '{want}' {'found' if ok else 'not found'} in the answer")
    if "expected_value" not in expectations:
        return None  # not applicable (e.g. out-of-scope questions); MLflow records no assessment

    want = float(expectations["expected_value"])
    tol = float(expectations.get("tolerance", 0.005))
    found = [float(n.replace(",", "")) for n in re.findall(r"-?\d[\d,]*(?:\.\d+)?", text)]
    # rates may be shown as percentages
    ok = any(abs(n - want) <= tol or (abs(want) <= 1 and abs(n - 100 * want) <= 0.05) for n in found)
    return Feedback(
        value=ok,
        rationale=f"expected {want:g}; {'found' if ok else 'not found'} among {len(found)} numbers in the answer",
    )


@scorer
def genie_answered(outputs, expectations=None) -> Feedback:
    """Genie completed the message with a result table (or, for out-of-scope questions, declined)."""
    custom = (outputs.get("custom_outputs") or {}) if isinstance(outputs, dict) else {}
    status = custom.get("status")
    rows = custom.get("row_count")
    answered = status == "COMPLETED" and rows is not None
    if (expectations or {}).get("expect_refusal"):
        return Feedback(value=not answered, rationale=f"out of scope: expected no result table; row_count={rows}")
    return Feedback(value=answered, rationale=f"status={status}, row_count={rows}")


@scorer
def sql_generated(trace: Trace, expectations=None) -> Feedback:
    """The genie.ask span produced SQL (or, for out-of-scope questions, did not)."""
    spans = trace.search_spans(name="genie.ask") if trace else []
    if not spans:
        return Feedback(value=False, rationale="no genie.ask span in the trace")
    sql = (spans[0].outputs or {}).get("sql")
    if (expectations or {}).get("expect_refusal"):
        return Feedback(value=not sql, rationale="out of scope: expected no SQL" + ("" if not sql else ", got SQL"))
    return Feedback(value=bool(sql), rationale="SQL generated" if sql else "Genie produced no SQL")


@scorer(aggregations=["mean", "p90", "max"])
def genie_latency_s(trace: Trace) -> Feedback:
    """End-to-end latency in seconds (budget: 60 s)."""
    ms = trace.info.execution_duration if trace else None
    if ms is None:
        return Feedback(value=None, error="trace has no duration")
    return Feedback(value=round(ms / 1000, 2), rationale=f"{ms / 1000:.1f}s against a 60s budget")


def judges() -> list:
    return [
        RelevanceToQuery(),
        Safety(),
        Guidelines(name="answer_guidelines", guidelines=ANSWER_GUIDELINES),
    ]


def evaluation_scorers() -> list:
    from mlflow.genai.scorers import Correctness

    return [numeric_match, genie_answered, sql_generated, genie_latency_s, Correctness(), *judges()]


# name -> (scorer, sample rate). Deterministic checks see every trace; LLM judges a sample.
def monitoring_scorers() -> dict[str, tuple[object, float]]:
    return {
        "genie_answered": (genie_answered, 1.0),
        "sql_generated": (sql_generated, 1.0),
        "genie_latency_s": (genie_latency_s, 1.0),
        "safety": (Safety(), 1.0),
        "relevance_to_query": (RelevanceToQuery(), 0.5),
        "answer_guidelines": (Guidelines(name="answer_guidelines", guidelines=ANSWER_GUIDELINES), 0.5),
    }
