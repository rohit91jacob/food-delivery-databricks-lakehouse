"""The Genie space over the gold marts, declared in code.

``build_serialized_space`` is a pure function: it returns the ``serialized_space`` JSON the Genie API
expects (version 2). The API rejects unsorted arrays and non-hex IDs, so every ID is derived from a
stable key (uuid5) and every collection is sorted before it is serialised. Deterministic IDs also
make ``ensure_space`` idempotent: re-running it updates the same space with the same content.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass

from fooddelivery.config import LakehouseTarget

log = logging.getLogger(__name__)

SPACE_TITLE = "Food Delivery Operations (gold)"
_NAMESPACE = uuid.UUID("6f1d3c52-6a7e-4c1b-9a59-6c3f0e3e2b10")

# table -> description. Grain first, because Genie uses it to choose GROUP BY keys.
GOLD_TABLES: dict[str, str] = {
    "fct_orders": "One row per order_id. Lifecycle timestamps, order_status (delivered/cancelled), cancellation "
    "reason and actor, money columns, commission at the SCD2 rate in force at placement, refunds, rider pay, "
    "ratings, SLA durations in minutes, is_late, eta_error_minutes, contribution_margin.",
    "fct_order_items": "One row per order_id, line_no. Item name, category, cuisine, veg flag, quantity, unit price "
    "and the menu price as of placement.",
    "gold_daily_kpis": "One row per business_date, city. The headline daily KPIs: orders placed/delivered/"
    "cancelled, cancellations by reason, late deliveries, GMV, AOV, fees, taxes, tips, commission, refunds, "
    "rider payout, contribution margin, on-time and cancellation rates. Reconciles exactly with the source.",
    "gold_delivery_sla_hourly": "One row per business_date, city, placed_hour, weather, traffic_level. On-time "
    "rate, p50/p90 delivery minutes, ETA error, average prep/travel/assignment minutes.",
    "gold_restaurant_daily": "One row per business_date, restaurant_id. Acceptance rate, prep time, food rating, "
    "food revenue, commission, restaurant-funded discounts.",
    "gold_rider_daily": "One row per business_date, rider_id. Shift and busy minutes, utilisation, deliveries, "
    "distance, earnings, tips, surge pay, delivery rating.",
    "gold_customer_cohorts": "One row per cohort_month, activity_month. Active customers, orders, GMV and "
    "retention rate by months since the first order.",
    "gold_customer_ltv": "One row per customer_id. First/last order, delivered and cancelled orders, lifetime GMV, "
    "contribution, refunds, AOV, tenure.",
    "gold_cancellations_daily": "One row per business_date, city, cancellation_reason, cancelled_by. Cancelled "
    "orders, GMV lost and average minutes to cancel.",
    "gold_refunds_daily": "One row per business_date (of the order), city, reason, funded_by, method. Refund count "
    "and amount.",
    "gold_menu_popularity_weekly": "One row per week_start (Monday), city, cuisine, item_name. Quantity, orders, "
    "revenue and rank within the city-week.",
    "gold_surge_promo_daily": "One row per business_date, city, surge_band, promo. Orders, AOV, platform and "
    "restaurant discount cost, delivery-fee revenue, delivery minutes, late rate.",
}

INSTRUCTIONS = """\
This space answers questions about a food-delivery marketplace operating in five Indian cities
(Bangalore, Mumbai, Delhi, Hyderabad, Pune). Money is in Indian rupees (INR).

* business_date is the order placement date in Asia/Kolkata. "Yesterday", "last week" etc. are relative to
  the latest business_date in gold_daily_kpis, not to today's date, because data arrives one day at a time.
* For daily or city-level totals (orders, GMV, cancellations, late deliveries, refunds, commission, margin)
  use gold_daily_kpis: it reconciles exactly with the source system. Use fct_orders only for order-level
  questions or breakdowns gold_daily_kpis does not have.
* GMV is the sum of total_amount over delivered orders. AOV = gmv / orders_delivered.
  on_time_rate = 1 - late_deliveries / orders_delivered. cancellation_rate = orders_cancelled / orders_placed.
* Rates are fractions between 0 and 1; show them as percentages with one decimal.
* When a question names a city, filter on city exactly as stored (e.g. 'Bangalore', not 'Bengaluru').
* Always return the numbers you computed in the result table; do not round money in SQL.
"""

SAMPLE_QUESTIONS = (
    "What was GMV by city on the latest business date?",
    "How did the on-time rate trend in Bangalore over the last 7 days?",
    "Which cancellation reasons cost the most GMV last week?",
    "Who were the top 10 riders by utilisation yesterday?",
    "What is the 3-month retention rate for each customer cohort?",
)


@dataclass(frozen=True)
class ExampleSql:
    question: str
    sql: str


def example_sqls(target: LakehouseTarget) -> list[ExampleSql]:
    kpis = _ident(target, "gold_daily_kpis")
    cancels = _ident(target, "gold_cancellations_daily")
    return [
        ExampleSql(
            "What was GMV by city on the latest business date?",
            f"SELECT city, gmv, orders_delivered, aov\nFROM {kpis}\n"
            f"WHERE business_date = (SELECT max(business_date) FROM {kpis})\nORDER BY gmv DESC",
        ),
        ExampleSql(
            "How many orders were placed in Mumbai on 2026-09-05?",
            f"SELECT orders_placed\nFROM {kpis}\nWHERE city = 'Mumbai' AND business_date = DATE'2026-09-05'",
        ),
        ExampleSql(
            "What was the on-time rate per city last week?",
            f"SELECT city, round(100 * (1 - sum(late_deliveries) / sum(orders_delivered)), 1) AS on_time_pct\n"
            f"FROM {kpis}\nWHERE business_date > (SELECT max(business_date) - INTERVAL 7 DAYS FROM {kpis})\n"
            "GROUP BY city\nORDER BY on_time_pct DESC",
        ),
        ExampleSql(
            "Which cancellation reasons cost the most GMV last week?",
            "SELECT cancellation_reason, cancelled_by, sum(cancelled_orders) AS cancelled_orders,\n"
            "       sum(gmv_lost) AS gmv_lost\n"
            f"FROM {cancels}\nWHERE business_date > (SELECT max(business_date) - INTERVAL 7 DAYS FROM {cancels})\n"
            "GROUP BY cancellation_reason, cancelled_by\nORDER BY gmv_lost DESC",
        ),
    ]


def _ident(target: LakehouseTarget, table: str) -> str:
    return f"{target.catalog}.{target.gold_schema}.{table}"


def _id(key: str) -> str:
    return uuid.uuid5(_NAMESPACE, key).hex  # 32 lowercase hex chars, no hyphens


def _lines(sql: str) -> list[str]:
    """The API stores SQL as an array of lines; keep the newlines so it round-trips verbatim."""
    lines = sql.splitlines(keepends=True)
    return lines or [sql]


def build_serialized_space(target: LakehouseTarget) -> dict:
    tables = sorted(
        ({"identifier": _ident(target, t), "description": [d]} for t, d in GOLD_TABLES.items()),
        key=lambda t: t["identifier"],
    )
    samples = sorted(({"id": _id(f"sample:{q}"), "question": [q]} for q in SAMPLE_QUESTIONS), key=lambda s: s["id"])
    examples = sorted(
        (
            {"id": _id(f"example:{e.question}"), "question": [e.question], "sql": _lines(e.sql)}
            for e in example_sqls(target)
        ),
        key=lambda e: e["id"],
    )
    return {
        "version": 2,
        "config": {"sample_questions": samples},
        "data_sources": {"tables": tables},
        "instructions": {
            # the API allows at most one text instruction
            "text_instructions": [{"id": _id("instructions"), "content": [INSTRUCTIONS]}],
            "example_question_sqls": examples,
        },
    }


def ensure_space(w, target: LakehouseTarget, warehouse_id: str, *, title: str = SPACE_TITLE) -> dict:
    """Create the space, or update the one with the same title. Returns ``{"space_id", "action"}``."""
    serialized = json.dumps(build_serialized_space(target))
    description = f"Gold marts of the food-delivery lakehouse ({target.catalog}.{target.gold_schema})."
    existing = _find_space(w, title)
    if existing:
        w.genie.update_space(
            existing, serialized_space=serialized, title=title, description=description, warehouse_id=warehouse_id
        )
        log.info("updated Genie space %s", existing)
        return {"space_id": existing, "action": "updated"}
    space = w.genie.create_space(warehouse_id, serialized, title=title, description=description)
    log.info("created Genie space %s", space.space_id)
    return {"space_id": space.space_id, "action": "created"}


def _find_space(w, title: str) -> str | None:
    token = None
    while True:
        page = w.genie.list_spaces(page_token=token)
        for s in page.spaces or []:
            if s.title == title:
                return s.space_id
        token = page.next_page_token
        if not token:
            return None
