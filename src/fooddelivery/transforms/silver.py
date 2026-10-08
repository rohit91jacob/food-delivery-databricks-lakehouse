"""Bronze -> silver: typing, derived columns and data-quality rules, one spec per entity.

The same specs drive both the Lakeflow pipeline (rules become ``expect_all*`` expectations and the
keys/sequence feed ``create_auto_cdc_flow``) and the local lakehouse, so production and the
offline verification share one definition.

Rule semantics:
  * ``drop``: the row is removed from silver and kept in ``quarantine_<entity>`` with the failed rule names
  * ``warn``: recorded in pipeline metrics and the row is kept
  * ``fail``: the pipeline update stops; reserved for "this should never happen" invariants
Every rule is written null-safe (``x IS NOT NULL AND ...``), so a NULL counts as a failure.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

ORDER_STATUSES = ("placed", "accepted", "rider_assigned", "preparing", "ready", "picked_up", "delivered", "cancelled")
PAYMENT_METHODS = ("upi", "card", "wallet", "cod")
PAYMENT_STATUSES = ("captured", "failed", "voided")


def _in(col: str, values: tuple[str, ...]) -> str:
    quoted = ", ".join(f"'{v}'" for v in values)
    return f"{col} IS NOT NULL AND {col} IN ({quoted})"


# ANSI mode is on (Spark 4 / Databricks serverless default), so parsing uses try_* variants:
# a malformed value becomes NULL and is caught by a null-safe drop rule instead of failing the update.
def parse_ts(col: str) -> Column:
    """ISO-8601 with offset (e.g. ``2026-09-01T12:30:00+05:30``) -> TIMESTAMP (an instant)."""
    return F.try_to_timestamp(F.col(col))


def parse_date(col: str) -> Column:
    return F.expr(f"try_cast({col} AS DATE)")


def local_hour(col: str) -> Column:
    """Wall-clock hour as emitted (IST), independent of the Spark session time zone."""
    return F.expr(f"try_cast(substring({col}, 12, 2) AS INT)")


def _with_lineage(df: DataFrame) -> DataFrame:
    return df.withColumn("business_date", parse_date("business_date"))


def _cdc_common(df: DataFrame) -> DataFrame:
    return _with_lineage(df).withColumn("updated_at", parse_ts("updated_at"))


@dataclass(frozen=True)
class SilverSpec:
    entity: str
    keys: tuple[str, ...]
    sequence_by: str
    typed: Callable[[DataFrame], DataFrame]
    scd_type: int = 1
    track_history: tuple[str, ...] | None = None
    is_cdc_feed: bool = False  # carries op = UPSERT | DELETE
    drop: dict[str, str] = field(default_factory=dict)
    warn: dict[str, str] = field(default_factory=dict)
    fail: dict[str, str] = field(default_factory=dict)
    source: str | None = None  # bronze table to read; defaults to bronze_<entity>
    comment: str = ""
    except_cols: tuple[str, ...] | None = None  # columns not carried into silver

    @property
    def bronze_table(self) -> str:
        return self.source or f"bronze_{self.entity}"

    @property
    def except_columns(self) -> tuple[str, ...]:
        if self.except_cols is not None:
            return self.except_cols
        return ("op", "_rescued_data") if self.is_cdc_feed else ("_rescued_data",)


# ----------------------------------------------------------------------------- typed views
def typed_restaurants(df: DataFrame) -> DataFrame:
    return _cdc_common(df)


def typed_menu_items(df: DataFrame) -> DataFrame:
    return _cdc_common(df)


def typed_customers(df: DataFrame) -> DataFrame:
    return _cdc_common(df).withColumn("signup_at", parse_ts("signup_at"))


def typed_riders(df: DataFrame) -> DataFrame:
    return _cdc_common(df).withColumn("joined_at", parse_ts("joined_at"))


def typed_promotions(df: DataFrame) -> DataFrame:
    return (
        _cdc_common(df)
        .withColumn("valid_from", parse_date("valid_from"))
        .withColumn("valid_to", parse_date("valid_to"))
    )


def typed_city_conditions(df: DataFrame) -> DataFrame:
    return (
        _with_lineage(df)
        .withColumn("hour_local", local_hour("hour_start"))
        .withColumn("hour_start", parse_ts("hour_start"))
    )


def typed_rider_shifts(df: DataFrame) -> DataFrame:
    df = (
        _with_lineage(df)
        .withColumn("shift_start", parse_ts("shift_start"))
        .withColumn("shift_end", parse_ts("shift_end"))
    )
    return df.withColumn("shift_minutes", (F.unix_timestamp("shift_end") - F.unix_timestamp("shift_start")) / 60.0)


_ITEMS_TOTAL = (
    "aggregate(items, CAST(0 AS DECIMAL(12,2)), (acc, x) -> CAST(acc + x.quantity * x.unit_price AS DECIMAL(12,2)))"
)


def typed_orders(df: DataFrame) -> DataFrame:
    return (
        _with_lineage(df)
        .withColumn("placed_hour", local_hour("placed_at"))
        .withColumn("placed_at", parse_ts("placed_at"))
        .withColumn("item_count", F.coalesce(F.size("items"), F.lit(0)))
        .withColumn("items_total", F.expr(_ITEMS_TOTAL))
    )


def typed_order_events(df: DataFrame) -> DataFrame:
    return _with_lineage(df).withColumn("event_at", parse_ts("event_at"))


def typed_payments(df: DataFrame) -> DataFrame:
    return _with_lineage(df).withColumn("processed_at", parse_ts("processed_at"))


def typed_refunds(df: DataFrame) -> DataFrame:
    return _with_lineage(df).withColumn("refunded_at", parse_ts("refunded_at"))


def typed_ratings(df: DataFrame) -> DataFrame:
    return _with_lineage(df).withColumn("rated_at", parse_ts("rated_at"))


def typed_rider_earnings(df: DataFrame) -> DataFrame:
    return _with_lineage(df).withColumn("earned_at", parse_ts("earned_at"))


def typed_rider_locations(df: DataFrame) -> DataFrame:
    return _with_lineage(df).withColumn("recorded_at", parse_ts("recorded_at"))


def typed_manifests(df: DataFrame) -> DataFrame:
    return _with_lineage(df)


ORDER_DROP_RULES = {
    "order_id_present": "order_id IS NOT NULL",
    "customer_present": "customer_id IS NOT NULL",
    "restaurant_present": "restaurant_id IS NOT NULL",
    "placed_at_parsed": "placed_at IS NOT NULL",
    "has_items": "item_count > 0",
    "total_non_negative": "total_amount IS NOT NULL AND total_amount >= 0",
    "subtotal_matches_items": "subtotal IS NOT NULL AND subtotal = items_total",
    "total_matches_components": (
        "total_amount IS NOT NULL AND total_amount = subtotal - discount + delivery_fee + platform_fee + tax + tip"
    ),
}


def valid_orders(df: DataFrame) -> DataFrame:
    """Typed orders that pass every drop rule (used to derive order lines from valid orders only)."""
    return df.where(all_rules_pass(ORDER_DROP_RULES))


def order_items(df: DataFrame) -> DataFrame:
    """Explode valid orders (raw bronze rows in, typed here) into one row per order line."""
    return (
        valid_orders(typed_orders(df))
        .select(
            "order_id",
            "business_date",
            "record_version",
            "placed_at",
            "restaurant_id",
            "city",
            F.explode("items").alias("line"),
            "_source_file",
            "_ingested_at",
        )
        .select(
            "order_id",
            F.col("line.line_no").alias("line_no"),
            F.col("line.item_id").alias("item_id"),
            F.col("line.item_name").alias("item_name"),
            F.col("line.quantity").alias("quantity"),
            F.col("line.unit_price").alias("unit_price"),
            (F.col("line.quantity") * F.col("line.unit_price")).cast("decimal(12,2)").alias("line_total"),
            "restaurant_id",
            "city",
            "placed_at",
            "business_date",
            "record_version",
            "_source_file",
            "_ingested_at",
        )
    )


SPECS: tuple[SilverSpec, ...] = (
    SilverSpec(
        "restaurants",
        ("restaurant_id",),
        "updated_at",
        typed_restaurants,
        scd_type=2,
        track_history=("price_for_two", "commission_rate", "name", "cuisines", "area"),
        is_cdc_feed=True,
        drop={
            "restaurant_id_present": "restaurant_id IS NOT NULL",
            "updated_at_parsed": "updated_at IS NOT NULL",
            "valid_op": _in("op", ("UPSERT", "DELETE")),
        },
        warn={
            "commission_in_range": "commission_rate IS NULL OR commission_rate BETWEEN 0.05 AND 0.40",
            "price_for_two_positive": "op = 'DELETE' OR price_for_two > 0",
        },
        comment="Restaurant master data, SCD type 2 on price_for_two / commission_rate / name / cuisines / area.",
    ),
    SilverSpec(
        "menu_items",
        ("item_id",),
        "updated_at",
        typed_menu_items,
        scd_type=2,
        track_history=("price", "name", "category", "is_veg"),
        is_cdc_feed=True,
        drop={
            "item_id_present": "item_id IS NOT NULL",
            "updated_at_parsed": "updated_at IS NOT NULL",
            "valid_op": _in("op", ("UPSERT", "DELETE")),
        },
        warn={"price_positive": "op = 'DELETE' OR price > 0"},
        comment="Menu items, SCD type 2 on price (point-in-time pricing for order lines).",
    ),
    SilverSpec(
        "customers",
        ("customer_id",),
        "updated_at",
        typed_customers,
        is_cdc_feed=True,
        drop={"customer_id_present": "customer_id IS NOT NULL", "updated_at_parsed": "updated_at IS NOT NULL"},
        warn={"known_segment": _in("segment", ("casual", "regular", "power"))},
        comment="Customers (current state, SCD type 1).",
    ),
    SilverSpec(
        "riders",
        ("rider_id",),
        "updated_at",
        typed_riders,
        is_cdc_feed=True,
        drop={"rider_id_present": "rider_id IS NOT NULL", "updated_at_parsed": "updated_at IS NOT NULL"},
        warn={"known_status": _in("status", ("active", "inactive"))},
        comment="Delivery riders (current state, SCD type 1).",
    ),
    SilverSpec(
        "promotions",
        ("promo_code",),
        "updated_at",
        typed_promotions,
        is_cdc_feed=True,
        drop={"promo_code_present": "promo_code IS NOT NULL"},
        warn={"known_funding": _in("funded_by", ("platform", "restaurant"))},
        comment="Promotion catalog (SCD type 1).",
    ),
    SilverSpec(
        "city_conditions",
        ("city", "hour_start"),
        "_ingested_at",
        typed_city_conditions,
        drop={"hour_parsed": "hour_start IS NOT NULL", "city_present": "city IS NOT NULL"},
        warn={"surge_in_range": "surge_multiplier BETWEEN 1.0 AND 2.5"},
        comment="Hourly weather, traffic and surge per city.",
    ),
    SilverSpec(
        "rider_shifts",
        ("shift_id",),
        "_ingested_at",
        typed_rider_shifts,
        drop={"shift_window_valid": "shift_start IS NOT NULL AND shift_end > shift_start"},
        comment="Planned rider shifts (log-in windows) per business date.",
    ),
    SilverSpec(
        "orders",
        ("order_id",),
        "_ingested_at",
        typed_orders,
        drop=ORDER_DROP_RULES,
        warn={
            "eta_plausible": "promised_eta_minutes BETWEEN 10 AND 120",
            "distance_plausible": "distance_km BETWEEN 0 AND 20",
            "known_payment_method": _in("payment_method", PAYMENT_METHODS),
        },
        fail={"business_date_present": "business_date IS NOT NULL"},
        comment="Order headers, de-duplicated on order_id; malformed orders go to quarantine_orders.",
    ),
    SilverSpec(
        "order_items",
        ("order_id", "line_no"),
        "_ingested_at",
        order_items,
        source="bronze_orders",
        except_cols=(),
        drop={
            "quantity_positive": "quantity IS NOT NULL AND quantity > 0",
            "unit_price_positive": "unit_price IS NOT NULL AND unit_price > 0",
        },
        comment="Order lines exploded from valid orders.",
    ),
    SilverSpec(
        "order_events",
        ("event_id",),
        "_ingested_at",
        typed_order_events,
        drop={
            "event_id_present": "event_id IS NOT NULL AND order_id IS NOT NULL",
            "event_at_parsed": "event_at IS NOT NULL",
            "known_status": _in("status", ORDER_STATUSES),
        },
        warn={"cancel_has_reason": "status <> 'cancelled' OR reason IS NOT NULL"},
        comment="Order lifecycle status events, de-duplicated on event_id.",
    ),
    SilverSpec(
        "payments",
        ("payment_id",),
        "_ingested_at",
        typed_payments,
        drop={
            "payment_id_present": "payment_id IS NOT NULL AND order_id IS NOT NULL",
            "amount_non_negative": "amount IS NOT NULL AND amount >= 0",
            "known_status": _in("status", PAYMENT_STATUSES),
        },
        comment="Payment attempts and captures.",
    ),
    SilverSpec(
        "refunds",
        ("refund_id",),
        "_ingested_at",
        typed_refunds,
        drop={
            "refund_id_present": "refund_id IS NOT NULL AND order_id IS NOT NULL",
            "amount_positive": "amount IS NOT NULL AND amount > 0",
        },
        warn={"known_funding": _in("funded_by", ("platform", "restaurant"))},
        comment="Refunds (cancellations, late deliveries, missing/damaged items).",
    ),
    SilverSpec(
        "ratings",
        ("order_id",),
        "_ingested_at",
        typed_ratings,
        drop={
            "order_id_present": "order_id IS NOT NULL",
            "ratings_in_range": "food_rating BETWEEN 1 AND 5 AND delivery_rating BETWEEN 1 AND 5",
        },
        comment="Post-delivery food and delivery ratings.",
    ),
    SilverSpec(
        "rider_earnings",
        ("earning_id",),
        "_ingested_at",
        typed_rider_earnings,
        drop={
            "earning_id_present": "earning_id IS NOT NULL AND rider_id IS NOT NULL",
            "total_non_negative": "total_pay IS NOT NULL AND total_pay >= 0",
        },
        warn={"components_add_up": "total_pay = base_pay + distance_pay + surge_pay + tip + cancellation_pay"},
        comment="Rider pay per order.",
    ),
    SilverSpec(
        "rider_locations",
        ("ping_id",),
        "_ingested_at",
        typed_rider_locations,
        drop={
            "ping_id_present": "ping_id IS NOT NULL AND rider_id IS NOT NULL",
            "within_india": "latitude BETWEEN 6 AND 37 AND longitude BETWEEN 68 AND 98",
            "recorded_at_parsed": "recorded_at IS NOT NULL",
        },
        warn={"speed_plausible": "speed_kmph BETWEEN 0 AND 90"},
        comment="Rider GPS pings while on an order.",
    ),
    SilverSpec(
        "manifests",
        ("business_date", "city"),
        "_ingested_at",
        typed_manifests,
        drop={"keys_present": "business_date IS NOT NULL AND city IS NOT NULL"},
        comment="Generator ground truth per business date and city (drives reconciliation checks).",
    ),
)

SPECS_BY_ENTITY = {s.entity: s for s in SPECS}


def all_rules_pass(rules: dict[str, str]) -> Column:
    cond = F.lit(True)
    for expr in rules.values():
        cond = cond & F.coalesce(F.expr(f"({expr})"), F.lit(False))
    return cond


def quarantine(df: DataFrame, rules: dict[str, str]) -> DataFrame:
    """Rows that fail at least one drop rule, annotated with the names of the failed rules."""
    failed = F.filter(
        F.array(*[F.when(~F.coalesce(F.expr(f"({e})"), F.lit(False)), F.lit(n)) for n, e in rules.items()]),
        lambda x: x.isNotNull(),
    )
    return (
        df.withColumn("_failed_rules", failed)
        .where(F.size("_failed_rules") > 0)
        .withColumn("_quarantined_at", F.current_timestamp())
    )
