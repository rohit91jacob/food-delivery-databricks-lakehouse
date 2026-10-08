"""Silver -> gold: the order fact, point-in-time joins and business marts.

Conventions:
  * ``business_date`` (the order placement date in Asia/Kolkata) is the partitioning grain everywhere,
    so marts reconcile 1:1 with the generator manifest for the same date.
  * Durations come from whole-second timestamps via ``unix_timestamp`` and do not depend on the
    session time zone.
  * Money stays DECIMAL end to end. Commission is recomputed from the SCD2 rate in force when the
    order was placed (a point-in-time join), never from the restaurant's current rate.
Metric definitions are documented in docs/metrics.md.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from fooddelivery.transforms.silver import ORDER_STATUSES

CANCEL_REASONS = (
    "payment_failed",
    "restaurant_rejected",
    "customer_changed_mind",
    "customer_delay",
    "no_rider_available",
)


def minutes_between(start: str | Column, end: str | Column) -> Column:
    s = F.col(start) if isinstance(start, str) else start
    e = F.col(end) if isinstance(end, str) else end
    return ((F.unix_timestamp(e) - F.unix_timestamp(s)) / 60.0).cast("double")


def as_of(fact: DataFrame, dim: DataFrame, key: str, fact_ts: str, prefix: str) -> DataFrame:
    """Left join each fact row to the SCD2 version that was current at ``fact_ts``."""
    d = dim.select(*[F.col(c).alias(f"{prefix}{c}") for c in dim.columns])
    cond = (
        (fact[key] == d[f"{prefix}{key}"])
        & (fact[fact_ts] >= d[f"{prefix}__START_AT"])
        & (d[f"{prefix}__END_AT"].isNull() | (fact[fact_ts] < d[f"{prefix}__END_AT"]))
    )
    return fact.join(d, cond, "left")


def order_timeline(events: DataFrame) -> DataFrame:
    def at(status: str) -> Column:
        # the order header already carries placed_at; the event copy is kept apart for a consistency check
        name = "placed_event_at" if status == "placed" else f"{status}_at"
        return F.min(F.when(F.col("status") == status, F.col("event_at"))).alias(name)

    return events.groupBy("order_id").agg(
        *[at(s) for s in ORDER_STATUSES],
        F.max(F.when(F.col("status") == "cancelled", F.col("reason"))).alias("cancellation_reason"),
        F.max(F.when(F.col("status") == "cancelled", F.col("actor"))).alias("cancelled_by"),
        F.max(F.when(F.col("status") == "rider_assigned", F.col("rider_id"))).alias("rider_id"),
        F.count("*").alias("event_count"),
    )


def fct_orders(
    orders: DataFrame,
    events: DataFrame,
    payments: DataFrame,
    refunds: DataFrame,
    ratings: DataFrame,
    restaurants: DataFrame,
    promotions: DataFrame,
    earnings: DataFrame,
    pings: DataFrame,
) -> DataFrame:
    tl = order_timeline(events)
    pay = payments.groupBy("order_id").agg(
        F.max_by("status", "processed_at").alias("payment_status"),
        F.count("*").alias("payment_count"),
        F.sum(F.when(F.col("status") == "captured", F.col("amount")).otherwise(F.lit(0)))
        .cast("decimal(12,2)")
        .alias("amount_captured"),
    )
    ref = refunds.groupBy("order_id").agg(
        F.count("*").alias("refund_count"),
        F.sum("amount").cast("decimal(12,2)").alias("refund_amount"),
        F.sum(F.when(F.col("funded_by") == "platform", F.col("amount")).otherwise(F.lit(0)))
        .cast("decimal(12,2)")
        .alias("platform_refund_amount"),
    )
    rat = ratings.select("order_id", "food_rating", "delivery_rating")
    pay_out = earnings.groupBy("order_id").agg(F.sum("total_pay").cast("decimal(12,2)").alias("rider_pay"))
    ping_counts = pings.groupBy("order_id").agg(F.count("*").alias("gps_ping_count"))
    promo = promotions.select("promo_code", F.col("funded_by").alias("promo_funded_by"))

    o = as_of(orders, restaurants, "restaurant_id", "placed_at", "r_")
    o = (
        o.join(tl, "order_id", "left")
        .join(pay, "order_id", "left")
        .join(ref, "order_id", "left")
        .join(rat, "order_id", "left")
        .join(pay_out, "order_id", "left")
        .join(ping_counts, "order_id", "left")
        .join(promo, "promo_code", "left")
    )

    zero = F.lit(0).cast("decimal(12,2)")
    restaurant_funded = F.when(F.col("promo_funded_by") == "restaurant", F.col("discount")).otherwise(zero)
    platform_funded = F.when(F.col("promo_funded_by") == "restaurant", zero).otherwise(F.col("discount"))
    status = (
        F.when(F.col("delivered_at").isNotNull(), "delivered")
        .when(F.col("cancelled_at").isNotNull(), "cancelled")
        .otherwise("in_progress")
    )
    delivery_minutes = minutes_between("placed_at", "delivered_at")
    commission = F.round(F.col("r_commission_rate") * (F.col("subtotal") - restaurant_funded), 2)

    return o.select(
        "order_id",
        "business_date",
        "city",
        "customer_id",
        "restaurant_id",
        F.col("r_name").alias("restaurant_name"),
        F.col("r_area").alias("restaurant_area"),
        F.element_at(F.col("r_cuisines"), 1).alias("primary_cuisine"),
        F.col("r_commission_rate").alias("commission_rate"),
        "rider_id",
        "delivery_area",
        "placed_at",
        "placed_hour",
        "accepted_at",
        "rider_assigned_at",
        "preparing_at",
        "ready_at",
        "picked_up_at",
        "delivered_at",
        "cancelled_at",
        status.alias("order_status"),
        "cancellation_reason",
        "cancelled_by",
        "item_count",
        "subtotal",
        "discount",
        "promo_code",
        "promo_funded_by",
        restaurant_funded.cast("decimal(12,2)").alias("restaurant_funded_discount"),
        platform_funded.cast("decimal(12,2)").alias("platform_funded_discount"),
        "delivery_fee",
        "platform_fee",
        "tax",
        "tip",
        "total_amount",
        "payment_method",
        "payment_status",
        F.coalesce("payment_count", F.lit(0)).alias("payment_count"),
        F.coalesce("amount_captured", zero).alias("amount_captured"),
        commission.cast("decimal(12,2)").alias("commission"),
        F.coalesce("refund_count", F.lit(0)).alias("refund_count"),
        F.coalesce("refund_amount", zero).alias("refund_amount"),
        F.coalesce("platform_refund_amount", zero).alias("platform_refund_amount"),
        F.coalesce("rider_pay", zero).alias("rider_pay"),
        "food_rating",
        "delivery_rating",
        "is_member",
        "is_new_customer",
        "distance_km",
        "promised_eta_minutes",
        "surge_multiplier",
        F.coalesce("event_count", F.lit(0)).alias("event_count"),
        F.coalesce("gps_ping_count", F.lit(0)).alias("gps_ping_count"),
        minutes_between("placed_at", "accepted_at").alias("accept_minutes"),
        minutes_between("accepted_at", "rider_assigned_at").alias("assignment_minutes"),
        minutes_between("preparing_at", "ready_at").alias("prep_minutes"),
        minutes_between("ready_at", "picked_up_at").alias("pickup_wait_minutes"),
        minutes_between("picked_up_at", "delivered_at").alias("travel_minutes"),
        delivery_minutes.alias("delivery_minutes"),
        (delivery_minutes - F.col("promised_eta_minutes")).alias("eta_error_minutes"),
        (F.col("delivered_at").isNotNull() & (delivery_minutes > F.col("promised_eta_minutes"))).alias("is_late"),
    ).withColumn(
        # what the platform keeps from a delivered order (refunds are attributed to the order's date)
        "contribution_margin",
        F.when(
            F.col("order_status") == "delivered",
            F.col("commission")
            + F.col("delivery_fee")
            + F.col("platform_fee")
            - F.col("platform_funded_discount")
            - F.col("platform_refund_amount")
            - F.col("rider_pay"),
        )
        .otherwise(-F.col("platform_refund_amount") - F.col("rider_pay"))
        .cast("decimal(12,2)"),
    )


def fct_order_items(items: DataFrame, menu_items: DataFrame, restaurants: DataFrame) -> DataFrame:
    i = as_of(items, menu_items, "item_id", "placed_at", "m_")
    r = restaurants.where(F.col("__END_AT").isNull()).select(
        "restaurant_id", F.element_at("cuisines", 1).alias("restaurant_primary_cuisine")
    )
    return i.join(r, "restaurant_id", "left").select(
        "order_id",
        "line_no",
        "business_date",
        "city",
        "restaurant_id",
        "item_id",
        "item_name",
        F.col("m_category").alias("category"),
        F.col("m_cuisine").alias("cuisine"),
        F.col("m_is_veg").alias("is_veg"),
        "quantity",
        "unit_price",
        "line_total",
        F.col("m_price").alias("menu_price_at_order"),
        (F.col("m_price") == F.col("unit_price")).alias("price_matches_menu"),
        "placed_at",
    )


def _count_if(cond: Column) -> Column:
    return F.sum(F.when(cond, 1).otherwise(0)).cast("bigint")


def _sum_if(cond: Column, col: str) -> Column:
    return F.sum(F.when(cond, F.col(col)).otherwise(F.lit(0))).cast("decimal(18,2)")


def daily_kpis(fct: DataFrame) -> DataFrame:
    delivered = F.col("order_status") == "delivered"
    aggs = [
        F.count("*").cast("bigint").alias("orders_placed"),
        _count_if(delivered).alias("orders_delivered"),
        _count_if(F.col("order_status") == "cancelled").alias("orders_cancelled"),
        *[_count_if(F.col("cancellation_reason") == r).alias(f"cancelled_{r}") for r in CANCEL_REASONS],
        _count_if(delivered & F.col("is_late")).alias("late_deliveries"),
        _count_if(F.col("is_new_customer")).alias("first_orders"),
        F.sum("item_count").cast("bigint").alias("order_items"),
        F.sum("event_count").cast("bigint").alias("order_events"),
        F.sum("payment_count").cast("bigint").alias("payments"),
        F.sum("gps_ping_count").cast("bigint").alias("gps_pings"),
        F.count("food_rating").cast("bigint").alias("ratings_count"),
        F.sum("refund_count").cast("bigint").alias("refunds_count"),
        F.sum("refund_amount").cast("decimal(18,2)").alias("refunds_amount"),
        _sum_if(delivered, "total_amount").alias("gmv"),
        _sum_if(delivered, "subtotal").alias("food_subtotal"),
        _sum_if(delivered, "discount").alias("discounts"),
        _sum_if(delivered, "delivery_fee").alias("delivery_fees"),
        _sum_if(delivered, "platform_fee").alias("platform_fees"),
        _sum_if(delivered, "tax").alias("taxes"),
        _sum_if(delivered, "tip").alias("tips"),
        _sum_if(delivered, "commission").alias("commission"),
        F.sum("rider_pay").cast("decimal(18,2)").alias("rider_payout"),
        F.sum("contribution_margin").cast("decimal(18,2)").alias("contribution_margin"),
        F.avg(F.when(delivered, F.col("delivery_minutes"))).alias("avg_delivery_minutes"),
    ]
    out = fct.groupBy("business_date", "city").agg(*aggs)
    return (
        out.withColumn("aov", F.round(F.col("gmv") / F.nullif(F.col("orders_delivered"), F.lit(0)), 2))
        .withColumn("cancellation_rate", F.round(F.col("orders_cancelled") / F.col("orders_placed"), 4))
        .withColumn(
            "on_time_rate", F.round(1 - F.col("late_deliveries") / F.nullif(F.col("orders_delivered"), F.lit(0)), 4)
        )
    )


def delivery_sla_hourly(fct: DataFrame, conditions: DataFrame) -> DataFrame:
    c = conditions.select(
        "city",
        "business_date",
        F.col("hour_local").alias("placed_hour"),
        "weather",
        "traffic_level",
        F.col("surge_multiplier").alias("hour_surge"),
    )
    d = fct.where(F.col("order_status") == "delivered").join(c, ["city", "business_date", "placed_hour"], "left")
    return d.groupBy("business_date", "city", "placed_hour", "weather", "traffic_level").agg(
        F.count("*").alias("delivered_orders"),
        _count_if(F.col("is_late")).alias("late_orders"),
        F.round(1 - F.avg(F.col("is_late").cast("double")), 4).alias("on_time_rate"),
        F.round(F.percentile_approx("delivery_minutes", 0.5), 2).alias("p50_delivery_minutes"),
        F.round(F.percentile_approx("delivery_minutes", 0.9), 2).alias("p90_delivery_minutes"),
        F.round(F.avg(F.abs("eta_error_minutes")), 2).alias("eta_mae_minutes"),
        F.round(F.avg("eta_error_minutes"), 2).alias("eta_bias_minutes"),
        F.round(F.avg("prep_minutes"), 2).alias("avg_prep_minutes"),
        F.round(F.avg("travel_minutes"), 2).alias("avg_travel_minutes"),
        F.round(F.avg("assignment_minutes"), 2).alias("avg_assignment_minutes"),
    )


def restaurant_daily(fct: DataFrame) -> DataFrame:
    delivered = F.col("order_status") == "delivered"
    return fct.groupBy("business_date", "city", "restaurant_id", "restaurant_name", "primary_cuisine").agg(
        F.count("*").alias("orders"),
        _count_if(delivered).alias("delivered_orders"),
        _count_if(F.col("cancellation_reason") == "restaurant_rejected").alias("rejected_orders"),
        F.round(1 - _count_if(F.col("cancellation_reason") == "restaurant_rejected") / F.count("*"), 4).alias(
            "acceptance_rate"
        ),
        F.round(F.avg("accept_minutes"), 2).alias("avg_accept_minutes"),
        F.round(F.avg("prep_minutes"), 2).alias("avg_prep_minutes"),
        F.round(F.percentile_approx("prep_minutes", 0.9), 2).alias("p90_prep_minutes"),
        F.round(F.avg("food_rating"), 2).alias("avg_food_rating"),
        _sum_if(delivered, "subtotal").alias("food_revenue"),
        _sum_if(delivered, "commission").alias("commission"),
        F.sum("restaurant_funded_discount").cast("decimal(18,2)").alias("restaurant_funded_discounts"),
    )


def rider_daily(fct: DataFrame, shifts: DataFrame, earnings: DataFrame, pings: DataFrame) -> DataFrame:
    shift_agg = shifts.groupBy("business_date", "rider_id", "city").agg(
        F.count("*").alias("shifts"), F.round(F.sum("shift_minutes"), 1).alias("shift_minutes")
    )
    busy_end = F.coalesce(F.col("delivered_at"), F.col("cancelled_at"))
    work = (
        fct.where(F.col("rider_id").isNotNull())
        .groupBy("business_date", "rider_id")
        .agg(
            _count_if(F.col("order_status") == "delivered").alias("deliveries"),
            _count_if(F.col("order_status") == "cancelled").alias("cancelled_after_assignment"),
            F.round(F.sum(minutes_between(F.col("rider_assigned_at"), busy_end)), 1).alias("busy_minutes"),
            F.round(F.sum(F.when(F.col("order_status") == "delivered", F.col("distance_km"))), 2).alias(
                "delivered_distance_km"
            ),
            F.round(F.avg("delivery_rating"), 2).alias("avg_delivery_rating"),
        )
    )
    pay = earnings.groupBy("business_date", "rider_id").agg(
        F.sum("total_pay").cast("decimal(18,2)").alias("earnings"),
        F.sum("tip").cast("decimal(18,2)").alias("tips"),
        F.sum("surge_pay").cast("decimal(18,2)").alias("surge_pay"),
    )
    w = Window.partitionBy("order_id").orderBy("recorded_at")
    hops = (
        pings.select("business_date", "rider_id", "order_id", "recorded_at", "latitude", "longitude")
        .withColumn("prev_lat", F.lag("latitude").over(w))
        .withColumn("prev_lon", F.lag("longitude").over(w))
        .where(F.col("prev_lat").isNotNull())
        .withColumn("hop_km", haversine_km("prev_lat", "prev_lon", "latitude", "longitude"))
    )
    gps = hops.groupBy("business_date", "rider_id").agg(F.round(F.sum("hop_km"), 2).alias("gps_distance_km"))
    out = (
        shift_agg.join(work, ["business_date", "rider_id"], "full")
        .join(pay, ["business_date", "rider_id"], "full")
        .join(gps, ["business_date", "rider_id"], "left")
    )
    return out.withColumn("utilisation", F.round(F.col("busy_minutes") / F.nullif(F.col("shift_minutes"), F.lit(0)), 4))


def haversine_km(lat1: str, lon1: str, lat2: str, lon2: str) -> Column:
    p1, p2 = F.radians(F.col(lat1)), F.radians(F.col(lat2))
    dp, dl = p2 - p1, F.radians(F.col(lon2) - F.col(lon1))
    a = F.pow(F.sin(dp / 2), 2) + F.cos(p1) * F.cos(p2) * F.pow(F.sin(dl / 2), 2)
    return 2 * 6371.0088 * F.asin(F.least(F.lit(1.0), F.sqrt(a)))


def _month(col: str) -> Column:
    return F.trunc(F.col(col), "month")


def customer_cohorts(fct: DataFrame) -> DataFrame:
    delivered = fct.where(F.col("order_status") == "delivered")
    first = delivered.groupBy("customer_id").agg(F.min("business_date").alias("first_order_date"))
    d = (
        delivered.join(first, "customer_id")
        .withColumn("cohort_month", _month("first_order_date"))
        .withColumn("activity_month", _month("business_date"))
    )
    sizes = d.groupBy("cohort_month").agg(F.countDistinct("customer_id").alias("cohort_size"))
    return (
        d.groupBy("cohort_month", "activity_month")
        .agg(
            F.countDistinct("customer_id").alias("active_customers"),
            F.count("*").alias("orders"),
            F.sum("total_amount").cast("decimal(18,2)").alias("gmv"),
        )
        .join(sizes, "cohort_month")
        .withColumn("months_since_first_order", F.round(F.months_between("activity_month", "cohort_month")).cast("int"))
        .withColumn("retention_rate", F.round(F.col("active_customers") / F.col("cohort_size"), 4))
    )


def customer_ltv(fct: DataFrame, customers: DataFrame) -> DataFrame:
    delivered = F.col("order_status") == "delivered"
    agg = fct.groupBy("customer_id").agg(
        F.min(F.when(delivered, F.col("business_date"))).alias("first_order_date"),
        F.max(F.when(delivered, F.col("business_date"))).alias("last_order_date"),
        _count_if(delivered).alias("delivered_orders"),
        _count_if(F.col("order_status") == "cancelled").alias("cancelled_orders"),
        _sum_if(delivered, "total_amount").alias("lifetime_gmv"),
        F.sum("contribution_margin").cast("decimal(18,2)").alias("lifetime_contribution"),
        F.sum("refund_amount").cast("decimal(18,2)").alias("lifetime_refunds"),
    )
    c = customers.select("customer_id", "city", "segment", "is_member", "signup_at")
    return (
        agg.join(c, "customer_id", "left")
        .withColumn(
            "avg_order_value", F.round(F.col("lifetime_gmv") / F.nullif(F.col("delivered_orders"), F.lit(0)), 2)
        )
        .withColumn("tenure_days", F.datediff("last_order_date", "first_order_date"))
    )


def cancellations_daily(fct: DataFrame) -> DataFrame:
    return (
        fct.where(F.col("order_status") == "cancelled")
        .groupBy("business_date", "city", "cancellation_reason", "cancelled_by")
        .agg(
            F.count("*").alias("cancelled_orders"),
            F.sum("total_amount").cast("decimal(18,2)").alias("gmv_lost"),
            F.round(F.avg(minutes_between("placed_at", "cancelled_at")), 2).alias("avg_minutes_to_cancel"),
        )
    )


def refunds_daily(refunds: DataFrame, orders: DataFrame) -> DataFrame:
    o = orders.select("order_id", "city", F.col("business_date").alias("order_business_date"))
    return (
        refunds.join(o, "order_id", "left")
        .groupBy(F.col("order_business_date").alias("business_date"), "city", "reason", "funded_by", "method")
        .agg(F.count("*").alias("refunds"), F.sum("amount").cast("decimal(18,2)").alias("refund_amount"))
    )


def _week_start(col: str) -> Column:
    return F.date_sub(F.col(col), (F.dayofweek(F.col(col)) + 5) % 7)


def menu_popularity_weekly(items: DataFrame, fct: DataFrame) -> DataFrame:
    delivered = fct.where(F.col("order_status") == "delivered").select("order_id")
    agg = (
        items.join(delivered, "order_id")
        .withColumn("week_start", _week_start("business_date"))
        .groupBy("week_start", "city", "cuisine", "item_name")
        .agg(
            F.sum("quantity").alias("quantity"),
            F.countDistinct("order_id").alias("orders"),
            F.sum("line_total").cast("decimal(18,2)").alias("revenue"),
        )
    )
    w = Window.partitionBy("week_start", "city").orderBy(F.desc("quantity"), F.asc("item_name"))
    return agg.withColumn("rank_in_city_week", F.row_number().over(w))


def surge_promo_daily(fct: DataFrame) -> DataFrame:
    band = (
        F.when(F.col("surge_multiplier") <= 1.0, "1.00 (none)")
        .when(F.col("surge_multiplier") <= 1.25, "1.05-1.25")
        .when(F.col("surge_multiplier") <= 1.5, "1.30-1.50")
        .otherwise(">1.50")
    )
    delivered = F.col("order_status") == "delivered"
    return (
        fct.withColumn("surge_band", band)
        .withColumn("promo", F.coalesce("promo_code", F.lit("NONE")))
        .groupBy("business_date", "city", "surge_band", "promo")
        .agg(
            F.count("*").alias("orders"),
            _count_if(delivered).alias("delivered_orders"),
            F.round(F.avg(F.when(delivered, F.col("total_amount"))), 2).alias("aov"),
            _sum_if(delivered, "platform_funded_discount").alias("platform_discount_cost"),
            _sum_if(delivered, "restaurant_funded_discount").alias("restaurant_discount_cost"),
            _sum_if(delivered, "delivery_fee").alias("delivery_fee_revenue"),
            F.round(F.avg(F.when(delivered, F.col("delivery_minutes"))), 2).alias("avg_delivery_minutes"),
            F.round(F.avg(F.when(delivered, F.col("is_late").cast("double"))), 4).alias("late_rate"),
        )
    )
