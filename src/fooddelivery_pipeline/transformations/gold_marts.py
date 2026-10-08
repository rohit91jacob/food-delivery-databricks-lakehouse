"""Gold: the order fact and business marts, as materialized views (incremental refresh on serverless)."""

from pyspark import pipelines as dp

from fooddelivery.transforms import gold

GOLD = {"quality": "gold"}


def t(name: str):
    return spark.read.table(name)


@dp.materialized_view(
    name="fct_orders",
    comment="One row per valid order: lifecycle timestamps, money, point-in-time restaurant attributes, SLA.",
    table_properties=GOLD,
    cluster_by=["business_date", "city"],
)
@dp.expect_or_fail("known_order_status", "order_status IN ('delivered', 'cancelled', 'in_progress')")
@dp.expect("restaurant_version_found", "commission_rate IS NOT NULL")
@dp.expect("one_payment", "payment_count = 1")
def fct_orders():
    return gold.fct_orders(
        t("silver_orders"),
        t("silver_order_events"),
        t("silver_payments"),
        t("silver_refunds"),
        t("silver_ratings"),
        t("silver_restaurants"),
        t("silver_promotions"),
        t("silver_rider_earnings"),
        t("silver_rider_locations"),
    )


@dp.materialized_view(
    name="fct_order_items",
    comment="Order lines joined to the menu price in force when the order was placed (SCD2 as-of).",
    table_properties=GOLD,
    cluster_by=["business_date"],
)
@dp.expect("priced_from_menu", "price_matches_menu")
def fct_order_items():
    return gold.fct_order_items(t("silver_order_items"), t("silver_menu_items"), t("silver_restaurants"))


@dp.materialized_view(
    name="gold_daily_kpis", comment="Daily KPIs per city; reconciles with the manifest.", table_properties=GOLD
)
@dp.expect_or_fail("orders_add_up", "orders_placed >= orders_delivered + orders_cancelled")
def gold_daily_kpis():
    return gold.daily_kpis(t("fct_orders"))


@dp.materialized_view(
    name="gold_delivery_sla_hourly",
    comment="Delivery SLA and ETA accuracy by city, hour, weather and traffic.",
    table_properties=GOLD,
)
def gold_delivery_sla_hourly():
    return gold.delivery_sla_hourly(t("fct_orders"), t("silver_city_conditions"))


@dp.materialized_view(
    name="gold_restaurant_daily", comment="Restaurant acceptance, prep time and revenue.", table_properties=GOLD
)
def gold_restaurant_daily():
    return gold.restaurant_daily(t("fct_orders"))


@dp.materialized_view(
    name="gold_rider_daily", comment="Rider utilisation, distance and earnings per day.", table_properties=GOLD
)
def gold_rider_daily():
    return gold.rider_daily(
        t("fct_orders"), t("silver_rider_shifts"), t("silver_rider_earnings"), t("silver_rider_locations")
    )


@dp.materialized_view(
    name="gold_customer_cohorts", comment="Monthly acquisition cohorts and retention.", table_properties=GOLD
)
def gold_customer_cohorts():
    return gold.customer_cohorts(t("fct_orders"))


@dp.materialized_view(
    name="gold_customer_ltv", comment="Customer lifetime value and order history.", table_properties=GOLD
)
def gold_customer_ltv():
    return gold.customer_ltv(t("fct_orders"), t("silver_customers"))


@dp.materialized_view(
    name="gold_cancellations_daily", comment="Cancellations by reason and actor.", table_properties=GOLD
)
def gold_cancellations_daily():
    return gold.cancellations_daily(t("fct_orders"))


@dp.materialized_view(
    name="gold_refunds_daily", comment="Refunds by reason, funding and method.", table_properties=GOLD
)
def gold_refunds_daily():
    return gold.refunds_daily(t("silver_refunds"), t("silver_orders"))


@dp.materialized_view(
    name="gold_menu_popularity_weekly", comment="Dish popularity per city and week.", table_properties=GOLD
)
def gold_menu_popularity_weekly():
    return gold.menu_popularity_weekly(t("fct_order_items"), t("fct_orders"))


@dp.materialized_view(
    name="gold_surge_promo_daily", comment="Surge bands and promotions vs demand and SLA.", table_properties=GOLD
)
def gold_surge_promo_daily():
    return gold.surge_promo_daily(t("fct_orders"))
