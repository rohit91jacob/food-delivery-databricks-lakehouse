"""Explicit raw (bronze) schemas, as Spark DDL strings.

Auto Loader runs with these schemas instead of inference. Values that do not fit, and fields
that are not declared, land in ``_rescued_data`` instead of being silently dropped.
Timestamps stay as strings in bronze (raw fidelity) and are parsed in silver.
"""

from __future__ import annotations

_COMMON = "business_date STRING, record_version INT"

RAW_SCHEMAS: dict[str, str] = {
    "restaurants": (
        "restaurant_id STRING, op STRING, updated_at STRING, name STRING, city STRING, area STRING, "
        "address STRING, latitude DOUBLE, longitude DOUBLE, cuisines ARRAY<STRING>, price_for_two INT, "
        "commission_rate DECIMAL(5,4), rating DOUBLE, rating_count INT, listed_delivery_minutes INT, "
        "opens_at STRING, closes_at STRING, source_id STRING, " + _COMMON
    ),
    "menu_items": (
        "item_id STRING, op STRING, updated_at STRING, restaurant_id STRING, name STRING, category STRING, "
        "cuisine STRING, is_veg BOOLEAN, price DECIMAL(10,2), " + _COMMON
    ),
    "customers": (
        "customer_id STRING, op STRING, updated_at STRING, city STRING, area STRING, latitude DOUBLE, "
        "longitude DOUBLE, signup_at STRING, segment STRING, is_member BOOLEAN, " + _COMMON
    ),
    "riders": (
        "rider_id STRING, op STRING, updated_at STRING, city STRING, vehicle_type STRING, joined_at STRING, "
        "status STRING, preferred_shift STRING, " + _COMMON
    ),
    "promotions": (
        "promo_code STRING, op STRING, updated_at STRING, description STRING, discount_type STRING, "
        "discount_value DECIMAL(10,2), max_discount DECIMAL(10,2), min_order_value DECIMAL(10,2), "
        "funded_by STRING, eligibility_rule STRING, valid_from STRING, valid_to STRING, " + _COMMON
    ),
    "city_conditions": (
        "city STRING, hour_start STRING, weather STRING, rain_mm DOUBLE, traffic_level STRING, "
        "riders_on_shift INT, surge_multiplier DECIMAL(4,2), " + _COMMON
    ),
    "rider_shifts": (
        "shift_id STRING, rider_id STRING, city STRING, shift_start STRING, shift_end STRING, template STRING, "
        + _COMMON
    ),
    "orders": (
        "order_id STRING, customer_id STRING, restaurant_id STRING, city STRING, delivery_area STRING, "
        "placed_at STRING, items ARRAY<STRUCT<line_no: INT, item_id: STRING, item_name: STRING, "
        "quantity: INT, unit_price: DECIMAL(10,2)>>, subtotal DECIMAL(12,2), discount DECIMAL(12,2), "
        "promo_code STRING, delivery_fee DECIMAL(12,2), platform_fee DECIMAL(12,2), tax DECIMAL(12,2), "
        "tip DECIMAL(12,2), total_amount DECIMAL(12,2), payment_method STRING, is_member BOOLEAN, "
        "is_new_customer BOOLEAN, distance_km DOUBLE, promised_eta_minutes INT, surge_multiplier DECIMAL(4,2), "
        + _COMMON
    ),
    "order_events": (
        "event_id STRING, order_id STRING, status STRING, event_at STRING, actor STRING, rider_id STRING, "
        "reason STRING, " + _COMMON
    ),
    "payments": (
        "payment_id STRING, order_id STRING, method STRING, amount DECIMAL(12,2), status STRING, "
        "processed_at STRING, " + _COMMON
    ),
    "refunds": (
        "refund_id STRING, order_id STRING, amount DECIMAL(12,2), reason STRING, method STRING, "
        "funded_by STRING, refunded_at STRING, " + _COMMON
    ),
    "ratings": (
        "order_id STRING, customer_id STRING, restaurant_id STRING, rider_id STRING, food_rating INT, "
        "delivery_rating INT, rated_at STRING, " + _COMMON
    ),
    "rider_earnings": (
        "earning_id STRING, rider_id STRING, order_id STRING, city STRING, base_pay DECIMAL(10,2), "
        "distance_pay DECIMAL(10,2), surge_pay DECIMAL(10,2), tip DECIMAL(10,2), cancellation_pay DECIMAL(10,2), "
        "total_pay DECIMAL(10,2), earned_at STRING, " + _COMMON
    ),
    "rider_locations": (
        "ping_id STRING, rider_id STRING, order_id STRING, city STRING, recorded_at STRING, latitude DOUBLE, "
        "longitude DOUBLE, speed_kmph DOUBLE, phase STRING, " + _COMMON
    ),
    "manifests": (
        "business_date STRING, city STRING, record_version INT, orders_placed BIGINT, orders_delivered BIGINT, "
        "orders_cancelled BIGINT, late_deliveries BIGINT, first_orders BIGINT, refunds_count BIGINT, "
        "ratings_count BIGINT, order_items BIGINT, order_events BIGINT, payments BIGINT, gps_pings BIGINT, "
        "duplicate_order_records BIGINT, invalid_order_records BIGINT, cancelled_payment_failed BIGINT, "
        "cancelled_restaurant_rejected BIGINT, cancelled_customer_changed_mind BIGINT, "
        "cancelled_customer_delay BIGINT, cancelled_no_rider_available BIGINT, gmv DECIMAL(18,2), "
        "food_subtotal DECIMAL(18,2), discounts DECIMAL(18,2), delivery_fees DECIMAL(18,2), "
        "platform_fees DECIMAL(18,2), taxes DECIMAL(18,2), tips DECIMAL(18,2), commission DECIMAL(18,2), "
        "refunds_amount DECIMAL(18,2), rider_payout DECIMAL(18,2)"
    ),
}

# Landing folder per bronze entity (the manifest feed lives in a reserved underscore folder).
LANDING_FOLDERS: dict[str, str] = {e: e for e in RAW_SCHEMAS} | {"manifests": "_manifests"}
