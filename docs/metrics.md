# Metric definitions

The generator (`fooddelivery.generator.simulator`) and the gold layer (`fooddelivery.transforms.gold`) both
implement these definitions. The `gold_reconciles_with_manifest` check (job) and the
`reconcile_gold_with_manifest` Airflow task require them to agree **exactly** for every business date and city.

| Metric | Definition |
|---|---|
| `orders_placed` | Valid orders placed on the business date (after dedupe; quarantined records excluded) |
| `orders_delivered` / `orders_cancelled` | Orders whose lifecycle ends in `delivered` / `cancelled` |
| `cancelled_<reason>` | Cancelled orders by reason: payment_failed, restaurant_rejected, customer_changed_mind, customer_delay, no_rider_available |
| `late_deliveries` | Delivered orders where `(unix_timestamp(delivered_at) − unix_timestamp(placed_at)) / 60 > promised_eta_minutes` (whole-second timestamps) |
| `first_orders` | Orders placed by customers on their sign-up day |
| `gmv` | Σ `total_amount` of delivered orders (food + fees + tax + tip − discount) |
| `food_subtotal`, `discounts`, `delivery_fees`, `platform_fees`, `taxes`, `tips` | Σ of the order fields over delivered orders |
| `commission` | Σ over delivered orders of `round(commission_rate_at_placement × (subtotal − restaurant_funded_discount), 2)`, half-up |
| `refunds_count`, `refunds_amount` | All refunds of orders placed on the date, whenever they were issued |
| `rider_payout` | Σ rider earnings (base + distance + surge + tip, or cancellation pay) for orders of the date |
| `order_items`, `order_events`, `payments`, `gps_pings`, `ratings_count` | Row counts attributed to orders of the date |
| `aov` | `gmv / orders_delivered` |
| `on_time_rate` | `1 − late_deliveries / orders_delivered` |
| `cancellation_rate` | `orders_cancelled / orders_placed` |
| `contribution_margin` | Delivered: `commission + delivery_fee + platform_fee − platform_funded_discount − platform_refunds − rider_pay`. Cancelled: `− platform_refunds − rider_pay` |
| `utilisation` (riders) | Busy minutes (assignment → delivery/cancel) / shift minutes |
| `retention_rate` (cohorts) | Active customers in month *m* / customers whose first delivered order was in the cohort month |

Money is DECIMAL end to end (JSON numbers are parsed directly into `DECIMAL(12,2)`), so reconciliation compares
with exact equality, not tolerances.
