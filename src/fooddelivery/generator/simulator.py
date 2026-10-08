"""Day-level simulation: turns the world state into raw landing records plus a ground-truth manifest.

Everything for one business date (Asia/Kolkata) comes from RNG streams keyed by that date.
That includes the full lifecycle of orders placed late in the evening, so a date can be
regenerated on its own and gives the same bytes every time.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import cache

from fooddelivery import GENERATOR_VERSION
from fooddelivery.config import GeneratorConfig
from fooddelivery.generator import catalog
from fooddelivery.generator.geo import City, approx_km, interpolate, offset
from fooddelivery.generator.rng import lognormal_factor, rng_for, unit, weighted_index
from fooddelivery.generator.seed import RestaurantSeed
from fooddelivery.generator.world import (
    DAY,
    SEGMENT_RATE,
    HourConditions,
    MenuItem,
    Restaurant,
    Rider,
    World,
    local_midnight,
    retention,
)

IST = "+05:30"

ENTITIES = (
    "restaurants",
    "menu_items",
    "customers",
    "riders",
    "promotions",
    "city_conditions",
    "rider_shifts",
    "orders",
    "order_events",
    "payments",
    "refunds",
    "ratings",
    "rider_earnings",
    "rider_locations",
)

MONEY_FIELDS = (
    "gmv",
    "food_subtotal",
    "discounts",
    "delivery_fees",
    "platform_fees",
    "taxes",
    "tips",
    "commission",
    "refunds_amount",
    "rider_payout",
)
COUNT_FIELDS = (
    "orders_placed",
    "orders_delivered",
    "orders_cancelled",
    "late_deliveries",
    "first_orders",
    "refunds_count",
    "ratings_count",
    "order_items",
    "order_events",
    "payments",
    "gps_pings",
    "duplicate_order_records",
    "invalid_order_records",
) + tuple(f"cancelled_{r}" for r in catalog.CANCELLATION_REASONS)


def ts(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%S") + IST


def rupees(paise: int) -> float:
    return paise / 100


@dataclass
class DayBatch:
    business_date: date
    records: dict[str, list[dict]]
    manifests: list[dict]

    def count(self, entity: str) -> int:
        return len(self.records[entity])


@dataclass
class _Shift:
    rider: Rider
    shift_id: str
    start: datetime
    end: datetime
    free_at: datetime


@dataclass
class _Totals:
    counts: Counter = field(default_factory=Counter)
    money: Counter = field(default_factory=Counter)


class Simulator:
    def __init__(self, cfg: GeneratorConfig, seeds: list[RestaurantSeed]):
        self.cfg = cfg
        self.world = World(cfg, seeds)

    @cache  # noqa: B019 - one simulator per process; riders are a pure function of config
    def _riders(self, city: City) -> list[Rider]:
        return self.world.riders(city)

    # ------------------------------------------------------------------ public API
    def generate_day(self, d: date) -> DayBatch:
        if d < self.cfg.start_date:
            raise ValueError(f"{d} is before FD_START_DATE {self.cfg.start_date}")
        records: dict[str, list[dict]] = {e: [] for e in ENTITIES}
        manifests = []
        for city in self.world.cities:
            totals = _Totals()
            w = self.world
            n_before = w.customer_count_by_end_of(city, d - DAY) if d > w.start else self.cfg.initial_customers_per_city
            customers = [w.customer(city, i) for i in range(w.customer_count_by_end_of(city, d))]
            self._dimensions(city, d, customers, n_before, records)
            shifts = self._shifts(city, d, records)
            conditions = self._conditions(city, d, shifts, records)
            self._orders(city, d, customers, n_before, shifts, conditions, records, totals)
            manifests.append(self._manifest(city, d, totals))
        for entity, rows in records.items():
            for row in rows:
                row["business_date"] = d.isoformat()
                row["record_version"] = GENERATOR_VERSION
            rows.sort(key=_sort_key(entity))
        return DayBatch(d, records, manifests)

    # ------------------------------------------------------------------ dimensions (CDC feeds)
    def _dimensions(self, city: City, d: date, customers, n_before: int, out: dict[str, list[dict]]) -> None:
        w = self.world
        start = local_midnight(w.start)
        day_start, day_end = local_midnight(d), local_midnight(d) + DAY

        def on_day(t: datetime | None) -> bool:
            return t is not None and day_start <= t < day_end

        for r in w.restaurants[city.name]:
            stamps: list[tuple[datetime, str]] = []
            if (d == w.start and r.onboarded_at == start) or (r.onboarded_at > start and on_day(r.onboarded_at)):
                stamps.append((r.onboarded_at, "UPSERT"))
            if r.active_at(day_start) or on_day(r.onboarded_at):
                stamps += [(t, "UPSERT") for t in r.price_change_at if on_day(t) and r.active_at(t)]
                stamps += [(t, "UPSERT") for t in r.commission_change_at if on_day(t) and r.active_at(t)]
                monday_refresh = day_start + timedelta(hours=3)
                if d.weekday() == 0 and d > w.start and r.active_at(monday_refresh):
                    stamps.append((monday_refresh, "UPSERT"))
            if on_day(r.churned_at):
                stamps.append((r.churned_at, "DELETE"))
            for t, op in sorted(set(stamps)):
                out["restaurants"].append(self._restaurant_record(r, t, op))
            for item in r.menu:
                for t, op in self._item_changes(r, item, d):
                    out["menu_items"].append(self._item_record(item, t, op))

        if d == w.start:
            for c in customers[: w.cfg.initial_customers_per_city]:
                out["customers"].append(self._customer_record(c, start))
        for c in customers[n_before:]:
            out["customers"].append(self._customer_record(c, c.signup_at))
        for c in customers:
            if on_day(c.moved_at) and c.moved_at > c.signup_at:
                out["customers"].append(self._customer_record(c, c.moved_at))

        for rider in self._riders(city):
            if (d == w.start and rider.joined_at <= start) or (rider.joined_at > start and on_day(rider.joined_at)):
                out["riders"].append(self._rider_record(rider, max(rider.joined_at, start), "active"))
            if rider.left_at is not None and on_day(rider.left_at) and rider.joined_at < rider.left_at:
                out["riders"].append(self._rider_record(rider, rider.left_at, "inactive"))

        if city is w.cities[0]:  # promotions are national
            for p in catalog.PROMOTIONS:
                if (d == w.start and p.valid_from <= w.start) or (p.valid_from > w.start and p.valid_from == d):
                    out["promotions"].append(self._promotion_record(p, max(start, local_midnight(p.valid_from))))

    def _item_changes(self, r: Restaurant, item: MenuItem, d: date) -> list[tuple[datetime, str]]:
        day_start, day_end = local_midnight(d), local_midnight(d) + DAY
        start = local_midnight(self.world.start)
        changes: list[tuple[datetime, str]] = []
        if r.onboarded_at > day_end or (r.churned_at is not None and r.churned_at < day_start):
            return changes
        added = item.added_at
        if (d == self.world.start and added <= start) or (added > start and day_start <= added < day_end):
            if r.active_at(added) or added == r.onboarded_at:
                changes.append((added, "UPSERT"))
        for t in item.price_change_at:
            if day_start <= t < day_end and r.active_at(t) and item.exists_at(t):
                changes.append((t, "UPSERT"))
        end = item.removed_at
        if r.churned_at is not None and (end is None or r.churned_at < end) and item.added_at < r.churned_at:
            end = r.churned_at
        if end is not None and day_start <= end < day_end and item.added_at < end:
            changes.append((end, "DELETE"))
        return sorted(set(changes))

    def _restaurant_record(self, r: Restaurant, t: datetime, op: str) -> dict:
        s = r.seed
        return {
            "restaurant_id": r.restaurant_id,
            "op": op,
            "updated_at": ts(t),
            "name": s.name,
            "city": r.city.name,
            "area": s.area,
            "address": s.address,
            "latitude": round(r.lat, 6),
            "longitude": round(r.lon, 6),
            "cuisines": list(r.cuisines),
            "price_for_two": r.price_for_two_at(t),
            "commission_rate": r.commission_bps_at(t) / 10000,
            "rating": r.rating_on(t.date()),
            "rating_count": s.rating_count,
            "listed_delivery_minutes": s.listed_delivery_minutes,
            "opens_at": f"{r.opens_minute // 60:02d}:{r.opens_minute % 60:02d}",
            "closes_at": f"{(r.closes_minute // 60) % 24:02d}:{r.closes_minute % 60:02d}",
            "source_id": s.source_id,
        }

    def _item_record(self, item: MenuItem, t: datetime, op: str) -> dict:
        return {
            "item_id": item.item_id,
            "op": op,
            "updated_at": ts(t),
            "restaurant_id": item.restaurant_id,
            "name": item.dish.name,
            "category": item.dish.category,
            "cuisine": item.cuisine,
            "is_veg": item.dish.is_veg,
            "price": float(item.price_at(t)),
        }

    def _customer_record(self, c, t: datetime) -> dict:
        area, lat, lon = c.location_at(t)
        return {
            "customer_id": c.customer_id,
            "op": "UPSERT",
            "updated_at": ts(t),
            "city": c.city.name,
            "area": area,
            "latitude": round(lat, 5),
            "longitude": round(lon, 5),
            "signup_at": ts(c.signup_at),
            "segment": c.segment,
            "is_member": c.is_member,
        }

    def _rider_record(self, rider: Rider, t: datetime, status: str) -> dict:
        return {
            "rider_id": rider.rider_id,
            "op": "UPSERT",
            "updated_at": ts(t),
            "city": rider.city.name,
            "vehicle_type": rider.vehicle_type,
            "joined_at": ts(rider.joined_at),
            "status": status,
            "preferred_shift": catalog.SHIFT_TEMPLATES[rider.preferred_shift][0],
        }

    def _promotion_record(self, p: catalog.Promotion, t: datetime) -> dict:
        return {
            "promo_code": p.code,
            "op": "UPSERT",
            "updated_at": ts(t),
            "description": p.description,
            "discount_type": p.discount_type,
            "discount_value": p.value if p.discount_type == "percent" else rupees(p.value),
            "max_discount": rupees(p.max_discount_paise),
            "min_order_value": rupees(p.min_order_paise),
            "funded_by": p.funded_by,
            "eligibility_rule": p.rule,
            "valid_from": p.valid_from.isoformat(),
            "valid_to": p.valid_to.isoformat() if p.valid_to else None,
        }

    # ------------------------------------------------------------------ supply & conditions
    def _shifts(self, city: City, d: date, out: dict[str, list[dict]]) -> list[_Shift]:
        shifts = []
        day0 = local_midnight(d)
        p_work = 0.88 if d.weekday() >= 5 else 0.82
        for rider in self._riders(city):
            if not rider.active_on(d) or unit(self.cfg.seed, "works", rider.rider_id, d) >= p_work:
                continue
            pick = rider.preferred_shift
            if unit(self.cfg.seed, "shift-swap", rider.rider_id, d) < 0.3:
                pick = int(unit(self.cfg.seed, "shift-pick", rider.rider_id, d) * len(catalog.SHIFT_TEMPLATES))
            for k, (a, b) in enumerate(catalog.SHIFT_TEMPLATES[pick][1]):
                ja = int((unit(self.cfg.seed, "jit-a", rider.rider_id, d, k) - 0.5) * 40)
                jb = int((unit(self.cfg.seed, "jit-b", rider.rider_id, d, k) - 0.5) * 40)
                start = day0 + timedelta(minutes=max(0, a + ja))
                end = day0 + timedelta(minutes=min(24 * 60 - 1, b + jb))
                if end - start < timedelta(minutes=90):
                    continue
                shift = _Shift(rider, f"{rider.rider_id}-{d:%Y%m%d}-{k}", start, end, start)
                shifts.append(shift)
                out["rider_shifts"].append(
                    {
                        "shift_id": shift.shift_id,
                        "rider_id": rider.rider_id,
                        "city": city.name,
                        "shift_start": ts(start),
                        "shift_end": ts(end),
                        "template": catalog.SHIFT_TEMPLATES[pick][0],
                    }
                )
        return shifts

    def _conditions(self, city: City, d: date, shifts: list[_Shift], out) -> list[HourConditions]:
        hours = []
        day0 = local_midnight(d)
        for h in range(24):
            mid = day0 + timedelta(hours=h, minutes=30)
            on_shift = sum(1 for s in shifts if s.start <= mid < s.end)
            c = self.world.hour_conditions(city, d, h, on_shift)
            hours.append(c)
            out["city_conditions"].append(
                {
                    "city": city.name,
                    "hour_start": ts(c.hour_start),
                    "weather": c.weather,
                    "rain_mm": c.rain_mm,
                    "traffic_level": catalog.TRAFFIC_LEVELS[c.traffic_level],
                    "riders_on_shift": on_shift,
                    "surge_multiplier": c.surge_multiplier,
                }
            )
        return hours

    # ------------------------------------------------------------------ demand & orders
    def _orders(self, city, d, customers, n_before, shifts, conditions, out, totals: _Totals) -> None:
        w, cfg = self.world, self.cfg
        rng = rng_for(cfg.seed, "orders", city.name, d.isoformat())
        day0 = local_midnight(d)

        existing = customers[:n_before]
        cum, total = [], 0.0
        for c in existing:
            age = max(0.0, (day0 - c.signup_at).total_seconds() / 86400)
            total += SEGMENT_RATE[c.segment] * retention(c.segment, age)
            cum.append(total)

        placements: list[tuple[datetime, object, bool]] = []
        expected = w.expected_orders(city, d)
        for h in range(24):
            mean = expected * w.hourly_share(d, h) * (1.15 if conditions[h].raining else 1.0)
            n = max(0, round(rng.gauss(mean, math.sqrt(mean)))) if mean > 0 else 0
            for _ in range(n):
                t = day0 + timedelta(hours=h, seconds=rng.randint(0, 3599))
                placements.append((t, existing[weighted_index(rng, cum)], False))
        for c in customers[n_before:]:
            if rng.random() < 0.55:
                t = c.signup_at + timedelta(minutes=rng.randint(2, 25), seconds=rng.randint(0, 59))
                if t < day0 + DAY:
                    placements.append((t, c, True))
        placements.sort(key=lambda p: (p[0], p[1].customer_id))

        chef15 = {r.restaurant_id for r in w.restaurants[city.name] if unit(cfg.seed, "chef15", r.restaurant_id) < 0.3}
        seq = 0
        for t, customer, is_new in placements:
            seq += 1
            self._one_order(city, d, seq, t, customer, is_new, shifts, conditions, chef15, rng, out, totals)

    def _choose_restaurant(self, city, t, lat, lon, rng) -> tuple[Restaurant, float] | None:
        part = catalog.daypart(t.hour)
        boost = catalog.DAYPART_CUISINE_BOOST[part]
        cands, cum, total = [], [], 0.0
        cos_lat = math.cos(math.radians(city.lat))
        for r in self.world.restaurants[city.name]:
            if not (r.active_at(t) and r.open_at(t)):
                continue
            dist = approx_km(lat, lon, r.lat, r.lon, cos_lat) * 1.25  # road-network detour factor
            if dist > 12:
                continue
            primary = next((c for c in r.cuisines if c in boost), None)
            weight = r.popularity * math.exp(-dist / 2.5) * (boost[primary] if primary else 1.0)
            total += weight
            cands.append((r, dist))
            cum.append(total)
        if not cands:
            return None
        return cands[weighted_index(rng, cum)]

    def _one_order(
        self, city, d, seq, placed, customer, is_new, shifts, conditions, chef15, rng, out, totals: _Totals
    ) -> None:
        area, lat, lon = customer.location_at(placed)
        choice = self._choose_restaurant(city, placed, lat, lon, rng)
        if choice is None:
            return  # no kitchen open nearby: demand is lost, nothing is recorded
        restaurant, distance = choice
        menu = restaurant.menu_at(placed)
        if not menu:
            return
        order_id = f"O{d:%Y%m%d}{city.code}{seq:05d}"
        cond = conditions[placed.hour]

        # ---- basket
        k = min(len(menu), rng.choices((1, 2, 3, 4), (0.45, 0.33, 0.15, 0.07))[0])
        weights = [m.popularity for m in menu]
        picked: list[MenuItem] = []
        while len(picked) < k:
            m = menu[weighted_index(rng, _cumulative(weights))]
            if m not in picked:
                picked.append(m)
        lines = []
        subtotal = 0
        for line_no, m in enumerate(picked, start=1):
            qty = rng.choices((1, 2, 3), (0.80, 0.15, 0.05))[0]
            unit_price = m.price_at(placed) * 100
            subtotal += qty * unit_price
            lines.append(
                {
                    "line_no": line_no,
                    "item_id": m.item_id,
                    "item_name": m.dish.name,
                    "quantity": qty,
                    "unit_price": rupees(unit_price),
                }
            )

        # ---- promotion
        promo, discount = self._promotion(d, placed, is_new, subtotal, cond, restaurant, chef15, rng)
        restaurant_funded = discount if promo is not None and promo.funded_by == "restaurant" else 0

        # ---- fees
        surge = cond.surge_multiplier
        if customer.is_member:
            delivery_fee = 0 if surge < 1.3 else int(round((surge - 1) * 2500 / 100)) * 100
        else:
            delivery_fee = int(round((2500 + max(0.0, distance - 3) * 700) * surge / 100)) * 100
        platform_fee = 500
        tax = (5 * (subtotal - discount) + 50) // 100
        method = catalog.PAYMENT_METHODS[weighted_index(rng, _cumulative([p for _, p in catalog.PAYMENT_METHODS]))][0]
        prepaid = method != "cod"
        tip = rng.choice((1000, 2000, 3000, 5000)) if prepaid and rng.random() < 0.12 else 0
        total = subtotal - discount + delivery_fee + platform_fee + tax + tip
        if method == "cod" and total > 150_000:
            method, prepaid = "upi", True
        commission_bps = restaurant.commission_bps_at(placed)
        commission = (commission_bps * (subtotal - restaurant_funded) + 5000) // 10000

        # ---- promised ETA (the platform's estimate at checkout)
        load = 1 + 0.6 * self.world.hourly_share(d, placed.hour) / max(self.world.hourly_share(d, h) for h in range(24))
        prep_estimate = restaurant.prep_base_minutes * load + 0.5 * len(lines)
        speed_estimate = 22.0 * catalog.TRAFFIC_SPEED_FACTOR[cond.traffic_level] * (0.8 if cond.raining else 1.0)
        promised = round(prep_estimate + distance / speed_estimate * 60 + 9 + 12 * (surge - 1))
        promised = max(15, min(90, promised))

        order = {
            "order_id": order_id,
            "customer_id": customer.customer_id,
            "restaurant_id": restaurant.restaurant_id,
            "city": city.name,
            "delivery_area": area,
            "placed_at": ts(placed),
            "items": lines,
            "subtotal": rupees(subtotal),
            "discount": rupees(discount),
            "promo_code": promo.code if promo else None,
            "delivery_fee": rupees(delivery_fee),
            "platform_fee": rupees(platform_fee),
            "tax": rupees(tax),
            "tip": rupees(tip),
            "total_amount": rupees(total),
            "payment_method": method,
            "is_member": customer.is_member,
            "is_new_customer": is_new,
            "distance_km": round(distance, 2),
            "promised_eta_minutes": promised,
            "surge_multiplier": surge,
        }
        out["orders"].append(order)
        if rng.random() < self.cfg.duplicate_rate:  # at-least-once delivery from the order service
            out["orders"].append(dict(order))
            totals.counts["duplicate_order_records"] += 1
        if rng.random() < self.cfg.invalid_rate:
            out["orders"].append(_corrupt(order, rng))
            totals.counts["invalid_order_records"] += 1

        totals.counts["orders_placed"] += 1
        totals.counts["order_items"] += len(lines)
        if is_new:
            totals.counts["first_orders"] += 1

        self._lifecycle(
            city,
            d,
            order_id,
            placed,
            restaurant,
            customer,
            lat,
            lon,
            distance,
            method,
            prepaid,
            total,
            subtotal,
            discount,
            delivery_fee,
            platform_fee,
            tax,
            tip,
            commission,
            surge,
            promised,
            lines,
            shifts,
            conditions,
            rng,
            out,
            totals,
        )

    def _promotion(self, d, placed, is_new, subtotal, cond, restaurant, chef15, rng):
        eligible = []
        for p in catalog.PROMOTIONS:
            if p.valid_from > d or (p.valid_to and p.valid_to < d) or subtotal < p.min_order_paise:
                continue
            ok = {
                "first_order": is_new,
                "weekend": d.weekday() >= 5,
                "weekday_lunch": d.weekday() < 5 and 12 <= placed.hour < 15,
                "rain": cond.raining,
                "festival": d in catalog.FESTIVALS,
                "always": restaurant.restaurant_id in chef15,
            }[p.rule]
            if ok:
                eligible.append(p)
        if not eligible:
            return None, 0
        welcome = next((p for p in eligible if p.rule == "first_order"), None)
        if welcome is not None and rng.random() < 0.85:
            chosen = welcome
        elif rng.random() < 0.22:
            chosen = max(eligible, key=lambda p: (_discount(p, subtotal), p.code))
        else:
            return None, 0
        return chosen, _discount(chosen, subtotal)

    # ------------------------------------------------------------------ lifecycle & dispatch
    def _lifecycle(
        self,
        city,
        d,
        order_id,
        placed,
        restaurant,
        customer,
        lat,
        lon,
        distance,
        method,
        prepaid,
        total,
        subtotal,
        discount,
        delivery_fee,
        platform_fee,
        tax,
        tip,
        commission,
        surge,
        promised,
        lines,
        shifts,
        conditions,
        rng,
        out,
        totals: _Totals,
    ) -> None:
        events: list[tuple[datetime, str, str, str | None, str | None]] = [(placed, "placed", "customer", None, None)]
        cond = conditions[placed.hour]
        peak = self.world.hourly_share(d, placed.hour) * 24
        pay_at = placed + timedelta(seconds=rng.randint(3, 25))

        def cancel(at: datetime, reason: str, actor: str, refundable: bool, rider_id: str | None = None):
            events.append((at, "cancelled", actor, rider_id, reason))
            totals.counts["orders_cancelled"] += 1
            totals.counts[f"cancelled_{reason}"] += 1
            captured = prepaid and reason != "payment_failed"
            if prepaid:
                self._payment(out, order_id, method, total, "captured" if captured else "failed", pay_at, totals)
            else:
                self._payment(out, order_id, method, total, "voided", at, totals)
            if captured and refundable:
                self._refund(
                    out,
                    totals,
                    order_id,
                    1,
                    total,
                    "order_cancelled",
                    "original",
                    "platform",
                    at + timedelta(minutes=rng.randint(5, 30)),
                )
            self._emit_events(out, totals, order_id, events, rng)

        # payment failure
        if prepaid and rng.random() < 0.012:
            cancel(placed + timedelta(seconds=rng.randint(30, 90)), "payment_failed", "system", False)
            return
        # customer changes mind quickly
        if rng.random() < 0.012:
            cancel(placed + timedelta(seconds=rng.randint(20, 150)), "customer_changed_mind", "customer", True)
            return
        accept_delay = 50 * (1 + 0.5 * max(0.0, peak - 1)) * lognormal_factor(rng, 0.5)
        accepted = placed + timedelta(seconds=min(600, accept_delay))
        if rng.random() < 0.015 + 0.01 * max(0.0, peak - 1.5):
            cancel(accepted, "restaurant_rejected", "restaurant", True)
            return
        events.append((accepted, "accepted", "restaurant", None, None))

        # rider dispatch: earliest-available rider on shift; ties (several idle riders) are broken
        # pseudo-randomly so work spreads across the fleet instead of always hitting rider #0
        request = accepted + timedelta(seconds=rng.randint(10, 45))
        latest_end = request + timedelta(minutes=10)
        horizon = request + timedelta(minutes=40)
        best_at, tied = None, []
        for s in shifts:
            if s.end < latest_end:
                continue
            avail = max(s.free_at, s.start, request)
            if avail > horizon:
                continue
            if best_at is None or avail < best_at:
                best_at, tied = avail, [s]
            elif avail == best_at:
                tied.append(s)
        best = tied[0] if len(tied) == 1 else (rng.choice(tied) if tied else None)
        if best is None:
            cancel(accepted + timedelta(minutes=rng.randint(25, 40)), "no_rider_available", "system", True)
            return
        assigned = best_at + timedelta(seconds=rng.randint(5, 40))
        rider = best.rider
        events.append((assigned, "rider_assigned", "system", rider.rider_id, None))

        preparing = accepted + timedelta(seconds=rng.randint(15, 90))
        events.append((preparing, "preparing", "restaurant", None, None))
        prep = restaurant.prep_base_minutes * (1 + 0.6 * max(0.0, peak - 1) / 2) * lognormal_factor(rng, 0.25)
        ready = preparing + timedelta(minutes=prep + 0.5 * len(lines))

        speed = catalog.VEHICLE_SPEED_KMPH[rider.vehicle_type] * catalog.TRAFFIC_SPEED_FACTOR[cond.traffic_level]
        speed *= 0.75 if cond.weather == "heavy_rain" else (0.88 if cond.raining else 1.0)
        to_rest_km = rng.uniform(0.3, 2.5)
        arrival = assigned + timedelta(minutes=to_rest_km / speed * 60 * lognormal_factor(rng, 0.2))
        picked_up = max(ready, arrival) + timedelta(seconds=rng.randint(30, 150))
        start_lat, start_lon = offset(restaurant.lat, restaurant.lon, to_rest_km, rng.uniform(0, 2 * math.pi))

        # customers give up when the food has not been picked up long after the promise
        give_up_at = placed + timedelta(minutes=promised * 0.9)
        if picked_up > give_up_at + timedelta(minutes=8) and rng.random() < 0.35:
            cancel_at = max(give_up_at, preparing + timedelta(minutes=1))
            if cancel_at < ready:
                self._pings(
                    out,
                    totals,
                    order_id,
                    rider,
                    city,
                    assigned,
                    cancel_at,
                    start_lat,
                    start_lon,
                    restaurant,
                    None,
                    None,
                    arrival,
                    picked_up,
                    speed,
                    rng,
                )
                self._earning(out, totals, order_id, rider, city, cancel_at, cancellation_pay=1500)
                best.free_at = cancel_at + timedelta(minutes=2)
                cancel(cancel_at, "customer_delay", "customer", True, rider.rider_id)
                return

        events.append((ready, "ready", "restaurant", None, None))
        events.append((picked_up, "picked_up", "rider", rider.rider_id, None))
        travel = distance / speed * 60 * lognormal_factor(rng, 0.15) + rng.uniform(1, 4)
        delivered = picked_up + timedelta(minutes=travel)
        events.append((delivered, "delivered", "rider", rider.rider_id, None))
        best.free_at = delivered + timedelta(seconds=rng.randint(60, 240))

        self._pings(
            out,
            totals,
            order_id,
            rider,
            city,
            assigned,
            delivered,
            start_lat,
            start_lon,
            restaurant,
            lat,
            lon,
            arrival,
            picked_up,
            speed,
            rng,
        )
        if prepaid:
            self._payment(out, order_id, method, total, "captured", pay_at, totals)
        else:
            self._payment(out, order_id, method, total, "captured", delivered, totals)
        self._emit_events(out, totals, order_id, events, rng)

        # money for delivered orders (definitions match gold_daily_kpis, see docs/metrics.md)
        totals.counts["orders_delivered"] += 1
        for key, value in (
            ("gmv", total),
            ("food_subtotal", subtotal),
            ("discounts", discount),
            ("delivery_fees", delivery_fee),
            ("platform_fees", platform_fee),
            ("taxes", tax),
            ("tips", tip),
            ("commission", commission),
        ):
            totals.money[key] += value
        # Lateness is judged on the whole-second timestamps that are actually emitted, exactly
        # like gold does it, so the manifest and gold agree on every boundary case.
        minutes = (delivered.replace(microsecond=0) - placed.replace(microsecond=0)).total_seconds() / 60
        late_by = minutes - promised
        if late_by > 0:
            totals.counts["late_deliveries"] += 1

        surge_pay = int(round((surge - 1) * 2000))
        self._earning(
            out,
            totals,
            order_id,
            rider,
            city,
            delivered,
            base=2500,
            distance_pay=int(round(distance * 600)),
            surge_pay=surge_pay,
            tip=tip,
        )

        refund_no = 0
        if late_by > 20 and rng.random() < 0.4:
            refund_no += 1
            self._refund(
                out,
                totals,
                order_id,
                refund_no,
                (subtotal * 20 + 50) // 100,
                "late_delivery",
                "original" if prepaid else "wallet_credit",
                "platform",
                delivered + timedelta(minutes=rng.randint(10, 120)),
            )
        if rng.random() < 0.015:
            line = rng.choice(lines)
            amount = round(line["unit_price"] * 100) * line["quantity"]
            refund_no += 1
            self._refund(
                out,
                totals,
                order_id,
                refund_no,
                amount,
                "missing_or_damaged_item",
                "original" if prepaid else "wallet_credit",
                "restaurant",
                delivered + timedelta(minutes=rng.randint(10, 180)),
            )
        if rng.random() < 0.45:
            food = min(5, max(1, round(rng.gauss(restaurant.rating_on(d), 0.8))))
            penalty = 2 if late_by > 20 else (1 if late_by > 10 else 0)
            delivery = min(5, max(1, round(rng.gauss(4.6 - penalty, 0.6))))
            out["ratings"].append(
                {
                    "order_id": order_id,
                    "customer_id": customer.customer_id,
                    "restaurant_id": restaurant.restaurant_id,
                    "rider_id": rider.rider_id,
                    "food_rating": food,
                    "delivery_rating": delivery,
                    "rated_at": ts(delivered + timedelta(minutes=rng.randint(10, 180))),
                }
            )
            totals.counts["ratings_count"] += 1

    def _emit_events(self, out, totals, order_id, events, rng) -> None:
        events.sort(key=lambda e: e[0])
        for n, (at, status, actor, rider_id, reason) in enumerate(events, start=1):
            record = {
                "event_id": f"{order_id}-{n:02d}",
                "order_id": order_id,
                "status": status,
                "event_at": ts(at),
                "actor": actor,
                "rider_id": rider_id,
                "reason": reason,
            }
            out["order_events"].append(record)
            totals.counts["order_events"] += 1
            if rng.random() < self.cfg.duplicate_rate:
                out["order_events"].append(dict(record))

    def _payment(self, out, order_id, method, total, status, at, totals) -> None:
        out["payments"].append(
            {
                "payment_id": f"P{order_id[1:]}",
                "order_id": order_id,
                "method": method,
                "amount": rupees(total),
                "status": status,
                "processed_at": ts(at),
            }
        )
        totals.counts["payments"] += 1

    def _refund(self, out, totals, order_id, n, amount, reason, method, funded_by, at) -> None:
        out["refunds"].append(
            {
                "refund_id": f"F{order_id[1:]}-{n}",
                "order_id": order_id,
                "amount": rupees(amount),
                "reason": reason,
                "method": method,
                "funded_by": funded_by,
                "refunded_at": ts(at),
            }
        )
        totals.counts["refunds_count"] += 1
        totals.money["refunds_amount"] += amount

    def _earning(
        self, out, totals, order_id, rider, city, at, *, base=0, distance_pay=0, surge_pay=0, tip=0, cancellation_pay=0
    ) -> None:
        total = base + distance_pay + surge_pay + tip + cancellation_pay
        out["rider_earnings"].append(
            {
                "earning_id": f"E{order_id[1:]}",
                "rider_id": rider.rider_id,
                "order_id": order_id,
                "city": city.name,
                "base_pay": rupees(base),
                "distance_pay": rupees(distance_pay),
                "surge_pay": rupees(surge_pay),
                "tip": rupees(tip),
                "cancellation_pay": rupees(cancellation_pay),
                "total_pay": rupees(total),
                "earned_at": ts(at),
            }
        )
        totals.money["rider_payout"] += total

    def _pings(
        self,
        out,
        totals,
        order_id,
        rider,
        city,
        start,
        end,
        start_lat,
        start_lon,
        restaurant,
        dest_lat,
        dest_lon,
        arrival,
        picked_up,
        speed,
        rng,
    ) -> None:
        step = timedelta(seconds=self.cfg.gps_ping_seconds)
        t = start + timedelta(seconds=rng.randint(0, self.cfg.gps_ping_seconds - 1))
        n = 0
        while t <= end:
            if t < arrival:
                f = (t - start) / (arrival - start) if arrival > start else 1.0
                lat, lon = interpolate(start_lat, start_lon, restaurant.lat, restaurant.lon, f)
                phase, kmph = "to_restaurant", speed
            elif t < picked_up or dest_lat is None:
                lat, lon = restaurant.lat, restaurant.lon
                phase, kmph = "at_restaurant", 0.0
            else:
                f = (t - picked_up) / (end - picked_up) if end > picked_up else 1.0
                lat, lon = interpolate(restaurant.lat, restaurant.lon, dest_lat, dest_lon, f)
                phase, kmph = "to_customer", speed
            n += 1
            out["rider_locations"].append(
                {
                    "ping_id": f"{order_id}-{n:03d}",
                    "rider_id": rider.rider_id,
                    "order_id": order_id,
                    "city": city.name,
                    "recorded_at": ts(t),
                    "latitude": round(lat + rng.gauss(0, 0.00008), 6),
                    "longitude": round(lon + rng.gauss(0, 0.00008), 6),
                    "speed_kmph": round(max(0.0, kmph * lognormal_factor(rng, 0.2)) if kmph else 0.0, 1),
                    "phase": phase,
                }
            )
            totals.counts["gps_pings"] += 1
            t += step

    # ------------------------------------------------------------------ manifest
    def _manifest(self, city: City, d: date, totals: _Totals) -> dict:
        m: dict = {"business_date": d.isoformat(), "city": city.name, "record_version": GENERATOR_VERSION}
        for k in COUNT_FIELDS:
            m[k] = int(totals.counts.get(k, 0))
        for k in MONEY_FIELDS:
            # JSON numbers (shortest round-trip repr of an exact paise amount), so Spark reads them
            # straight into DECIMAL(18,2) and Python can recover them exactly via Decimal(repr(x)).
            m[k] = rupees(int(totals.money.get(k, 0)))
        return m


def _discount(p: catalog.Promotion, subtotal: int) -> int:
    if p.discount_type == "percent":
        return min(p.max_discount_paise, subtotal * p.value // 100, subtotal)
    return min(p.value, subtotal)


def _cumulative(weights: list[float]) -> list[float]:
    out, total = [], 0.0
    for wgt in weights:
        total += wgt
        out.append(total)
    return out


def _corrupt(order: dict, rng) -> dict:
    """A broken copy with its own id, so silver quarantines it without touching the real order."""
    bad = dict(order)
    bad["order_id"] = "X" + order["order_id"][1:]
    kind = rng.choice(("null_customer", "negative_total", "empty_items", "total_mismatch"))
    if kind == "null_customer":
        bad["customer_id"] = None
    elif kind == "negative_total":
        bad["total_amount"] = -abs(order["total_amount"]) - 1.0
    elif kind == "empty_items":
        bad["items"] = []
    else:
        bad["total_amount"] = round(order["total_amount"] + 13.37, 2)
    return bad


def _sort_key(entity: str):
    keys = {
        "restaurants": ("updated_at", "restaurant_id"),
        "menu_items": ("updated_at", "item_id"),
        "customers": ("updated_at", "customer_id"),
        "riders": ("updated_at", "rider_id"),
        "promotions": ("updated_at", "promo_code"),
        "city_conditions": ("city", "hour_start"),
        "rider_shifts": ("shift_id",),
        "orders": ("order_id",),
        "order_events": ("event_id",),
        "payments": ("payment_id",),
        "refunds": ("refund_id",),
        "ratings": ("order_id",),
        "rider_earnings": ("earning_id",),
        "rider_locations": ("ping_id",),
    }[entity]
    return lambda r: tuple("" if r.get(k) is None else str(r.get(k)) for k in keys)
