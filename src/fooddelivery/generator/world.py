"""The simulated world: restaurants, menus, customers, riders, promotions and city conditions.

The world state is a pure function of ``(config, seed data, timestamp)``. Each entity gets
its own RNG stream plus a precomputed schedule of changes (price changes, onboarding, churn,
address moves, rider attrition), so any business date can be generated on its own.
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import cache, cached_property

from fooddelivery.config import GeneratorConfig
from fooddelivery.generator import catalog
from fooddelivery.generator.catalog import Dish
from fooddelivery.generator.geo import CITIES, City, area_location, jitter
from fooddelivery.generator.rng import rng_for, unit
from fooddelivery.generator.seed import RestaurantSeed

HORIZON_DAYS = 3 * 365
DAY = timedelta(days=1)


def local_midnight(d: date) -> datetime:
    return datetime(d.year, d.month, d.day)


def _round5(rupees: float) -> int:
    return max(20, int(5 * round(rupees / 5)))


def _change_times(rng, start: datetime, mean_days: float, *, night_share: float = 0.8) -> list[datetime]:
    """Poisson-process change timestamps from ``start`` to the horizon; mostly 04:00-06:00."""
    out: list[datetime] = []
    t = start
    end = start + timedelta(days=HORIZON_DAYS)
    while True:
        t = t + timedelta(days=rng.expovariate(1.0 / mean_days))
        if t >= end:
            return out
        day = t.date()
        if rng.random() < night_share:
            minute = rng.randint(4 * 60, 6 * 60 - 1)
        else:
            minute = rng.randint(10 * 60, 22 * 60)
        stamp = local_midnight(day) + timedelta(minutes=minute, seconds=rng.randint(0, 59))
        if stamp > start:
            out.append(stamp)


@dataclass
class MenuItem:
    item_id: str
    restaurant_id: str
    dish: Dish
    cuisine: str
    base_price: int  # rupees
    added_at: datetime
    removed_at: datetime | None
    popularity: float
    price_change_at: list[datetime] = field(default_factory=list)
    price_after: list[int] = field(default_factory=list)

    def price_at(self, ts: datetime) -> int:
        i = bisect.bisect_right(self.price_change_at, ts)
        return self.base_price if i == 0 else self.price_after[i - 1]

    def exists_at(self, ts: datetime) -> bool:
        return self.added_at <= ts and (self.removed_at is None or ts < self.removed_at)


@dataclass
class Restaurant:
    restaurant_id: str
    seed: RestaurantSeed
    city: City
    lat: float
    lon: float
    cuisines: tuple[str, ...]
    popularity: float
    prep_base_minutes: float
    opens_minute: int
    closes_minute: int  # may exceed 1440 for late-night kitchens
    onboarded_at: datetime
    churned_at: datetime | None
    menu: list[MenuItem]
    price_change_at: list[datetime]
    price_after: list[int]
    commission_change_at: list[datetime]
    commission_after: list[int]
    base_commission_bps: int
    rating_base: float

    def active_at(self, ts: datetime) -> bool:
        return self.onboarded_at <= ts and (self.churned_at is None or ts < self.churned_at)

    def open_at(self, ts: datetime) -> bool:
        m = ts.hour * 60 + ts.minute
        if self.opens_minute <= m < min(self.closes_minute, 1440):
            return True
        return self.closes_minute > 1440 and m < self.closes_minute - 1440

    def price_for_two_at(self, ts: datetime) -> int:
        i = bisect.bisect_right(self.price_change_at, ts)
        return self.seed.price_for_two if i == 0 else self.price_after[i - 1]

    def commission_bps_at(self, ts: datetime) -> int:
        i = bisect.bisect_right(self.commission_change_at, ts)
        return self.base_commission_bps if i == 0 else self.commission_after[i - 1]

    def rating_on(self, d: date) -> float:
        """Weekly-refreshed rating (an untracked, in-place attribute in SCD2 terms)."""
        week = d.toordinal() // 7
        drift = (unit(self.restaurant_id, "rating-drift", week) - 0.5) * 0.3
        return round(min(5.0, max(1.0, self.rating_base + drift)), 1)

    def menu_at(self, ts: datetime) -> list[MenuItem]:
        return [m for m in self.menu if m.exists_at(ts)]


@dataclass(frozen=True)
class Customer:
    customer_id: str
    city: City
    index: int
    signup_at: datetime
    area: str
    lat: float
    lon: float
    segment: str
    is_member: bool
    moved_at: datetime | None
    moved_area: str | None
    moved_lat: float | None
    moved_lon: float | None

    def location_at(self, ts: datetime) -> tuple[str, float, float]:
        if self.moved_at is not None and ts >= self.moved_at:
            return self.moved_area, self.moved_lat, self.moved_lon  # type: ignore[return-value]
        return self.area, self.lat, self.lon


SEGMENT_MIX = (("casual", 0.60), ("regular", 0.30), ("power", 0.10))
SEGMENT_RATE = {"casual": 0.035, "regular": 0.11, "power": 0.32}
MEMBER_SHARE = {"casual": 0.03, "regular": 0.20, "power": 0.70}


def retention(segment: str, age_days: float) -> float:
    if segment == "power":
        return 0.85 + 0.15 * math.exp(-age_days / 365)
    if segment == "regular":
        return 0.50 + 0.50 * math.exp(-age_days / 240)
    return 0.25 + 0.75 * math.exp(-age_days / 120)


@dataclass(frozen=True)
class Rider:
    rider_id: str
    city: City
    vehicle_type: str
    joined_at: datetime
    left_at: datetime | None
    preferred_shift: int

    def active_on(self, d: date) -> bool:
        return self.joined_at.date() <= d and (self.left_at is None or d < self.left_at.date())


@dataclass(frozen=True)
class HourConditions:
    city: str
    hour_start: datetime
    weather: str  # clear | cloudy | light_rain | heavy_rain
    rain_mm: float
    traffic_level: int  # 0..3
    surge_multiplier: float

    @property
    def raining(self) -> bool:
        return self.weather in ("light_rain", "heavy_rain")


def _pick(rng, options: tuple[tuple[str, float], ...]) -> str:
    r = rng.random() * sum(w for _, w in options)
    for value, weight in options:
        r -= weight
        if r < 0:
            return value
    return options[-1][0]


class World:
    def __init__(self, cfg: GeneratorConfig, seeds: list[RestaurantSeed]):
        self.cfg = cfg
        self.start = cfg.start_date
        self.cities = [CITIES[c] for c in cfg.cities]
        by_city: dict[str, list[RestaurantSeed]] = {}
        for s in seeds:
            by_city.setdefault(s.city, []).append(s)
        self.restaurants: dict[str, list[Restaurant]] = {}
        for city in self.cities:
            pool = sorted(by_city.get(city.name, []), key=lambda s: (s.source_id, s.name))
            if not pool:
                raise ValueError(f"seed has no restaurants for {city.name}")
            ranked = sorted(pool, key=lambda s: unit(cfg.seed, "restaurant-pick", city.name, s.source_id))
            chosen = sorted(ranked[: cfg.restaurants_per_city], key=lambda s: s.source_id)
            self.restaurants[city.name] = [self._build_restaurant(city, i, s) for i, s in enumerate(chosen)]

    # ------------------------------------------------------------------ restaurants & menus
    def _build_restaurant(self, city: City, index: int, s: RestaurantSeed) -> Restaurant:
        rid = f"R{city.code}{index:04d}"
        rng = rng_for(self.cfg.seed, "restaurant", rid)
        lat, lon = jitter(self.cfg.seed, *area_location(self.cfg.seed, city, s.area), 0.8, rid)
        start = local_midnight(self.start)
        if rng.random() < 0.85:
            onboarded = start
        else:
            onboarded = start + timedelta(
                days=rng.randint(1, 150), hours=rng.randint(9, 17), minutes=rng.randint(0, 59)
            )
        churned = None
        if rng.random() < 0.05:
            churned = onboarded + timedelta(days=rng.randint(20, 200), hours=rng.randint(1, 5))
        dish_cuisines = []
        for c in s.cuisines:
            key = c if c in catalog.DISHES else catalog.CUISINE_ALIASES.get(c)
            if key and key not in dish_cuisines:
                dish_cuisines.append(key)
        if not dish_cuisines:
            dish_cuisines = [catalog.FALLBACK_CUISINE]
        primary = dish_cuisines[0]
        late_night = rng.random() < 0.22 or primary in ("Fast Food", "Pizzas", "Desserts")
        breakfast = primary in ("South Indian", "Bakery", "Beverages", "Snacks")
        opens = 7 * 60 if breakfast else 10 * 60 + rng.choice((0, 30, 60))
        closes = 26 * 60 if late_night else 23 * 60 + rng.choice((0, 15, 30))
        mean_prep = sum(d.prep_minutes for d in catalog.DISHES[primary]) / len(catalog.DISHES[primary])
        prep_base = max(5.0, min(32.0, 0.55 * mean_prep + 0.18 * s.listed_delivery_minutes))
        popularity = math.log10(10 + s.rating_count) * (s.rating / 4.0) ** 2

        price_times = _change_times(rng, onboarded, 75.0)
        prices, current = [], s.price_for_two
        for _ in price_times:
            current = _round5(current * (1 + rng.uniform(0.03, 0.12) * (1 if rng.random() < 0.8 else -1)))
            prices.append(current)
        commission_times = _change_times(rng, onboarded, 160.0)
        base_commission = rng.choice((1500, 1800, 2000, 2200, 2500))
        commissions, cur_c = [], base_commission
        for _ in commission_times:
            cur_c = max(1200, min(3000, cur_c + rng.choice((-200, 100, 200, 300))))
            commissions.append(cur_c)

        restaurant = Restaurant(
            restaurant_id=rid,
            seed=s,
            city=city,
            lat=lat,
            lon=lon,
            cuisines=s.cuisines,
            popularity=popularity,
            prep_base_minutes=prep_base,
            opens_minute=opens,
            closes_minute=closes,
            onboarded_at=onboarded,
            churned_at=churned,
            menu=[],
            price_change_at=price_times,
            price_after=prices,
            commission_change_at=commission_times,
            commission_after=commissions,
            base_commission_bps=base_commission,
            rating_base=s.rating,
        )
        restaurant.menu = self._build_menu(restaurant, dish_cuisines, rng)
        return restaurant

    def _build_menu(self, r: Restaurant, cuisines: list[str], rng) -> list[MenuItem]:
        dishes: list[tuple[str, Dish]] = []
        for c in cuisines:
            dishes.extend((c, d) for d in catalog.DISHES[c])
        rng.shuffle(dishes)
        initial = min(len(dishes), rng.randint(8, 16))
        half_for_two = r.seed.price_for_two / 2
        items = []
        for n, (cuisine, dish) in enumerate(dishes):
            added = r.onboarded_at
            if n >= initial:  # dishes launched later
                added = r.onboarded_at + timedelta(days=rng.randint(15, 400), hours=rng.randint(4, 5))
            removed = None
            if rng.random() < 0.08:
                removed = added + timedelta(days=rng.randint(30, 300), hours=rng.randint(4, 5))
            item = MenuItem(
                item_id=f"{r.restaurant_id}-I{n:02d}",
                restaurant_id=r.restaurant_id,
                dish=dish,
                cuisine=cuisine,
                base_price=_round5(half_for_two * dish.price_factor * math.exp(rng.gauss(0, 0.12))),
                added_at=added,
                removed_at=removed,
                popularity=1.0 / (1 + n) ** 0.6 * (1.3 if dish.category in ("main", "combo") else 1.0),
            )
            change_times = [t for t in _change_times(rng, added, 55.0) if removed is None or t < removed]
            price = item.base_price
            for _ in change_times:
                price = _round5(price * (1 + rng.uniform(0.03, 0.12) * (1 if rng.random() < 0.75 else -1)))
                item.price_after.append(price)
            item.price_change_at = change_times
            items.append(item)
        return items

    # ------------------------------------------------------------------ customers
    def _daily_signups(self, city: City, d: date) -> int:
        rng = rng_for(self.cfg.seed, "signups", city.name, d.isoformat())
        base = self.cfg.initial_customers_per_city * 0.0045 * catalog.WEEKDAY_FACTOR[d.weekday()]
        return max(0, round(base * rng.uniform(0.8, 1.2)))

    def customer_count_by_end_of(self, city: City, d: date) -> int:
        n = self.cfg.initial_customers_per_city
        day = self.start
        while day <= d:
            n += self._daily_signups(city, day)
            day += DAY
        return n

    @cached_property
    def _area_weights(self) -> dict[str, tuple[list[str], list[float]]]:
        out = {}
        for city in self.cities:
            counts: dict[str, int] = {}
            for r in self.restaurants[city.name]:
                counts[r.seed.area] = counts.get(r.seed.area, 0) + 1
            areas = sorted(counts)
            cum, total = [], 0.0
            for a in areas:
                total += counts[a] ** 0.7
                cum.append(total)
            out[city.name] = (areas, cum)
        return out

    @cache  # noqa: B019 - customers are immutable and the world lives for the whole run
    def customer(self, city: City, index: int) -> Customer:
        cid = f"C{city.code}{index:07d}"
        rng = rng_for(self.cfg.seed, "customer", cid)
        areas, cum = self._area_weights[city.name]
        area = areas[min(bisect.bisect_right(cum, rng.random() * cum[-1]), len(areas) - 1)]
        lat, lon = jitter(self.cfg.seed, *area_location(self.cfg.seed, city, area), 1.5, cid)
        if index < self.cfg.initial_customers_per_city:
            signup = local_midnight(self.start) - timedelta(days=rng.randint(1, 720), minutes=rng.randint(0, 1439))
        else:
            signup = self._signup_time(city, index)
        segment = _pick(rng, SEGMENT_MIX)
        moved_at = moved_area = moved_lat = moved_lon = None
        if rng.random() < 0.10:
            moved_at = max(signup, local_midnight(self.start)) + timedelta(
                days=rng.randint(1, 400), hours=rng.randint(7, 21), minutes=rng.randint(0, 59)
            )
            moved_area = areas[min(bisect.bisect_right(cum, rng.random() * cum[-1]), len(areas) - 1)]
            moved_lat, moved_lon = jitter(
                self.cfg.seed, *area_location(self.cfg.seed, city, moved_area), 1.5, cid, "moved"
            )
        return Customer(
            customer_id=cid,
            city=city,
            index=index,
            signup_at=signup,
            area=area,
            lat=lat,
            lon=lon,
            segment=segment,
            is_member=rng.random() < MEMBER_SHARE[segment],
            moved_at=moved_at,
            moved_area=moved_area,
            moved_lat=moved_lat,
            moved_lon=moved_lon,
        )

    def _signup_time(self, city: City, index: int) -> datetime:
        n = self.cfg.initial_customers_per_city
        day = self.start
        while True:
            s = self._daily_signups(city, day)
            if index < n + s:
                k = index - n
                rng = rng_for(self.cfg.seed, "signup-time", city.name, day.isoformat(), k)
                minute = int(rng.triangular(7 * 60, 23 * 60 + 59, 20 * 60))
                return local_midnight(day) + timedelta(minutes=minute, seconds=rng.randint(0, 59))
            n += s
            day += DAY

    def customers_signed_up_on(self, city: City, d: date) -> list[Customer]:
        before = (
            self.customer_count_by_end_of(city, d - DAY) if d > self.start else (self.cfg.initial_customers_per_city)
        )
        after = self.customer_count_by_end_of(city, d)
        return [self.customer(city, i) for i in range(before, after)]

    # ------------------------------------------------------------------ riders
    def riders(self, city: City) -> list[Rider]:
        rng = rng_for(self.cfg.seed, "rider-pool", city.name)
        riders = []
        start = local_midnight(self.start)
        pool = max(5, round(self.cfg.base_orders_per_city * city.demand_scale * self.cfg.riders_per_100_orders / 100))
        joins = [start - timedelta(days=rng.randint(1, 500)) for _ in range(pool)]
        t = start
        while t < start + timedelta(days=HORIZON_DAYS):
            t += timedelta(days=rng.expovariate(1 / 3.5))
            joins.append(t.replace(hour=rng.randint(8, 18), minute=rng.randint(0, 59), second=0))
        for i, joined in enumerate(joins):
            rid = f"D{city.code}{i:05d}"
            r = rng_for(self.cfg.seed, "rider", rid)
            left = joined + timedelta(days=r.expovariate(1 / 300)) if r.random() < 0.6 else None
            if left is not None and left < start:
                left = start + timedelta(days=r.randint(1, 120))  # attrition is observed after go-live
            riders.append(
                Rider(
                    rid,
                    city,
                    _pick(r, catalog.VEHICLE_MIX),
                    joined,
                    left,
                    r.choices(range(len(catalog.SHIFT_TEMPLATES)), catalog.SHIFT_MIX)[0],
                )
            )
        return riders

    # ------------------------------------------------------------------ conditions
    def hour_conditions(self, city: City, d: date, hour: int, riders_on_shift: int) -> HourConditions:
        block = hour // 3
        p_rain = city.rain_by_month[d.month - 1]
        raining = unit(self.cfg.seed, "rain", city.name, d.isoformat(), block) < p_rain
        heavy = raining and unit(self.cfg.seed, "rain-heavy", city.name, d.isoformat(), block) < 0.35
        if heavy:
            weather, rain_mm = "heavy_rain", round(8 + 30 * unit(self.cfg.seed, "mm", city.name, d, hour), 1)
        elif raining:
            weather, rain_mm = "light_rain", round(0.5 + 6 * unit(self.cfg.seed, "mm", city.name, d, hour), 1)
        elif unit(self.cfg.seed, "cloud", city.name, d.isoformat(), block) < 0.3:
            weather, rain_mm = "cloudy", 0.0
        else:
            weather, rain_mm = "clear", 0.0
        traffic = catalog.TRAFFIC_BY_HOUR[hour] + (1 if raining else 0) + (1 if heavy else 0)
        if d.weekday() >= 5 and hour < 12:
            traffic -= 1
        traffic = max(0, min(3, traffic))
        expected = self.expected_orders(city, d) * self.hourly_share(d, hour)
        capacity = max(1.0, riders_on_shift * 1.8)
        load = expected / capacity
        surge = 1.0 + 0.2 * raining + 0.25 * heavy + 0.35 * max(0.0, load - 1.0)
        if d in catalog.FESTIVALS and 19 <= hour <= 22:
            surge += 0.15
        surge = round(min(2.0, max(1.0, surge)) * 20) / 20
        return HourConditions(city.name, local_midnight(d) + timedelta(hours=hour), weather, rain_mm, traffic, surge)

    def hourly_share(self, d: date, hour: int) -> float:
        curve = catalog.HOURLY_DEMAND_WEEKEND if d.weekday() >= 5 else catalog.HOURLY_DEMAND_WEEKDAY
        return curve[hour] / sum(curve)

    def expected_orders(self, city: City, d: date) -> float:
        days = (d - self.start).days
        growth = 1 + 0.002 * max(0, days)
        festival = 1.3 if d in catalog.FESTIVALS else 1.0
        return (
            self.cfg.base_orders_per_city * city.demand_scale * catalog.WEEKDAY_FACTOR[d.weekday()] * growth * festival
        )
