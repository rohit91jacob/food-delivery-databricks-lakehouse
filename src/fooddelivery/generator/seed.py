"""Restaurant seed: normalised from the Kaggle Swiggy dataset, or synthesised as a fallback.

The seed is the only real-world input. It gives the simulator realistic restaurant names,
neighbourhoods, cuisines, price bands, ratings, popularity and listed delivery times.
"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from fooddelivery.generator.geo import CITIES
from fooddelivery.generator.rng import rng_for

KAGGLE_DATASET = "abhijitdahatonde/swiggy-restuarant-dataset"
KAGGLE_FILE = "swiggy.csv"
KAGGLE_LICENSE = "CC0-1.0"
EXPECTED_COLUMNS = (
    "ID",
    "Area",
    "City",
    "Restaurant",
    "Price",
    "Avg ratings",
    "Total ratings",
    "Food type",
    "Address",
    "Delivery time",
)


@dataclass(frozen=True)
class RestaurantSeed:
    source_id: str
    name: str
    city: str
    area: str
    address: str
    cuisines: tuple[str, ...]
    price_for_two: int  # rupees
    rating: float
    rating_count: int
    listed_delivery_minutes: int

    def to_json(self) -> dict:
        d = asdict(self)
        d["cuisines"] = list(self.cuisines)
        return d

    @classmethod
    def from_json(cls, d: dict) -> RestaurantSeed:
        return cls(**{**d, "cuisines": tuple(d["cuisines"])})


class SeedError(ValueError):
    pass


def _clean(text: str) -> str:
    return " ".join(text.replace("\u00a0", " ").split())


def normalize_swiggy_rows(rows: list[dict[str, str]]) -> list[RestaurantSeed]:
    """Validate and normalise raw Kaggle rows. Rows that cannot be repaired are dropped."""
    if rows and set(EXPECTED_COLUMNS) - set(rows[0]):
        raise SeedError(f"unexpected columns: missing {sorted(set(EXPECTED_COLUMNS) - set(rows[0]))}")
    seen: set[tuple[str, str, str]] = set()
    out: list[RestaurantSeed] = []
    prices_by_city: dict[str, list[int]] = {}
    parsed = []
    for row in rows:
        city = _clean(row["City"]).title()
        if city not in CITIES:
            continue
        name = _clean(row["Restaurant"])
        area = _clean(row["Area"]).title()
        if not name or not area:
            continue
        key = (city, area.lower(), name.lower())
        if key in seen:
            continue
        seen.add(key)
        try:
            price = int(float(row["Price"] or 0))
            rating = float(row["Avg ratings"] or 0)
            rating_count = int(float(row["Total ratings"] or 0))
            delivery = int(float(row["Delivery time"] or 0))
        except ValueError:
            continue
        cuisines = tuple(dict.fromkeys(c.strip().title() for c in row["Food type"].split(",") if c.strip()))
        parsed.append((row, city, name, area, price, rating, rating_count, delivery, cuisines[:4]))
        if price > 0:
            prices_by_city.setdefault(city, []).append(price)
    medians = {c: sorted(p)[len(p) // 2] for c, p in prices_by_city.items()}
    for row, city, name, area, price, rating, rating_count, delivery, cuisines in parsed:
        out.append(
            RestaurantSeed(
                source_id=str(row["ID"]).strip(),
                name=name,
                city=city,
                area=area,
                address=_clean(row["Address"]),
                cuisines=cuisines or ("North Indian",),
                price_for_two=price if 50 <= price <= 5000 else medians.get(city, 300),
                rating=min(5.0, max(1.0, rating if rating > 0 else 3.8)),
                rating_count=max(rating_count, 1),
                listed_delivery_minutes=min(120, max(15, delivery or 45)),
            )
        )
    out.sort(key=lambda s: (s.city, s.source_id, s.name))
    return out


def load_swiggy_csv(path: Path) -> list[RestaurantSeed]:
    with path.open(newline="", encoding="utf-8") as fh:
        return normalize_swiggy_rows(list(csv.DictReader(fh)))


_ADJ = ("Royal", "Spice", "Green", "Urban", "Golden", "Little", "Grand", "Desi", "Coastal", "Midnight")
_NOUN = ("Kitchen", "Bowl", "House", "Bistro", "Dhaba", "Cafe", "Express", "Tandoor", "Wok", "Table")
_AREAS = ("Central", "North", "South", "East", "West", "Old Town", "Tech Park", "Lakeside", "Market", "Station")


def synthetic_seed(cities: tuple[str, ...], per_city: int, seed: int = 7) -> list[RestaurantSeed]:
    """A Kaggle-free seed with the same shape, for CI and for users without a Kaggle token."""
    from fooddelivery.generator.catalog import DISHES

    cuisines = sorted(DISHES)
    out = []
    for city in cities:
        rng = rng_for(seed, "synthetic-seed", city)
        for i in range(per_city):
            cs = tuple(rng.sample(cuisines, k=rng.choice((1, 2, 2, 3))))
            out.append(
                RestaurantSeed(
                    source_id=f"SYN-{CITIES[city].code}-{i:04d}",
                    name=f"{rng.choice(_ADJ)} {rng.choice(_NOUN)} {i:03d}",
                    city=city,
                    area=rng.choice(_AREAS),
                    address=f"{rng.randint(1, 300)} Main Road",
                    cuisines=cs,
                    price_for_two=rng.choice((150, 200, 250, 300, 350, 400, 500, 600, 800, 1200)),
                    rating=round(rng.uniform(3.2, 4.8), 1),
                    rating_count=rng.choice((20, 50, 100, 500, 1000, 5000)),
                    listed_delivery_minutes=rng.randint(25, 75),
                )
            )
    return out


def write_seed(seeds: list[RestaurantSeed], path: Path, *, source: str, license_name: str) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(json.dumps(s.to_json(), sort_keys=True) + "\n" for s in seeds).encode()
    path.write_bytes(payload)
    meta = {
        "source": source,
        "license": license_name,
        "restaurants": len(seeds),
        "cities": sorted({s.city for s in seeds}),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    path.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    return meta


def read_seed(path: Path) -> list[RestaurantSeed]:
    with path.open(encoding="utf-8") as fh:
        return [RestaurantSeed.from_json(json.loads(line)) for line in fh if line.strip()]
