"""Geography helpers: city centroids, deterministic area locations and great-circle maths."""

from __future__ import annotations

import math
from dataclasses import dataclass

from fooddelivery.generator.rng import unit

EARTH_RADIUS_KM = 6371.0088


@dataclass(frozen=True)
class City:
    name: str
    code: str
    lat: float
    lon: float
    radius_km: float
    demand_scale: float
    # Probability that a given 3-hour block is rainy, by month (1..12).
    rain_by_month: tuple[float, ...]


# Approximate city-centre coordinates (public knowledge) and a coarse service radius.
CITIES: dict[str, City] = {
    c.name: c
    for c in (
        City(
            "Bangalore",
            "BLR",
            12.9716,
            77.5946,
            11.0,
            1.20,
            (0.02, 0.02, 0.05, 0.12, 0.25, 0.30, 0.30, 0.32, 0.38, 0.40, 0.25, 0.06),
        ),
        City(
            "Mumbai",
            "BOM",
            19.0760,
            72.8777,
            10.0,
            1.15,
            (0.00, 0.00, 0.00, 0.01, 0.05, 0.50, 0.70, 0.65, 0.45, 0.12, 0.03, 0.01),
        ),
        City(
            "Delhi",
            "DEL",
            28.6139,
            77.2090,
            14.0,
            1.10,
            (0.06, 0.06, 0.04, 0.03, 0.05, 0.12, 0.35, 0.35, 0.18, 0.03, 0.02, 0.03),
        ),
        City(
            "Hyderabad",
            "HYD",
            17.3850,
            78.4867,
            11.0,
            1.00,
            (0.01, 0.01, 0.02, 0.04, 0.08, 0.22, 0.30, 0.30, 0.32, 0.20, 0.06, 0.02),
        ),
        City(
            "Pune",
            "PNQ",
            18.5204,
            73.8567,
            9.0,
            0.80,
            (0.00, 0.00, 0.01, 0.03, 0.08, 0.40, 0.55, 0.50, 0.35, 0.12, 0.03, 0.01),
        ),
        City(
            "Chennai",
            "MAA",
            13.0827,
            80.2707,
            10.0,
            0.95,
            (0.08, 0.03, 0.02, 0.03, 0.05, 0.10, 0.15, 0.18, 0.20, 0.40, 0.55, 0.35),
        ),
        City(
            "Kolkata",
            "CCU",
            22.5726,
            88.3639,
            9.0,
            0.95,
            (0.02, 0.03, 0.05, 0.08, 0.15, 0.40, 0.50, 0.48, 0.38, 0.15, 0.03, 0.01),
        ),
        City(
            "Ahmedabad",
            "AMD",
            23.0225,
            72.5714,
            9.0,
            0.75,
            (0.00, 0.00, 0.00, 0.00, 0.02, 0.18, 0.40, 0.38, 0.22, 0.04, 0.01, 0.00),
        ),
        City(
            "Surat",
            "STV",
            21.1702,
            72.8311,
            7.0,
            0.60,
            (0.00, 0.00, 0.00, 0.00, 0.02, 0.25, 0.48, 0.42, 0.25, 0.05, 0.01, 0.00),
        ),
    )
}


def offset(lat: float, lon: float, distance_km: float, bearing_rad: float) -> tuple[float, float]:
    dlat = (distance_km / 111.32) * math.cos(bearing_rad)
    dlon = (distance_km / (111.32 * math.cos(math.radians(lat)))) * math.sin(bearing_rad)
    return lat + dlat, lon + dlon


def area_location(seed: int, city: City, area: str) -> tuple[float, float]:
    """A stable point for a neighbourhood, uniformly distributed over the city's disc."""
    r = city.radius_km * math.sqrt(unit(seed, "area-r", city.name, area))
    theta = 2 * math.pi * unit(seed, "area-theta", city.name, area)
    return offset(city.lat, city.lon, r, theta)


def jitter(seed: int, lat: float, lon: float, max_km: float, *scope: object) -> tuple[float, float]:
    r = max_km * math.sqrt(unit(seed, "jitter-r", *scope))
    theta = 2 * math.pi * unit(seed, "jitter-theta", *scope)
    return offset(lat, lon, r, theta)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(a)))


def approx_km(lat1: float, lon1: float, lat2: float, lon2: float, cos_lat: float) -> float:
    """Equirectangular distance: within a city it is within 0.1% of haversine and far cheaper."""
    dx = (lon2 - lon1) * cos_lat * 111.32
    dy = (lat2 - lat1) * 110.57
    return math.sqrt(dx * dx + dy * dy)


def interpolate(lat1: float, lon1: float, lat2: float, lon2: float, f: float) -> tuple[float, float]:
    f = max(0.0, min(1.0, f))
    return lat1 + (lat2 - lat1) * f, lon1 + (lon2 - lon1) * f
