"""Stable randomness.

Every random decision is drawn from a ``random.Random`` seeded by hashing the global seed
together with a *scope* (entity id, business date, ...). That makes each business date
reproducible on its own, so a backfill or rerun of one date never depends on which other
dates were generated before it, or in what order.
"""

from __future__ import annotations

import hashlib
import math
import random


def stable_seed(*parts: object) -> int:
    digest = hashlib.sha256("\x1f".join(str(p) for p in parts).encode()).digest()
    return int.from_bytes(digest[:8], "big")


def rng_for(*parts: object) -> random.Random:
    return random.Random(stable_seed(*parts))


def unit(*parts: object) -> float:
    """A stable pseudo-random float in [0, 1) for the given scope."""
    return stable_seed(*parts) / 2**64


def lognormal_factor(rng: random.Random, sigma: float) -> float:
    """A multiplicative noise factor with median 1.0."""
    return math.exp(rng.gauss(0.0, sigma))


def weighted_index(rng: random.Random, cumulative: list[float]) -> int:
    """Index drawn proportionally to weights given as a cumulative list."""
    import bisect

    total = cumulative[-1]
    return min(bisect.bisect_right(cumulative, rng.random() * total), len(cumulative) - 1)
