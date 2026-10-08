from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import functions as F

from fooddelivery.local.scd import scd1, scd2

pytestmark = pytest.mark.spark

T = datetime.fromisoformat


def test_scd1_latest_wins_and_delete_removes(spark):
    df = spark.createDataFrame(
        [
            ("a", "UPSERT", T("2026-09-01 00:00:00"), 1),
            ("a", "UPSERT", T("2026-09-02 00:00:00"), 2),
            ("b", "UPSERT", T("2026-09-01 00:00:00"), 5),
            ("b", "DELETE", T("2026-09-03 00:00:00"), 5),
        ],
        "k STRING, op STRING, ts TIMESTAMP, v INT",
    )
    out = scd1(df, ["k"], F.col("ts"), apply_as_deletes=F.expr("op = 'DELETE'"), except_columns=["op"])
    assert [(r.k, r.v) for r in out.collect()] == [("a", 2)]
    assert "op" not in out.columns


def test_scd2_tracks_history_and_updates_untracked_in_place(spark):
    df = spark.createDataFrame(
        [
            ("r1", "UPSERT", T("2026-09-01 00:00:00"), 300, 4.1),
            ("r1", "UPSERT", T("2026-09-07 03:00:00"), 300, 4.3),  # rating only: in place
            ("r1", "UPSERT", T("2026-09-10 05:00:00"), 350, 4.3),  # price: new version
            ("r1", "DELETE", T("2026-09-20 02:00:00"), 350, 4.3),  # closes the version
            ("r2", "UPSERT", T("2026-09-01 00:00:00"), 200, 3.9),
        ],
        "k STRING, op STRING, ts TIMESTAMP, price INT, rating DOUBLE",
    )
    out = (
        scd2(
            df,
            ["k"],
            F.col("ts"),
            track_history=["price"],
            apply_as_deletes=F.expr("op = 'DELETE'"),
            except_columns=["op"],
        )
        .orderBy("k", "__START_AT")
        .collect()
    )
    assert [(r.k, r.price, r.rating, r["__START_AT"], r["__END_AT"]) for r in out] == [
        ("r1", 300, 4.3, T("2026-09-01 00:00:00"), T("2026-09-10 05:00:00")),
        ("r1", 350, 4.3, T("2026-09-10 05:00:00"), T("2026-09-20 02:00:00")),
        ("r2", 200, 3.9, T("2026-09-01 00:00:00"), None),
    ]
