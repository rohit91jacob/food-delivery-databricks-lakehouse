"""Local re-implementations of the AUTO CDC semantics used by the Lakeflow pipeline.

These run only offline (tests and ``fd local-run``); on Databricks the pipeline uses
``dp.create_auto_cdc_flow``. They follow the documented behaviour:

* SCD1: the latest record per key (by ``sequence_by``) wins; a winning DELETE removes the key.
* SCD2: a new version starts when a tracked column changes or the key reappears after a DELETE.
  Changes to untracked columns update the current version in place. A DELETE closes the
  current version (``__END_AT`` = the DELETE's sequence value).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

SEQ = "__seq"
IS_DELETE = "__is_delete"


def _prepare(df: DataFrame, sequence_by: Column, apply_as_deletes: Column | None) -> DataFrame:
    df = df.withColumn(SEQ, sequence_by)
    deletes = F.coalesce(apply_as_deletes, F.lit(False)) if apply_as_deletes is not None else F.lit(False)
    return df.withColumn(IS_DELETE, deletes)


def _finish(df: DataFrame, except_columns: list[str]) -> DataFrame:
    drop = [c for c in [*except_columns, SEQ, IS_DELETE] if c in df.columns]
    return df.drop(*drop)


def scd1(
    df: DataFrame,
    keys: list[str],
    sequence_by: Column,
    *,
    apply_as_deletes: Column | None = None,
    except_columns: list[str] | None = None,
) -> DataFrame:
    df = _prepare(df, sequence_by, apply_as_deletes)
    w = Window.partitionBy(*keys).orderBy(F.col(SEQ).desc())
    latest = df.withColumn("__rn", F.row_number().over(w)).where(F.col("__rn") == 1).drop("__rn")
    return _finish(latest.where(~F.col(IS_DELETE)), except_columns or [])


def scd2(
    df: DataFrame,
    keys: list[str],
    sequence_by: Column,
    *,
    track_history: list[str] | None = None,
    apply_as_deletes: Column | None = None,
    except_columns: list[str] | None = None,
) -> DataFrame:
    except_columns = except_columns or []
    df = _prepare(df, sequence_by, apply_as_deletes)
    tracked = track_history or [c for c in df.columns if c not in (*keys, SEQ, IS_DELETE, *except_columns)]
    w = Window.partitionBy(*keys).orderBy(SEQ)
    marked = (
        df.withColumn("__tracked", F.sha2(F.to_json(F.struct(*tracked)), 256))
        .withColumn("__prev_tracked", F.lag("__tracked").over(w))
        .withColumn("__prev_delete", F.lag(IS_DELETE).over(w))
    )
    starts_version = ~F.col(IS_DELETE) & (
        F.col("__prev_tracked").isNull()
        | F.coalesce(F.col("__prev_delete"), F.lit(False))
        | (F.col("__tracked") != F.col("__prev_tracked"))
    )
    boundaries = marked.where(starts_version | F.col(IS_DELETE)).withColumn("__is_start", starts_version)
    versions = (
        boundaries.withColumn("__END_AT", F.lead(SEQ).over(w))
        .where(F.col("__is_start"))
        .select(*keys, F.col(SEQ).alias("__START_AT"), "__END_AT")
    )
    u = marked.where(~F.col(IS_DELETE)).alias("u")
    v = versions.alias("v")
    cond = [F.col(f"u.{k}") == F.col(f"v.{k}") for k in keys] + [
        F.col(f"u.{SEQ}") >= F.col("v.__START_AT"),
        F.col("v.__END_AT").isNull() | (F.col(f"u.{SEQ}") < F.col("v.__END_AT")),
    ]
    joined = u.join(v, cond).select("u.*", "v.__START_AT", "v.__END_AT")
    lw = Window.partitionBy(*keys, "__START_AT").orderBy(F.col(SEQ).desc())
    out = (
        joined.withColumn("__rn", F.row_number().over(lw))
        .where(F.col("__rn") == 1)
        .drop("__rn", "__tracked", "__prev_tracked", "__prev_delete")
    )
    return _finish(out, except_columns)
