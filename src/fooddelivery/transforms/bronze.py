"""Bronze helpers shared by the Lakeflow pipeline (Auto Loader) and the local lakehouse (batch JSON)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RESCUED_COLUMN = "_rescued_data"


def autoloader_options() -> dict[str, str]:
    """Auto Loader options for the landing feeds. Each file is written once per generator version."""
    return {
        "cloudFiles.format": "json",
        "rescuedDataColumn": RESCUED_COLUMN,
        # explicit schema: nothing to infer, and the dt=YYYY-MM-DD folders are not data columns
        "cloudFiles.partitionColumns": "",
        "cloudFiles.includeExistingFiles": "true",
    }


def with_ingest_metadata(df: DataFrame) -> DataFrame:
    """Lineage columns on every bronze row: the source file and the ingestion timestamp."""
    return df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())
