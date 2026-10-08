"""Bronze: one Auto Loader streaming table per landing feed.

Raw, append-only and replayable from the landing volume, which is the system of record. Rows
that don't fit the explicit schema keep their payload in ``_rescued_data``.
"""

from pyspark import pipelines as dp

from fooddelivery.transforms.bronze import autoloader_options, with_ingest_metadata
from fooddelivery.transforms.schemas import LANDING_FOLDERS, RAW_SCHEMAS

LANDING_PATH = spark.conf.get("fooddelivery.landing_path")


def define_bronze(entity: str) -> None:
    @dp.table(
        name=f"bronze_{entity}",
        comment=f"Raw `{entity}` records ingested by Auto Loader from {LANDING_PATH}/{LANDING_FOLDERS[entity]}.",
        table_properties={"quality": "bronze"},
    )
    def bronze():
        reader = spark.readStream.format("cloudFiles")
        for key, value in autoloader_options().items():
            reader = reader.option(key, value)
        return with_ingest_metadata(
            reader.schema(RAW_SCHEMAS[entity]).load(f"{LANDING_PATH}/{LANDING_FOLDERS[entity]}/")
        )


for _entity in RAW_SCHEMAS:
    define_bronze(_entity)
