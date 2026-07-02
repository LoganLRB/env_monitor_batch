from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from pipeline.config import settings
from pipeline.spark import get_spark


def transform_sensor_readings(
    bronze_uri: str,
    execution_date: datetime | None = None,
    spark: SparkSession | None = None,
) -> str:
    """Bronze JSON → Silver Parquet.

    Explodes the top-level ``readings`` array, casts numeric columns,
    and adds a boolean ``is_alert`` column for HIGH/CRITICAL risk readings.
    """
    dt = execution_date or datetime.now(timezone.utc)
    own_spark = spark is None
    if own_spark:
        spark = get_spark()

    df = spark.read.option("multiline", "true").json(bronze_uri)
    df = df.select(F.explode("readings").alias("r")).select("r.*")

    df = (
        df.withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("temperature_f", F.round(F.col("temperature_f").cast(DoubleType()), 2))
        .withColumn("humidity_pct", F.round(F.col("humidity_pct").cast(DoubleType()), 2))
        .withColumn("wind_speed_mph", F.round(F.col("wind_speed_mph").cast(DoubleType()), 2))
        .withColumn("pm25_ugm3", F.round(F.col("pm25_ugm3").cast(DoubleType()), 2))
        .withColumn("battery_pct", F.round(F.col("battery_pct").cast(DoubleType()), 1))
        .withColumn("is_alert", F.col("wildfire_risk").isin("HIGH", "CRITICAL"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    out_prefix = (
        f"{settings.silver_prefix}"
        f"/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    )

    df.write.mode("overwrite").parquet(out_prefix)

    print(f"[sensor_transform] Silver written → {out_prefix}")

    if own_spark:
        spark.stop()

    return out_prefix
