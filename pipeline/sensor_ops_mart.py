from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.config import settings
from pipeline.spark import get_spark


def build_sensor_ops_mart(
    silver_prefix: str,
    execution_date: datetime | None = None,
    spark: SparkSession | None = None,
) -> str:
    """Silver Parquet -> Gold sensor ops mart.

    Per-sensor daily stats consumed by ops and maintenance teams to track
    sensor health, battery levels, and data completeness.
    """
    dt = execution_date or datetime.now(timezone.utc)
    own_spark = spark is None
    if own_spark:
        spark = get_spark()

    df = spark.read.parquet(silver_prefix)
    alert_int = F.col("is_alert").cast("int")

    mart = (
        df.groupBy("sensor_id", "zone_id", F.to_date("timestamp").alias("date"))
        .agg(
            F.count("*").alias("reading_count"),
            F.round(F.avg("temperature_f"), 2).alias("avg_temp_f"),
            F.round(F.max("temperature_f"), 2).alias("max_temp_f"),
            F.round(F.min("humidity_pct"), 2).alias("min_humidity_pct"),
            F.round(F.max("pm25_ugm3"), 2).alias("max_pm25"),
            F.round(F.min("battery_pct"), 1).alias("min_battery_pct"),
            F.sum(alert_int).alias("alert_count"),
            F.round(F.first("latitude", ignorenulls=True), 6).alias("latitude"),
            F.round(F.first("longitude", ignorenulls=True), 6).alias("longitude"),
        )
    )

    date_part = f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    out_prefix = f"{settings.gold_prefix}/sensor_ops/{date_part}"

    mart.write.mode("overwrite").parquet(out_prefix)

    print(f"[sensor_ops_mart] written -> {out_prefix}")

    if own_spark:
        spark.stop()

    return out_prefix
