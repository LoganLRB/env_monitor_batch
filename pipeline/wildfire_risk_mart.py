from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.config import settings
from pipeline.spark import get_spark


def build_wildfire_risk_mart(
    silver_prefix: str,
    execution_date: datetime | None = None,
    spark: SparkSession | None = None,
) -> str:
    """Silver Parquet → Gold wildfire risk mart.

    Zone-level daily aggregations consumed by ranger dashboards
    and emergency response systems.
    """
    dt = execution_date or datetime.now(timezone.utc)
    own_spark = spark is None
    if own_spark:
        spark = get_spark()

    df = spark.read.parquet(silver_prefix)
    alert_int = F.col("is_alert").cast("int")

    mart = (
        df.groupBy("zone_id", "zone_name", F.to_date("timestamp").alias("date"))
        .agg(
            F.count("*").alias("reading_count"),
            F.round(F.avg("temperature_f"), 2).alias("avg_temp_f"),
            F.round(F.max("temperature_f"), 2).alias("max_temp_f"),
            F.round(F.min("humidity_pct"), 2).alias("min_humidity_pct"),
            F.round(F.avg("humidity_pct"), 2).alias("avg_humidity_pct"),
            F.round(F.max("wind_speed_mph"), 2).alias("max_wind_mph"),
            F.round(F.avg("wind_speed_mph"), 2).alias("avg_wind_mph"),
            F.round(F.max("pm25_ugm3"), 2).alias("max_pm25"),
            F.round(F.avg("pm25_ugm3"), 2).alias("avg_pm25"),
            F.sum(alert_int).alias("alert_count"),
            F.round(F.avg(alert_int.cast("double")) * 100, 1).alias("alert_pct"),
        )
        .orderBy("date", "zone_id")
    )

    date_part = f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    out_prefix = f"{settings.gold_prefix}/wildfire_risk/{date_part}"

    mart.write.mode("overwrite").parquet(out_prefix)

    print(f"[wildfire_risk_mart] written → {out_prefix}")

    if own_spark:
        spark.stop()

    return out_prefix
