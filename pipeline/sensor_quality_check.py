from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.config import SECONDS_PER_DAY, settings
from pipeline.spark import get_spark


def _expected_reading_bounds() -> tuple[int, int]:
    readings_per_day = settings.expected_sensor_count * (SECONDS_PER_DAY / settings.fetch_interval_seconds)
    return int(readings_per_day * 0.80), int(readings_per_day * 1.20)


@dataclass
class QualityReport:
    passed: bool = True
    failures: list[str] = field(default_factory=list)

    def fail(self, message: str) -> None:
        self.passed = False
        self.failures.append(message)

    def __str__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        lines = [f"[quality_check] {status}"]
        for f in self.failures:
            lines.append(f"  x {f}")
        return "\n".join(lines)


_REQUIRED_COLUMNS = [
    "sensor_id", "zone_id", "timestamp", "temperature_f",
    "humidity_pct", "wind_speed_mph", "pm25_ugm3", "battery_pct",
]

# (column_name, SQL expression string for out-of-range condition)
# Using strings avoids calling F.col() at import time, which requires an active SparkContext.
_RANGE_CHECKS: list[tuple[str, str]] = [
    ("temperature_f",  "temperature_f < -60 OR temperature_f > 160"),
    ("humidity_pct",   "humidity_pct < 0 OR humidity_pct > 100"),
    ("wind_speed_mph", "wind_speed_mph < 0 OR wind_speed_mph > 200"),
    ("pm25_ugm3",      "pm25_ugm3 < 0"),
    ("battery_pct",    "battery_pct < 0 OR battery_pct > 100"),
]


def check_silver_quality(
    silver_prefix: str,
    execution_date: datetime | None = None,
    spark: SparkSession | None = None,
) -> QualityReport:
    """Run data quality checks against a Silver partition.

    Raises RuntimeError if any check fails, causing Airflow to mark the task
    FAILED and preventing bad data from reaching Gold.
    """
    dt = execution_date or datetime.now(timezone.utc)
    own_spark = spark is None
    if own_spark:
        spark = get_spark()

    df = spark.read.parquet(silver_prefix)
    report = QualityReport()

    # Single aggregation pass: completeness, nulls, range violations, sensor count.
    # Duplicate check requires a separate groupBy and cannot be folded in here.
    agg_exprs = [
        F.count("*").alias("total"),
        F.countDistinct("sensor_id").alias("unique_sensors"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in _REQUIRED_COLUMNS],
        *[F.sum(F.when(F.expr(cond), 1).otherwise(0)).alias(f"bad_{col}") for col, cond in _RANGE_CHECKS],
    ]
    row = df.agg(*agg_exprs).collect()[0]

    duplicates = (
        df.groupBy("sensor_id", "timestamp")
        .count()
        .filter("count > 1")
        .count()
    )

    expected_min, expected_max = _expected_reading_bounds()
    total = row["total"]
    if total < expected_min:
        report.fail(f"reading_count {total} below minimum {expected_min}")
    if total > expected_max:
        report.fail(f"reading_count {total} above maximum {expected_max}")

    for col in _REQUIRED_COLUMNS:
        null_count = row[f"null_{col}"]
        if null_count > 0:
            report.fail(f"null values in required column '{col}': {null_count} rows")

    for col_name, condition_str in _RANGE_CHECKS:
        bad = row[f"bad_{col_name}"]
        if bad > 0:
            report.fail(f"out-of-range values in '{col_name}': {bad} rows ({condition_str})")

    if duplicates > 0:
        report.fail(f"duplicate sensor_id + timestamp combinations: {duplicates}")

    if row["unique_sensors"] < settings.expected_sensor_count:
        report.fail(f"only {row['unique_sensors']}/{settings.expected_sensor_count} sensors reported data")

    print(str(report))

    if own_spark:
        spark.stop()

    if not report.passed:
        raise RuntimeError(
            f"Silver quality check failed for {dt.date()}: "
            f"{len(report.failures)} issue(s):\n"
            + "\n".join(f"  * {f}" for f in report.failures)
        )

    return report
