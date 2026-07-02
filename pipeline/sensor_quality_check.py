from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.config import settings
from pipeline.spark import get_spark

def _expected_reading_bounds() -> tuple[int, int]:
    """Derive expected daily reading count from config. ±20% tolerance for drift."""
    readings_per_day = settings.expected_sensor_count * (86400 / settings.fetch_interval_seconds)
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
            lines.append(f"  ✗ {f}")
        return "\n".join(lines)


def check_silver_quality(
    silver_prefix: str,
    execution_date: datetime | None = None,
    spark: SparkSession | None = None,
) -> QualityReport:
    """Run data quality checks against a Silver partition.

    Raises RuntimeError if any check fails — this causes Airflow to
    mark the task FAILED and prevents bad data from reaching Gold.
    """
    dt = execution_date or datetime.now(timezone.utc)
    own_spark = spark is None
    if own_spark:
        spark = get_spark()

    df = spark.read.parquet(silver_prefix)
    report = QualityReport()

    # ── Completeness ──────────────────────────────────────────────────────────
    expected_min, expected_max = _expected_reading_bounds()
    total = df.count()
    if total < expected_min:
        report.fail(f"reading_count {total} below minimum {expected_min}")
    if total > expected_max:
        report.fail(f"reading_count {total} above maximum {expected_max}")

    # ── Nulls on required fields ───────────────────────────────────────────────
    required = ["sensor_id", "zone_id", "timestamp", "temperature_f", "humidity_pct",
                "wind_speed_mph", "pm25_ugm3", "battery_pct"]
    null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in required]).collect()[0]
    for col in required:
        if null_counts[col] > 0:
            report.fail(f"null values in required column '{col}': {null_counts[col]} rows")

    # ── Value range checks ────────────────────────────────────────────────────
    range_checks = [
        ("temperature_f",  "temperature_f < -60 OR temperature_f > 160"),
        ("humidity_pct",   "humidity_pct < 0 OR humidity_pct > 100"),
        ("wind_speed_mph", "wind_speed_mph < 0 OR wind_speed_mph > 200"),
        ("pm25_ugm3",      "pm25_ugm3 < 0"),
        ("battery_pct",    "battery_pct < 0 OR battery_pct > 100"),
    ]
    for col_name, condition in range_checks:
        bad = df.filter(condition).count()
        if bad > 0:
            report.fail(f"out-of-range values in '{col_name}': {bad} rows ({condition})")

    # ── Duplicate sensor + timestamp combinations ─────────────────────────────
    duplicates = (
        df.groupBy("sensor_id", "timestamp")
        .count()
        .filter("count > 1")
        .count()
    )
    if duplicates > 0:
        report.fail(f"duplicate sensor_id + timestamp combinations: {duplicates}")

    # ── All expected sensors present ──────────────────────────────────────────
    actual_sensors = df.select("sensor_id").distinct().count()
    if actual_sensors < settings.expected_sensor_count:
        report.fail(f"only {actual_sensors}/{settings.expected_sensor_count} sensors reported data")

    print(str(report))

    if own_spark:
        spark.stop()

    if not report.passed:
        raise RuntimeError(
            f"Silver quality check failed for {dt.date()} — "
            f"{len(report.failures)} issue(s):\n" +
            "\n".join(f"  • {f}" for f in report.failures)
        )

    return report
