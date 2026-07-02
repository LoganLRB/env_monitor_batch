import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType,
    StructField, StructType, TimestampType,
)

SILVER_SCHEMA = StructType([
    StructField("sensor_id", StringType()),
    StructField("zone_id", StringType()),
    StructField("zone_name", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("temperature_f", DoubleType()),
    StructField("humidity_pct", DoubleType()),
    StructField("wind_speed_mph", DoubleType()),
    StructField("pm25_ugm3", DoubleType()),
    StructField("battery_pct", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("wildfire_risk", StringType()),
    StructField("is_alert", BooleanType()),
])

_ts = lambda s: datetime.fromisoformat(s)

# 12 sensors × 2 readings = 24 rows (below min threshold for real data, but
# quality check thresholds are patched per test)
_GOOD_ROWS = [
    Row(f"SNS-{i:03d}", "zone-a", "Northern Forest Ridge", _ts("2026-07-01T08:00:00"),
        82.5, 45.0, 8.0, 12.0, 91.0, 37.51, -119.53, "LOW", False)
    for i in range(1, 13)
]


class TestSensorQualityCheck(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("test-quality-check")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _write_silver(self, rows, tmp_path):
        path = str(tmp_path / "silver")
        self.spark.createDataFrame(rows, schema=SILVER_SCHEMA).write.mode("overwrite").parquet(path)
        return path

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    @patch("pipeline.sensor_quality_check._EXPECTED_MIN_READINGS", 1)
    @patch("pipeline.sensor_quality_check._EXPECTED_MAX_READINGS", 9999)
    def test_passes_on_clean_data(self):
        from pipeline.sensor_quality_check import check_silver_quality

        silver = self._write_silver(_GOOD_ROWS, self.tmp_path)
        report = check_silver_quality(silver, execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc), spark=self.spark)
        self.assertTrue(report.passed)

    @patch("pipeline.sensor_quality_check._EXPECTED_MIN_READINGS", 1)
    @patch("pipeline.sensor_quality_check._EXPECTED_MAX_READINGS", 9999)
    def test_fails_on_out_of_range_temperature(self):
        from pipeline.sensor_quality_check import check_silver_quality

        bad_rows = [
            Row("SNS-001", "zone-a", "Northern Forest Ridge", _ts("2026-07-01T08:00:00"),
                -999.0, 45.0, 8.0, 12.0, 91.0, 37.51, -119.53, "LOW", False)
        ]
        silver = self._write_silver(bad_rows, self.tmp_path)
        with self.assertRaises(RuntimeError):
            check_silver_quality(silver, execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc), spark=self.spark)

    @patch("pipeline.sensor_quality_check._EXPECTED_MIN_READINGS", 1)
    @patch("pipeline.sensor_quality_check._EXPECTED_MAX_READINGS", 9999)
    def test_fails_on_duplicate_sensor_timestamp(self):
        from pipeline.sensor_quality_check import check_silver_quality

        ts = _ts("2026-07-01T08:00:00")
        dup_rows = [
            Row("SNS-001", "zone-a", "Northern Forest Ridge", ts, 82.5, 45.0, 8.0, 12.0, 91.0, 37.51, -119.53, "LOW", False),
            Row("SNS-001", "zone-a", "Northern Forest Ridge", ts, 83.0, 44.0, 9.0, 11.0, 90.0, 37.51, -119.53, "LOW", False),
        ]
        silver = self._write_silver(dup_rows, self.tmp_path)
        with self.assertRaises(RuntimeError):
            check_silver_quality(silver, execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc), spark=self.spark)

    def test_fails_when_below_minimum_reading_count(self):
        from pipeline.sensor_quality_check import check_silver_quality

        silver = self._write_silver(_GOOD_ROWS[:1], self.tmp_path)
        with self.assertRaises(RuntimeError):
            check_silver_quality(silver, execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc), spark=self.spark)


if __name__ == "__main__":
    unittest.main()
