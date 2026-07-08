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

SILVER_ROWS = [
    Row("SNS-001", "zone-a", "Northern Forest Ridge", _ts("2026-07-01T08:00:00"), 82.5,  45.0, 8.0,  12.0,  91.0, 37.51, -119.53, "LOW",      False),
    Row("SNS-001", "zone-a", "Northern Forest Ridge", _ts("2026-07-01T09:00:00"), 88.0,  38.0, 10.0, 15.0,  90.5, 37.51, -119.53, "MODERATE", False),
    Row("SNS-002", "zone-a", "Northern Forest Ridge", _ts("2026-07-01T08:00:00"), 112.0, 11.0, 25.0, 200.0, 88.0, 37.52, -119.52, "CRITICAL", True),
    Row("SNS-002", "zone-a", "Northern Forest Ridge", _ts("2026-07-01T09:00:00"), 114.0,  9.0, 28.0, 250.0, 87.5, 37.52, -119.52, "CRITICAL", True),
]


class TestWildfireRiskMart(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("test-wildfire-risk-mart")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)
        silver_path = str(self.tmp_path / "silver")
        self.spark.createDataFrame(SILVER_ROWS, schema=SILVER_SCHEMA).write.mode("overwrite").parquet(silver_path)
        self.silver_prefix = silver_path

    def tearDown(self):
        self.tmp.cleanup()

    @patch("pipeline.wildfire_risk_mart.settings")
    def test_one_row_per_zone_per_day(self, mock_settings):
        mock_settings.gold_prefix = str(self.tmp_path / "gold")

        from pipeline.wildfire_risk_mart import build_wildfire_risk_mart

        prefix = build_wildfire_risk_mart(self.silver_prefix, execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc), spark=self.spark)
        self.assertEqual(self.spark.read.parquet(prefix).count(), 1)

    @patch("pipeline.wildfire_risk_mart.settings")
    def test_alert_count(self, mock_settings):
        mock_settings.gold_prefix = str(self.tmp_path / "gold")

        from pipeline.wildfire_risk_mart import build_wildfire_risk_mart

        prefix = build_wildfire_risk_mart(self.silver_prefix, execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc), spark=self.spark)
        row = self.spark.read.parquet(prefix).collect()[0]
        self.assertEqual(row["alert_count"], 2)

    @patch("pipeline.wildfire_risk_mart.settings")
    def test_max_temp(self, mock_settings):
        mock_settings.gold_prefix = str(self.tmp_path / "gold")

        from pipeline.wildfire_risk_mart import build_wildfire_risk_mart

        prefix = build_wildfire_risk_mart(self.silver_prefix, execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc), spark=self.spark)
        self.assertEqual(self.spark.read.parquet(prefix).collect()[0]["max_temp_f"], 114.0)


if __name__ == "__main__":
    unittest.main()
