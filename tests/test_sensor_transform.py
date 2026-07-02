import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

SAMPLE_PAYLOAD = {
    "sensor_count": 2,
    "reading_count": 2,
    "hours": 1,
    "interval_seconds": 3600,
    "readings": [
        {
            "sensor_id": "SNS-001",
            "zone_id": "zone-a",
            "zone_name": "Northern Forest Ridge",
            "timestamp": "2026-07-01T00:00:00+00:00",
            "temperature_f": 82.5,
            "humidity_pct": 45.0,
            "wind_speed_mph": 8.0,
            "pm25_ugm3": 12.0,
            "battery_pct": 91.0,
            "latitude": 37.5123,
            "longitude": -119.5341,
            "wildfire_risk": "LOW",
        },
        {
            "sensor_id": "SNS-002",
            "zone_id": "zone-a",
            "zone_name": "Northern Forest Ridge",
            "timestamp": "2026-07-01T00:00:00+00:00",
            "temperature_f": 112.0,
            "humidity_pct": 11.0,
            "wind_speed_mph": 25.0,
            "pm25_ugm3": 200.0,
            "battery_pct": 88.0,
            "latitude": 37.5187,
            "longitude": -119.5218,
            "wildfire_risk": "CRITICAL",
        },
    ],
}


class TestTransformSensorReadings(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("test-sensor-transform")
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
        self.bronze_file = self.tmp_path / "readings.json"
        self.bronze_file.write_text(json.dumps(SAMPLE_PAYLOAD))

    def tearDown(self):
        self.tmp.cleanup()

    @patch("pipeline.sensor_transform.settings")
    def test_row_count(self, mock_settings):
        mock_settings.silver_prefix = str(self.tmp_path / "silver")

        from pipeline.sensor_transform import transform_sensor_readings

        prefix = transform_sensor_readings(str(self.bronze_file), execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc), spark=self.spark)
        self.assertEqual(self.spark.read.parquet(prefix).count(), 2)

    @patch("pipeline.sensor_transform.settings")
    def test_is_alert_column(self, mock_settings):
        mock_settings.silver_prefix = str(self.tmp_path / "silver")

        from pipeline.sensor_transform import transform_sensor_readings

        prefix = transform_sensor_readings(str(self.bronze_file), execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc), spark=self.spark)
        rows = {r["sensor_id"]: r["is_alert"] for r in self.spark.read.parquet(prefix).collect()}
        self.assertFalse(rows["SNS-001"])
        self.assertTrue(rows["SNS-002"])

    @patch("pipeline.sensor_transform.settings")
    def test_has_ingested_at(self, mock_settings):
        mock_settings.silver_prefix = str(self.tmp_path / "silver")

        from pipeline.sensor_transform import transform_sensor_readings

        prefix = transform_sensor_readings(str(self.bronze_file), execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc), spark=self.spark)
        self.assertIn("ingested_at", self.spark.read.parquet(prefix).columns)

    @patch("pipeline.sensor_transform.settings")
    def test_output_path_date_partitioned(self, mock_settings):
        mock_settings.silver_prefix = str(self.tmp_path / "silver")

        from pipeline.sensor_transform import transform_sensor_readings

        prefix = transform_sensor_readings(str(self.bronze_file), execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc), spark=self.spark)
        self.assertIn("year=2026/month=07/day=01", prefix)


if __name__ == "__main__":
    unittest.main()
