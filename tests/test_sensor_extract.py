import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import boto3
from moto import mock_aws

SAMPLE_PAYLOAD = {
    "sensor_count": 2,
    "reading_count": 2,
    "start": "2026-07-01T00:00:00+00:00",
    "end": "2026-07-02T00:00:00+00:00",
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

_BUCKET = "test-data-lake"
_PREFIX = "sensor-data"


def _mock_http_client():
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_PAYLOAD
    mock_resp.raise_for_status.return_value = None
    mock_client = MagicMock()
    mock_client.__enter__ = lambda s: s
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.return_value = mock_resp
    return mock_client


class TestExtractSensorReadings(unittest.TestCase):
    def _patch_settings(self, mock_settings):
        mock_settings.api_base_url = "http://testhost"
        mock_settings.fetch_interval_seconds = 3600
        mock_settings.s3_bucket = _BUCKET
        mock_settings.s3_prefix = _PREFIX

    @mock_aws
    def test_writes_to_s3(self):
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=_BUCKET)

        with patch("pipeline.sensor_extract.settings") as ms, \
             patch("pipeline.sensor_extract.httpx.Client") as mc:
            self._patch_settings(ms)
            mc.return_value = _mock_http_client()

            from pipeline.sensor_extract import extract_sensor_readings

            s3_uri = extract_sensor_readings(
                execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
                interval_seconds=3600,
            )

        self.assertTrue(s3_uri.startswith(f"s3a://{_BUCKET}/"))
        key = s3_uri.removeprefix(f"s3a://{_BUCKET}/")
        payload = json.loads(
            boto3.client("s3", region_name="us-east-1")
            .get_object(Bucket=_BUCKET, Key=key)["Body"]
            .read()
        )
        self.assertEqual(payload["reading_count"], 2)

    @mock_aws
    def test_s3_key_partitioned_by_date(self):
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=_BUCKET)

        with patch("pipeline.sensor_extract.settings") as ms, \
             patch("pipeline.sensor_extract.httpx.Client") as mc:
            self._patch_settings(ms)
            mc.return_value = _mock_http_client()

            from pipeline.sensor_extract import extract_sensor_readings

            s3_uri = extract_sensor_readings(execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc))

        self.assertIn("2026/07/01", s3_uri)

    @mock_aws
    def test_object_encrypted(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=_BUCKET)

        with patch("pipeline.sensor_extract.settings") as ms, \
             patch("pipeline.sensor_extract.httpx.Client") as mc:
            self._patch_settings(ms)
            mc.return_value = _mock_http_client()

            from pipeline.sensor_extract import extract_sensor_readings

            s3_uri = extract_sensor_readings(execution_date=datetime(2026, 7, 1, tzinfo=timezone.utc))

        key = s3_uri.removeprefix(f"s3a://{_BUCKET}/")
        self.assertEqual(s3.head_object(Bucket=_BUCKET, Key=key)["ServerSideEncryption"], "AES256")


if __name__ == "__main__":
    unittest.main()
