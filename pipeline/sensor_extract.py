import json
import os
from datetime import datetime, timedelta, timezone

import boto3
import httpx

from pipeline.config import settings


def _s3_client():
    """Return a boto3 S3 client, routing to LocalStack when running locally."""
    kwargs = {}
    if endpoint_url := os.environ.get("AWS_ENDPOINT_URL"):
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client("s3", **kwargs)


def extract_sensor_readings(
    execution_date: datetime | None = None,
    interval_seconds: int | None = None,
) -> str:
    """Fetch bulk sensor readings from env_monitor_api and write raw JSON to Bronze (S3)."""
    dt = execution_date or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    interval_seconds = interval_seconds or settings.fetch_interval_seconds

    url = f"{settings.api_base_url}/v1/sensors/readings/bulk"
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "interval_seconds": interval_seconds,
    }

    with httpx.Client(timeout=120.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()

    payload = response.json()

    s3_key = (
        f"{settings.s3_prefix}/bronze"
        f"/{dt.strftime('%Y/%m/%d')}"
        f"/readings_{dt.strftime('%Y%m%d_%H%M%S')}.json"
    )

    _s3_client().put_object(
        Bucket=settings.s3_bucket,
        Key=s3_key,
        Body=json.dumps(payload).encode("utf-8"),
        ServerSideEncryption="AES256",
        ContentType="application/json",
    )

    s3_uri = f"s3://{settings.s3_bucket}/{s3_key}"
    print(f"[sensor_extract] {payload['reading_count']} readings → {s3_uri}")
    return s3_uri
