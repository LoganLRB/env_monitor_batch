#!/usr/bin/env python3
"""EMR Serverless entry point: Bronze -> Silver."""
import sys
from datetime import datetime, timezone

from pipeline.sensor_transform import transform_sensor_readings

if __name__ == "__main__":
    bronze_uri = sys.argv[1]
    dt = datetime.fromisoformat(sys.argv[2])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    silver_prefix = transform_sensor_readings(bronze_uri, execution_date=dt)
    print(f"Done. Silver prefix: {silver_prefix}")
