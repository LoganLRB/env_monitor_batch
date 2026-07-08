#!/usr/bin/env python3
"""EMR Serverless entry point: Silver -> Gold sensor ops mart."""
import sys
from datetime import datetime, timezone

from pipeline.sensor_ops_mart import build_sensor_ops_mart

if __name__ == "__main__":
    silver_prefix = sys.argv[1]
    dt = datetime.fromisoformat(sys.argv[2])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    out_prefix = build_sensor_ops_mart(silver_prefix, execution_date=dt)
    print(f"Done. Sensor ops mart: {out_prefix}")
