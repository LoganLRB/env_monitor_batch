#!/usr/bin/env python3
"""EMR Serverless entry point — Silver data quality check."""
import sys
from datetime import datetime, timezone

from pipeline.sensor_quality_check import check_silver_quality

if __name__ == "__main__":
    silver_prefix = sys.argv[1]
    dt = datetime.fromisoformat(sys.argv[2]).replace(tzinfo=timezone.utc)
    check_silver_quality(silver_prefix, execution_date=dt)
