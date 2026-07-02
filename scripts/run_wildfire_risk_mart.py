#!/usr/bin/env python3
"""EMR Serverless entry point — Silver → Gold wildfire risk mart."""
import sys
from datetime import datetime, timezone

from pipeline.wildfire_risk_mart import build_wildfire_risk_mart

if __name__ == "__main__":
    silver_prefix = sys.argv[1]
    dt = datetime.fromisoformat(sys.argv[2]).replace(tzinfo=timezone.utc)
    out_prefix = build_wildfire_risk_mart(silver_prefix, execution_date=dt)
    print(f"Done. Wildfire risk mart: {out_prefix}")
