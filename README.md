# env_monitor_batch

Daily ELT batch pipeline for the Smart City Wildfire & Environmental Monitoring system. Fetches raw sensor data from [env_monitor_api](https://github.com/LoganLRB/env_monitor_api) and builds a medallion lakehouse on AWS S3, orchestrated by MWAA with PySpark jobs running on EMR Serverless.

## Architecture

```
env_monitor_api
  GET /v1/sensors/readings/bulk
        │
        ▼  MWAA worker (PythonOperator)
  Bronze  s3://<data-lake>/sensor-data/bronze/YYYY/MM/DD/   ← raw JSON, WORM-locked
        │
        ▼  EMR Serverless (PySpark)
  Silver  s3://<data-lake>/sensor-data/silver/year=/month=/day=/  ← typed Parquet, zstd
        │
        ▼  EMR Serverless (PySpark)
  Gold    s3://<data-lake>/sensor-data/gold/wildfire_risk/year=/month=/day=/
          s3://<data-lake>/sensor-data/gold/sensor_ops/year=/month=/day=/
```

Airflow DAG `sensor_batch_pipeline` runs `@daily` on MWAA:
```
extract_sensor_readings (MWAA worker)
  → transform_sensor_readings (EMR)
  → check_silver_quality (EMR)       ← blocks Gold if data quality fails
  → build_wildfire_risk_mart (EMR) ┐
  → build_sensor_ops_mart (EMR)    ┘ parallel
```

## Infrastructure (Terraform)

Provision with:
```bash
# One-time: create the Terraform state bucket and DynamoDB lock table manually
aws s3 mb s3://env-monitor-terraform-state
aws dynamodb create-table \
  --table-name env-monitor-terraform-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

cd terraform
terraform init
terraform apply -var="api_base_url=https://your-api-url"
```

Resources created:
| Resource | Purpose |
|---|---|
| S3 `env-monitor-data-lake-<acct>` | Bronze/Silver/Gold data, WORM on Bronze |
| S3 `env-monitor-airflow-<acct>` | MWAA DAGs, requirements, pipeline.zip |
| MWAA environment | Managed Airflow (scheduler + workers) |
| EMR Serverless application | Auto-scaling PySpark execution |
| VPC + private subnets + NAT GW | Network isolation for MWAA |
| IAM roles | Least-privilege for MWAA and EMR |

## Known Behaviors & Limitations

### `catchup=False`
The DAG is set to `catchup=False`, meaning Airflow will **not** automatically run missed historical dates when first deployed. This is intentional for development — deploying with `catchup=True` and a `start_date` of `2026-01-01` would immediately queue 180+ DAG runs.

For production backfill, either:
1. Set `catchup=True` in the DAG and let Airflow schedule missed intervals automatically
2. Or trigger explicitly: `airflow dags backfill -s 2026-01-01 -e 2026-06-30 sensor_batch_pipeline`

Control parallelism with `max_active_runs` on the DAG (default: 16).


## CI/CD (GitHub Actions)

| Workflow | Trigger | What it does |
|---|---|---|
| `deploy.yml` | Push to `main` | Runs tests → packages `pipeline.zip` → syncs DAGs, scripts, requirements to MWAA S3 bucket |
| `terraform.yml` | PR or push to `main` touching `terraform/` | `plan` on PR, `apply` on merge |

**Required GitHub secrets:**
- `AWS_DEPLOY_ROLE_ARN` — OIDC role for GitHub Actions (no long-lived keys)
- `MWAA_BUCKET` — output of `terraform output mwaa_bucket`
- `API_BASE_URL` — URL of the running env_monitor_api

## Local Development (Docker Compose + LocalStack)

Runs the full Airflow stack locally. LocalStack replaces S3 — no AWS account needed.

```bash
cp .env.example .env
docker compose up --build -d
```

- Airflow UI: http://localhost:8080 (admin / admin)
- LocalStack S3: http://localhost:4566

## Tests

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Tests use `moto` to mock S3 for the extract step and a local `SparkSession` for transform, quality checks, and Gold marts — no AWS credentials required.

## Data Layers

| Layer | Format | Compression | Notes |
|---|---|---|---|
| Bronze | JSON | none | Immutable — S3 Object Lock COMPLIANCE mode, 7-year retention |
| Silver | Parquet | zstd | Typed, `is_alert` bool, `ingested_at` audit timestamp |
| Gold (zone) | Parquet | zstd | Daily avg/max temp, humidity, wind, PM2.5, alert % per zone |
| Gold (sensor) | Parquet | zstd | Same metrics per sensor + min battery |

## Project Structure

```
env_monitor_batch/
├── dags/
│   └── sensor_pipeline.py              # MWAA DAG — PythonOperator + EmrServerlessStartJobRunOperator
├── pipeline/
│   ├── config.py                       # Pydantic Settings (S3 paths, EMR config, sensor count)
│   ├── spark.py                        # SparkSession factory
│   ├── sensor_extract.py               # Bronze: API → S3 JSON (boto3)
│   ├── sensor_transform.py             # Silver: S3 JSON → S3 Parquet (PySpark)
│   ├── sensor_quality_check.py         # Quality gate: blocks Gold if Silver data fails checks
│   ├── wildfire_risk_mart.py           # Gold: zone-level daily wildfire risk mart
│   └── sensor_ops_mart.py             # Gold: per-sensor daily ops mart
├── scripts/
│   ├── run_sensor_transform.py         # EMR Serverless entry point for Silver step
│   ├── run_sensor_quality_check.py     # EMR Serverless entry point for quality gate
│   ├── run_wildfire_risk_mart.py       # EMR Serverless entry point for wildfire risk mart
│   └── run_sensor_ops_mart.py         # EMR Serverless entry point for sensor ops mart
├── terraform/
│   ├── providers.tf / backend.tf / variables.tf / outputs.tf
│   ├── s3.tf                           # Data lake + MWAA buckets
│   ├── iam.tf                          # MWAA and EMR execution roles
│   ├── vpc.tf                          # VPC, private subnets, NAT GW, S3 endpoint
│   ├── mwaa.tf                         # MWAA environment
│   └── emr.tf                          # EMR Serverless application
├── .github/workflows/
│   ├── deploy.yml                      # Test + deploy pipeline code to MWAA
│   └── terraform.yml                   # Plan on PR, apply on merge
├── tests/
│   ├── test_sensor_extract.py          # moto S3 mocks
│   ├── test_sensor_transform.py        # Local SparkSession
│   ├── test_sensor_quality_check.py    # Local SparkSession
│   ├── test_wildfire_risk_mart.py      # Local SparkSession
│   └── test_sensor_ops_mart.py        # Local SparkSession
├── requirements.txt                    # Pipeline + test dependencies
├── requirements-mwaa.txt               # Installed on MWAA workers
├── Dockerfile                          # Local dev image (Airflow + Java 17)
└── docker-compose.yml                  # Local dev: Airflow + LocalStack + Postgres
```
