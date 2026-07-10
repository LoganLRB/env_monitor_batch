# env_monitor_batch

Daily ELT batch pipeline for the Smart City Wildfire & Environmental Monitoring system. Fetches raw sensor data from [env_monitor_api](https://github.com/LoganLRB/env_monitor_api) and builds a medallion lakehouse on AWS S3, orchestrated by MWAA with PySpark jobs running on EMR Serverless.

## Architecture

```
env_monitor_api
  GET /v1/sensors/readings/bulk
        |
        v  MWAA worker (PythonOperator)
  Bronze  s3://<data-lake>/sensor-data/bronze/YYYY/MM/DD/   <- raw JSON, WORM-locked
        |
        v  EMR Serverless (PySpark)
  Silver  s3://<data-lake>/sensor-data/silver/year=/month=/day=/  <- typed Parquet, zstd
        |
        v  EMR Serverless (PySpark)
  Gold    s3://<data-lake>/sensor-data/gold/wildfire_risk/year=/month=/day=/
          s3://<data-lake>/sensor-data/gold/sensor_ops/year=/month=/day=/
                |
                v  env_monitor_dashboard
          GET /v1/zones/{zone_id}/history  <- reads wildfire_risk Gold mart via pyarrow + s3fs
```

Airflow DAG `sensor_batch_pipeline` runs `@daily` on MWAA:
```
extract_sensor_readings (MWAA worker)
  -> transform_sensor_readings (EMR)
  -> check_silver_quality (EMR)       <- blocks Gold if data quality fails
  -> build_wildfire_risk_mart (EMR) |
  -> build_sensor_ops_mart (EMR)    | parallel
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

## Monitoring & Alerting

### What's in place

| Signal | Where |
|---|---|
| Airflow task logs | CloudWatch: all 5 log streams (scheduler, worker, task, webserver, dag_processing) enabled via Terraform |
| EMR Serverless job logs | CloudWatch log group `/aws/emr-serverless/env-monitor-batch` |
| Quality gate | `check_silver_quality` raises `RuntimeError` on any failure, marking the Airflow task FAILED and blocking both Gold mart tasks |
| Retries | Each task retries twice (5-minute delay) before final failure |

### What's not yet built

**Failure notification** is the highest-priority gap. When a DAG run fails, nothing currently notifies anyone. The plan:

1. Add an SNS topic (`env-monitor-pipeline-alerts`) in Terraform
2. Add an `on_failure_callback` to the DAG that publishes to it
3. Subscribe email or a Slack webhook to the SNS topic
4. Add a CloudWatch Alarm on the EMR Serverless `JobRunFailed` metric to catch failures at the Spark layer before Airflow sees them

**Data freshness alarm**: if Bronze data doesn't appear in S3 by ~1 AM UTC, either the API is down or Airflow failed silently. A CloudWatch Metric Filter on task logs watching for a successful extract completion, with an alarm if no match arrives within the window, would catch this.

### How to detect missing data today

Without automated alerting, the detection methods are:

1. **Airflow UI Grid view** (`http://localhost:8080` or the MWAA console): a missing column in the grid means a run didn't complete for that date.
2. **S3 key inspection**: check `sensor-data/bronze/year=YYYY/month=MM/day=*/` directly; a missing date directory means the extract never ran.
3. **CloudWatch Logs Insights**: query the Airflow task log group for `sensor_batch_pipeline` completions across a date range.

### Backfill process

If data is missing for a date range (pipeline was down, API was unreachable, a bug was introduced and fixed), trigger a backfill:

```bash
# CLI: runs all missed dates sequentially, controlled by max_active_runs (default 16)
docker compose exec airflow-scheduler \
  airflow dags backfill sensor_batch_pipeline \
  -s 2026-07-01 -e 2026-07-07

# Or trigger individual dates manually
docker compose exec airflow-scheduler \
  airflow dags trigger sensor_batch_pipeline \
  -e 2026-07-03T00:00:00+00:00
```

Each backfill run is idempotent: Bronze JSON is overwritten at the same S3 key (keyed by execution date, not wall-clock time), Silver and Gold are written with `mode("overwrite")`.

For large historical backfills in production, set `catchup=True` on the DAG temporarily and let MWAA schedule missed intervals automatically. Control parallelism with `max_active_runs`.

## Known Behaviors & Limitations

### `catchup=False`
The DAG is set to `catchup=False`, meaning Airflow will **not** automatically run missed historical dates when first deployed. This is intentional for development. Deploying with `catchup=True` and a `start_date` of `2026-01-01` would immediately queue 180+ DAG runs.

For production backfill, either:
1. Set `catchup=True` in the DAG and let Airflow schedule missed intervals automatically
2. Or trigger explicitly: `airflow dags backfill -s 2026-01-01 -e 2026-06-30 sensor_batch_pipeline`

Control parallelism with `max_active_runs` on the DAG (default: 16).


## Configuration in Production (SSM Parameter Store)

In production, pipeline config is read from AWS SSM Parameter Store rather than environment variables. `pipeline/config.py` calls `ssm:GetParametersByPath` at startup when `ENVIRONMENT != "local"` and pre-populates `os.environ` before `Settings` is constructed, following the same pattern used in `social_app_api`.

Terraform writes all values to `/{project}/{KEY}` parameters via `terraform/ssm.tf`. The MWAA execution role has `ssm:GetParameter*` on the `/env-monitor/*` path.

In local dev, `_load_ssm_env()` is a no-op and `docker-compose` injects values via `.env`.

## CI/CD (GitHub Actions)

| Workflow | Trigger | What it does |
|---|---|---|
| `plan.yml` | PR to `main` | `terraform init` + `plan`; posts collapsible output as a PR comment; blocks merge if plan fails |
| `deploy.yml` | Manual (`workflow_dispatch`) | Runs tests, packages `pipeline.zip`, `terraform apply`, syncs DAGs/scripts/requirements to MWAA S3 bucket |

**Required GitHub Actions variables** (Settings > Secrets and variables > Actions > Variables):
- `AWS_ROLE_DEV`: OIDC role ARN for the dev environment (no long-lived keys)
- `API_BASE_URL`: URL of the running env_monitor_api

No `MWAA_BUCKET` secret needed; the deploy workflow reads it directly from `terraform output -raw mwaa_bucket` after apply.

## Local Development (Docker Compose + LocalStack)

Runs the full Airflow stack locally. LocalStack replaces S3 so no AWS account is needed.

**Prerequisites:** Docker Desktop and [env_monitor_api](https://github.com/LoganLRB/env_monitor_api) running on port 8000.

```bash
cp .env.example .env
docker compose up --build -d
```

- Airflow UI: http://localhost:8080 (admin / admin)
- LocalStack S3: http://localhost:4566

### Trigger a run

When the stack starts, the scheduler automatically picks up the current day's `@daily` interval and runs it. No manual trigger needed for today. For past dates, trigger manually.

#### Using the Airflow UI

1. Open http://localhost:8080 and log in as **admin / admin**.
2. Find `sensor_batch_pipeline` in the DAGs list. If the toggle on the left shows it as paused, click it to unpause.
3. **Today's run**: the scheduler queues it automatically within a few seconds of startup. Refresh the page and you will see it appear.
4. **A specific past date**: click the **Trigger DAG** button (the play icon on the right side of the DAG row), then choose **Trigger DAG w/ config**. Set the **Logical date** field to the target date (e.g. `2026-07-01T00:00:00+00:00`) and click **Trigger**.
5. Click the DAG name to open the **Grid view**. Each column is a run; each row is a task. Boxes turn green as tasks succeed. Click any box to open its log.

The task order is always: `extract` -> `transform` -> `check_silver_quality` -> `wildfire_risk_mart` + `sensor_ops_mart` (last two run in parallel).

#### Using the CLI

```bash
# Trigger for today
docker compose exec airflow-scheduler \
  airflow dags trigger sensor_batch_pipeline

# Trigger for a specific past date (-e / --exec-date; --logical-date does not exist in Airflow 2.9.x)
docker compose exec airflow-scheduler \
  airflow dags trigger sensor_batch_pipeline \
  -e 2026-07-01T00:00:00+00:00

# List all runs and their states
docker compose exec airflow-scheduler \
  airflow dags list-runs -d sensor_batch_pipeline

# Check individual task states (use the execution_date value from list-runs, not the run_id)
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run sensor_batch_pipeline 2026-07-01T00:00:00+00:00
```

### Verify output

```bash
docker compose exec localstack \
  awslocal s3 ls s3://env-monitor-local/sensor-data/ --recursive
```

## Tests

Requires Python 3.9+ and Java 17 (PySpark runs on the JVM even in local mode).

```bash
# Install Java if not present (macOS)
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

python -m venv env_monitor_batch
source env_monitor_batch/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Tests use `moto` to mock S3 for the extract step and a local `SparkSession` for transform, quality checks, and Gold marts. No AWS credentials required.

## Configuration

All settings are managed via `pipeline/config.py` (pydantic-settings). The following environment variables are read; see `.env.example` for descriptions.

| Variable | Default | Local dev (docker-compose) |
|---|---|---|
| `ENVIRONMENT` | `local` | `local` |
| `API_BASE_URL` | `http://localhost:8000` | `http://host.docker.internal:8000` |
| `API_TIMEOUT_SECONDS` | `120` | `120` |
| `S3_BUCKET` | `env-monitor-data-lake` | `env-monitor-local` |
| `S3_PREFIX` | `sensor-data` | `sensor-data` |
| `MWAA_BUCKET` | `env-monitor-airflow` | `env-monitor-local` |
| `EMR_APPLICATION_ID` | (Terraform output) | `local` |
| `EMR_EXECUTION_ROLE_ARN` | (Terraform output) | dummy ARN |
| `EMR_LOG_GROUP` | `/aws/emr-serverless/env-monitor-batch` | `/aws/emr-serverless/env-monitor-batch` |
| `FETCH_INTERVAL_SECONDS` | `300` | `300` |
| `EXPECTED_SENSOR_COUNT` | `12` | `12` |
| `JAR_PATH` | `/opt/airflow/jars` | `/opt/airflow/jars` |

## Data Layers

| Layer | Format | Compression | Notes |
|---|---|---|---|
| Bronze | JSON | none | Immutable (S3 Object Lock COMPLIANCE mode, 7-year retention) |
| Silver | Parquet | zstd | Typed, `is_alert` bool, `ingested_at` audit timestamp |
| Gold (zone) | Parquet | zstd | Daily avg/max temp, humidity, wind, PM2.5, alert % per zone |
| Gold (sensor) | Parquet | zstd | Same metrics per sensor + min battery |

## Future Development

| Area | Description |
|---|---|
| **Failure alerting** | SNS topic + `on_failure_callback` in the DAG + CloudWatch Alarm on EMR `JobRunFailed` metric. Nothing currently notifies anyone when the pipeline fails. Highest-priority gap before production use. |
| **Data freshness alarm** | CloudWatch Metric Filter on Airflow task logs + alarm if no successful extract is logged by ~1 AM UTC. Catches silent failures where Airflow itself is healthy but no data arrived. |
| **Quality check metrics** | Emit row count, null counts, and out-of-range counts as custom CloudWatch metrics after each quality check pass. Enables trending and early detection of slow API data degradation before hard failures. |
| **Multi-environment Terraform** | Follow `social_app_database` pattern: separate `terraform/envs/dev`, `envs/stg`, `envs/prod` directories with shared modules. Currently single-environment. |
| **Automated backfill detection** | Scheduled Lambda or Airflow sensor that checks S3 for expected date partitions and alerts (via SNS) if any are missing beyond a configurable lag window. |

## Project Structure

```
env_monitor_batch/
├── dags/
│   └── sensor_pipeline.py              # Airflow DAG: PythonOperator locally, EmrServerlessStartJobRunOperator in production
├── pipeline/
│   ├── config.py                       # Pydantic Settings (S3 paths, EMR config, sensor count)
│   ├── spark.py                        # SparkSession factory
│   ├── sensor_extract.py               # Bronze: API -> S3 JSON (boto3)
│   ├── sensor_transform.py             # Silver: S3 JSON -> S3 Parquet (PySpark)
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
│   ├── s3.tf                           # Data lake + MWAA buckets (WORM, lifecycle, encryption)
│   ├── iam.tf                          # MWAA and EMR execution roles (least-privilege)
│   ├── vpc.tf                          # VPC, private subnets, NAT GW, S3 VPC endpoint
│   ├── mwaa.tf                         # MWAA environment (logging, networking, worker config)
│   ├── emr.tf                          # EMR Serverless application (auto-start/stop, capacity)
│   └── ssm.tf                          # SSM parameters: pipeline config read by workers at startup
├── .github/workflows/
│   ├── plan.yml                        # Terraform plan on PR, output posted as comment
│   └── deploy.yml                      # Manual dispatch: test + terraform apply + sync to MWAA
├── tests/
│   ├── test_sensor_extract.py          # moto S3 mocks
│   ├── test_sensor_transform.py        # Local SparkSession
│   ├── test_sensor_quality_check.py    # Local SparkSession
│   ├── test_wildfire_risk_mart.py      # Local SparkSession
│   └── test_sensor_ops_mart.py        # Local SparkSession
├── requirements.txt                    # Pipeline + test dependencies
├── requirements-mwaa.txt               # Installed on MWAA workers
├── Dockerfile                          # Local dev image (Airflow + Java 17 + Hadoop AWS jars)
└── docker-compose.yml                  # Local dev: Airflow + LocalStack + Postgres
```
