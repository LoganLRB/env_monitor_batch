import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobRunOperator

EMR_APPLICATION_ID = os.environ["EMR_APPLICATION_ID"]
EMR_EXECUTION_ROLE_ARN = os.environ["EMR_EXECUTION_ROLE_ARN"]
MWAA_BUCKET = os.environ["MWAA_BUCKET"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "sensor-data")

_PY_FILES = f"s3://{MWAA_BUCKET}/pipeline.zip"
_SCRIPTS = f"s3://{MWAA_BUCKET}/scripts"
_LOGS = "/aws/emr-serverless/env-monitor-batch"

_SPARK_CONF = (
    "--conf spark.executor.memory=4g "
    "--conf spark.executor.cores=2 "
    "--conf spark.driver.memory=2g "
    "--conf spark.sql.sources.partitionOverwriteMode=dynamic"
)

_MONITORING = {
    "cloudWatchLoggingConfiguration": {
        "enabled": True,
        "logGroupName": _LOGS,
    }
}

_SILVER_PREFIX = (
    f"s3://{S3_BUCKET}/{S3_PREFIX}/silver"
    "/year={{{{ logical_date.year }}}}"
    "/month={{{{ '{:02d}'.format(logical_date.month) }}}}"
    "/day={{{{ '{:02d}'.format(logical_date.day) }}}}"
)

default_args = {
    "owner": "env-monitor",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _extract_sensor_readings(**context) -> None:
    """Runs on the MWAA worker — HTTP call + S3 put, no Spark needed."""
    from pipeline.sensor_extract import extract_sensor_readings

    dt: datetime = context["logical_date"]
    s3_uri = extract_sensor_readings(execution_date=dt)
    context["task_instance"].xcom_push(key="bronze_uri", value=s3_uri)


def _emr_job(script: str, args: list[str]) -> dict:
    return {
        "sparkSubmit": {
            "entryPoint": f"{_SCRIPTS}/{script}",
            "entryPointArguments": args,
            "sparkSubmitParameters": f"--py-files {_PY_FILES} {_SPARK_CONF}",
        }
    }


with DAG(
    dag_id="sensor_batch_pipeline",
    default_args=default_args,
    description="Daily ELT: env_monitor_api → Bronze (S3) → Silver (EMR) → Gold marts (EMR, parallel)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["env-monitor", "batch", "elt", "pyspark", "emr-serverless"],
) as dag:

    # Step 1 — Extract: API → Bronze JSON on S3 (MWAA worker)
    extract_task = PythonOperator(
        task_id="extract_sensor_readings",
        python_callable=_extract_sensor_readings,
    )

    # Step 2 — Transform: Bronze → Silver Parquet (EMR Serverless)
    transform_task = EmrServerlessStartJobRunOperator(
        task_id="transform_sensor_readings",
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver=_emr_job(
            "run_sensor_transform.py",
            [
                "{{ ti.xcom_pull(task_ids='extract_sensor_readings', key='bronze_uri') }}",
                "{{ logical_date.isoformat() }}",
            ],
        ),
        configuration_overrides={"monitoringConfiguration": _MONITORING},
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    # Steps 3a + 3b — Gold marts run in parallel (neither depends on the other)
    wildfire_risk_task = EmrServerlessStartJobRunOperator(
        task_id="build_wildfire_risk_mart",
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver=_emr_job(
            "run_wildfire_risk_mart.py",
            [_SILVER_PREFIX, "{{ logical_date.isoformat() }}"],
        ),
        configuration_overrides={"monitoringConfiguration": _MONITORING},
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    sensor_ops_task = EmrServerlessStartJobRunOperator(
        task_id="build_sensor_ops_mart",
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver=_emr_job(
            "run_sensor_ops_mart.py",
            [_SILVER_PREFIX, "{{ logical_date.isoformat() }}"],
        ),
        configuration_overrides={"monitoringConfiguration": _MONITORING},
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    quality_check_task = EmrServerlessStartJobRunOperator(
        task_id="check_silver_quality",
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver=_emr_job(
            "run_sensor_quality_check.py",
            [_SILVER_PREFIX, "{{ logical_date.isoformat() }}"],
        ),
        configuration_overrides={"monitoringConfiguration": _MONITORING},
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    extract_task >> transform_task >> quality_check_task >> [wildfire_risk_task, sensor_ops_task]
