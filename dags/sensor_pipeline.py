from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.config import settings

_PY_FILES = f"s3://{settings.mwaa_bucket}/pipeline.zip"
_SCRIPTS = f"s3://{settings.mwaa_bucket}/scripts"

_SPARK_CONF = (
    "--conf spark.executor.memory=4g "
    "--conf spark.executor.cores=2 "
    "--conf spark.driver.memory=2g"
)

_MONITORING = {
    "cloudWatchLoggingConfiguration": {
        "enabled": True,
        "logGroupName": settings.emr_log_group,
    }
}

# Jinja template evaluated at task runtime to resolve the Silver partition path.
# Four braces needed: outer pair is Python f-string escaping, inner pair is Jinja.
_SILVER_PREFIX = (
    f"s3a://{settings.s3_bucket}/{settings.s3_prefix}/silver"
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
    from pipeline.sensor_extract import extract_sensor_readings

    dt: datetime = context["logical_date"]
    s3_uri = extract_sensor_readings(execution_date=dt)
    context["ti"].xcom_push(key="bronze_uri", value=s3_uri)


def _transform_local(**context) -> None:
    from pipeline.sensor_transform import transform_sensor_readings

    bronze_uri = context["ti"].xcom_pull(task_ids="extract_sensor_readings", key="bronze_uri")
    silver_prefix = transform_sensor_readings(bronze_uri=bronze_uri, execution_date=context["logical_date"])
    context["ti"].xcom_push(key="silver_prefix", value=silver_prefix)


def _quality_check_local(**context) -> None:
    from pipeline.sensor_quality_check import check_silver_quality

    silver_prefix = context["ti"].xcom_pull(task_ids="transform_sensor_readings", key="silver_prefix")
    check_silver_quality(silver_prefix=silver_prefix, execution_date=context["logical_date"])


def _wildfire_risk_local(**context) -> None:
    from pipeline.wildfire_risk_mart import build_wildfire_risk_mart

    silver_prefix = context["ti"].xcom_pull(task_ids="transform_sensor_readings", key="silver_prefix")
    build_wildfire_risk_mart(silver_prefix=silver_prefix, execution_date=context["logical_date"])


def _sensor_ops_local(**context) -> None:
    from pipeline.sensor_ops_mart import build_sensor_ops_mart

    silver_prefix = context["ti"].xcom_pull(task_ids="transform_sensor_readings", key="silver_prefix")
    build_sensor_ops_mart(silver_prefix=silver_prefix, execution_date=context["logical_date"])


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
    description="Daily ELT: env_monitor_api -> Bronze (S3) -> Silver (EMR) -> Gold marts (EMR, parallel)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["env-monitor", "batch", "elt", "pyspark", "emr-serverless"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_sensor_readings",
        python_callable=_extract_sensor_readings,
    )

    if settings.is_local:
        transform_task = PythonOperator(
            task_id="transform_sensor_readings",
            python_callable=_transform_local,
        )
        quality_check_task = PythonOperator(
            task_id="check_silver_quality",
            python_callable=_quality_check_local,
        )
        wildfire_risk_task = PythonOperator(
            task_id="build_wildfire_risk_mart",
            python_callable=_wildfire_risk_local,
        )
        sensor_ops_task = PythonOperator(
            task_id="build_sensor_ops_mart",
            python_callable=_sensor_ops_local,
        )
    else:
        from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobRunOperator

        transform_task = EmrServerlessStartJobRunOperator(
            task_id="transform_sensor_readings",
            application_id=settings.emr_application_id,
            execution_role_arn=settings.emr_execution_role_arn,
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
        quality_check_task = EmrServerlessStartJobRunOperator(
            task_id="check_silver_quality",
            application_id=settings.emr_application_id,
            execution_role_arn=settings.emr_execution_role_arn,
            job_driver=_emr_job(
                "run_sensor_quality_check.py",
                [_SILVER_PREFIX, "{{ logical_date.isoformat() }}"],
            ),
            configuration_overrides={"monitoringConfiguration": _MONITORING},
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )
        wildfire_risk_task = EmrServerlessStartJobRunOperator(
            task_id="build_wildfire_risk_mart",
            application_id=settings.emr_application_id,
            execution_role_arn=settings.emr_execution_role_arn,
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
            application_id=settings.emr_application_id,
            execution_role_arn=settings.emr_execution_role_arn,
            job_driver=_emr_job(
                "run_sensor_ops_mart.py",
                [_SILVER_PREFIX, "{{ logical_date.isoformat() }}"],
            ),
            configuration_overrides={"monitoringConfiguration": _MONITORING},
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

    extract_task >> transform_task >> quality_check_task >> [wildfire_risk_task, sensor_ops_task]
