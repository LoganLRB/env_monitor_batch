import os
from datetime import timedelta

from pydantic_settings import BaseSettings, SettingsConfigDict

SECONDS_PER_DAY = int(timedelta(days=1).total_seconds())

_SSM_PREFIX = "/env-monitor"


def _load_ssm_env() -> None:
    """Populate os.environ from SSM Parameter Store (production only).

    Mirrors the pattern in social_app_api: application code reads config
    from SSM at startup rather than relying on externally injected env vars.
    Skipped entirely in local dev so docker-compose .env values are used as-is.
    Silently no-ops if SSM is unreachable; pydantic-settings defaults take over.
    """
    if os.environ.get("ENVIRONMENT", "local").lower() == "local":
        return
    try:
        import boto3
        client = boto3.client("ssm")
        resp = client.get_parameters_by_path(Path=_SSM_PREFIX + "/", Recursive=False)
        for param in resp.get("Parameters", []):
            key = param["Name"].rsplit("/", 1)[-1]
            os.environ.setdefault(key, param["Value"])
    except Exception as exc:
        print(f"[config] WARNING: could not load SSM parameters from {_SSM_PREFIX}/: {exc}")


_load_ssm_env()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_base_url: str = "http://localhost:8000"
    api_timeout_seconds: int = 120
    environment: str = "local"

    # S3 data lake
    s3_bucket: str = "env-monitor-data-lake"
    s3_prefix: str = "sensor-data"

    # EMR Serverless: populated by Terraform outputs via MWAA env vars
    emr_application_id: str = ""
    emr_execution_role_arn: str = ""
    emr_log_group: str = "/aws/emr-serverless/env-monitor-batch"

    # MWAA assets bucket
    mwaa_bucket: str = "env-monitor-airflow"

    fetch_interval_seconds: int = 300
    expected_sensor_count: int = 12

    # Path to hadoop-aws and aws-sdk-bundle jars; overridable for non-Docker envs
    jar_path: str = "/opt/airflow/jars"

    @property
    def is_local(self) -> bool:
        return self.environment.lower() == "local"

    @property
    def bronze_prefix(self) -> str:
        return f"s3a://{self.s3_bucket}/{self.s3_prefix}/bronze"

    @property
    def silver_prefix(self) -> str:
        return f"s3a://{self.s3_bucket}/{self.s3_prefix}/silver"

    @property
    def gold_prefix(self) -> str:
        return f"s3a://{self.s3_bucket}/{self.s3_prefix}/gold"


settings = Settings()
