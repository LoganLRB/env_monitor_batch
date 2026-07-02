import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_base_url: str = "http://localhost:8000"

    # S3 data lake
    s3_bucket: str = "env-monitor-data-lake"
    s3_prefix: str = "sensor-data"

    # EMR Serverless — populated by Terraform outputs via MWAA env vars
    emr_application_id: str = ""
    emr_execution_role_arn: str = ""

    # MWAA assets bucket
    mwaa_bucket: str = "env-monitor-airflow"

    fetch_interval_seconds: int = 300
    expected_sensor_count: int = 12

    @property
    def bronze_prefix(self) -> str:
        return f"s3://{self.s3_bucket}/{self.s3_prefix}/bronze"

    @property
    def silver_prefix(self) -> str:
        return f"s3://{self.s3_bucket}/{self.s3_prefix}/silver"

    @property
    def gold_prefix(self) -> str:
        return f"s3://{self.s3_bucket}/{self.s3_prefix}/gold"


settings = Settings()
