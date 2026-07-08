import os

from pyspark.sql import SparkSession

from pipeline.config import settings


def get_spark(app_name: str = "env-monitor-batch") -> SparkSession:
    jars = f"{settings.jar_path}/hadoop-aws.jar,{settings.jar_path}/aws-sdk-bundle.jar"

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
    )

    if settings.is_local:
        builder = builder.master("local[*]")

    if endpoint_url := os.environ.get("AWS_ENDPOINT_URL"):
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )

    return builder.getOrCreate()
