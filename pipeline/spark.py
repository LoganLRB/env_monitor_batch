from pyspark.sql import SparkSession


def get_spark(app_name: str = "env-monitor-batch") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "zstd")
        # Overwrite only the affected partition, not the whole table
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
