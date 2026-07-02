resource "aws_mwaa_environment" "main" {
  name               = "${var.project}-airflow"
  airflow_version    = var.mwaa_airflow_version
  environment_class  = var.mwaa_environment_class
  min_workers        = var.mwaa_min_workers
  max_workers        = var.mwaa_max_workers
  execution_role_arn = aws_iam_role.mwaa.arn

  source_bucket_arn    = aws_s3_bucket.mwaa.arn
  dag_s3_path          = "dags/"
  requirements_s3_path = "requirements.txt"

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = aws_subnet.private[*].id
  }

  # Environment variables injected into every Airflow worker process
  airflow_configuration_options = {
    "core.default_timezone"           = "utc"
    "core.load_examples"              = "false"
    "scheduler.dag_dir_list_interval" = "30"
  }

  environment_variables = {
    API_BASE_URL           = var.api_base_url
    S3_BUCKET              = aws_s3_bucket.data_lake.id
    S3_PREFIX              = "sensor-data"
    MWAA_BUCKET            = aws_s3_bucket.mwaa.id
    EMR_APPLICATION_ID     = aws_emrserverless_application.spark.id
    EMR_EXECUTION_ROLE_ARN = aws_iam_role.emr_serverless.arn
  }

  logging_configuration {
    dag_processing_logs { enabled = true; log_level = "INFO" }
    scheduler_logs      { enabled = true; log_level = "INFO" }
    task_logs           { enabled = true; log_level = "INFO" }
    webserver_logs      { enabled = true; log_level = "WARNING" }
    worker_logs         { enabled = true; log_level = "INFO" }
  }

  depends_on = [
    aws_s3_bucket_versioning.mwaa,
    aws_iam_role_policy.mwaa,
  ]
}
