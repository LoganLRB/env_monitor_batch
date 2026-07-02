output "data_lake_bucket" {
  description = "S3 bucket holding Bronze/Silver/Gold data"
  value       = aws_s3_bucket.data_lake.id
}

output "mwaa_bucket" {
  description = "S3 bucket holding MWAA DAGs, requirements, and pipeline code"
  value       = aws_s3_bucket.mwaa.id
}

output "mwaa_webserver_url" {
  description = "MWAA Airflow UI URL"
  value       = aws_mwaa_environment.main.webserver_url
}

output "emr_application_id" {
  description = "EMR Serverless application ID — used in the Airflow DAG"
  value       = aws_emrserverless_application.spark.id
}

output "emr_execution_role_arn" {
  description = "IAM role ARN assumed by EMR Serverless jobs"
  value       = aws_iam_role.emr_serverless.arn
}
