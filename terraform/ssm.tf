# Pipeline configuration stored in SSM Parameter Store.
# MWAA workers read these at task startup via pydantic-settings' AWS SSM support,
# or via a startup script that exports them as environment variables.
# The aws_mwaa_environment Terraform resource does not yet support environment_variables
# (it's a console-only feature in the provider version pinned here).

resource "aws_ssm_parameter" "api_base_url" {
  name  = "/${var.project}/API_BASE_URL"
  type  = "String"
  value = var.api_base_url
}

resource "aws_ssm_parameter" "s3_bucket" {
  name  = "/${var.project}/S3_BUCKET"
  type  = "String"
  value = aws_s3_bucket.data_lake.id
}

resource "aws_ssm_parameter" "mwaa_bucket" {
  name  = "/${var.project}/MWAA_BUCKET"
  type  = "String"
  value = aws_s3_bucket.mwaa.id
}

resource "aws_ssm_parameter" "emr_application_id" {
  name  = "/${var.project}/EMR_APPLICATION_ID"
  type  = "String"
  value = aws_emrserverless_application.spark.id
}

resource "aws_ssm_parameter" "emr_execution_role_arn" {
  name  = "/${var.project}/EMR_EXECUTION_ROLE_ARN"
  type  = "String"
  value = aws_iam_role.emr_serverless.arn
}
