locals {
  region = var.aws_region
  aid    = local.account_id
}

# ── MWAA execution role ────────────────────────────────────────────────────────

data "aws_iam_policy_document" "mwaa_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "mwaa" {
  name               = "${var.project}-mwaa-execution"
  assume_role_policy = data.aws_iam_policy_document.mwaa_assume.json
}

data "aws_iam_policy_document" "mwaa" {
  # Read DAGs, requirements, pipeline code from the MWAA bucket
  statement {
    actions   = ["s3:GetObject*", "s3:GetBucket*", "s3:List*"]
    resources = [aws_s3_bucket.mwaa.arn, "${aws_s3_bucket.mwaa.arn}/*"]
  }

  # Write Bronze JSON to the data lake (extract task runs on the MWAA worker)
  statement {
    actions   = ["s3:PutObject", "s3:GetObject*", "s3:List*"]
    resources = [aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*"]
  }

  # Submit and monitor EMR Serverless jobs
  statement {
    actions = [
      "emr-serverless:StartJobRun",
      "emr-serverless:GetJobRun",
      "emr-serverless:ListJobRuns",
      "emr-serverless:CancelJobRun",
    ]
    resources = [aws_emrserverless_application.spark.arn]
  }

  # Pass the EMR execution role to submitted jobs
  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_serverless.arn]
    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["emr-serverless.amazonaws.com"]
    }
  }

  # CloudWatch Logs: task logs, scheduler logs
  statement {
    actions = [
      "logs:CreateLogGroup", "logs:CreateLogStream",
      "logs:PutLogEvents",   "logs:GetLogEvents",
      "logs:GetLogRecord",   "logs:GetQueryResults",
      "logs:DescribeLogGroups", "logs:DescribeLogStreams",
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aid}:log-group:*"]
  }

  # SSM Parameter Store: pipeline config (API URL, bucket names, EMR IDs)
  statement {
    actions   = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath"]
    resources = ["arn:aws:ssm:${local.region}:${local.aid}:parameter/${var.project}/*"]
  }

  # Secrets Manager: for API key if env_monitor_api adds auth
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${local.region}:${local.aid}:secret:${var.project}/*"]
  }
}

resource "aws_iam_role_policy" "mwaa" {
  name   = "${var.project}-mwaa-policy"
  role   = aws_iam_role.mwaa.id
  policy = data.aws_iam_policy_document.mwaa.json
}

# ── EMR Serverless execution role ─────────────────────────────────────────────

data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_serverless" {
  name               = "${var.project}-emr-serverless-execution"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

data "aws_iam_policy_document" "emr_serverless" {
  # Full read/write to both S3 buckets (pipeline code + data)
  statement {
    actions = [
      "s3:GetObject*", "s3:PutObject*", "s3:DeleteObject*",
      "s3:List*",      "s3:GetBucket*",
    ]
    resources = [
      aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*",
      aws_s3_bucket.mwaa.arn,      "${aws_s3_bucket.mwaa.arn}/*",
    ]
  }

  # Emit Spark logs to CloudWatch
  statement {
    actions = [
      "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
      "logs:DescribeLogGroups", "logs:DescribeLogStreams",
    ]
    resources = [
      "arn:aws:logs:${local.region}:${local.aid}:log-group:/aws/emr-serverless/*",
    ]
  }
}

resource "aws_iam_role_policy" "emr_serverless" {
  name   = "${var.project}-emr-serverless-policy"
  role   = aws_iam_role.emr_serverless.id
  policy = data.aws_iam_policy_document.emr_serverless.json
}
