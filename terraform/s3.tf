locals {
  account_id = data.aws_caller_identity.current.account_id
}

# ── Data lake: Bronze / Silver / Gold ─────────────────────────────────────────

resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project}-data-lake-${local.account_id}"
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# WORM on bronze/ — raw data is immutable for compliance (NIST AU-9, FedRAMP)
resource "aws_s3_bucket_object_lock_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    default_retention {
      mode = "COMPLIANCE"
      days = 2555 # 7-year retention for government data
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-to-glacier"
    status = "Enabled"
    filter { prefix = "sensor-data/bronze/" }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }

  rule {
    id     = "silver-to-ia"
    status = "Enabled"
    filter { prefix = "sensor-data/silver/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

# ── MWAA assets: DAGs, requirements, pipeline.zip, scripts ────────────────────

resource "aws_s3_bucket" "mwaa" {
  bucket = "${var.project}-airflow-${local.account_id}"
}

resource "aws_s3_bucket_versioning" "mwaa" {
  bucket = aws_s3_bucket.mwaa.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mwaa" {
  bucket = aws_s3_bucket.mwaa.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "mwaa" {
  bucket                  = aws_s3_bucket.mwaa.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
