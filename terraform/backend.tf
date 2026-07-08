terraform {
  backend "s3" {
    # Pre-create this bucket manually before first `terraform init`
    bucket         = "env-monitor-terraform-state"
    key            = "batch/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "env-monitor-terraform-locks"
  }
}
