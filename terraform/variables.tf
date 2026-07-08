variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Name prefix applied to all resources"
  type        = string
  default     = "env-monitor"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "prod"
}

variable "api_base_url" {
  description = "URL of the running env_monitor_api"
  type        = string
}

variable "mwaa_airflow_version" {
  type    = string
  default = "2.9.2"
}

variable "mwaa_environment_class" {
  description = "MWAA worker size (mw1.small | mw1.medium | mw1.large)"
  type        = string
  default     = "mw1.small"
}

variable "mwaa_min_workers" {
  type    = number
  default = 1
}

variable "mwaa_max_workers" {
  type    = number
  default = 5
}

variable "emr_release_label" {
  description = "EMR Serverless release label"
  type        = string
  default     = "emr-7.2.0"
}
