# File: variables.tf

variable "project_name" {
  description = "A short, unique name for the project (e.g., 'data-move')."
  type        = string
  default     = "data_move"
}

variable "environment" {
  description = "The deployment environment (e.g., 'dev', 'stg', 'prd')."
  type        = string
  default     = "dev"
}

variable "aws_region_code" {
  description = "The short code for the AWS region (e.g., 'euw2')."
  type        = string
  default     = "euw2"
}

variable "tags" {
  description = "A map of tags to apply to all resources."
  type        = map(string)
  default = {
    Project     = "Data Move Pipeline"
    ManagedBy   = "Terraform"
    Environment = "Dev"
  }
}