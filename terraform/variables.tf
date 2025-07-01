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

variable "aws_region" {
  description = "The full AWS region string where resources will be deployed (e.g., 'eu-west-2')."
  type        = string
  default     = "eu-west-2"
}

variable "tags" {
  description = "A map of tags to apply to all resources. Add CostCenter or Owner as needed."
  type        = map(string)
  # No default makes this a required input, forcing environment-specific tagging.
}