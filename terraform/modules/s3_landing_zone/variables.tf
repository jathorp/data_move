# File: modules/s3_landing_zone/variables.tf

variable "resource_prefix" {
  description = "The standard prefix for resource names (e.g., 'data_move-dev')."
  type        = string
}

variable "tags" {
  description = "A map of tags to apply to resources."
  type        = map(string)
}

variable "notification_queue_arn" {
  description = "The ARN of the SQS queue to send event notifications to."
  type        = string
}

# --- FIX: Add a variable for the queue URL ---
variable "notification_queue_url" {
  description = "The URL of the SQS queue."
  type        = string
}