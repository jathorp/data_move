# File: outputs.tf

output "s3_landing_bucket_name" {
  description = "The globally unique name of the S3 landing zone bucket."
  value       = module.s3_landing_zone.bucket_name
  sensitive   = true # Mask this value in CI/CD logs
}

output "main_sqs_queue_url" {
  description = "The URL of the main SQS queue for file notifications."
  value       = module.sqs_queues.main_queue_url
  sensitive   = true
}

output "dlq_url" {
  description = "The URL of the Dead-Letter Queue for failed messages."
  value       = module.sqs_queues.dlq_url
  sensitive   = true
}