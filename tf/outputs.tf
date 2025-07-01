# File: outputs.tf

output "data_move_s3_landing_bucket_name" {
  description = "The globally unique name of the S3 landing zone bucket."
  value       = aws_s3_bucket.landing_zone.bucket
}

output "data_move_main_sqs_queue_url" {
  description = "The URL of the main SQS queue for file notifications."
  value       = aws_sqs_queue.file_queue.id
}

output "data_move_dlq_url" {
  description = "The URL of the Dead-Letter Queue for failed messages."
  value       = aws_sqs_queue.dead_letter_queue.id
}