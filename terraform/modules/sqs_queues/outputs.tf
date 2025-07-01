output "main_queue_arn" {
  description = "ARN of the main SQS queue."
  value       = aws_sqs_queue.main_queue.arn
}

output "main_queue_url" {
  description = "URL of the main SQS queue."
  value       = aws_sqs_queue.main_queue.id
}

output "dlq_url" {
  description = "URL of the dead-letter queue."
  value       = aws_sqs_queue.dead_letter_queue.id
}