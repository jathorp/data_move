resource "aws_sqs_queue" "dead_letter_queue" {
  name = "${var.resource_prefix}-sqs-dlq"
  tags = var.tags
}

resource "aws_sqs_queue" "main_queue" {
  name                       = "${var.resource_prefix}-sqs-main-queue"
  visibility_timeout_seconds = 300 # Placeholder - should be 6x avg Lambda runtime
  tags                       = var.tags

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter_queue.arn
    maxReceiveCount     = 5
  })
}