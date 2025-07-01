# File: main.tf (Corrected Version)

# A 'locals' block is the best place to build complex variables like names.
locals {
  # This creates our standard naming prefix, e.g., "data_move-dev-euw2"
  resource_prefix = "${var.project_name}-${var.environment}-${var.aws_region_code}"

  # A random suffix just for the S3 bucket to ensure global uniqueness.
  bucket_suffix = random_id.bucket_suffix.hex
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# --- Resource Definitions ---

# 1. The Dead-Letter Queue (DLQ) for storing failed messages.
resource "aws_sqs_queue" "dead_letter_queue" {
  # Name will be e.g., "data_move-dev-euw2-sqs-dlq"
  name = "${local.resource_prefix}-sqs-dlq"
  tags = var.tags
}

# 2. The main SQS queue that will receive S3 event notifications.
resource "aws_sqs_queue" "file_queue" {
  # Name will be e.g., "data_move-dev-euw2-sqs-main-queue"
  name = "${local.resource_prefix}-sqs-main-queue"
  tags = var.tags

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter_queue.arn
    maxReceiveCount     = 5
  })
}

# 3. The S3 bucket where files will be landed.
resource "aws_s3_bucket" "landing_zone" {
  # Name will be e.g., "data_move-dev-euw2-s3-landing-zone-a9b3"
  bucket = "${local.resource_prefix}-s3-landing-zone-${local.bucket_suffix}"
  tags   = var.tags
}

# 4. A security block to ensure the S3 bucket is private.
resource "aws_s3_bucket_public_access_block" "landing_zone_pab" {
  bucket = aws_s3_bucket.landing_zone.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 5. The "glue" that connects the S3 bucket to the SQS queue.
resource "aws_s3_bucket_notification" "s3_to_sqs_notification" {
  bucket = aws_s3_bucket.landing_zone.id

  queue {
    queue_arn     = aws_sqs_queue.file_queue.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "incoming/"
  }

  depends_on = [aws_sqs_queue_policy.s3_to_sqs_policy]
}

# 6. The PERMISSION that allows S3 to send messages to our SQS queue.
resource "aws_sqs_queue_policy" "s3_to_sqs_policy" { # <-- FIX 1: Corrected resource name from "s3_to_sgs_policy"
  queue_url = aws_sqs_queue.file_queue.id             # <-- FIX 2: Corrected resource type from "aws_s3_queue"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          Service = "s3.amazonaws.com"
        },
        Action    = "SQS:SendMessage",
        Resource  = aws_sqs_queue.file_queue.arn,
        Condition = {
          ArnEquals = { "aws:SourceArn" = aws_s3_bucket.landing_zone.arn }
        }
      }
    ]
  })
}