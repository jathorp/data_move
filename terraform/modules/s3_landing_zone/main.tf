# File: modules/s3_landing_zone/main.tf (Corrected Version)

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "landing_zone" {
  bucket = "${var.resource_prefix}-s3-landing-zone-${random_id.bucket_suffix.hex}"
  tags   = var.tags
  force_destroy = false
}

resource "aws_s3_bucket_lifecycle_configuration" "landing_zone_lifecycle" {
  bucket = aws_s3_bucket.landing_zone.id
  rule {
    id     = "archive-rule"
    status = "Enabled"
    filter {
      prefix = "incoming/"
    }
    transition {
      days          = 7
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "pab" {
  bucket = aws_s3_bucket.landing_zone.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_notification" "s3_to_sqs" {
  bucket = aws_s3_bucket.landing_zone.id
  queue {
    queue_arn     = var.notification_queue_arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "incoming/"
  }
  depends_on = [aws_sqs_queue_policy.allow_s3_to_send]
}

resource "aws_sqs_queue_policy" "allow_s3_to_send" {
  # --- FIX: Use the URL variable directly ---
  queue_url = var.notification_queue_url

  # --- FIX: Re-instate the IAM policy document data source here directly ---
  policy = data.aws_iam_policy_document.s3_to_sqs.json
}

# --- FIX: The data source for the policy document is still needed ---
data "aws_iam_policy_document" "s3_to_sqs" {
  statement {
    effect    = "Allow"
    actions   = ["SQS:SendMessage"]
    resources = [var.notification_queue_arn]
    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.landing_zone.arn]
    }
  }
}