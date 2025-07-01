# File: 1-terraform-backend-setup/main.tf

variable "project_name" {
  description = "The root name for the backend resources."
  type        = string
  default     = "data-move"
}

# The S3 bucket to store the terraform.tfstate file
resource "aws_s3_bucket" "terraform_state" {
  # Bucket names must be globally unique, so we add a random suffix.
  bucket = "${var.project_name}-tf-state-${random_id.suffix.hex}"
}

# Enable versioning on the S3 bucket as a safety measure
resource "aws_s3_bucket_versioning" "terraform_state_versioning" {
  bucket = aws_s3_bucket.terraform_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# The DynamoDB table for state locking to prevent concurrent runs
resource "aws_dynamodb_table" "terraform_lock" {
  name         = "${var.project_name}-tf-lock"
  billing_mode = "PAY_PER_REQUEST" # Simplest and often cheapest for this use case
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S" # S for String
  }
}

# A helper to ensure a unique bucket name
resource "random_id" "suffix" {
  byte_length = 4
}