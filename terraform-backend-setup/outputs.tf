# File: 1-terraform-backend-setup/outputs.tf

output "s3_bucket_for_terraform_state" {
  description = "The name of the S3 bucket created for Terraform state."
  value       = aws_s3_bucket.terraform_state.id
}

output "dynamodb_table_for_terraform_lock" {
  description = "The name of the DynamoDB table for state locking."
  value       = aws_dynamodb_table.terraform_lock.name
}