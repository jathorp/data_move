# File: providers.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Configure the S3 backend for remote state management and collaboration.
  # The bucket and table must exist before you run `terraform init`.
  backend "s3" {
    bucket         = "data-move-tf-state-bucket-51426"
    key            = "global/s3-sqs/terraform.tfstate"
    region         = "eu-west-2"
    dynamodb_table = "data-move-tf-lock-table"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
}