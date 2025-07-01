# File: main.tf

locals {
  # Construct a standard prefix for resource names.
  resource_prefix = "${var.project_name}-${var.environment}"
}

module "sqs_queues" {
  source = "./modules/sqs_queues"

  # Pass variables into the module
  resource_prefix = local.resource_prefix
  tags            = var.tags
}

module "s3_landing_zone" {
  source = "./modules/s3_landing_zone"

  # Pass variables into the module
  resource_prefix = local.resource_prefix
  tags            = var.tags

  notification_queue_arn = module.sqs_queues.main_queue_arn
  notification_queue_url = module.sqs_queues.main_queue_url
}