variable "resource_prefix" {
  description = "The standard prefix for resource names (e.g., 'data_move-dev')."
  type        = string
}

variable "tags" {
  description = "A map of tags to apply to resources."
  type        = map(string)
}