variable "env" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "aws_account_id" {
  type = string
}

variable "image_uri" {
  type = string
}

# comp_lambda.tf
variable "sns_out_arn" {
  type = string
}
variable "sqs_in_arn" {
  type = string
}

variable "sts_lambda_role_name" {
  description = "Existing Lambda IAM role name to use. Leave empty to let Terraform create an env-scoped role."
  type        = string
}

variable "bucket_name" {
  type = string
}
