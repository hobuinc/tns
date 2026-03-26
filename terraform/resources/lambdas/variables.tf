variable "env" {
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
  type = string
}

variable "bucket_name" {
  type = string
}
