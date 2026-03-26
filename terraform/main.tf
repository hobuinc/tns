terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.84.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

module "tns_base" {
  source         = "./resources/base"
  env            = var.env
  modify_bucket  = var.modify_bucket
  s3_bucket_name = var.s3_bucket_name
  ecr_image_uri  = var.ecr_image_uri
}

module "tns_lambdas" {
  source               = "./resources/lambdas"
  env                  = var.env
  sts_lambda_role_name = var.sts_lambda_role_name

  image_uri   = module.tns_base.image_uri
  bucket_name = module.tns_base.s3_bucket_name

  sqs_in_arn  = module.tns_base.sqs_in_arn
  sns_out_arn = module.tns_base.sns_out_arn
}

####################################
##            Inputs              ##
####################################

variable "aws_region" {
  type    = string
  default = "us-west-2"
}

variable "env" {
  type    = string
  default = "dev"
  validation {
    condition     = can(regex("^(dev|test|prod)$", var.env))
    error_message = "dev, test, or prod are the available env types."
  }
}

#defaults of "" allow easier conditionals
variable "sts_lambda_role_name" {
  type    = string
  default = ""
}

variable "s3_bucket_name" {
  type    = string
  default = ""
}

variable "modify_bucket" {
  type    = bool
  default = false
}

variable "ecr_image_uri" {
  type    = string
  default = ""
}

#####################################
##            Outputs              ##
#####################################

output "aws_region" {
  value = var.aws_region
}
output "s3_bucket_name" {
  value = module.tns_base.s3_bucket_name
}

#comp
output "sqs_out" {
  value = module.tns_base.sqs_out_arn
}
output "sns_out" {
  value = module.tns_base.sns_out_arn
}
output "sqs_in" {
  value = module.tns_base.sqs_in_arn
}
output "sns_in" {
  value = module.tns_base.sns_in_arn
}

output "container" {
  value = module.tns_base.image_uri
}

######################################
