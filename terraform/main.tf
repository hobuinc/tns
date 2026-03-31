terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.84.0"
    }
  }
  backend local {
    path = "terraform.tfstate"
  }
}


provider "aws" {
    region = var.aws_region
}

module tns_base {
    source = "./resources/base"
    modify_bucket = var.modify_bucket
    s3_bucket_name = var.s3_bucket_name
    ecr_image_uri = var.ecr_image_uri
    env = var.env
}

module tns_lambdas {
    # only make this in prod env #
    count = var.env == "prod" ? 1 : 0
    source = "./resources/lambdas"
    conda_env_name = var.conda_env_name
    sts_lambda_role_name = var.sts_lambda_role_name

    image_uri = module.tns_base.image_uri
    bucket_name = module.tns_base.s3_bucket_name

    sqs_in_arn = module.tns_base.sqs_in_arn
    sns_out_arn = module.tns_base.sns_out_arn
}

####################################
##            Inputs              ##
####################################

variable aws_region {
    type = string
    default = "us-west-2"
}

variable env {
    description="Determines which set of resources are created."
    type = string
    default = "prod"
    validation {
        condition = can(regex("^(prod|test)$", var.env))
        error_message = "prod or test are only available env types."
    }
}

variable conda_env_name {
    description="Conda environment to use."
    type = string
    default = "tns"
}

#defaults of "" allow easier conditionals
variable sts_lambda_role_name {
    description="Name of previously created IAM role for Compare lambda function."
    type = string
    default = ""
}

variable s3_bucket_name {
    description="Name of previously created S3 bucket."
    type = string
    default = ""
}

variable modify_bucket {
    description="If the S3 bucket should be modified with lifecycle events."
    type = bool
    default = "false"
}

variable ecr_image_uri {
    description="ECR Image URI, can be obtained from docker_init script."
    type = string
    default = ""
}

#####################################
##            Outputs              ##
#####################################

output env {
    value = var.env
}
output aws_region {
    value = var.aws_region
}
output s3_bucket_name {
    value = module.tns_base.s3_bucket_name
}

#comp
output sqs_out {
    value = module.tns_base.sqs_out_arn
}
output sns_out {
    value = module.tns_base.sns_out_arn
}
output sqs_in {
    value = module.tns_base.sqs_in_arn
}
output sns_in {
    value = module.tns_base.sns_in_arn
}
output container {
    value = module.tns_base.image_uri
}

######################################
