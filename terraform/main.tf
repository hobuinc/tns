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

module tns_base {
    source = "./resources/base"
}

module tns_lambdas {
    # only make this in prod env #
    count=var.env=="prod" ? 1 : 0

    source = "./resources/lambdas"
    conda_env_name = var.conda_env_name
    logging_policy_arn=var.logging_policy_arn
    sts_lambda_role_name=var.sts_lambda_role_name

    table_name = module.tns_base.table_name
    table_arn = module.tns_base.table_arn

    db_comp_sqs_in_arn = module.tns_base.db_comp_sqs_in_arn
    db_comp_sns_out_arn = module.tns_base.db_comp_sns_out_arn

    db_add_sqs_in_arn = module.tns_base.db_add_sqs_in_arn
    db_add_sns_out_arn = module.tns_base.db_add_sns_out_arn

    db_delete_sqs_in_arn =  module.tns_base.db_delete_sqs_in_arn
    db_delete_sns_out_arn = module.tns_base.db_delete_sns_out_arn
}

variable aws_region {
    type = string
    default = "us-west-2"
}

variable env {
    type = string
    default = "prod"
    validation {
        condition = can(regex("^(prod|test)$", var.env))
        error_message = "prod or test are only available env types."
    }
}

variable conda_env_name {
    type = string
    default = "tns"
}

#defaults of "" allow easier conditionals
variable logging_policy_arn {
    type=string
    default=""
}
variable sts_lambda_role_name {
    type = string
    default=""
}

############# Outputs ###############
output aws_region {
    value = var.aws_region
}
output table_name {
    value = module.tns_base.table_name
}

#comp
output db_comp_sqs_out {
    value = module.tns_base.db_comp_sqs_out_arn
}
output db_comp_sns_out {
    value = module.tns_base.db_comp_sns_out_arn
}
output db_comp_sqs_in {
    value = module.tns_base.db_comp_sqs_in_arn
}
output db_comp_sns_in {
    value = module.tns_base.db_comp_sns_in_arn
}

#db_add/update
output db_add_sns_in {
    value = module.tns_base.db_add_sns_in_arn
}
output db_add_sns_out {
    value = module.tns_base.db_add_sns_out_arn
}
output db_add_sqs_in {
    value = module.tns_base.db_add_sqs_in_arn
}
output db_add_sqs_out {
    value = module.tns_base.db_add_sqs_out_arn
}

#db_delete
output db_delete_sns_in {
    value = module.tns_base.db_delete_sns_in_arn
}
output db_delete_sns_out {
    value = module.tns_base.db_delete_sns_out_arn
}
output db_delete_sqs_in {
    value = module.tns_base.db_delete_sqs_in_arn
}
output db_delete_sqs_out {
    value = module.tns_base.db_delete_sqs_out_arn
}

######################################
