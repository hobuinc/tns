terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.84.0"
    }
  }
}

provider "aws" {
    region = "us-west-2"
}

module resources {
    source = "./resources"
    conda_env_name = var.conda_env_name
    logging_policy_arn=var.logging_policy_arn
    sts_lambda_role_name=var.sts_lambda_role_name
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
output table_name {
    value = module.resources.table_name
}

output comp_sqs_out {
    value = module.resources.comp_sqs_out_arn
}
output comp_sns_out {
    value = module.resources.comp_sns_out_arn
}
output comp_sns_in {
    value = module.resources.comp_sns_in_arn
}

output db_add_sns_out {
    value = module.resources.db_add_sns_out_arn
}
output db_add_sns_in {
    value = module.resources.db_add_sns_in_arn
}

output sub_sns_out {
    value = module.resources.sub_sns_out_arn
}
output sub_sns_in {
    value = module.resources.sub_sns_in_arn
}
######################################
