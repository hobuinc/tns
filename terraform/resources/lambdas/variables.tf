# comp_lambda.tf and db_add.tf
variable table_name {
    type = string
}
variable table_arn {
    type = string
}

# comp_lambda.tf
variable comp_sns_out_arn {
    type = string
}
variable comp_sns_in_arn {
    type = string
}

# db_add_lambda.tf
variable db_add_sns_out_arn {
    type = string
}
variable db_add_sns_in_arn {
    type = string
}

# common.tf
variable logging_policy_arn {
    type = string
}
variable sts_lambda_role_name {
    type = string
}
