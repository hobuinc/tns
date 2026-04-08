variable image_uri {
    type = string
}

# compare.tf
variable sns_out_arn {
    type = string
}
variable sqs_in_arn {
    type = string
}
variable prefix {
    type = string
}
variable memory_size {
    default = 5120
    type = number
}


# common.tf
variable sts_lambda_role_name {
    type = string
}
