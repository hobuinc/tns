variable image_uri {
    type = string
}

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
variable s3_cert_path {
    type = string
}

variable sts_lambda_role_name {
    type = string
}

variable s3_endpoint {
    type = string
}