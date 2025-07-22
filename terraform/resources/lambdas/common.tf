## Lambda Execution Role ##
variable bucket_name {
    type = string
}

resource null_resource always_run {
    triggers = {
        timestamp = "${timestamp()}"
    }
}

data aws_iam_role sts_lambda_role {
    count = var.sts_lambda_role_name == "" ? 0 : 1
    name = var.sts_lambda_role_name
}

resource aws_iam_role sts_lambda_role {
    count = var.sts_lambda_role_name == "" ? 1 : 0
    name = "tns_lambda_role"
    assume_role_policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
            {
                Action = "sts:AssumeRole"
                Effect = "Allow"
                Sid = ""
                Principal = {
                    Service = "lambda.amazonaws.com"
                }
            }
        ]
    })
}

resource aws_iam_role_policy lambda_policy {
    count = var.sts_lambda_role_name == "" ? 1 : 0
    name = "tns_lambda_policy"
    role = aws_iam_role.sts_lambda_role[0].name
    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
            {
                Sid = "PublishSnsMessage"
                Effect = "Allow"
                Action = "sns:Publish"
                Resource = [
                    "${var.db_comp_sns_out_arn}",
                    "${var.db_add_sns_out_arn}",
                    "${var.db_delete_sns_out_arn}"
                ]
            },
            {
                Sid = "ReceiveSqsMessage"
                Effect = "Allow"
                Action = [
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                    "sqs:ReceiveMessage",
                    "sqs:ChangeMessageVisibility",
                    "sqs:GetQueueUrl"
                ]
                Resource = [
                    "${var.db_comp_sqs_in_arn}",
                    "${var.db_add_sqs_in_arn}",
                    "${var.db_delete_sqs_in_arn}",
                ]

            },
            {
                Sid = "QueryDynamo"
                Effect = "Allow"
                Action = [
                    "dynamodb:GetItem",
                    "dynamodb:BatchGetItem",
                    "dynamodb:Query",
                    "dynamodb:PutItem",
                    "dynamodb:BatchWriteItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:Scan"
                ]
                Resource = "${var.table_arn}"
            },
            {
                Sid = "LogCreation"
                Effect = "Allow"
                Action = [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ]
                Resource: "*"
            },
            {
                Sid = "GetS3Object"
                Effect = "Allow"
                Action = [
                    "s3:GetObject"
                ],
                Resource = [
                    "arn:aws:s3:::${var.bucket_name}/add/*.parquet",
                    "arn:aws:s3:::${var.bucket_name}/compare/*.parquet",
                    "arn:aws:s3:::${var.bucket_name}/delete/*.parquet"
                ]
            }

        ]
    })
}
