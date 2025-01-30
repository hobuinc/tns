## Lambda Logging ##
data aws_iam_policy logging_policy {
    count = var.logging_policy_arn == "" ? 0 : 1
    arn = var.logging_policy_arn
}

resource aws_iam_policy logging_policy {
    count = var.logging_policy_arn == "" ? 1 : 0
    name = "tns_lambda_logging_policy"
    policy = jsonencode({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "*"
            }
        ]
    })
}

## Lambda Execution Role ##
variable sts_lambda_role_name {
    type = string
}

resource aws_iam_role_policy_attachment log_policy_attach {
    role = var.sts_lambda_role_name == "" ? aws_iam_role.sts_lambda_role[0].name : data.aws_iam_role.sts_lambda_role[0].name
    policy_arn = var.logging_policy_arn == "" ? aws_iam_policy.logging_policy[0].arn : data.aws_iam_policy.logging_policy[0].arn
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
                    "${var.comp_sns_out_arn}",
                    "${var.db_add_sns_out_arn}"
                ]
            },
            {
                Sid = "FilterSnsMessage"
                Effect = "Allow"
                Action = [
                    "sns:SetSubscriptionAttributes",
                    "sns:*"
                    # cut this down to sns:ListSubscriptionByTopic
                ]
                Resource = [
                    "${var.comp_sns_out_arn}",
                ]
            },
            {
                Sid = "QueryDynamo"
                Effect = "Allow",
                Action = [
                    "dynamodb:GetItem",
                    "dynamodb:BatchGetItem",
                    "dynamodb:Query",
                    "dynamodb:PutItem",
                    "dynamodb:BatchWriteItem"
                ]
                Resource = "${var.table_arn}"
            }
        ]
    })
}
