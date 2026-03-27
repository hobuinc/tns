locals {
  name_prefix          = "tns-${var.env}"
  lambda_function_name = "${local.name_prefix}-comp-lambda"
  lambda_log_group_arn = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/lambda/${local.lambda_function_name}"
}

data "aws_iam_role" "sts_lambda_role" {
  count = var.sts_lambda_role_name == "" ? 0 : 1
  name  = var.sts_lambda_role_name
}

resource "aws_iam_role" "sts_lambda_role" {
  count = var.sts_lambda_role_name == "" ? 1 : 0
  name  = "${local.name_prefix}-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  count = var.sts_lambda_role_name == "" ? 1 : 0
  name  = "${local.name_prefix}-lambda-policy"
  role  = aws_iam_role.sts_lambda_role[0].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "PublishSnsMessage"
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = ["${var.sns_out_arn}"]
      },
      {
        Sid    = "ReceiveSqsMessage"
        Effect = "Allow"
        Action = [
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ReceiveMessage",
          "sqs:ChangeMessageVisibility",
          "sqs:GetQueueUrl"
        ]
        Resource = ["${var.sqs_in_arn}"]

      },
      {
        Sid    = "WriteFunctionLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          "${local.lambda_log_group_arn}:*"
        ]
      },
      {
        Sid    = "GetS3Object"
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ],
        Resource = [
          "arn:aws:s3:::${var.bucket_name}/compare/*.parquet",
          "arn:aws:s3:::${var.bucket_name}/subs/*.parquet"
        ]
      },
      {
        Sid    = "WriteIntersects"
        Effect = "Allow"
        Action = [
          "s3:PutObject"
        ],
        Resource = [
          "arn:aws:s3:::${var.bucket_name}/intersects/*.parquet",
        ]
      }
    ]
  })
}

output "lambda_role_name" {
  value = var.sts_lambda_role_name == "" ? aws_iam_role.sts_lambda_role[0].name : data.aws_iam_role.sts_lambda_role[0].name
}
