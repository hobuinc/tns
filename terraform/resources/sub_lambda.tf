resource aws_lambda_function sub_lambda_function {
    depends_on = [ data.archive_file.lambda_zip ]
    filename = local.zip_path
    function_name = "tns_sub_lambda"
    role = var.sts_lambda_role_name == "" ? aws_iam_role.sts_lambda_role[0].arn : data.aws_iam_role.sts_lambda_role[0].arn
    handler = "sub_lambda.handler"
    runtime = "python3.12"

    environment {
        variables = {
            DB_TABLE_NAME: aws_dynamodb_table.geodata_table.name,
            SNS_OUT_ARN: aws_sns_topic.sub_sns_out.arn
        }
    }
}

resource aws_lambda_permission sub_lambda_perm {
    statement_id  = "AllowExecutionFromSNS"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.sub_lambda_function.function_name
    principal     = "sns.amazonaws.com"
    source_arn    = aws_sns_topic.sub_sns_in.arn
}

resource aws_sns_topic_subscription sub_sns_in_subscription {
    topic_arn = aws_sns_topic.sub_sns_in.arn
    protocol  = "lambda"
    endpoint  = aws_lambda_function.sub_lambda_function.arn
}
