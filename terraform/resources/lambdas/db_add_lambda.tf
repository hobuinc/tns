resource aws_lambda_function db_add_lambda_function {
    depends_on = [ data.archive_file.lambda_zip ]
    filename = local.zip_path
    function_name = "tns_db_add_lambda"
    role = var.sts_lambda_role_name ==  "" ? aws_iam_role.sts_lambda_role[0].arn : data.aws_iam_role.sts_lambda_role[0].arn
    handler = "db_add_lambda.handler"
    runtime = "python3.12"

    environment {
        variables = {
            DB_TABLE_NAME: var.table_name
            SNS_OUT_ARN: var.db_add_sns_in_arn
        }
    }
}

resource aws_lambda_permission db_add_lambda_perm {
    statement_id  = "AllowExecutionFromSNS"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.db_add_lambda_function.function_name
    principal     = "sns.amazonaws.com"
    source_arn    = var.db_add_sns_in_arn
}

resource aws_sns_topic_subscription db_add_sns_in_sub {
    topic_arn = var.db_add_sns_in_arn
    protocol  = "lambda"
    endpoint  = aws_lambda_function.db_add_lambda_function.arn
}