resource aws_lambda_function comp_lambda_function {
    depends_on = [ data.archive_file.lambda_zip ]
    filename = local.zip_path
    function_name = "tns_comp_lambda"
    role = var.sts_lambda_role_name == "" ? aws_iam_role.sts_lambda_role[0].arn : data.aws_iam_role.sts_lambda_role[0].arn
    handler = "db_lambda.comp_handler"
    runtime = "python3.12"

    environment {
        variables = {
            DB_TABLE_NAME: var.table_name
            SNS_OUT_ARN: var.comp_sns_out_arn
        }
    }
}

resource aws_lambda_permission comp_lambda_perm {
    statement_id  = "AllowExecutionFromSQS"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.comp_lambda_function.function_name
    principal     = "sns.amazonaws.com"
    source_arn    = var.comp_sqs_in_arn
}
