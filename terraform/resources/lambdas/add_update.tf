resource aws_lambda_function db_add_lambda_function {
    depends_on = [ data.archive_file.lambda_zip ]
    filename = local.zip_path
    function_name = "tns_db_add_lambda"
    role = var.sts_lambda_role_name ==  "" ? aws_iam_role.sts_lambda_role[0].arn : data.aws_iam_role.sts_lambda_role[0].arn
    handler = "db_lambda.db_add_handler"
    runtime = "python3.12"
    timeout=300

    environment {
        variables = {
            DB_TABLE_NAME: var.table_name
            SNS_OUT_ARN: var.db_add_sns_out_arn
        }
    }
    lifecycle {
        replace_triggered_by = [ terraform_data.replacement ]
    }
}

resource aws_lambda_event_source_mapping add_update_event_map {
    event_source_arn = var.db_add_sqs_in_arn
    function_name    = aws_lambda_function.db_add_lambda_function.arn
}
