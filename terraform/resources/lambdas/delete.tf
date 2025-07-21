resource aws_lambda_function db_delete_lambda_function {
    # depends_on = [ data.archive_file.lambda_zip ]
    # filename = local.zip_path
    # handler = "db_lambda.db_delete_handler"

    function_name = "tns_delete_lambda"
    role = var.sts_lambda_role_name ==  "" ? aws_iam_role.sts_lambda_role[0].arn : data.aws_iam_role.sts_lambda_role[0].arn
    timeout=300

    image_uri = var.image_uri
    package_type="Image"
    architectures = ["arm64"]
    image_config {
        command = ["tns_lambda.db_lambda.db_delete_handler"]
    }

    environment {
        variables = {
            DB_TABLE_NAME: var.table_name
            SNS_OUT_ARN: var.db_delete_sns_out_arn
        }
    }
}

resource aws_lambda_event_source_mapping delete_event_map {
    event_source_arn = var.db_delete_sqs_in_arn
    function_name    = aws_lambda_function.db_delete_lambda_function.arn
}
