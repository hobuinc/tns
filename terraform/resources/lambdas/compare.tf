resource aws_lambda_function compare_function {

    function_name = "tns_comp_lambda"
    role = (var.sts_lambda_role_name == "" ?
            aws_iam_role.sts_lambda_role[0].arn :
            data.aws_iam_role.sts_lambda_role[0].arn)
    timeout=300
    memory_size = 3072

    image_uri = var.image_uri
    package_type="Image"
    architectures = ["x86_64"]
    image_config {
        command = ["tns_lambda.intersects_lambda.handler"]
    }

    environment {
        variables = {
            SNS_OUT_ARN: var.sns_out_arn
            S3_BUCKET: var.bucket_name
        }
    }

    lifecycle {
        replace_triggered_by = [ null_resource.always_run.id ]
    }

}

resource aws_lambda_event_source_mapping compare_event_map {
    event_source_arn = var.sqs_in_arn
    function_name    = aws_lambda_function.compare_function.arn
    batch_size = 30
    maximum_batching_window_in_seconds = 20
}
