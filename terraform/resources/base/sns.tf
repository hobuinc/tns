####### lambda SNS/SQS In and Out ######
########### input resources ############
resource aws_sns_topic sns_in {
    name = "${var.prefix}_tns_compare_sns_in"
}

resource aws_sns_topic_policy sns_in_policy {
    arn = aws_sns_topic.sns_in.arn
    policy = data.aws_iam_policy_document.sns_in_policy_doc.json
}

data aws_iam_policy_document sns_in_policy_doc {
    statement {
        effect = "Allow"
        principals {
            type = "Service"
            identifiers = ["s3.amazonaws.com"]
        }
        actions = [ "SNS:Publish" ]
        resources = [aws_sns_topic.sns_in.arn]
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [local.bucket_arn]
        }
    }
}

resource aws_sqs_queue sqs_in {
    name = "${var.prefix}_tns_compare_sqs_input"
    visibility_timeout_seconds=300
    redrive_policy = jsonencode({
        deadLetterTargetArn = aws_sqs_queue.dlq_in.arn
        maxReceiveCount = 10
    })
}

resource aws_sqs_queue dlq_in {
    name = "${var.prefix}_tns_compare_dlq_in"
}

resource aws_sqs_queue_redrive_allow_policy dlq_in_redrive_policy {
    queue_url = aws_sqs_queue.dlq_in.url

    redrive_allow_policy = jsonencode({
        redrivePermission = "byQueue",
        sourceQueueArns   = [aws_sqs_queue.sqs_in.arn]
    })
}

resource aws_sns_topic_subscription sqs_sns_in_sub {
    topic_arn = aws_sns_topic.sns_in.arn
    protocol = "sqs"
    endpoint = aws_sqs_queue.sqs_in.arn
}

resource aws_sqs_queue_policy sqs_in_policy {
    queue_url = aws_sqs_queue.sqs_in.url
    policy = data.aws_iam_policy_document.sqs_in_policy_doc.json
}

data aws_iam_policy_document sqs_in_policy_doc {
    statement {
        principals {
            type = "AWS"
            identifiers= ["*"]
        }
        sid = "AllowSQSFromSNS"
        effect = "Allow"
        actions = ["sqs:SendMessage"]
        resources = [aws_sqs_queue.sqs_in.arn]
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [aws_sns_topic.sns_in.arn]
        }
    }
}

########### output resources ############
resource aws_sns_topic sns_out {
    name = "${var.prefix}_tns_compare_sns_output"
}

resource aws_sqs_queue sqs_out {
    name = "${var.prefix}_tns_compare_sqs_output"
    redrive_policy = jsonencode({
        deadLetterTargetArn = aws_sqs_queue.dlq_out.arn
        maxReceiveCount = 10
    })
}

resource aws_sqs_queue dlq_out {
    name = "${var.prefix}_tns_compare_dlq_out"
}

resource aws_sqs_queue_redrive_allow_policy out_redrive_allow {
    queue_url = aws_sqs_queue.dlq_out.url
    redrive_allow_policy = jsonencode({
        redrivePermission = "byQueue",
        sourceQueueArns   = [aws_sqs_queue.sqs_out.arn]
    })
}

resource aws_sns_topic_subscription sqs_sns_out_sub {
    topic_arn = aws_sns_topic.sns_out.arn
    protocol = "sqs"
    endpoint = aws_sqs_queue.sqs_out.arn
}

data aws_iam_policy_document sqs_out_policy_doc {
    statement {
        principals {
            type = "AWS"
            identifiers= ["*"]
        }
        sid = "AllowSQSFromSNS"
        effect = "Allow"
        actions = ["sqs:SendMessage"]
        resources = [aws_sqs_queue.sqs_out.arn]
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [aws_sns_topic.sns_out.arn]
        }
    }
}

resource aws_sqs_queue_policy sqs_out_policy {
    queue_url = aws_sqs_queue.sqs_out.url
    policy = data.aws_iam_policy_document.sqs_out_policy_doc.json
}

#### OUTPUTS ####

output sns_in_arn {
    value = aws_sns_topic.sns_in.arn
}
output sns_out_arn {
    value = aws_sns_topic.sns_out.arn
}
output sqs_in_arn {
    value = aws_sqs_queue.sqs_in.arn
}
output sqs_out_arn {
    value = aws_sqs_queue.sqs_out.arn
}
output dlq_in_arn {
    value = aws_sqs_queue.dlq_in.arn
}
output dlq_out_arn {
    value = aws_sqs_queue.dlq_out.arn
}