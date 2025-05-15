### Database Addition lambda SNS/SQS In and Out
# input resources
resource aws_sns_topic db_add_sns_in {
    name = "tns_db_add_sns_input"
    display_name = "tns_db_add_sns_input"
}
resource aws_sqs_queue db_add_sqs_in {
    name = "tns_db_add_sqs_input"
    visibility_timeout_seconds=30
    redrive_policy = jsonencode({
        deadLetterTargetArn = aws_sqs_queue.db_add_dlq_in.arn
        maxReceiveCount = 10
    })
}
resource aws_sqs_queue db_add_dlq_in {
    name = "tns_db_add_dlq_in"
}
resource aws_sqs_queue_redrive_allow_policy db_add_in_redrive_allow {
    queue_url = aws_sqs_queue.db_add_dlq_in.id

    redrive_allow_policy = jsonencode({
        redrivePermission = "byQueue",
        sourceQueueArns   = [aws_sqs_queue.db_add_sqs_in.arn]
    })
}
resource aws_sns_topic_subscription db_add_sqs_sns_in_sub {
    topic_arn = aws_sns_topic.db_add_sns_in.arn
    protocol = "sqs"
    endpoint = aws_sqs_queue.db_add_sqs_in.arn
}
data aws_iam_policy_document db_add_sqs_in_policy_doc {
    statement {
        principals {
            type = "AWS"
            identifiers= ["*"]
        }
        sid = "AllowSQSFromSNS"
        effect = "Allow"
        actions = ["sqs:SendMessage"]
        resources = [aws_sqs_queue.db_add_sqs_in.arn]
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [aws_sns_topic.db_add_sns_in.arn]
        }
    }
}
resource aws_sqs_queue_policy db_add_sqs_in_policy {
    queue_url = aws_sqs_queue.db_add_sqs_in.id
    policy = data.aws_iam_policy_document.db_add_sqs_in_policy_doc.json
}


# output resources
resource aws_sns_topic db_add_sns_out {
    name = "tns_db_add_sns_output"
    display_name = "tns_db_add_sns_output"
}
resource aws_sqs_queue db_add_sqs_out {
    name = "tns_db_add_sqs_output"
    redrive_policy = jsonencode({
        deadLetterTargetArn = aws_sqs_queue.db_add_dlq_out.arn
        maxReceiveCount = 10
    })
}
resource aws_sqs_queue db_add_dlq_out {
    name = "tns_db_add_dlq_out"
}
resource aws_sqs_queue_redrive_allow_policy db_add_out_redrive_allow {
    queue_url = aws_sqs_queue.db_add_dlq_out.id

    redrive_allow_policy = jsonencode({
        redrivePermission = "byQueue",
        sourceQueueArns   = [aws_sqs_queue.db_add_sqs_out.arn]
    })
}
resource aws_sns_topic_subscription db_add_sqs_sns_out_sub {
    topic_arn = aws_sns_topic.db_add_sns_out.arn
    protocol = "sqs"
    endpoint = aws_sqs_queue.db_add_sqs_out.arn
}
data aws_iam_policy_document db_add_sqs_out_policy_doc {
    statement {
        principals {
            type = "AWS"
            identifiers= ["*"]
        }
        sid = "AllowSQSFromSNS"
        effect = "Allow"
        actions = ["sqs:SendMessage"]
        resources = [aws_sqs_queue.db_add_sqs_out.arn]
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [aws_sns_topic.db_add_sns_out.arn]
        }
    }
}
resource aws_sqs_queue_policy db_add_sqs_out_policy {
    queue_url = aws_sqs_queue.db_add_sqs_out.id
    policy = data.aws_iam_policy_document.db_add_sqs_out_policy_doc.json
}

#outputs
output db_add_sns_in_arn {
    value = aws_sns_topic.db_add_sns_in.arn
}
output db_add_sqs_in_arn {
    value = aws_sqs_queue.db_add_sqs_in.arn
}
output db_add_sns_out_arn {
    value = aws_sns_topic.db_add_sns_out.arn
}
output db_add_sqs_out_arn {
    value = aws_sqs_queue.db_add_sqs_out.arn
}
output db_add_in_dlq_arn {
    value = aws_sqs_queue.db_add_dlq_in
}
output db_add_out_dlq_arn {
    value = aws_sqs_queue.db_add_dlq_out
}