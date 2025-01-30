### Database Addition lambda SNS/SQS In and Out
resource aws_sns_topic db_add_sns_in {
    name_prefix = "tns_db_add_input"
}

resource aws_sns_topic db_add_sns_out {
    name_prefix = "tns_db_add_sns_output"
}

resource aws_sqs_queue db_add_sqs_out {
    name_prefix = "tns_db_add_sqs_output"
}

resource aws_sns_topic_subscription db_add_sqs_sns_sub {
    topic_arn = aws_sns_topic.db_add_sns_out.arn
    protocol = "sqs"
    endpoint = aws_sqs_queue.db_add_sqs_out.arn
}

# TODO move this to inside the policy so it can be more easily replaced by import
data aws_iam_policy_document db_add_sqs_policy_doc {
    statement {
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

resource aws_sqs_queue_policy db_add_sqs_policy {
    queue_url = aws_sqs_queue.db_add_sqs_out.id
    policy = data.aws_iam_policy_document.db_add_sqs_policy_doc.json
}

output db_add_sns_in_arn {
    value = aws_sns_topic.db_add_sns_in.arn
}

output db_add_sns_out_arn {
    value = aws_sns_topic.db_add_sns_out.arn
}