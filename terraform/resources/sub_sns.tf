### Comparison lambda SNS/SQS In and Out
resource aws_sns_topic sub_sns_in {
    name_prefix = "tns_sub_sns_input"
}

resource aws_sns_topic sub_sns_out {
    name_prefix = "tns_sub_sns_output"
}

resource aws_sqs_queue sub_sqs_out {
    name_prefix = "tns_sub_sqs_output"

}

resource aws_sns_topic_subscription sub_sqs_sns_sub {
    topic_arn = aws_sns_topic.sub_sns_out.arn
    protocol = "sqs"
    endpoint = aws_sqs_queue.sub_sqs_out.arn
}

data aws_iam_policy_document sub_sqs_policy_doc {
    statement {
        sid = "AllowSQSFromSNS"
        effect = "Allow"
        actions = ["sqs:SendMessage"]
        resources = [aws_sqs_queue.sub_sqs_out.arn]
        principals {
            type        = "*"
            identifiers = ["*"]
        }
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [aws_sns_topic.sub_sns_out.arn]
        }
    }
}

resource aws_sqs_queue_policy sub_sqs_policy {
    queue_url = aws_sqs_queue.sub_sqs_out.id
    policy = data.aws_iam_policy_document.sub_sqs_policy_doc.json
}

# outputs
output sub_sns_in_arn {
    value = aws_sns_topic.sub_sns_in.arn
}

output sub_sns_out_arn {
    value = aws_sns_topic.sub_sns_out.arn
}