### Comparison lambda SNS/SQS In and Out
resource aws_sns_topic comp_sns_in {
    name_prefix = "tns_comp_sns_input"
}

resource aws_sns_topic comp_sns_out {
    name_prefix = "tns_comp_sns_output"
}

resource aws_sqs_queue comp_sqs_out {
    name_prefix = "tns_comp_sqs_output"

}

resource aws_sns_topic_subscription comp_sqs_sns_sub {
    topic_arn = aws_sns_topic.comp_sns_out.arn
    protocol = "sqs"
    endpoint = aws_sqs_queue.comp_sqs_out.arn
}

data aws_iam_policy_document comp_sqs_policy_doc {
    statement {
        sid = "AllowSQSFromSNS"
        effect = "Allow"
        actions = ["sqs:SendMessage"]
        resources = [aws_sqs_queue.comp_sqs_out.arn]
        principals {
            type        = "*"
            identifiers = ["*"]
        }
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [aws_sns_topic.comp_sns_out.arn]
        }
    }
}

resource aws_sqs_queue_policy comp_sqs_policy {
    queue_url = aws_sqs_queue.comp_sqs_out.id
    policy = data.aws_iam_policy_document.comp_sqs_policy_doc.json
}

# outputs
output comp_sns_in_arn {
    value = aws_sns_topic.comp_sns_in.arn
}

output comp_sns_out_arn {
    value = aws_sns_topic.comp_sns_out.arn
}

output comp_sqs_out_arn {
    value = aws_sqs_queue.comp_sqs_out.arn
}