### Database Addition lambda SNS/SQS In and Out
# input resources
resource aws_sns_topic db_comp_sns_in {
    name_prefix = "tns_db_comp_sns_input"
}
resource aws_sqs_queue db_comp_sqs_in {
    name_prefix = "tns_db_comp_sqs_input"
    visibility_timeout_seconds=300
}
resource aws_sns_topic_subscription db_comp_sqs_sns_in_sub {
    topic_arn = aws_sns_topic.db_comp_sns_in.arn
    protocol = "sqs"
    endpoint = aws_sqs_queue.db_comp_sqs_in.arn
}
data aws_iam_policy_document db_comp_sqs_in_policy_doc {
    statement {
        principals {
            type = "AWS"
            identifiers= ["*"]
        }
        sid = "AllowSQSFromSNS1"
        effect = "Allow"
        actions = ["sqs:SendMessage"]
        resources = [aws_sqs_queue.db_comp_sqs_in.arn]
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [aws_sns_topic.db_comp_sns_in.arn]
        }
    }
}
resource aws_sqs_queue_policy db_comp_sqs_in_policy {
    queue_url = aws_sqs_queue.db_comp_sqs_in.id
    policy = data.aws_iam_policy_document.db_comp_sqs_in_policy_doc.json
}


# output resources
resource aws_sns_topic db_comp_sns_out {
    name_prefix = "tns_db_comp_sns_output"
}
resource aws_sqs_queue db_comp_sqs_out {
    name_prefix = "tns_db_comp_sqs_output"
}
resource aws_sns_topic_subscription db_comp_sqs_sns_out_sub {
    topic_arn = aws_sns_topic.db_comp_sns_out.arn
    protocol = "sqs"
    endpoint = aws_sqs_queue.db_comp_sqs_out.arn
}
data aws_iam_policy_document db_comp_sqs_out_policy_doc {
    statement {
        principals {
            type = "AWS"
            identifiers= ["*"]
        }
        sid = "AllowSQSFromSNS2"
        effect = "Allow"
        actions = ["sqs:SendMessage"]
        resources = [aws_sqs_queue.db_comp_sqs_out.arn]
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [aws_sns_topic.db_comp_sns_out.arn]
        }
    }
}
resource aws_sqs_queue_policy db_comp_sqs_out_policy {
    queue_url = aws_sqs_queue.db_comp_sqs_out.id
    policy = data.aws_iam_policy_document.db_comp_sqs_out_policy_doc.json
}

#outputs
output db_comp_sns_in_arn {
    value = aws_sns_topic.db_comp_sns_in.arn
}
output db_comp_sns_out_arn {
    value = aws_sns_topic.db_comp_sns_out.arn
}
output db_comp_sqs_in_arn {
    value = aws_sqs_queue.db_comp_sqs_in.arn
}
output db_comp_sqs_out_arn {
    value = aws_sqs_queue.db_comp_sqs_out.arn
}