### Database Addition lambda SNS/SQS In and Out
locals {
    actions = toset(["add", "delete", "compare"])

    sns_in = zipmap(local.actions, [for sns in aws_sns_topic.sns_in: sns])
    sqs_in = zipmap(local.actions, [for sqs in aws_sqs_queue.sqs_in: sqs])
    dlq_in = zipmap(local.actions, [for dlq in aws_sqs_queue.dlq_in: dlq])
    sns_in_policy_doc = zipmap(
        local.actions,
        [for pd in data.aws_iam_policy_document.sns_in_policy_doc: pd]
    )
    sqs_in_pd = zipmap(
        local.actions,
        [for pd in data.aws_iam_policy_document.sqs_in_policy_doc: pd]
    )
    sns_out = zipmap(local.actions, [for sns in aws_sns_topic.sns_out: sns])
    sqs_out = zipmap(local.actions, [for sqs in aws_sqs_queue.sqs_out: sqs])
    dlq_out = zipmap(local.actions, [for dlq in aws_sqs_queue.dlq_out: dlq])
    sqs_out_pd = zipmap(
        local.actions,
        [for pd in data.aws_iam_policy_document.sqs_out_policy_doc: pd]
    )

}

########### input resources ############
resource aws_sns_topic sns_in {
    for_each = local.actions
    name = "tns_${each.key}_sns_in"
}

resource aws_sns_topic_policy sns_in_policy {
    for_each = local.actions
    arn = aws_sns_topic.sns_in[each.value].arn
    policy = data.aws_iam_policy_document.sns_in_policy_doc[each.value].json
}

data aws_iam_policy_document sns_in_policy_doc {
    for_each = local.actions
    statement {
        effect = "Allow"
        principals {
            type = "Service"
            identifiers = ["s3.amazonaws.com"]
        }
        actions = [ "SNS:Publish" ]
        resources = [aws_sns_topic.sns_in[each.key].arn]
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [local.bucket_arn]
        }
    }
}

resource aws_sqs_queue sqs_in {
    for_each = local.actions
    name = "tns_${each.key}_sqs_input"
    visibility_timeout_seconds=300
    redrive_policy = jsonencode({
        deadLetterTargetArn = aws_sqs_queue.dlq_in[each.key].arn
        maxReceiveCount = 10
    })
}

resource aws_sqs_queue dlq_in {
    for_each = local.actions
    name = "tns_${each.key}_dlq_in"
}

resource aws_sqs_queue_redrive_allow_policy dlq_in_redrive_policy {
    for_each = local.actions
    queue_url = aws_sqs_queue.dlq_in[each.key].url

    redrive_allow_policy = jsonencode({
        redrivePermission = "byQueue",
        sourceQueueArns   = [aws_sqs_queue.sqs_in[each.key].arn]
    })
}

resource aws_sns_topic_subscription sqs_sns_in_sub {
    for_each = local.actions
    topic_arn = aws_sns_topic.sns_in[each.key].arn
    protocol = "sqs"
    endpoint = aws_sqs_queue.sqs_in[each.key].arn
}

resource aws_sqs_queue_policy sqs_in_policy {
    for_each = local.actions
    queue_url = aws_sqs_queue.sqs_in[each.key].url
    policy = data.aws_iam_policy_document.sqs_in_policy_doc[each.key].json
}

data aws_iam_policy_document sqs_in_policy_doc {
    for_each = local.actions
    statement {
        principals {
            type = "AWS"
            identifiers= ["*"]
        }
        sid = "AllowSQSFromSNS"
        effect = "Allow"
        actions = ["sqs:SendMessage"]
        resources = [aws_sqs_queue.sqs_in[each.key].arn]
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [aws_sns_topic.sns_in[each.key].arn]
        }
    }
}

########### output resources ############
resource aws_sns_topic sns_out {
    for_each = local.actions
    name = "tns_${each.key}_sns_output"
    display_name = "tns_${each.key}_sns_output"
}

resource aws_sqs_queue sqs_out {
    for_each = local.actions
    name = "tns_${each.value}_sqs_output"
    redrive_policy = jsonencode({
        deadLetterTargetArn = aws_sqs_queue.dlq_out[each.key].arn
        maxReceiveCount = 10
    })
}

resource aws_sqs_queue dlq_out {
    for_each = local.actions
    name = "tns_${each.key}_dlq_out"
}

resource aws_sqs_queue_redrive_allow_policy out_redrive_allow {
    for_each = local.actions
    queue_url = aws_sqs_queue.dlq_out[each.key].url
    redrive_allow_policy = jsonencode({
        redrivePermission = "byQueue",
        sourceQueueArns   = [aws_sqs_queue.sqs_out[each.key].arn]
    })
}

resource aws_sns_topic_subscription sqs_sns_out_sub {
    for_each = local.actions
    topic_arn = aws_sns_topic.sns_out[each.key].arn
    protocol = "sqs"
    endpoint = aws_sqs_queue.sqs_out[each.key].arn
}

data aws_iam_policy_document sqs_out_policy_doc {
    for_each = local.actions
    statement {
        principals {
            type = "AWS"
            identifiers= ["*"]
        }
        sid = "AllowSQSFromSNS"
        effect = "Allow"
        actions = ["sqs:SendMessage"]
        resources = [aws_sqs_queue.sqs_out[each.key].arn]
        condition {
            test = "ArnEquals"
            variable = "aws:SourceArn"
            values = [aws_sns_topic.sns_out[each.key].arn]
        }
    }
}

resource aws_sqs_queue_policy sqs_out_policy {
    for_each = local.actions
    queue_url = aws_sqs_queue.sqs_out[each.key].url
    policy = data.aws_iam_policy_document.sqs_out_policy_doc[each.key].json
}

#### OUTPUTS ####

#### Add ####
output db_add_sns_in_arn {
    value = aws_sns_topic.sns_in["add"].arn
}
output db_add_sns_out_arn {
    value = aws_sns_topic.sns_out["add"].arn
}
output db_add_sqs_in_arn {
    value = aws_sqs_queue.sqs_in["add"].arn
}
output db_add_sqs_out_arn {
    value = aws_sqs_queue.sqs_out["add"].arn
}
output db_add_dlq_in_arn {
    value = aws_sqs_queue.dlq_in["add"].arn
}
output db_add_dlq_out_arn {
    value = aws_sqs_queue.dlq_out["add"].arn
}

#### Delete ####
output db_delete_sns_in_arn {
    value = aws_sns_topic.sns_in["delete"].arn
}
output db_delete_sns_out_arn {
    value = aws_sns_topic.sns_out["delete"].arn
}
output db_delete_sqs_in_arn {
    value = aws_sqs_queue.sqs_in["delete"].arn
}
output db_delete_sqs_out_arn {
    value = aws_sqs_queue.sqs_out["delete"].arn
}
output db_delete_dlq_in_arn {
    value = aws_sqs_queue.dlq_in["delete"].arn
}
output db_delete_dlq_out_arn {
    value = aws_sqs_queue.dlq_out["delete"].arn
}

#### Compare ####
output db_comp_sns_in_arn {
    value = aws_sns_topic.sns_in["compare"].arn
}
output db_comp_sns_out_arn {
    value = aws_sns_topic.sns_out["compare"].arn
}
output db_comp_sqs_in_arn {
    value = aws_sqs_queue.sqs_in["compare"].arn
}
output db_comp_sqs_out_arn {
    value = aws_sqs_queue.sqs_out["compare"].arn
}
output db_comp_dlq_in_arn {
    value = aws_sqs_queue.dlq_in["compare"].arn
}
output db_comp_dlq_out_arn {
    value = aws_sqs_queue.dlq_out["compare"].arn
}