# this assumes that the bucket you're supplying has the necessary
# policies and notifications attached to it already
variable s3_bucket_name {
    description = "Name of bucket to use. If empty, a new bucket will be created."
    type        = string
    default     = ""
}

locals {
    bucket_name = (
        var.s3_bucket_name == "" ?
        aws_s3_bucket.tns_bucket[0].id :
        var.s3_bucket_name
    )
    bucket_arn = (
        var.s3_bucket_name == "" ?
        aws_s3_bucket.tns_bucket[0].arn :
        data.aws_s3_bucket.tns_bucket_premade[0].arn
    )
}

resource aws_s3_bucket tns_bucket {
    count = var.s3_bucket_name == "" ? 1 : 0
    bucket = "tns-geodata-bucket"
}

data aws_s3_bucket tns_bucket_premade {
    count = var.s3_bucket_name == "" ? 0 : 1
    bucket = var.s3_bucket_name
}

# connect to db_add_sns_in
data aws_iam_policy_document sns_connect_policy {
    for_each = aws_sns_topic.sns_in
    statement {
        effect = "Allow"

        principals {
            type        = "Service"
            identifiers = ["s3.amazonaws.com"]
        }

        actions   = ["SNS:Publish"]
        resources = [each.value.arn]

        condition {
            test     = "ArnLike"
            variable = "aws:SourceArn"
            values   = [local.bucket_arn]
        }
    }
}

resource aws_s3_bucket_notification bucket_notification {
    bucket = local.bucket_name
    dynamic topic {
        for_each = local.actions
        content {
            topic_arn     = local.sns_in[topic.key].arn
            events        = ["s3:ObjectCreated:*"]
            filter_prefix = "${topic.key}/"
        }
    }
    depends_on = [aws_sns_topic_policy.sns_in_policy]
}

output s3_bucket_name {
    value = local.bucket_name
}