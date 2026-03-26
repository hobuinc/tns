# this assumes that the bucket you're supplying has the necessary
# policies and notifications attached to it already

variable "s3_bucket_name" {
  description = "Name of bucket to use. If empty, a new bucket will be created."
  type        = string
  default     = ""
}

variable "modify_bucket" {
  type = bool
}

locals {
  name_prefix = "tns-${var.env}"
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

resource "aws_s3_bucket" "tns_bucket" {
  count  = var.s3_bucket_name == "" ? 1 : 0
  bucket = "${local.name_prefix}-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}"
}

resource "aws_s3_bucket_lifecycle_configuration" "action_lifecycles" {
  count  = var.modify_bucket ? 1 : 0
  bucket = local.bucket_name
  dynamic "rule" {
    for_each = ["compare", "intersects"]
    content {
      id     = "${rule.key}_${local.bucket_name}_lifecycle"
      status = "Enabled"
      filter {
        prefix = "${rule.key}/"
      }
      expiration {
        days = 14
      }
    }
  }
}

data "aws_s3_bucket" "tns_bucket_premade" {
  count  = var.s3_bucket_name == "" ? 0 : 1
  bucket = var.s3_bucket_name
}

# connect to db_add_sns_in
data "aws_iam_policy_document" "sns_connect_policy" {
  statement {
    effect    = "Allow"
    actions   = ["SNS:Publish"]
    resources = [aws_sns_topic.sns_in.arn]
    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [local.bucket_arn]
    }
  }
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = local.bucket_name
  topic {
    topic_arn     = aws_sns_topic.sns_in.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "compare/"
  }
  depends_on = [aws_sns_topic_policy.sns_in_policy]
}

output "s3_bucket_name" {
  value = local.bucket_name
}
