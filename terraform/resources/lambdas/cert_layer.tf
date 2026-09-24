variable "env_name" {
  type = string
}
variable "cert_path" {
  type = string
}

locals {
  layer_cert_path = "${path.module}/certs/cert.pem"
}

data "local_file" "cert_data" {
  count    = can(regex("^(SC|TC)$", var.env_name)) ? 1 : 0
  filename = var.cert_path
}
resource "local_file" "cert_file" {
  count           = can(regex("^(SC|TC)$", var.env_name)) ? 1 : 0
  filename        = local.layer_cert_path
  content         = data.local_file.cert_data[0].content
  file_permission = "0600"
}

data "archive_file" "cert_layer_archive" {
  count       = can(regex("^(SC|TC)$", var.env_name)) ? 1 : 0
  type        = "zip"
  source_file = local_file.cert_file[0].filename
  output_path = "${path.module}/certs/cert_layer.zip"
}

resource "aws_lambda_layer_version" "cert_layer" {
  count = can(regex("^(SC|TC)$", var.env_name)) ? 1 : 0

  layer_name          = "${var.prefix}_${var.env_name}_tns_cert_layer"
  filename            = data.archive_file.cert_layer_archive[0].output_path
  source_code_hash    = data.archive_file.cert_layer_archive[0].output_base64sha256
  compatible_runtimes = ["provided.al2"]
}

