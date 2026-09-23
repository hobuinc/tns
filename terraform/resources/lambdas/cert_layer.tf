variable env_name {
    type = string
}
variable ca_path {
    type = string
}
variable pem_path {
    type = string
}

locals {
    layer_cert_dir = "${path.module}/certs"
    layer_ca_path = "${local.layer_cert_dir}/ca-bundle.crt"
    layer_pem_path = "${local.layer_cert_dir}/cert.pem"
}

data local_sensitive_file cert_data {
    count = can(regex("^(SC|TC)$", var.env_name)) ? 1 : 0
    filename = var.ca_path
}
resource local_sensitive_file cert_file {
    filename = "${local.layer_ca_path}"
}

