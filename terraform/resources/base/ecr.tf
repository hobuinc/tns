data aws_region current { }
data "aws_caller_identity" "current" { }
variable ecr_image_uri {
    type=string
}
variable env {
    type=string
}

locals {
    ecr_repository_name = "tns-${var.env}-ecr"
    platform = "linux/amd64"
    image_tag = "amd64"
    rie_arch = "x86_64"
    python_version = "3.13"
    lambda_base_image = "amazon/aws-lambda-provided:al2023.2025.12.22.12-x86_64"
    image_uri = (var.ecr_image_uri == "" ?
        "${aws_ecr_repository.runner_ecr_repo[0].repository_url}:${local.image_tag}" :
        "${var.ecr_image_uri}"
    )
}

resource aws_ecr_repository runner_ecr_repo {
    count = var.ecr_image_uri == "" ? 1 : 0
    name = local.ecr_repository_name
    image_tag_mutability = "MUTABLE"
    force_delete = true
}

resource null_resource ecr_image {
    count = var.ecr_image_uri == "" ? 1 : 0
    triggers = {
        docker_file = md5(file("${path.module}/docker/Dockerfile"))
        environment_file = md5(file("${path.module}/docker/run-environment.yml"))
        entry_file = md5(file("${path.module}/docker/python-entry.sh"))
        handlers = sha1(join("", [for f in fileset("${path.module}/../../../src/", "**"): filesha1("${path.module}/../../../src/${f}")]))
    }

    provisioner "local-exec" {
        command = <<EOF
                set -e
                aws ecr get-login-password --region ${data.aws_region.current.name} \
                | docker login --username AWS \
                --password-stdin "https://${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com"


                rm -rf "${path.module}/docker/handlers"
                mkdir -p "${path.module}/docker/handlers"
                cp -r "${path.module}/../../../src/." "${path.module}/docker/handlers/"
                echo "Building image platform ${local.platform} with image $LAMBDA_IMAGE"
                docker buildx build --platform linux/${local.platform} \
                    --no-cache \
                    --build-arg LAMBDA_IMAGE="${local.lambda_base_image}" \
                    --build-arg PYTHON_VERSION="${local.python_version}" \
                    --build-arg RIE_ARCH=${local.rie_arch} \
                    --load \
                    -t ${local.image_uri} \
                    "${path.module}/docker/" \
                    -f "${path.module}/docker/Dockerfile"
                docker push ${local.image_uri} -q
            EOF
        }
}


data aws_ecr_image runner_image {
    count = var.ecr_image_uri == "" ? 1 : 0
    repository_name = aws_ecr_repository.runner_ecr_repo[0].name
    image_tag = local.arch
    depends_on = [ null_resource.ecr_image, aws_ecr_repository.runner_ecr_repo ]
}

output image_uri {
    value = local.image_uri
}
