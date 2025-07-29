data aws_region current { }
data "aws_caller_identity" "current" { }
variable ecr_image_uri {
    type=string
}

locals {
    ecr_repository_name = "tns_sm_ecr"
    arch = "arm64"
    python_version = "3.13"
    image_uri = (var.ecr_image_uri == "" ?
        "${aws_ecr_repository.runner_ecr_repo[0].repository_url}:${local.arch}" :
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
    depends_on = [
        aws_ecr_repository.runner_ecr_repo
    ]
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
                if [ "${local.arch}" = "arm64" ]; then
                    LAMBDA_IMAGE="amazon/aws-lambda-provided:al2023.2024.05.01.10"
                else
                    LAMBDA_IMAGE="amazon/aws-lambda-provided:al2"
                fi

                cp -r "${path.module}/../../../src/" "${path.module}/docker/handlers"
                echo "Building image architecture ${local.arch} with image $LAMBDA_IMAGE"
                docker buildx build --platform linux/${local.arch} \
                    --no-cache \
                    --build-arg LAMBDA_IMAGE="$LAMBDA_IMAGE" \
                    --build-arg PYTHON_VERSION="${local.python_version}" \
                    --build-arg RIE_ARCH=${local.arch == "amd64" ? "x86_64" : "arm64"} \
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
