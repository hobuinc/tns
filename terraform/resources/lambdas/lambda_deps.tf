variable conda_env_name {
    type = string
}

locals {
    lambda_dir = "${path.root}/../src"
    docker_dir = "${path.root}/lambdas/docker"
    build_dir = "${path.root}/../build"
    zip_path = "${local.build_dir}/lambda_package.zip"
}