variable conda_env_name {
    type = string
}

locals {
    lambda_dir = "${path.root}/../src"
    build_dir = "${path.root}/../build"
    zip_path = "${local.build_dir}/lambda_package.zip"
}

# this is for both db_add and comp functions
resource local_file db_code {
    # depends_on = [ null_resource.lambda_deps ]
    content = file("${local.lambda_dir}/db_lambda.py")
    filename = "${local.build_dir}/lambda_deps/db_lambda.py"
}

data archive_file lambda_zip {
    depends_on = [ local_file.db_code ]
    type = "zip"
    source_dir = "${local.build_dir}/lambda_deps"
    output_path = "${local.build_dir}/lambda_package.zip"
}

resource terraform_data replacement {
    input = local_file.db_code.content
    triggers_replace = [local_file.db_code.content]
}