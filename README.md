# Tile Notification System (TNS)

## Overview
The Tile Notification System (TNS) creates a group of cloud architecture resources that respond to parquet files being pushed to a S3 bucket. These parquet files represent the latest Tiles to be ingested by GRiD and their associated geometries. TNS will then find the intersection between these Tiles and a set of AOI Subscriptions and return the results via S3.

TNS should be deployable on an AWS EC2 instance with no internet connection (beside the ability to interact with AWS API), given it has the correct permissions.

The main vehicle for this project is [Terraform](https://www.terraform.io/), which allows TNS to cohesively deploy and link AWS cloud architecture components together. Terraform's [provider mirror](https://developer.hashicorp.com/terraform/cli/commands/providers/mirror) capability allows projects to create a cache of language information and install from there, rather than reaching out to the Terraform registry over HTTP.

## Getting Started

TNS is split into 4 sections of operation:
1. Installing Dependencies
2. Quickstart
3. Initializing
4. Managing Infrastructure
5. Testing
6. Building Docker Image


### Quickstart Deployment
*Before beginning: Make sure system has appropriate permissions (see permissions.json, adjust `account_id` and `aws_region`).*

#### From a system with internet access
1. Install dependencies using conda.

    ```
    conda env create -f deploy_environment.yaml
    conda activate tns
    ```

2. Run the TNS Terraform init script.

    ```
    ./scripts/init
    ```

3. If deploying docker container *separate* from Terraform, run the `docker_init` script and copy `ecr_image_uri` output to terraform variables. The default value for REPO in `docker_init` will be `tns_ecr`, which would use (or create if it's not made yet) an ECR container named `{account_id}.dkr.ecr.{aws_region}.amazonaws.com/tns_ecr:amd64`.

    ```
    export DEFAULT_AWS_REGION="your_region"
    REPO="desired_repo_name"
    ./scripts/docker_init $REPO
    # copy output to ecr_image_uri in your terraform variables file
    ```


#### From the system without internet access
1. Create `terraform` env file if necessary, see [Set The Environment](#Set-The-Environment) for an example on how to do so.

2. Run terraform
    ```
    VAR_PATH="var-file.tfvars" # the path to the variables file
    ./scripts/up $VAR_PATH
    ```

**note: This is only for deployment, see [Testing](#testing)

### Installing Dependencies
Create an environment that has packages specified in `environment.yaml`. If you choose to use `conda` for this, run `conda env create -f environment.yaml`. This will create a conda environment with the name `tns`. This environment is only needed on your local machine for the initial installation so that we can install python packages and run `terraform`.

```
conda env create -f environment.yaml
conda activate tns
```

### Initializing on Local
This process will install all of the providers to the default location that `Terraform` looks for them, in `.terraform/plugins`. This will also install the necessary python packages into a zip file for usage with the lambda functions that are created. You will need access to the open web for this action.

To initialize the project, run the `init` script in the `scripts` directory.

```
./scripts/init
```

### Building the Docker image
The docker image can be built and deployed separately from Terraform.

```
./scripts/docker_init
```

Once the container is built, copy the image uri into the ecr_image_uri variable in your terraform variables file.

**note: You may need to first install QEMU by following the directions at https://docs.docker.com/build/building/multi-platform/#qemu.

**note: You may also need to set env variable AWS_DEFAULT_REGION="aws_region".

### Initialize on EC2 instance
From whatever system you're deploying from, you will need the copy of TNS that that was just initialized. From this TNS directory on your deployment system, run the `init_instance` script.

This will set the `TF_CLI_CONFIG_FILE` environment variable and run `terraform init` again for this system.

This assumes that you have installed `terraform` and are in an environment with it active.

Note: The file `permissions.json` has the required permissions for the ec2 instance that deploys these cloud resources. The variables `accound_id` and `aws_region` just need to be replaced with the correct variables.

### Infrastructure

#### Set the Environment

To use a `terraform` environment file, create a file in the base directory with
the variables you would like to change.

Sample:

```
aws_region="us-west-2"
env="prod"
sts_lambda_role_name="TNS_Testing_Role"
s3_bucket_name="tns-bucket-premade"
ecr_image_uri="copied_from_docker_init_output"
```

All of these variables have a default, so if you don't set them, that's okay. If you set the `sts_lambda_role_name` variable, no role will be created, and the one that you have supplied will be used as the profile for the lambdas.

**note: that the permissions in `permissions.json` are permissions *without*:
1. Lambda role creation
2. S3 bucket creation
3. S3 bucket modification
4. ECR image creation

Full variable options:

```
variable aws_region {
    type = string
    default = "us-west-2"
}

variable env {
    description="Determines which set of resources are created."
    type = string
    default = "prod"
    validation {
        condition = can(regex("^(prod|test)$", var.env))
        error_message = "prod or test are only available env types."
    }
}

variable conda_env_name {
    description="Conda environment to use."
    type = string
    default = "tns"
}

#defaults of "" allow easier conditionals
variable sts_lambda_role_name {
    description="Name of previously created IAM role for Compare lambda function."
    type = string
    default = ""
}

variable s3_bucket_name {
    description="Name of previously created S3 bucket."
    type = string
    default = ""
}

variable modify_bucket {
    description="If the S3 bucket should be modified with lifecycle events."
    type = bool
    default = "false"
}

variable ecr_image_uri {
    description="ECR Image URI, can be obtained from docker_init script."
    type = string
    default = ""
}

```

#### Deploy Resources

To deploy the cloud infrastructure, run the `up` script with the path to your variables file as the argument. `up` will ask you for a variable path if you don't provide one.

```
VAR_PATH="var-file.tfvars" # the path to the variables file you're using.
./scripts/up $VAR_PATH
```

```
export VAR_PATH="var-file.tfvars"
./scripts/up
```

```
./scripts/up

# and enter the variable file path in the terminal
> Variable file path? [None]: var-file.tfvars
```

#### Destroy Resources

To destroy the cloud resources, run the `down` script with the path to your variables file as an argument. This will reference `terraforms.tfstate` for the current state of the architecture.

```
./scripts/down
```

### Testing

There are three available ways to run tests on the infrastructure made from this
1. With unit tests, which require no resources are deployed and is the fasted repl.
2. With env=test, which tests the interaction between SNS/SQS and you can still develop the lambda code locally.
3. With env=prod, which is a full integration test, making sure that a deployment is fully operational.

In each of these scenarios, you can simply run `pytest` from the root directory and the correct tests will be selected based on the environment.

#### Unit tests
Unit tests are only run when there is no terraform deployment active.

#### Dev Mode
To run in development mode, you will create a base set of cloud resources needed to tie things together, and then you can run `pytest`. This set of tests will be using the local lambda file, allowing for you to develop those functions, while also pushing files to `S3`, prompting `SNS` and `SQS` messages. We'll then grab those and create an event from them, allowing this lambda to effectively act as if it were deployed.

```
### VAR_FILE contents
# env="test"
# aws_region="us-east-1"
# sts_lambda_role_name=""
```

#### Deployed Mode
For deployment mode, you'll be deploying the full set of architecture and using the outputs from `terraform` to populate the pytest fixtures again. This time though, the tests will push files to `S3`, prompting a message to the starting `SNS Topic`, and will listen to the outgoing `SQS Queue` for a response, testing the response for what it expects.

```
### VAR_FILE contents
# env="prod"
# aws_region="your_region"
# sts_lambda_role_name="your_role" # this is the default
# s3_bucket_name="your_bucket" # specify a custom bucket name as needed
# ecr_image_uri="ACCOUNT_NUMBER.dkr.ecr.REGION.amazonaws.com/REPO:amd64"
```
