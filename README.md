# Tile Notification System (TNS)

## Overview
The Tile Notification System (TNS) creates a group of cloud architecture resources meant to manage a database of Areas of Interest (AOIs) with corresponding global index codes, in this case using [Uber's H3](https://www.uber.com/blog/h3/), which will allow the users to automatically compare geometries against those present in the database and detect overlaps.

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


### Quickstart
*Before beginning: Make sure system has appropriate permissions (see permissions.json, adjust `account_id` and `aws_region`).*

#### From a system with internet access
1. Install dependencies using conda.
    ```
    conda env create -f environment.yaml
    conda activate tns
    ```
2. Run the TNS Terraform init script.
    ```
    ./scripts/init
    ```
3. If deploying docker container separate from Terraform, run docker init script and copy ecr_image_uri output to terraform variables.
    ```
    ./scripts/docker_init $REPO
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

Note that the permissions in `permissions.json` are permissions *without*:
1. Lambda role creation
2. S3 bucket creation
3. S3 bucket modification
4. ECR image creation

Full permissions options:

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
1. With unit tests, which require no resources deployed and is the fasted repl.
2. With env=test, which tests the interaction between SNS/SQS and you can still develop the lambda code locally.
3. With env=prod, which is a full integration test, making sure that a deployment is fully operational.

#### Unit tests
To run the unit tests, you can you use `pytest` on `src/test_units.py`.

```
pytest src/test_units.py
```

#### Dev Mode
To run in development mode, you will create a base set of cloud resources needed to tie things together, and then you can run the file `test_lambdas.py` with `pytest`. These tests will grab the resource names from the terraform outputs, and will populate `pytest` fixtures with those values.

```
### VAR_FILE contents
# env="test"
# aws_region="us-east-1"
# sts_lambda_role_name="" # this is the default

./scripts/up $VAR_FILE # deploy the dev environment
pytest src/test_lambdas.py # test against that dev environment
```

#### Deployed Mode
For deployment mode, you'll be deploying the full set of architecture, and then using the outputs from `terraform` to populate the pytest fixtures again. This time though, the tests will send a message to the starting `SNS Topic` and will listen to the outgoing `SQS Queue` for a response, testing the response for what it expects.

```
### VAR_FILE contents
# env="test"
# aws_region="us-east-1"
# sts_lambda_role_name="TNS_Testing_Role" # this is the default
# s3_bucket_name="grid-dev-tns" # specify a custom bucket name as needed
# ecr_image_uri="ACCOUNT_NUMBER.dkr.ecr.us-east-1.amazonaws.com/tns_ecr:amd64" # specify ecr_image_uri if you built the image outside of terraform using the docker_init script

./scripts/up $VAR_FILE # deploy the production environment
pytest src/test_deployment.py
```
