# Tile Notification System (TNS)

## Overview
The Tile Notification System (TNS) creates a group of cloud architecture resources meant to manage a database of Areas of Interest (AOIs) with corresponding global index codes, in this case using [Uber's H3](https://www.uber.com/blog/h3/), which will allow the users to automatically compare geometries against those present in the database and detect overlaps.

TNS should be deployable on an AWS EC2 instance with no internet connection (beside the ability to interact with AWS API), given it has the correct permissions.

The main vehicle for this project is [Terraform](https://www.terraform.io/), which allows TNS to cohesively deploy and link AWS cloud architecture components together. Terraform's [provider mirror](https://developer.hashicorp.com/terraform/cli/commands/providers/mirror) capability allows projects to create a cache of language information and install from there, rather than reaching out to the Terraform registry over HTTP.

## Getting Started

TNS is split into 4 sections of operation:
1. Installing Dependencies
2. Initializing
3. Managing Infrastructure
4. Testing

### Installing Dependencies
Create an environment that has packages specified in `environment.yaml`. If you choose to use `conda` for this, run `conda env create -f environment.yaml`. This will create a conda environment with the name `tns`. This environment is only needed on your local machine for the initial installation so that we can install python packages and run `terraform`.

```
git clone git@github.com/hobuinc/tns.git
cd tns
conda env create -f environment.yaml
```

### Initializing on Local
This process will install all of the providers to the default location that `Terraform` looks for them, in `.terraform/plugins`. This will also install the necessary python packages into a zip file for usage with the lambda functions that are created. You will need access to the open web for this action.

To initialize the project, run the `init` script in the `scripts` directory.

```
./scripts/init
```

### Initialize on EC2 instance
From whatever system you're deploying from, you will need the copy of TNS that that was just initialized. From this TNS directory on your deployment system, run the `init_instance` script.

This will set the `TF_CLI_CONFIG_FILE` environment variable and run `terraform init` again for this system.

This assumes that you have installed `terraform` and are in an environment with it active.

Note: The file `permissions.json` has the required permissions for the ec2 instance that deploys these cloud resources. The variables `accound_id` and `aws_region` just need to be replaced with the correct variables.

### Infrastructure
To create the TNS infrastructure, run

#### Set the Environment

To use a `terraform` environment file, create a file in the base directory with
the variables you would like to change. Here is a sample:

```
aws_region="us-west-2"
env="prod"
sts_lambda_role_name="TNS_Testing_Role"
```

All of these variables have a default, so if you don't set them, that's okay. If you set the `sts_lambda_role_name` variable, no role will be created, and the one that you have supplied will be used as the profile for the lambdas.

#### Deploy Resources

To deploy the cloud infrastructure, run the `up` script with the path to your variables file as the argument.

```
VAR_PATH=var-file.tfvars # the path to the variables file you're using.
./scripts/up $VAR_PATH
```

##### [Optional] Deploy Docker Container

You can optionally build and push the docker container that all of the lambdas are built upon before building any other resources.

```
VAR_PATH=var-file.tfvars
AWS_REGION=us-west-2
REPO_NAME=tns_ecr
./scripts/docker_init ${REPO_NAME}
# copy paste the line below (with subbed ecr image uri) into variable file
# ecr_image_uri="${ecr_image_uri}"
./scripts/up $VAR_PATH
```

#### Destroy Resources

To destroy the cloud resources, run the `down` script with the path to your variables file as an argument.

```
VAR_PATH="var-file.tfvars"
./scripts/down $VAR_PATH
```

### Testing

There are two available ways to run tests on the infrastructure made from this, either in a development mode, in which you're actively developing the lambda code, or in a deployment mode, where you're testing that the created resources work as intended. Both of these require more advanced permissions than you would need for just deploying resources from whatever system you're on.

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

./scripts/up $VAR_FILE # deploy the production environment
pytest src/test_deployment.py
```