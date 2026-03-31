# Tile Notification System

## Overview

The Tile Notification System (TNS) is an event-driven AWS workflow for comparing incoming GeoParquet tile datasets against a stored set of Areas of Interest (AOIs).

When a GeoParquet file is written to the `compare/` prefix in S3:

1. S3 publishes an object-created notification to an SNS topic.
2. The SNS topic fans that event into an SQS queue.
3. A Lambda consumes the SQS batch, reads the incoming GeoParquet tile files and the AOI GeoParquet dataset, runs a DuckDB spatial join, writes a Parquet result under `intersects/`, and publishes a success or failure message to an output SNS topic.

The spatial compare logic is implemented with DuckDB's spatial extension, which keeps the runtime and dependency footprint smaller than a GeoPandas/Shapely stack while still allowing local unit coverage.

## Repository Layout

- `src/tns_core.py`: DuckDB-backed GeoParquet staging, spatial join logic, and result writing.
- `src/db_lambda.py`: AWS Lambda handler, event parsing, and SNS publishing.
- `src/test_intersections.py`: local unit tests for the DuckDB GeoParquet intersection path using `src/geoms.json`.
- `terraform/`: AWS infrastructure definitions.
- `scripts/`: helper scripts for initialization, deployment, teardown, and Docker image builds.

## Development Environment

This repository now uses Pixi for local development and deployment tooling.

Install the workspace environment:

```bash
pixi install
```

Run commands from the workspace with `pixi exec` or `pixi run`:

```bash
pixi run test
pixi exec python --version
```

The top-level [pixi.toml](/Users/hobu/dev/git/tns/pixi.toml) defines the local toolchain used by the
deployment scripts. The Lambda image build also uses Pixi from
[terraform/resources/base/docker/pixi.toml](/Users/hobu/dev/git/tns/terraform/resources/base/docker/pixi.toml),
so both local tooling and runtime packaging resolve dependencies through the same ecosystem.

## Local Testing

Run the fast local test suite:

```bash
pixi run test
```

This covers the DuckDB GeoParquet intersection logic and Lambda message handling without requiring AWS.

Live AWS tests are intentionally opt-in. To enable them, export `TNS_RUN_AWS_TESTS=1` and run the integration tests against deployed infrastructure.

## Infrastructure Workflow

### 1. Initialize Terraform providers

On a machine with internet access:

```bash
./scripts/init
```

This prepares Terraform plugins under `terraform/.terraform/plugins` so they can be reused later. The
helper script runs Terraform from the repo's Pixi environment.

### 2. Initialize a deployment host

On the machine that will run Terraform:

```bash
./scripts/init_instance
```

### 3. Configure variables

Create a `.tfvars` file, for example:

```tfvars
aws_region = "us-west-2"
env = "dev"
sts_lambda_role_name = ""
```

Optional variables:

- `s3_bucket_name`: use an existing bucket instead of creating one.
- `modify_bucket`: apply lifecycle rules to the selected bucket.
- `ecr_image_uri`: use an already-pushed Lambda image instead of building one in Terraform.
- `sts_lambda_role_name`: use an existing Lambda execution role instead of letting Terraform create `tns-<env>-lambda-role`.

### 4. Deploy

```bash
./scripts/up path/to/vars.tfvars
```

You can also pass extra Terraform CLI arguments through the helper script. For example, to explicitly supply an existing role name from the command line:

```bash
./scripts/up path/to/vars.tfvars -var="sts_lambda_role_name=my-existing-lambda-role"
```

### 5. Destroy

```bash
./scripts/down path/to/vars.tfvars
```

## Docker Image Builds

You can build and push the Lambda image manually:

```bash
TNS_ENV=dev ./scripts/docker_init
```

The script will create or reuse an ECR repository named `tns-<env>-ecr` and print the resulting `ecr_image_uri`.

## Notes

- Resource names are environment-scoped using `env` values such as `dev`, `test`, or `prod`.
- The AOI source dataset is expected at `subs/subscriptions.parquet`.
- Result records are written as Parquet under `intersects/`.
- Output messages contain `source_files`, `aoi_list`, `s3_output_path`, and `status` attributes.
- `./scripts/init`, `./scripts/up`, `./scripts/down`, and `./scripts/docker_init` all resolve
  `terraform`, `aws`, `jq`, and `python` from the repo's Pixi workspace.
