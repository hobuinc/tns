# Tile Notification System

## Overview

The Tile Notification System (TNS) is an event-driven AWS workflow for comparing incoming GeoParquet tile datasets against a stored set of Areas of Interest (AOIs).

When a GeoParquet file is written to the `compare/` prefix in S3:

1. S3 publishes an object-created notification to an SNS topic.
2. The SNS topic fans that event into an SQS queue.
3. A Lambda consumes the SQS batch, reads the incoming GeoParquet tile files and the AOI GeoParquet dataset, computes spatial intersections, writes a Parquet result under `intersects/`, and publishes a success or failure message to an output SNS topic.

The spatial compare logic is implemented in pure Python with GeoPandas and Shapely, which makes it testable locally without live AWS resources.

## Repository Layout

- `src/tns_core.py`: GeoParquet loading, intersection logic, and result writing.
- `src/db_lambda.py`: AWS Lambda handler, event parsing, and SNS publishing.
- `src/test_intersections.py`: local unit tests for the GeoParquet intersection path using `src/geoms.json`.
- `terraform/`: AWS infrastructure definitions.
- `scripts/`: helper scripts for initialization, deployment, teardown, and Docker image builds.

## Development Environment

Create and activate the conda environment:

```bash
conda env create -f environment.yaml
conda activate tns
```

If the environment already exists and you updated `environment.yaml`, run:

```bash
conda env update -f environment.yaml --prune
```

## Local Testing

Run the fast local test suite:

```bash
pytest -q
```

This covers the GeoParquet intersection logic and Lambda message handling without requiring AWS.

Live AWS tests are intentionally opt-in. To enable them, export `TNS_RUN_AWS_TESTS=1` and run the integration tests against deployed infrastructure.

## Infrastructure Workflow

### 1. Initialize Terraform providers

On a machine with internet access:

```bash
./scripts/init
```

This prepares Terraform plugins under `terraform/.terraform/plugins` so they can be reused later.

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

### 4. Deploy

```bash
./scripts/up path/to/vars.tfvars
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
