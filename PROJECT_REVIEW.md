# Project Review

TNS is an AWS-based geospatial notification pipeline. It watches for parquet files written under `compare/` in S3, routes those object-created events through SNS and SQS, runs a Lambda that uses DuckDB spatial functions to intersect incoming tile geometries with stored AOIs in `subs/subscriptions.parquet`, writes the intersection results back to S3, and publishes a success or failure message to an output SNS topic for downstream consumers. The main behavior lives in `src/db_lambda.py`, while Terraform provisions the S3/SNS/SQS/Lambda/ECR pieces in `terraform/main.tf` and the `resources/` modules.

## Findings

1. Critical: the ECR image/tag logic in `terraform/resources/base/ecr.tf` is broken. `local.arch` is set to `"linux/amd64"`, then reused as a Docker tag and again in `docker buildx build --platform linux/${local.arch}`, which expands to `linux/linux/amd64`. It also makes the `RIE_ARCH` branch choose the wrong value. As written, image build/push is unlikely to work reliably.

2. Critical: the Lambda IAM policy in `terraform/resources/lambdas/common.tf` uses `Resource: "*"` inside `jsonencode`. In Terraform object syntax that should be `Resource = "*"`. If this file parses at all, it is by accident; more likely it breaks planning/apply.

3. High: the deployment test path is internally inconsistent and likely fails immediately. `put_parquet` expects a bucket name string in `src/conftest.py`, but `src/test_deployment.py` passes the whole Terraform output dict in several places.

4. High: one test fixture depends on a file that is not in the repository. `src/conftest.py` reads `../data/aoi_uc.parquet`, but there is no `data/` directory in this checkout. That makes `big_aoi_fill` unusable and the stress tests non-reproducible.

5. High: the documented test flow does not match the Terraform behavior. README says “dev mode” uses `env="test"` and then runs Lambda tests, but the Lambda module is only created when `env == "prod"` in `terraform/main.tf`. That makes the documented test environment misleading.

6. High: failure reporting can mask the original error. In `src/db_lambda.py`, `CloudConfig` catches initialization failures and immediately publishes to SNS using `self.sns_out_arn`; if `SNS_OUT_ARN` is missing, that publish can fail and replace the real root cause. This is especially risky during bootstrap failures.

7. Medium: message sizing in `src/db_lambda.py` uses `__sizeof__()` on encoded JSON, which is not the actual payload byte size AWS enforces. That can under- or over-estimate message size and make the recursive split logic unreliable for large AOI lists.

8. Medium: test control flow has at least one clear bug. The loop in `src/conftest.py` uses `while len(messages) < amt or count > retry`, which becomes effectively unbounded once `count > retry`. It looks like `and` or the inverse condition was intended.

9. Medium: cleanup in tests is brittle. `src/test_lambdas.py` deletes `/tmp` artifacts without checking whether they exist first, so tests can fail for environmental reasons unrelated to behavior.

10. Medium: infrastructure defaults are not production-safe. The S3 bucket name is hardcoded to `tns-geodata-bucket` in `terraform/resources/base/s3.tf`, which will collide globally in AWS. Resource names throughout `terraform/resources/base/sns.tf` are also fixed rather than environment-scoped.

11. Medium: several scripts are unsafe or unnecessarily fragile. `scripts/init` uses unquoted `rm` on computed paths, `scripts/up` uses `eval`, and `scripts/docker_init` copies source trees into the Docker context without cleanup, so repeated runs can accumulate stale files.

12. Low: the codebase has drift and dead material. H3 is described as central in `README.md`, and constants like `H3_RESOLUTION`, `MAX_H3_IDS_SQL`, and `SNS_BATCH_LIMIT` exist in `src/db_lambda.py`, but none of that is used in the actual compare path. The commented-out DynamoDB module in `terraform/resources/base/dynamo.tf` reinforces that the design changed without the docs/code being cleaned up.

## What Should Be Improved

1. Make deployment reproducible. Fix the Terraform syntax issues, repair the ECR build/tag logic, parameterize names by environment, and remove hardcoded global resource names.
2. Separate concerns in the Lambda. Move AWS client/bootstrap setup, DuckDB setup, event parsing, and compare execution into smaller functions with validation and explicit error types.
3. Make tests runnable without a live AWS stack for basic coverage. Add true unit tests for `get_data_paths`, `get_pass_res`, error handling, and SQL-generation behavior, then keep a smaller integration suite for AWS wiring.
4. Fix the repo’s source-of-truth story. README, Terraform behavior, and tests currently disagree about how environments work; choose one workflow and document only that.
5. Improve observability. Replace `print` statements with structured logging, emit correlation IDs, and log counts and S3 object paths in a machine-readable format.
6. Tighten operational safeguards. Add dead-letter handling strategy, idempotency considerations for repeated S3 events, and explicit handling for empty input batches or malformed messages.
7. Clean up packaging/build tooling. Prefer a single supported Docker/Terraform path, pin runtime images intentionally, and avoid scripts that depend on mutable working directories or `eval`.
8. Add project hygiene. Type hints are partial, error handling is broad, and there is duplication across tests; linting, formatting, static type checking, and a small CI pipeline would raise the floor quickly.

## Overall Assessment

The project has a solid core idea and a sensible event-driven shape, but right now it feels more like a working prototype than robust software. The biggest gaps are deployability, test reliability, and consistency between docs, infra, and code.

This review is based on static inspection of the checked-in code. The end-to-end tests were not run because they depend on live AWS resources and the repository is missing at least one required test dataset.
