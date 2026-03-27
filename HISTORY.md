# History

## Refactor Overview

This document records the changes made during the `codex-refactor` branch work, along with the reasoning behind each change. The goal of the refactor was to turn the repository from a tightly coupled prototype into a more testable, maintainable, and operationally consistent codebase.

## 1. Reworked the Python architecture

### What changed

- Added [src/tns_core.py](/Users/hobu/dev/git/tns/src/tns_core.py) as a new pure-processing module.
- Rebuilt [src/db_lambda.py](/Users/hobu/dev/git/tns/src/db_lambda.py) as a thin Lambda adapter.
- Added [src/__init__.py](/Users/hobu/dev/git/tns/src/__init__.py) to make the source directory behave as a package.

### Why

The original implementation mixed AWS client creation, DuckDB extension bootstrapping, S3 path handling, SQL execution, and message publishing in one file. That made the most important logic hard to test and hard to reason about. The refactor separates concerns:

- `tns_core.py` owns GeoParquet staging, validation, DuckDB spatial joins, and output writing.
- `db_lambda.py` owns environment config, event parsing, and SNS publishing.

This split makes the spatial workflow locally testable without AWS and makes failures easier to isolate.

## 2. Returned to a DuckDB-centric compare path

### What changed

- Implemented the core compare path with DuckDB and its spatial extension in [src/tns_core.py](/Users/hobu/dev/git/tns/src/tns_core.py).
- Kept the thinner Lambda structure introduced during the refactor.
- Reworked the local fixtures so test parquet files are generated with DuckDB instead of GeoPandas/Shapely.

### Why

After the first refactor pass, the in-memory GeoPandas/Shapely path proved too heavy in both memory use and dependency footprint. Moving the spatial work back into DuckDB keeps the package leaner and aligns better with the project's original execution model, while still preserving the improved modularity and testability from the refactor.

## 3. Added repository-local unit tests for intersection logic

### What changed

- Added [src/test_intersections.py](/Users/hobu/dev/git/tns/src/test_intersections.py).
- Replaced the old infrastructure-heavy fixture setup in [src/conftest.py](/Users/hobu/dev/git/tns/src/conftest.py) with local DuckDB-backed parquet fixtures based on [src/geoms.json](/Users/hobu/dev/git/tns/src/geoms.json).
- Reworked [src/test_lambdas.py](/Users/hobu/dev/git/tns/src/test_lambdas.py) to test event parsing, message sizing, local compare execution, and failure publishing without live AWS.
- Converted [src/test_deployment.py](/Users/hobu/dev/git/tns/src/test_deployment.py) into an explicit opt-in placeholder for live infrastructure tests.

### Why

The original tests assumed deployed AWS resources, live Terraform output, and even a missing local dataset under `data/`. That meant the most important behavior could not be verified from a clean checkout. The new test strategy creates a reliable local safety net:

- Unit tests verify the actual intersection path.
- Lambda-facing tests verify message generation and error handling.
- Integration coverage is still acknowledged, but no longer blocks routine local development.

## 4. Improved runtime configuration and error handling

### What changed

- Added `AppConfig` and `AppContext` in [src/db_lambda.py](/Users/hobu/dev/git/tns/src/db_lambda.py).
- Stopped trying to publish failures before configuration is safely established.
- Added clearer boundaries between config loading, processing, and publish steps.
- Switched success payload sizing to real UTF-8 byte length instead of `__sizeof__()`.

### Why

The previous initialization path could mask the original failure by trying to publish to SNS before the runtime was fully configured. The new structure preserves root causes better and makes the Lambda path safer during startup and failure handling.

## 5. Added docstrings across the Python code

### What changed

- Added module-level and function-level docstrings to all Python modules in `src/`.

### Why

The refactor introduced clearer boundaries, and the docstrings make those boundaries explicit for future maintainers. This improves onboarding and reduces the amount of reverse engineering required to understand how the DuckDB compare path works.

## 6. Updated the development environment

### What changed

- Updated [environment.yaml](/Users/hobu/dev/git/tns/environment.yaml) to reflect the actual development and test dependencies.
- Added `duckdb` and `pytest` explicitly.
- Removed the extra GeoPandas/Shapely application dependency weight from the runtime story.

### Why

The original environment file did not fully represent what was needed to develop and test the project after the refactor. The environment should be a truthful source of tooling requirements, not a partial hint.

## 7. Cleaned up pytest configuration

### What changed

- Updated [pytest.ini](/Users/hobu/dev/git/tns/pytest.ini) to document the `integration` marker.

### Why

The test suite now has a deliberate distinction between local unit coverage and live AWS integration coverage. The pytest configuration should reflect that structure.

## 8. Repaired and simplified Terraform

### What changed

- Updated [terraform/main.tf](/Users/hobu/dev/git/tns/terraform/main.tf) to always create the Lambda module and to support `dev`, `test`, and `prod` environments.
- Fixed Terraform typing issues such as the boolean default for `modify_bucket`.
- Scoped resource names by environment across the Terraform modules.
- Fixed the IAM policy object syntax in [terraform/resources/lambdas/common.tf](/Users/hobu/dev/git/tns/terraform/resources/lambdas/common.tf).
- Fixed image tag and platform handling in [terraform/resources/base/ecr.tf](/Users/hobu/dev/git/tns/terraform/resources/base/ecr.tf).

### Why

The original infrastructure definitions had inconsistencies between documentation and behavior, fragile hardcoded names, and at least one HCL syntax problem. Environment-scoped names and corrected types make the deployment story more predictable and less collision-prone.

## 9. Deleted unused Terraform configuration

### What changed

- Removed the commented DynamoDB file at [terraform/resources/base/dynamo.tf](/Users/hobu/dev/git/tns/terraform/resources/base/dynamo.tf).
- Removed the unused Lambda dependency locals file at [terraform/resources/lambdas/lambda_deps.tf](/Users/hobu/dev/git/tns/terraform/resources/lambdas/lambda_deps.tf).

### Why

These files were artifacts of an older design and no longer contributed to the refactored deployment. Leaving dead Terraform files in place increases confusion and makes the configuration harder to audit.

## 10. Reworked Docker packaging

### What changed

- Updated [terraform/resources/base/docker/Dockerfile](/Users/hobu/dev/git/tns/terraform/resources/base/docker/Dockerfile) to use a pinned Lambda Runtime Interface Emulator release.
- Removed the stale `config.py` copy step that no longer matched the repository layout.
- Updated [terraform/resources/base/docker/run-environment.yml](/Users/hobu/dev/git/tns/terraform/resources/base/docker/run-environment.yml) to match the refactored DuckDB runtime.

### Why

The container build should reflect the actual code path being deployed. The previous Docker setup carried stale assumptions from the older Lambda implementation and risked packaging unused or misleading runtime dependencies.

## 11. Hardened the helper scripts

### What changed

- Updated [scripts/init](/Users/hobu/dev/git/tns/scripts/init), [scripts/up](/Users/hobu/dev/git/tns/scripts/up), [scripts/down](/Users/hobu/dev/git/tns/scripts/down), [scripts/init_instance](/Users/hobu/dev/git/tns/scripts/init_instance), and [scripts/docker_init](/Users/hobu/dev/git/tns/scripts/docker_init).
- Added `set -euo pipefail`.
- Removed `eval`.
- Added safer quoting and path handling.
- Cleaned up Docker build staging behavior.
- Made manual image build naming environment-aware.

### Why

The original scripts were fragile because they used unquoted variables, `eval`, and mutable build staging without cleanup. Those choices are easy to trip over in real operational use and make debugging harder than it needs to be.

## 12. Rewrote the README

### What changed

- Replaced [README.md](/Users/hobu/dev/git/tns/README.md) with documentation that describes the current event flow, test strategy, environment setup, and deployment process.

### Why

The original README still described H3 and a deployment/testing flow that no longer matched the code. Documentation should tell the truth about how the project works today, not preserve outdated architectural intent.

## 13. Added Terraform lockfile and ignored transient artifacts

### What changed

- Added [terraform/.terraform.lock.hcl](/Users/hobu/dev/git/tns/terraform/.terraform.lock.hcl) for provider reproducibility.
- Updated [.gitignore](/Users/hobu/dev/git/tns/.gitignore) to ignore `.terraform/`.

### Why

The lockfile helps make provider resolution reproducible across machines. The local `.terraform/` working directory is generated state and should not be treated as source.

## 14. Verification performed

### Completed checks

- `conda run -n tns pytest -q`
- `bash -n` against the refactored shell scripts
- `terraform fmt -recursive`
- `terraform init -backend=false`

### Remaining caveat

`terraform validate` still fails in this environment because the provider plugins are not loading correctly under the current `conda run` runtime. The failure appears to be environmental and occurs after provider installation, not during HCL parsing or module resolution.

## 15. Deployment debugging after the last commit

### What happened

After the last commit, live AWS deployment work continued against the `hobutnstest` environment in `us-west-2`. That deployment work exposed several issues that did not appear in the local unit suite:

- The first deployment attempts failed before reaching a healthy Lambda because the image packaging path in [terraform/resources/base/docker/Dockerfile](/Users/hobu/dev/git/tns/terraform/resources/base/docker/Dockerfile) relied on `conda-pack` to relocate the runtime environment, and that step failed with `CondaPackError`.
- Once the image packaging was repaired, Terraform still raced ahead and attempted to create the Lambda function before the ECR image tag was available to Lambda.
- After the Lambda could be created, the first end-to-end smoke test still failed because [src/db_lambda.py](/Users/hobu/dev/git/tns/src/db_lambda.py) imported `tns_core` as a top-level module, while the deployed image packages the source under `tns_lambda/`.

### What changed

- Reworked [terraform/resources/base/docker/Dockerfile](/Users/hobu/dev/git/tns/terraform/resources/base/docker/Dockerfile) so the runtime Conda environment is created directly at `/var/task` instead of being relocated with `conda-pack`.
- Pinned Python in [terraform/resources/base/docker/run-environment.yml](/Users/hobu/dev/git/tns/terraform/resources/base/docker/run-environment.yml) so the packaged site-packages path matches the Lambda image layout.
- Updated [terraform/resources/base/ecr.tf](/Users/hobu/dev/git/tns/terraform/resources/base/ecr.tf) to output the resolved ECR image URI from `aws_ecr_image`, not just the tag string.
- Added an explicit `depends_on = [module.tns_base]` to the Lambda module call in [terraform/main.tf](/Users/hobu/dev/git/tns/terraform/main.tf) so the function deployment waits for the image-producing module.
- Updated [src/db_lambda.py](/Users/hobu/dev/git/tns/src/db_lambda.py) to use a package-relative import for `tns_core`, with a local fallback for direct module execution during development.

### Why

These changes were all driven by real deployment failures:

- Removing `conda-pack` eliminated a packaging path that was fragile in practice and unnecessary once the environment was built directly at the Lambda runtime prefix.
- Returning the ECR image output as a resolved image digest gave Terraform a real dependency edge, which stopped Lambda creation from racing the image push.
- The relative import change made the Lambda code behave the same way in both local development and the packaged container image.

### Outcome

- A full `terraform apply` completed successfully for `hobutnstest`.
- An end-to-end smoke test uploaded `subs/subscriptions.parquet` and a `compare/smoke-*.parquet` input object, and the deployed stack produced a valid `intersects/*.parquet` output with the expected three AOI-to-tile matches for Alabama, Alaska, and Arizona.
- The smoke objects were removed and the `hobutnstest` deployment was fully destroyed afterward, so the repository history now records both the successful deployment path and the completed teardown.

## Stress Test

## What was tested

This session focused on measuring the deployed compare pipeline under larger live AWS workloads and then tightening the stack based on what the measurements showed. The tests were run against the `hobutnstest` environment in `us-west-2` with the CLI utility in [src/test_deployment.py](/Users/hobu/dev/git/tns/src/test_deployment.py).

The main live scenarios exercised were:

- `100000` tiles with the default `--tiles-per-file 1000`
- `1000000` tiles with the default `--tiles-per-file 1000`
- `100000` tiles with `--tiles-per-file 10000`
- `1000000` tiles with `--tiles-per-file 10000`
- `100000` tiles with `--tiles-per-file 20000`
- `1000000` tiles with `--tiles-per-file 20000`

## What we found

The earliest stress runs exposed two important truths:

- The deployed Lambda was not failing or throttling under load.
- The original stress utility expected the wrong success-message shape.

CloudWatch showed that the Lambda had zero throttles and zero errors even when the stress utility timed out. The real issue was that the output queue carried one success message per processed batch, not one per AOI, while the old stress logic expected `file_count * state_count` success messages. That mismatch made healthy runs look like failures.

Once the stress utility was updated to validate source-file coverage and output parquet correctness instead of raw AOI message count, the benchmark story became much clearer.

## Stress benchmark results

### Before optimization

- `100000` tiles with `--tiles-per-file 1000`
  - initial stress utility failed with `TimeoutError`
  - root cause was incorrect message-count expectations, not Lambda failure
- `1000000` tiles with `--tiles-per-file 1000`
  - processing largely completed, but the run ended with an S3 cleanup failure
  - root cause was `DeleteObjects` being called with more than 1000 keys in one request

### Utility fixes made during investigation

- Reworked [src/test_deployment.py](/Users/hobu/dev/git/tns/src/test_deployment.py) so the stress scenario:
  - tracks all uploaded compare parquet source URIs
  - verifies those source URIs are represented across success messages
  - validates each output parquet file
  - cleans up output objects as part of the scenario
- Fixed S3 cleanup to delete keys in chunks of 1000 instead of one oversized request

### After optimization and utility fixes

- `100000` tiles with `--tiles-per-file 10000`
  - `success_messages`: `6`
  - `duration_seconds`: `21.2`
- `100000` tiles with `--tiles-per-file 20000`
  - `success_messages`: `4`
  - `duration_seconds`: `23.9`
- `1000000` tiles with `--tiles-per-file 10000`
  - `success_messages`: `59`
  - `duration_seconds`: `66.81`
- `1000000` tiles with `--tiles-per-file 20000`
  - `success_messages`: `33`
  - `duration_seconds`: `43.52`

### What changed in response

#### Batching and queue behavior

The original Lambda event source mapping in [terraform/resources/lambdas/compare.tf](/Users/hobu/dev/git/tns/terraform/resources/lambdas/compare.tf) was configured with:

- `batch_size = 30`
- `maximum_batching_window_in_seconds = 20`

That configuration favored relatively small Lambda batches and a longer queue wait before invocation. Based on the stress runs, the mapping was changed to:

- `batch_size = 100`
- `maximum_batching_window_in_seconds = 5`

This reduces orchestration overhead by allowing each invocation to consume more SQS work while also shortening the delay before batches begin draining.

#### Lambda sizing

The Lambda memory size in [terraform/resources/lambdas/compare.tf](/Users/hobu/dev/git/tns/terraform/resources/lambdas/compare.tf) was increased from:

- `3072 MB` to `5120 MB`

That also increases available CPU, which is useful because the workload is not just network-bound. Each invocation is doing S3 I/O, schema validation, DuckDB geometry conversion, and the spatial join itself.

#### Lambda runtime behavior

The DuckDB/S3 runtime in [src/tns_core.py](/Users/hobu/dev/git/tns/src/tns_core.py) was optimized to reduce warm-invocation overhead:

- DuckDB connections are now reused across warm Lambda invocations instead of recreated every time.
- S3-backed parquet inputs are now cached locally by `URI + ETag`.

That change especially helps the AOI parquet under `subs/subscriptions.parquet`, which is read repeatedly across many invocations during a stress run.

### What the results suggest

The `tiles-per-file` tuning has a workload-size crossover:

- For `100000` tiles, `--tiles-per-file 10000` was slightly better than `20000`.
- For `1000000` tiles, `--tiles-per-file 20000` was significantly better than `10000`.

That implies two opposing costs:

- more files increase S3/SNS/SQS/Lambda orchestration overhead
- larger files increase per-batch DuckDB work

At smaller scales, the extra compute per larger batch can outweigh the reduced orchestration. At larger scales, the orchestration savings become more important and larger files win.

### Other opportunities to make things faster

- Reuse the AOI dataset inside DuckDB as a temporary table during a warm container lifecycle instead of reparsing the parquet for every invocation.
- Prevalidate or preproject compare parquet files earlier in the pipeline so Lambda does less repeated per-file setup work.
- Add explicit Lambda reserved concurrency tuning so high-volume runs can scale out more predictably.
- Consider dynamic `tiles-per-file` sizing in the deployment utility so small and large scenarios use different optimal object counts.
- Reduce duplicated schema validation when multiple files are known to come from the same generator and format contract.
- Capture CloudWatch benchmark summaries automatically from [src/test_deployment.py](/Users/hobu/dev/git/tns/src/test_deployment.py) so throughput, average duration, and output-file counts are preserved with each run.
