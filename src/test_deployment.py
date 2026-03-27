"""CLI utility for exercising a live TNS deployment.

This module is intentionally separate from the fast local unit suite. It is a
deployment utility that uploads repository fixture data to a provisioned AWS
stack and validates the compare pipeline end to end.

Scenarios
---------
- ``lambda`` uploads a single AOI parquet and one compare parquet, then checks
  that a success message and output parquet are produced.
- ``stress`` uploads many compare parquet files and waits for the expected
  number of success messages, while also summarizing Lambda metrics.

How to execute
--------------
1. Activate the environment that has the deployment dependencies installed.
2. Ensure the Terraform stack already exists and that ``terraform output
   --json`` works in the chosen Terraform directory.
3. Run one of the following commands from the repository root:

   ``python src/test_deployment.py --scenario lambda``

   ``python src/test_deployment.py --scenario stress --tile-count 100000``

Important arguments
-------------------
- ``--scenario`` chooses the live test scenario to run.
- ``--terraform-dir`` points at the Terraform directory that contains the live
  deployment state.
- ``--state-count`` limits how many state polygons from ``src/geoms.json`` are
  used for AOIs and tile generation.
- ``--tiles-per-file`` controls how many tile features are written into each
  compare parquet object.
- ``--tile-count`` controls the total number of tile features generated during
  the stress scenario.
- ``--max-wait-seconds`` and ``--poll-seconds`` control how long the utility
  waits for SQS output before declaring the run failed.
- ``--cleanup`` removes the uploaded compare and AOI objects after the scenario
  completes.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
from dataclasses import dataclass
from math import ceil
from pathlib import Path
from tempfile import TemporaryDirectory
from time import time
from typing import Any

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


FIXTURE_PATH = Path(__file__).resolve().parent / "geoms.json"
DEFAULT_TERRAFORM_DIR = Path(__file__).resolve().parent.parent / "terraform"


@dataclass(frozen=True)
class DeploymentTargets:
    """Resolved AWS outputs required to exercise the live deployment."""

    aws_region: str
    s3_bucket_name: str
    sqs_in: str
    sqs_out: str
    sns_out: str
    lambda_name: str = "tns_comp_lambda"


@dataclass(frozen=True)
class ScenarioResult:
    """Structured summary emitted after a scenario completes."""

    scenario: str
    success_messages: int
    output_path: str | None
    duration_seconds: float


def quote_sql_string(value: str) -> str:
    """Escape a string so it can be safely embedded in a DuckDB SQL literal."""
    return value.replace("'", "''")


def feature_to_wkt(feature: dict[str, Any]) -> str:
    """Convert a fixture feature from ``geoms.json`` into polygon WKT."""
    coordinates = ", ".join(
        f"{x} {y}" for x, y in feature["geometry"]["rings"][0]
    )
    return f"POLYGON (({coordinates}))"


def load_state_features(limit: int) -> list[dict[str, Any]]:
    """Load the first ``limit`` state features from the repository fixture."""
    with FIXTURE_PATH.open("r", encoding="utf-8") as stream:
        features = json.load(stream)["features"]
    return features[:limit]


def build_aoi_rows(state_count: int) -> list[dict[str, str]]:
    """Build AOI rows using the first ``state_count`` state polygons."""
    return [
        {
            "pk_and_model": feature["attributes"]["STATE_NAME"],
            "geometry_wkt": feature_to_wkt(feature),
        }
        for feature in load_state_features(state_count)
    ]


def build_tile_frame(
    state_count: int, tiles_per_file: int, file_index: int = 0
) -> list[dict[str, str]]:
    """Build compare rows with every AOI represented at least once."""
    features = load_state_features(state_count)
    rows = []
    for row_index in range(tiles_per_file):
        feature = features[row_index % len(features)]
        rows.append(
            {
                "pk_and_model": f"raster_{file_index}_{row_index}",
                "geometry_wkt": feature_to_wkt(feature),
            }
        )
    return rows


def write_rows_to_s3(bucket_name: str, key: str, rows: list[dict[str, str]]) -> None:
    """Write WKT geometry rows to S3 as the WKB parquet format TNS expects."""
    s3 = boto3.client("s3")
    with TemporaryDirectory(prefix="tns-upload-") as tmp_dir:
        raw_path = Path(tmp_dir) / "input.raw.parquet"
        final_path = Path(tmp_dir) / "input.parquet"
        raw_table = pa.table(
            {
                "pk_and_model": [row["pk_and_model"] for row in rows],
                "geometry_wkt": [row["geometry_wkt"] for row in rows],
            }
        )
        pq.write_table(raw_table, raw_path)

        connection = duckdb.connect()
        try:
            connection.execute("LOAD spatial")
        except duckdb.Error:
            connection.execute("INSTALL spatial")
            connection.execute("LOAD spatial")
        connection.execute(
            f"""
            COPY (
                SELECT
                    pk_and_model,
                    ST_AsWKB(ST_GeomFromText(geometry_wkt)) AS geometry
                FROM read_parquet('{quote_sql_string(str(raw_path))}')
            ) TO '{quote_sql_string(str(final_path))}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        connection.close()
        s3.upload_file(str(final_path), bucket_name, key)


def load_terraform_output(terraform_dir: Path) -> DeploymentTargets:
    """Read the active Terraform outputs and convert them into deployment targets."""
    completed = subprocess.run(
        ["terraform", "output", "--json"],
        cwd=terraform_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    values = {key: value["value"] for key, value in payload.items()}
    return DeploymentTargets(
        aws_region=values["aws_region"],
        s3_bucket_name=values["s3_bucket_name"],
        sqs_in=values["sqs_in"],
        sqs_out=values["sqs_out"],
        sns_out=values["sns_out"],
    )


def queue_url_from_arn(queue_arn: str, region: str) -> str:
    """Resolve an SQS queue ARN into its queue URL."""
    sqs = boto3.client("sqs", region_name=region)
    queue_name = queue_arn.split(":")[-1]
    return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]


def clear_sqs(queue_arn: str, region: str) -> None:
    """Delete all currently available messages from an SQS queue."""
    sqs = boto3.client("sqs", region_name=region)
    queue_url = queue_url_from_arn(queue_arn, region)
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        messages = response.get("Messages", [])
        if not messages:
            return
        for message in messages:
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )


def receive_messages(
    queue_arn: str,
    region: str,
    wait_time_seconds: int,
    max_number_of_messages: int = 10,
) -> list[dict[str, Any]]:
    """Receive a batch of SQS messages from the provided queue."""
    sqs = boto3.client("sqs", region_name=region)
    queue_url = queue_url_from_arn(queue_arn, region)
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=min(max_number_of_messages, 10),
        WaitTimeSeconds=wait_time_seconds,
    )
    return response.get("Messages", [])


def delete_sqs_message(message: dict[str, Any], queue_arn: str, region: str) -> None:
    """Delete a single SQS message using its receipt handle."""
    sqs = boto3.client("sqs", region_name=region)
    queue_url = queue_url_from_arn(queue_arn, region)
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=message["ReceiptHandle"],
    )


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an S3 URI into bucket and key components."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected an s3:// URI, got {uri}")
    bucket, key = uri[5:].split("/", 1)
    return bucket, key


def read_output_parquet(s3_uri: str, region: str) -> list[dict[str, Any]]:
    """Download an output parquet object from S3 and return its rows."""
    bucket, key = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3", region_name=region)
    with TemporaryDirectory(prefix="tns-deployment-") as tmp_dir:
        local_path = Path(tmp_dir) / Path(key).name
        s3.download_file(bucket, key, str(local_path))
        return pq.read_table(local_path).to_pylist()


def cleanup_s3_objects(bucket_name: str, keys: list[str], region: str) -> None:
    """Delete uploaded S3 objects that were created for a scenario run."""
    if not keys:
        return
    s3 = boto3.client("s3", region_name=region)
    s3.delete_objects(
        Bucket=bucket_name,
        Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
    )


def get_message_attributes(message: dict[str, Any]) -> dict[str, Any]:
    """Extract SNS-style message attributes from an SQS delivery body."""
    body = json.loads(message["Body"])
    return body["MessageAttributes"]


def summarize_lambda_metrics(
    lambda_name: str, region: str, start_time: dt.datetime, end_time: dt.datetime
) -> dict[str, list[float]]:
    """Fetch simple Lambda CloudWatch metrics for the scenario window."""
    cloudwatch = boto3.client("cloudwatch", region_name=region)
    errors = cloudwatch.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName="Errors",
        Dimensions=[{"Name": "FunctionName", "Value": lambda_name}],
        StartTime=start_time,
        EndTime=end_time,
        Period=60,
        Statistics=["Sum"],
    )
    durations = cloudwatch.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName="Duration",
        Dimensions=[{"Name": "FunctionName", "Value": lambda_name}],
        StartTime=start_time,
        EndTime=end_time,
        Period=60,
        Statistics=["Average"],
    )
    return {
        "errors": [point["Sum"] for point in errors["Datapoints"]],
        "durations": [point["Average"] for point in durations["Datapoints"]],
    }


def run_lambda_scenario(
    targets: DeploymentTargets,
    state_count: int,
    wait_time_seconds: int,
    max_wait_seconds: int,
    cleanup: bool,
) -> ScenarioResult:
    """Run the single-file Lambda scenario against the deployed stack."""
    uploaded_keys = ["subs/subscriptions.parquet", "compare/geom.parquet"]
    aoi_rows = build_aoi_rows(state_count)
    tile_rows = build_tile_frame(state_count, state_count)

    clear_sqs(targets.sqs_out, targets.aws_region)
    clear_sqs(targets.sqs_in, targets.aws_region)

    write_rows_to_s3(targets.s3_bucket_name, uploaded_keys[0], aoi_rows)
    write_rows_to_s3(targets.s3_bucket_name, uploaded_keys[1], tile_rows)

    start = time()
    while time() - start < max_wait_seconds:
        messages = receive_messages(
            targets.sqs_out,
            targets.aws_region,
            wait_time_seconds=wait_time_seconds,
            max_number_of_messages=10,
        )
        if not messages:
            continue

        message = messages[0]
        body = json.loads(message["Body"])
        attrs = body["MessageAttributes"]
        status = attrs["status"]["Value"]
        delete_sqs_message(message, targets.sqs_out, targets.aws_region)

        if status != "succeeded":
            raise AssertionError(
                f"Lambda scenario failed: {attrs['error']['Value']}"
            )

        output_uri = attrs["s3_output_path"]["Value"]
        rows = read_output_parquet(output_uri, targets.aws_region)
        if len(rows) != state_count:
            raise AssertionError(
                f"Expected {state_count} output rows, received {len(rows)}."
            )
        if cleanup:
            cleanup_s3_objects(targets.s3_bucket_name, uploaded_keys, targets.aws_region)
        return ScenarioResult(
            scenario="lambda",
            success_messages=1,
            output_path=output_uri,
            duration_seconds=time() - start,
        )

    if cleanup:
        cleanup_s3_objects(targets.s3_bucket_name, uploaded_keys, targets.aws_region)
    raise TimeoutError("Timed out waiting for the lambda scenario output message.")


def run_stress_scenario(
    targets: DeploymentTargets,
    state_count: int,
    tile_count: int,
    tiles_per_file: int,
    wait_time_seconds: int,
    max_wait_seconds: int,
    cleanup: bool,
) -> ScenarioResult:
    """Run the multi-file scenario and validate batched success semantics.

    The deployed Lambda consumes SQS records in batches, so one success message
    can cover multiple uploaded compare parquet objects. This scenario therefore
    verifies that all uploaded compare files are represented across the returned
    success messages and that each published output parquet is structurally
    valid, rather than assuming one success message per AOI or per input file.
    """
    if tiles_per_file < state_count:
        raise ValueError("--tiles-per-file must be at least as large as --state-count")

    file_count = ceil(tile_count / tiles_per_file)
    uploaded_keys = ["subs/subscriptions.parquet"]
    failures: list[dict[str, Any]] = []
    expected_source_uris: set[str] = set()
    observed_source_uris: set[str] = set()
    output_uris: set[str] = set()

    clear_sqs(targets.sqs_out, targets.aws_region)
    clear_sqs(targets.sqs_in, targets.aws_region)

    write_rows_to_s3(
        targets.s3_bucket_name,
        uploaded_keys[0],
        build_aoi_rows(state_count),
    )

    for file_index in range(file_count):
        key = f"compare/geom_{file_index}.parquet"
        uploaded_keys.append(key)
        expected_source_uris.add(f"s3://{targets.s3_bucket_name}/{key}")
        write_rows_to_s3(
            targets.s3_bucket_name,
            key,
            build_tile_frame(state_count, tiles_per_file, file_index),
        )

    start_clock = time()
    metric_start = dt.datetime.now(dt.timezone.utc)

    while (
        time() - start_clock < max_wait_seconds
        and observed_source_uris != expected_source_uris
    ):
        messages = receive_messages(
            targets.sqs_out,
            targets.aws_region,
            wait_time_seconds=wait_time_seconds,
            max_number_of_messages=10,
        )
        if not messages:
            continue

        for message in messages:
            attrs = get_message_attributes(message)
            delete_sqs_message(message, targets.sqs_out, targets.aws_region)
            if attrs["status"]["Value"] == "failed":
                failures.append(attrs)
                continue

            source_files = set(json.loads(attrs["source_files"]["Value"]))
            observed_source_uris.update(source_files)

            output_uri = attrs["s3_output_path"]["Value"]
            if output_uri not in output_uris:
                rows = read_output_parquet(output_uri, targets.aws_region)
                if len(rows) != state_count:
                    raise AssertionError(
                        "Expected each stress output parquet to contain "
                        f"{state_count} AOI rows, received {len(rows)} from "
                        f"{output_uri}."
                    )
                output_uris.add(output_uri)

    metric_end = dt.datetime.now(dt.timezone.utc)
    metrics = summarize_lambda_metrics(
        targets.lambda_name,
        targets.aws_region,
        metric_start,
        metric_end,
    )

    if cleanup:
        output_keys = [parse_s3_uri(uri)[1] for uri in output_uris]
        cleanup_s3_objects(
            targets.s3_bucket_name,
            uploaded_keys + output_keys,
            targets.aws_region,
        )

    if failures:
        raise AssertionError(f"Stress scenario failures: {failures}")
    missing_sources = sorted(expected_source_uris.difference(observed_source_uris))
    if missing_sources:
        raise TimeoutError(
            "Timed out before all compare parquet objects were represented in "
            "success messages. Missing sources: "
            f"{missing_sources[:5]}{'...' if len(missing_sources) > 5 else ''}"
        )
    if any(metrics["errors"]):
        raise AssertionError(f"Lambda Errors metrics were non-zero: {metrics['errors']}")
    if not output_uris:
        raise AssertionError("Stress scenario produced no output parquet files.")

    return ScenarioResult(
        scenario="stress",
        success_messages=len(output_uris),
        output_path=None,
        duration_seconds=time() - start_clock,
    )


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the command-line parser for the deployment utility."""
    parser = argparse.ArgumentParser(
        description="Run live deployment scenarios against a provisioned TNS stack.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python src/test_deployment.py --scenario lambda\n"
            "  python src/test_deployment.py --scenario stress --tile-count 100000\n"
        ),
    )
    parser.add_argument(
        "--scenario",
        choices=("lambda", "stress"),
        required=True,
        help="Live deployment scenario to execute.",
    )
    parser.add_argument(
        "--terraform-dir",
        type=Path,
        default=DEFAULT_TERRAFORM_DIR,
        help="Terraform directory that contains the deployment state and outputs.",
    )
    parser.add_argument(
        "--state-count",
        type=int,
        default=50,
        help="Number of state polygons from src/geoms.json to include in the scenario.",
    )
    parser.add_argument(
        "--tiles-per-file",
        type=int,
        default=1000,
        help="Number of tile features written into each compare parquet file.",
    )
    parser.add_argument(
        "--tile-count",
        type=int,
        default=100000,
        help="Total number of tile features to generate for the stress scenario.",
    )
    parser.add_argument(
        "--wait-time-seconds",
        type=int,
        default=10,
        help="Long-poll duration used for each SQS receive request.",
    )
    parser.add_argument(
        "--max-wait-seconds",
        type=int,
        default=600,
        help="Maximum end-to-end wait time before the scenario is considered failed.",
    )
    parser.add_argument(
        "--cleanup",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Delete the uploaded compare and AOI objects after the scenario finishes.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments, execute the chosen scenario, and print a summary."""
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    targets = load_terraform_output(args.terraform_dir)

    if args.scenario == "lambda":
        result = run_lambda_scenario(
            targets=targets,
            state_count=args.state_count,
            wait_time_seconds=args.wait_time_seconds,
            max_wait_seconds=args.max_wait_seconds,
            cleanup=args.cleanup,
        )
    else:
        result = run_stress_scenario(
            targets=targets,
            state_count=args.state_count,
            tile_count=args.tile_count,
            tiles_per_file=args.tiles_per_file,
            wait_time_seconds=args.wait_time_seconds,
            max_wait_seconds=args.max_wait_seconds,
            cleanup=args.cleanup,
        )

    print(
        json.dumps(
            {
                "scenario": result.scenario,
                "success_messages": result.success_messages,
                "output_path": result.output_path,
                "duration_seconds": round(result.duration_seconds, 2),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
