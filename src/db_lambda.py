"""AWS Lambda integration layer for the Tile Notification System.

The handler in this module is responsible for turning SQS-wrapped S3 events
into calls into ``tns_core``, where DuckDB executes the spatial join, and then
publishing success or failure messages to the configured SNS topic.
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from uuid import UUID, uuid4

import boto3

try:
    from .tns_core import CompareArtifacts, S3GeoParquetStore, compare_geoparquets
except ImportError:  # pragma: no cover - supports local direct module execution
    from tns_core import CompareArtifacts, S3GeoParquetStore, compare_geoparquets


LOGGER = logging.getLogger(__name__)
MAX_MSG_BYTES = 2**10 * 256  # 256KB


@dataclass(frozen=True)
class AppConfig:
    """Runtime configuration derived from the Lambda environment."""

    region: str
    bucket: str
    sns_out_arn: str | None
    aoi_key: str = "subs/subscriptions.parquet"
    output_prefix: str = "intersects"

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Build configuration from process environment variables."""
        return cls(
            region=os.environ.get("AWS_REGION", "us-west-2"),
            bucket=os.environ["S3_BUCKET"],
            sns_out_arn=os.environ.get("SNS_OUT_ARN"),
        )

    @property
    def aoi_uri(self) -> str:
        """Return the AOI dataset URI for this runtime."""
        if self.aoi_key.startswith("s3://") or Path(self.aoi_key).is_absolute():
            return self.aoi_key
        return f"s3://{self.bucket}/{self.aoi_key}"

    def output_uri_for(self, run_id: UUID) -> str:
        """Return the output URI for a single comparison run."""
        if self.output_prefix.startswith("s3://"):
            return f"{self.output_prefix.rstrip('/')}/{run_id}.parquet"
        if Path(self.output_prefix).is_absolute():
            return str(Path(self.output_prefix) / f"{run_id}.parquet")
        return f"s3://{self.bucket}/{self.output_prefix}/{run_id}.parquet"


@dataclass(frozen=True)
class AppContext:
    """Bundled runtime collaborators used by the Lambda handler."""

    config: AppConfig
    store: S3GeoParquetStore
    sns_client: object | None


def create_app_context() -> AppContext:
    """Create AWS clients and the storage adapter for a Lambda invocation."""
    config = AppConfig.from_env()
    session = boto3.session.Session(region_name=config.region)
    sns_client = (
        session.client("sns", region_name=config.region)
        if config.sns_out_arn
        else None
    )
    store = S3GeoParquetStore(session.client("s3", region_name=config.region))
    return AppContext(config=config, store=store, sns_client=sns_client)


def get_data_paths(sqs_event: dict[str, object]) -> list[str]:
    """Extract S3 GeoParquet paths from an SQS-wrapped SNS notification."""
    body = json.loads(sqs_event["body"])
    message = json.loads(body["Message"])

    if message.get("Event") == "s3:TestEvent":
        return []

    paths = []
    for sns_event in message["Records"]:
        s3_info = sns_event["s3"]
        bucket = s3_info["bucket"]["name"]
        key = s3_info["object"]["key"]
        paths.append(f"s3://{bucket}/{key}")
    return paths


def get_pass_res(
    name: UUID, dpaths: list[str], aois: list[str], output_path: str
) -> list[dict[str, object]]:
    """Build one or more success payloads sized for SNS message limits."""
    attrs = {
        "source_files": {
            "DataType": "String",
            "StringValue": json.dumps(dpaths),
        },
        "aoi_list": {
            "DataType": "String",
            "StringValue": json.dumps(aois),
        },
        "s3_output_path": {"DataType": "String", "StringValue": output_path},
        "status": {"DataType": "String", "StringValue": "succeeded"},
    }
    res = {"MessageAttributes": attrs, "Message": str(name)}

    payload_size = len(json.dumps(res).encode("utf-8"))
    if payload_size > MAX_MSG_BYTES and len(aois) > 1:
        split = len(aois) // 2
        return [
            *get_pass_res(name, dpaths, aois[:split], output_path),
            *get_pass_res(name, dpaths, aois[split:], output_path),
        ]
    return [res]


def get_fail_res(name: UUID, dpaths: list[str], err_str: str) -> dict[str, object]:
    """Build a failure payload for downstream consumers."""
    return {
        "MessageAttributes": {
            "source_files": {
                "DataType": "String",
                "StringValue": json.dumps(dpaths),
            },
            "status": {"DataType": "String", "StringValue": "failed"},
            "error": {"DataType": "String", "StringValue": err_str},
        },
        "Message": str(name),
    }


def build_success_messages(
    run_id: UUID, source_paths: list[str], artifacts: CompareArtifacts
) -> list[dict[str, object]]:
    """Translate compare artifacts into SNS-ready success messages."""
    return get_pass_res(
        run_id, source_paths, artifacts.matched_aois, artifacts.output_uri
    )


def process_data_paths(
    data_paths: list[str], app_context: AppContext
) -> list[dict[str, object]]:
    """Run the GeoParquet comparison for a batch of source files."""
    if not data_paths:
        LOGGER.info("No GeoParquet paths found in event payload.")
        return []

    run_id = uuid4()
    artifacts = compare_geoparquets(
        aoi_uri=app_context.config.aoi_uri,
        tile_uris=data_paths,
        output_uri=app_context.config.output_uri_for(run_id),
        store=app_context.store,
    )
    LOGGER.info(
        "Processed %s source files into %s AOI matches.",
        len(data_paths),
        artifacts.row_count,
    )
    return build_success_messages(run_id, data_paths, artifacts)


def publish_messages(
    sns_client, topic_arn: str | None, messages: Iterable[dict[str, object]]
) -> None:
    """Publish a batch of messages when SNS is configured."""
    if not sns_client or not topic_arn:
        return

    for message in messages:
        sns_client.publish(TopicArn=topic_arn, **message)


def handler(event: dict[str, object], context):
    """Lambda entry point for TNS compare events."""
    LOGGER.info("Received event with %s records.", len(event.get("Records", [])))
    app_context: AppContext | None = None
    data_paths: list[str] = []

    try:
        app_context = create_app_context()
        for sqs_event in event["Records"]:
            data_paths.extend(get_data_paths(sqs_event))

        success_messages = process_data_paths(data_paths, app_context)
        publish_messages(
            app_context.sns_client,
            app_context.config.sns_out_arn,
            success_messages,
        )
        return success_messages
    except Exception:
        LOGGER.exception("TNS processing failed.")
        failure_message = get_fail_res(uuid4(), data_paths, traceback.format_exc())
        if app_context is not None:
            publish_messages(
                app_context.sns_client,
                app_context.config.sns_out_arn,
                [failure_message],
            )
        raise
