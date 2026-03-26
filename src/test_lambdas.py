"""Tests for Lambda-facing event parsing and message handling."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from conftest import write_geometry_parquet
from db_lambda import (
    AppConfig,
    AppContext,
    MAX_MSG_BYTES,
    get_data_paths,
    get_fail_res,
    get_pass_res,
    handler,
    process_data_paths,
)
from tns_core import FilesystemGeoParquetStore


@dataclass
class RecordingPublisher:
    """Simple SNS publisher stub used to capture published payloads."""

    published: list[dict[str, object]]

    def publish(self, **kwargs):
        """Record publish calls for later assertions."""
        self.published.append(kwargs)


def build_sqs_event(path: str) -> dict[str, object]:
    """Build a minimal SQS event payload that wraps an S3 notification."""
    return {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "Message": json.dumps(
                            {
                                "Records": [
                                    {
                                        "s3": {
                                            "bucket": {"name": "tns-bucket"},
                                            "object": {
                                                "key": path.removeprefix(
                                                    "s3://tns-bucket/"
                                                )
                                            },
                                        }
                                    }
                                ]
                            }
                        )
                    }
                )
            }
        ]
    }


def test_get_data_paths_skips_s3_test_event():
    """S3 test events should not produce data paths for processing."""
    event = {
        "body": json.dumps(
            {"Message": json.dumps({"Event": "s3:TestEvent"})}
        )
    }
    assert get_data_paths(event) == []


def test_process_data_paths_returns_success_messages(
    aoi_rows, tile_rows, parquet_dir: Path
):
    """Processing local parquet inputs should return a succeeded result message."""
    aoi_path = parquet_dir / "subs.parquet"
    tile_path = parquet_dir / "tile.parquet"
    output_dir = parquet_dir / "output"
    write_geometry_parquet(aoi_path, aoi_rows)
    write_geometry_parquet(tile_path, tile_rows)

    app_context = AppContext(
        config=AppConfig(
            region="us-west-2",
            bucket="unused",
            sns_out_arn=None,
            aoi_key=str(aoi_path),
            output_prefix=str(output_dir),
        ),
        store=FilesystemGeoParquetStore(),
        sns_client=None,
    )

    messages = process_data_paths([str(tile_path)], app_context)
    assert len(messages) == 1
    attrs = messages[0]["MessageAttributes"]
    assert attrs["status"]["StringValue"] == "succeeded"
    assert len(json.loads(attrs["aoi_list"]["StringValue"])) == 50


def test_get_pass_res_splits_large_payload():
    """Large AOI lists should be split to stay within SNS payload limits."""
    paths = ["s3://tns-sample-bucket/compare/key.parquet"]
    aois = ["0123456789" for _ in range(20000)]

    responses = get_pass_res(name="12345678-1234-5678-1234-567812345678", dpaths=paths, aois=aois, output_path=paths[0])  # type: ignore[arg-type]
    assert len(responses) > 1
    for response in responses:
        assert len(json.dumps(response).encode("utf-8")) <= MAX_MSG_BYTES


def test_get_fail_res_marks_status_failed():
    """Failure messages should carry a failed status attribute."""
    response = get_fail_res(
        name="12345678-1234-5678-1234-567812345678",  # type: ignore[arg-type]
        dpaths=["s3://tns/path.parquet"],
        err_str="boom",
    )
    assert response["MessageAttributes"]["status"]["StringValue"] == "failed"


def test_handler_publishes_failure_message(monkeypatch, parquet_dir: Path):
    """The handler should publish a failure message when processing raises."""
    publisher = RecordingPublisher([])

    def fake_create_context():
        return AppContext(
            config=AppConfig(
                region="us-west-2",
                bucket="unused",
                sns_out_arn="arn:aws:sns:us-west-2:123456789012:tns",
                aoi_key=str(parquet_dir / "missing.parquet"),
                output_prefix=str(parquet_dir / "output"),
            ),
            store=FilesystemGeoParquetStore(),
            sns_client=publisher,
        )

    monkeypatch.setattr("db_lambda.create_app_context", fake_create_context)

    event = build_sqs_event("s3://tns-bucket/compare/missing.parquet")
    with pytest.raises(Exception):
        handler(event, None)

    assert len(publisher.published) == 1
    attrs = publisher.published[0]["MessageAttributes"]
    assert attrs["status"]["StringValue"] == "failed"
