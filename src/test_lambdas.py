import os
import boto3
import json

import shutil
import time
import pytest
import polars as pl
from duckdb import OutOfMemoryException

from conftest import EventType
from intersects_lambda import CloudConfig, handler


def clear_sqs(sqs_arn: str, region: str):
    sqs = boto3.client("sqs", region_name=region)
    queue_name = sqs_arn.split(":")[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    messages = []
    while not len(messages):
        res = sqs.receive_message(
            QueueUrl=queue_url,
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )
        if "Messages" in res.keys():
            messages = res["Messages"]
            for m in messages:
                receipt_handle = m["ReceiptHandle"]
                sqs.delete_message(
                    QueueUrl=queue_url, ReceiptHandle=receipt_handle
                )
        else:
            break
    return messages


@pytest.mark.parametrize("env_type", ("test",), indirect=True)
def test_big(
    env_type: str,
    region: str,
    sqs_in: str,
    sqs_out: str,
    big_event: EventType,
    big_aoi_fill: None,
    env_vars: None
):
    """Test lambda function's ability to coordinate large amounts of data."""

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)

    time1 = time.time()
    aois = handler(big_event, None)
    res_time = time.time() - time1
    assert res_time < 500
    assert len(aois) == 1
    for aoi_res in aois:
        attrs = aoi_res["MessageAttributes"]

        status = attrs["status"]["StringValue"]
        assert status == "succeeded", json.dumps(attrs["error"])

        sources = json.loads(attrs['source_files']['StringValue'])
        assert len(sources) == 10

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)


@pytest.mark.parametrize("env_type", ("test",), indirect=True)
def test_handler(
    env_type: str,
    sqs_in: str,
    sqs_out: str,
    region: str,
    bucket_name: str,
    prefix: str,
    event: EventType,
    config: CloudConfig,
    aoi_fill: None,
    env_vars: None,
):
    """
    Test that lambda function is correctly interacting with supporting
    resources like SQS and S3.
    """

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)

    aoi_res = handler(event, None)
    assert len(aoi_res) == 1

    aoi_res = aoi_res[0]
    attrs = aoi_res["MessageAttributes"]
    assert "error" not in attrs.keys(), (
        f"Error in messages: {attrs['error']['StringValue']}"
    )

    source_files = json.loads(attrs["source_files"]["StringValue"])
    assert len(source_files) == 1
    assert (
        source_files[0] == f"s3://{bucket_name}/{prefix}/compare/geom.parquet"
    )
    s3_path = attrs["s3_output_path"]["StringValue"]

    s3_info = pl.read_parquet(s3_path)
    s3_aois = s3_info.get_column("aois").to_list()

    assert len(s3_aois) == 50

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)


@pytest.mark.parametrize("env_type", ("test",), indirect=True)
def test_failures(env_type: str, sqs_out: str, region: str, env_vars: None):
    """
    Test that lambda function fails in expected ways and advertises those
    errors in the correct way via SQS.
    """

    clear_sqs(sqs_out, region)

    def get_attrs(msg):
        body = json.loads(msg[0]["Body"])
        return body["MessageAttributes"]

    # test bad event creation error catching
    fake_event = {"Records": ["asdf"]}
    with pytest.raises(Exception) as e1:
        handler(fake_event, None)
    assert "string indices must be integers" in str(e1)
    msg1 = clear_sqs(sqs_out, region)
    a1 = get_attrs(msg1)
    assert a1["status"]["Value"] == "failed"
    assert "string indices must be integers" in a1["error"]["Value"]

    # test cloudconfig failure
    s3_bucket = os.environ.pop("S3_BUCKET")
    with pytest.raises(Exception) as e2:
        handler(fake_event, None)
    os.environ["S3_BUCKET"] = s3_bucket
    assert "Required variable S3_BUCKET missing from environment" in str(e2)
    msg2 = clear_sqs(sqs_out, region)
    a2 = get_attrs(msg2)
    assert a2["status"]["Value"] == "failed"
    assert (
        "Required variable S3_BUCKET missing from environment"
        in a2["error"]["Value"]
    )

    clear_sqs(sqs_out, region)


@pytest.mark.parametrize("env_type", ("test",), indirect=True)
def test_mem_handle(
    env_type: str,
    sqs_in: str,
    sqs_out: str,
    region: str,
    bucket_name: str,
    prefix: str,
    mem_test_event: EventType,
    low_mem_config: CloudConfig,
    big_aoi_fill: None,
    env_vars: None,
):
    """
    Test that when memory limits are hit the lambda will attempt to split and
    rerun.
    """

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)

    # set memory to 3GB to force memory problem
    os.environ["MEMORY_LIMIT"] = "3072"

    aoi_res = handler(mem_test_event, None)
    assert len(aoi_res) == 2

    for res in aoi_res:
        attrs = res["MessageAttributes"]
        assert "error" not in attrs.keys(), (
            f"Error in messages: {attrs['error']['StringValue']}"
        )

        source_files = json.loads(attrs["source_files"]["StringValue"])
        assert len(source_files) == 1
        assert any(
            x in set(source_files)
            for x in [
                f"s3://{bucket_name}/{prefix}/compare/stress_496.parquet",
                f"s3://{bucket_name}/{prefix}/compare/stress_494.parquet",
            ]
        )
        s3_path = attrs["s3_output_path"]["StringValue"]
        with low_mem_config:
            s3_info = low_mem_config.con.sql(
                f"select aois from read_parquet('{s3_path}')"
            )
            s3_aois = s3_info.pl().get_column("aois").to_list()
            assert len(s3_aois)

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)


@pytest.mark.parametrize("env_type", ("test",), indirect=True)
def test_mem_failure(
    env_type: str,
    sqs_in: str,
    sqs_out: str,
    region: str,
    mem_test_event: EventType,
    big_aoi_fill: None,
    env_vars: None,
):
    """
    Test that we get failure messages if TNS runs out of memory even after
    splitting events up.
    """

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)

    # set memory to 1GB to force error
    os.environ["MEMORY_LIMIT"] = "1000"

    with pytest.raises(OutOfMemoryException):
        handler(mem_test_event, None)
    fail_messages = clear_sqs(sqs_out, region)
    for res in fail_messages:
        res = json.loads(res["Body"])
        attrs = res["MessageAttributes"]
        assert "error" in attrs.keys()
        assert attrs["status"]["Value"] == "failed"

    clear_sqs(sqs_in, region)


@pytest.mark.parametrize("env_type", ("test",), indirect=True)
def test_945(
    env_type: str,
    sqs_in: str,
    sqs_out: str,
    region: str,
    event_945: EventType,
    config: CloudConfig,
    env_vars
):
    """Testing a scenario that can easily have memory problems."""
    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)

    aoi_res = handler(event_945, None)
    assert len(aoi_res) == 1

    for res in aoi_res:
        attrs = res["MessageAttributes"]
        assert "error" not in attrs.keys(), (
            f"Error in messages: {attrs['error']['StringValue']}"
        )

        source_files = json.loads(attrs["source_files"]["StringValue"])
        assert len(source_files) == 1
        s3_path = attrs["s3_output_path"]["StringValue"]

        with config:
            s3_info = config.con.sql(
                f"select aois from read_parquet('{s3_path}')"
            )
            s3_aois = s3_info.pl().get_column("aois").to_list()
            assert len(s3_aois)

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)
