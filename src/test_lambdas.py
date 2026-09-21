import os
from pathlib import Path
import boto3
import json

import time
import pytest
import polars as pl
from duckdb import OutOfMemoryException
from botocore.exceptions import ClientError, SSLError

from conftest import EventType
import intersects_lambda

@pytest.fixture(scope="function", autouse=True)
def reset_lambda_global_state(env):
    """
    Guarantees test isolation for unit/local tests while preserving the
    connection for high-throughput deployment/stress tests.
    """
    # For 'prod' (deployment) tests, do nothing to keep the connection warm.
    if env == "prod":
        yield
        return

    # For 'test' and 'unit' runs, perform a clean state reset.
    intersects_lambda.GLOBAL_CONFIG = None
    # set in the config init, reset if they were changed
    if os.environ.get('REQUESTS_CA_BUNDLE') is not None:
        os.environ.pop('REQUESTS_CA_BUNDLE')
    if os.environ.get('AWS_CA_BUNDLE') is not None:
        os.environ.pop('AWS_CA_BUNDLE')
    yield
    # Teardown after the test
    if intersects_lambda.GLOBAL_CONFIG is not None:
        try:
            if hasattr(intersects_lambda.GLOBAL_CONFIG, 'con'):
                intersects_lambda.GLOBAL_CONFIG.con.close()
        except Exception:
            pass
        intersects_lambda.GLOBAL_CONFIG = None

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
def test_local_config_bad_path(
    env_type: str,
    region: str,
    sns_out: str,
    bucket_name: str,
    prefix: str,
    mem_size: str,
    nonexistent_s3_cert_path: Path,
):
    """Test Cloud/DuckDB coordination client work correctly."""
    # set environment variables, which config will pull from
    # then test that cloud config correctly pulls from those
    with pytest.raises(ClientError):
        intersects_lambda.CloudConfig(
            region,
            sns_out,
            bucket_name,
            prefix,
            mem_size,
            nonexistent_s3_cert_path,
        )




@pytest.mark.parametrize("env_type", ("test",), indirect=True)
def test_local_config_bad_cert(
    env_type: str,
    region: str,
    sns_out: str,
    bucket_name: str,
    prefix: str,
    mem_size: str,
    bad_s3_cert_path: Path,
):
    """Test Cloud/DuckDB coordination client work correctly."""
    cert_path = bad_s3_cert_path

    config = intersects_lambda.CloudConfig(
        region, sns_out, bucket_name, prefix, mem_size, cert_path
    )
    with pytest.raises(SSLError):
        config.sns.publish(TopicArn=sns_out, Message="asdf")


@pytest.mark.parametrize("env_type", ("test",), indirect=True)
def test_local_config(
    env_type: str,
    region: str,
    sns_out: str,
    bucket_name: str,
    prefix: str,
    mem_size: str,
    s3_cert_path: Path,
):
    """Test Cloud/DuckDB coordination client work correctly."""
    # set environment variables, which config will pull from
    # then test that cloud config correctly pulls from those

    s3_endpoint = "s3.amazonaws.com"
    config = intersects_lambda.CloudConfig(
        region,
        sns_out,
        bucket_name,
        prefix,
        mem_size,
        s3_cert_path,
        s3_endpoint,
    )
    assert config.region == region
    assert config.sns_out_arn == sns_out
    assert config.bucket == bucket_name
    assert (
        config.aois_path
        == f"s3://{bucket_name}/{prefix}/subs/subscriptions.parquet"
    )
    assert config.cert_path
    assert os.path.exists(config.cert_dest)
    assert config.using_certs
    assert config.s3_endpoint == s3_endpoint

    with config:
        assert hasattr(config, "active_tempdir")
        temp_dir_path = config.active_tempdir
        assert os.path.exists(temp_dir_path)
        assert "/tmp" in temp_dir_path
        a = config.con.sql("select 1")
        assert a.pl().get_column("1").to_list()[0] == 1
    # verify context manager removes temp dir path
    assert not os.path.exists(temp_dir_path)


@pytest.mark.parametrize("env_type", ("test",), indirect=True)
def test_big(
    env_type: str,
    region: str,
    sqs_in: str,
    sqs_out: str,
    big_event: EventType,
    big_aoi_fill: None,
    # env_vars: None,
):
    """Test lambda function's ability to coordinate large amounts of data."""

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)

    time1 = time.time()
    aois = intersects_lambda.handler(big_event, None)
    res_time = time.time() - time1
    assert res_time < 500
    assert len(aois) == 1
    for aoi_res in aois:
        attrs = aoi_res["MessageAttributes"]

        status = attrs["status"]["StringValue"]
        assert status == "succeeded", json.dumps(attrs["error"])

        sources = json.loads(attrs["source_files"]["StringValue"])
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
    # config: CloudConfig,
    aoi_fill: None,
    # env_vars: None,
):
    """
    Test that lambda function is correctly interacting with supporting
    resources like SQS and S3.
    """
    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)

    aoi_res = intersects_lambda.handler(event, None)
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
def test_bad_event(env_type: str, sqs_out: str, region: str):
    """
    Test that lambda function fails with a bad event JSON and advertises that
    error in the correct way via SQS.
    """
    clear_sqs(sqs_out, region)

    def get_attrs(msg):
        body = json.loads(msg[0]["Body"])
        return body["MessageAttributes"]

    # test bad event creation error catching
    fake_event = {"Records": ["asdf"]}
    with pytest.raises(Exception) as e1:
        intersects_lambda.handler(fake_event, None)
    assert "string indices must be integers" in str(e1)
    msg1 = clear_sqs(sqs_out, region)
    a1 = get_attrs(msg1)
    assert a1["status"]["Value"] == "failed"
    assert "string indices must be integers" in a1["error"]["Value"]

@pytest.mark.parametrize("env_type", ("test",), indirect=True)
def test_missing_env_variable(env_type: str, sqs_out: str, region: str):
    """
    Test that lambda function fails with a missing environment variable
    errors in the correct way via SQS.
    """
    # test cloudconfig failure
    clear_sqs(sqs_out, region)
    def get_attrs(msg):
        body = json.loads(msg[0]["Body"])
        return body["MessageAttributes"]

    fake_event = {"Records": ["asdf"]}

    s3_bucket = os.environ.pop('S3_BUCKET')
    intersects_lambda.GLOBAL_CONFIG = None

    with pytest.raises(Exception) as e2:
        intersects_lambda.handler(fake_event, None)
        assert "Required variable S3_BUCKET missing from environment" in str(e2)

    # replace so that cleanup still works
    os.environ['S3_BUCKET'] = s3_bucket
    msg2 = clear_sqs(sqs_out, region)
    a2 = get_attrs(msg2)
    assert a2["status"]["Value"] == "failed"
    assert (
        "Required variable S3_BUCKET missing from environment"
        in a2["error"]["Value"]
    )

    clear_sqs(sqs_out, region)


@pytest.mark.parametrize("env_type", ("test",), indirect=True)
def test_mem_failure(
    env_type: str,
    sqs_in: str,
    sqs_out: str,
    region: str,
    mem_test_event: EventType,
    big_aoi_fill: None,
    # env_vars: None,
):
    """
    Test that we get failure messages if TNS runs out of memory even after
    splitting events up.
    """
    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)

    # set memory very low to force error
    prev = os.environ.pop("MEMORY_LIMIT")
    os.environ["MEMORY_LIMIT"] = "5"
    # prev_config = GLOBAL_CONFIG
    intersects_lambda.GLOBAL_CONFIG = None
    # assert GLOBAL_CONFIG is None
    # GLOBAL_CONFIG='asdfasdf'

    with pytest.raises(OutOfMemoryException):
        intersects_lambda.handler(mem_test_event, None)


    # reset environment variable
    os.environ["MEMORY_LIMIT"] = prev
    # GLOBAL_CONFIG = prev_config

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
    # config: CloudConfig,
    # env_vars,
):
    """Testing a scenario that can easily have memory problems."""
    from intersects_lambda import handler
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
        s3_info = pl.read_parquet(s3_path)
        s3_aois = s3_info.get_column("aois").to_list()
        assert len(s3_aois)

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)
