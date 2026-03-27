import os
import boto3
import json

import shutil
import time
import pytest

from conftest import EventType
from intersects_lambda import CloudConfig, handler, EXT_PATH


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


def test_big(
    region: str,
    sqs_in: str,
    sqs_out: str,
    big_event: EventType,
    big_aoi_fill: None,
    env_vars: None
):
    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)

    shutil.rmtree(EXT_PATH)

    time1 = time.time()
    aois = handler(big_event, None)
    res_time = time.time() - time1
    assert res_time < 500
    assert len(aois)
    for aoi_res in aois:
        attrs = aoi_res["MessageAttributes"]

        status = attrs["status"]["StringValue"]
        assert status == "succeeded", json.dumps(attrs["error"])

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)


def test_handler(
    sqs_in: str,
    sqs_out: str,
    region: str,
    bucket_name: str,
    event: EventType,
    config: CloudConfig,
    aoi_fill: None,
    env_vars: None
):
    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)

    aoi_res = handler(event, None)
    assert len(aoi_res) == 1

    aoi_res = aoi_res[0]
    attrs = aoi_res["MessageAttributes"]
    assert "error" not in attrs.keys(), (
        f"Error in messages: {attrs['error']['StringValue']}"
    )

    aois = json.loads(attrs["aoi_list"]["StringValue"])
    assert len(aois) == 50
    source_files = json.loads(attrs["source_files"]["StringValue"])
    assert len(source_files) == 1
    assert source_files[0] == f"s3://{bucket_name}/compare/geom.parquet"
    s3_path = attrs["s3_output_path"]["StringValue"]

    s3_info = config.con.sql(f"select aois from read_parquet('{s3_path}')")
    s3_aois = s3_info.pl().get_column("aois").to_list()
    assert len(aois) == len(s3_aois)
    assert set(s3_aois) == set(aois)

    clear_sqs(sqs_in, region)
    clear_sqs(sqs_out, region)


def test_failures(sqs_out: str, region: str, env_vars: None):
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
