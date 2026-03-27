import json
from math import ceil
from pathlib import Path

import time
import datetime

import pytest
import boto3
import polars_st as st

from conftest import put_parquet, clear_sqs
from intersects_lambda import CloudConfig


def delete_sqs_message(e, sqs_arn, region):
    sqs = boto3.client("sqs", region_name=region)
    queue_name = sqs_arn.split(":")[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    receipt_handle = e["ReceiptHandle"]
    return sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def sns_publish(sns_arn, region, aoi=None, polygon=None):
    sns = boto3.client("sns", region_name=region)
    message_attributes = {}
    if aoi is not None:
        message_attributes["aoi"] = {
            "DataType": "Number",
            "StringValue": f"{aoi}",
        }
    if polygon is not None:
        message_attributes["polygon"] = {
            "DataType": "String",
            "StringValue": f"{polygon}",
        }
    res = sns.publish(
        TopicArn=sns_arn, MessageAttributes=message_attributes, Message=f"{aoi}"
    )
    return res


def sqs_get_messages(sqs_arn, region):
    sqs = boto3.client("sqs", region_name=region)
    queue_name = sqs_arn.split(":")[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    return sqs.receive_message(
        QueueUrl=queue_url,
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=10,
        WaitTimeSeconds=10,
    )


def sqs_listen(sqs_arn, region, retries=5):
    sqs = boto3.client("sqs", region_name=region)
    queue_name = sqs_arn.split(":")[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    messages = []
    retry_count = 0
    while not len(messages):
        res = sqs.receive_message(
            QueueUrl=queue_url,
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10
        )
        if "Messages" in res.keys():
            messages = res["Messages"]
            for m in messages:
                receipt_handle = m["ReceiptHandle"]
                sqs.delete_message(
                    QueueUrl=queue_url, ReceiptHandle=receipt_handle
                )
            retry_count = 0
        else:
            # set retry to 0 if infinite retries
            if retries:
                retry_count += 1
                if retry_count >= retries:
                    return messages
    return messages


def test_lambda(
    region: str,
    sqs_out: str,
    sqs_in: str,
    bucket_name: str,
    small_tiles_path: Path,
    small_aois_path: Path,
    aoi_fill: None,
    config: CloudConfig,
):
    # clear potential previous run data
    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)
    key = "compare/geom.parquet"

    filepath = f"s3://{bucket_name}/{key}"
    states = st.read_file(small_aois_path).to_pandas().pk_and_model.to_list()

    put_parquet(bucket_name, key, small_tiles_path, config)

    messages = sqs_listen(sqs_out, region)
    # if lambda is cold started it can take an extra cycle
    if not messages:
        messages = sqs_listen(sqs_out, region)
    assert len(messages) == 1
    m = messages[0]

    message = json.loads(m["Body"])
    attrs = message["MessageAttributes"]

    status = attrs["status"]["Value"]
    assert status == "succeeded", (
        f"Error from SQS {message['MessageAttributes']['error']['Value']}"
    )

    msg_states = json.loads(attrs["aoi_list"]["Value"])
    assert len(msg_states) == len(states)
    assert set(msg_states) == set(states)

    source_file = json.loads(attrs["source_files"]["Value"])
    assert len(source_file) == 1
    assert source_file[0] == filepath

    s3_path = attrs["s3_output_path"]["Value"]
    s3_info = config.con.sql(f"select aois from read_parquet('{s3_path}')")
    s3_aois = s3_info.pl().get_column("aois").to_list()
    assert len(s3_aois) == len(states)
    assert set(s3_aois) == set(states)

    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)


pytest.mark.skip()
def test_stress(
    config: CloudConfig,
    bucket_name: str,
    region: str,
    sqs_out: str,
    sqs_in: str,
    big_aoi_fill: None,
    big_tiles_path: Path
):

    # clear potential previous run data
    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)

    tile_count = 10**6
    batch_size = 1000
    count = ceil(tile_count / batch_size)
    print(f"creating {count} files.")
    start = time.time()
    for n in range(count):
        key = f"compare/stress_{n}.parquet"
        put_parquet(bucket_name, key, big_tiles_path, config)

    msg_count = 0
    failed = []
    while True:
        messages = sqs_listen(sqs_out, region, 10)
        if not messages:
            break
        for msg in messages:
            body = json.loads(msg["Body"])

            attrs = body["MessageAttributes"]
            status = attrs["status"]["Value"]

            if status == "failed":
                failed.append(attrs)

            msg_count += 1

    # seconds to remove from time approx:
    # 10 retries * 10 seconds per * 20 seconds sqs lead
    retry_time = 10 * 10 + 20
    proc_time = time.time() - start - retry_time
    print(f"Took around {proc_time} seconds.")
    assert not failed