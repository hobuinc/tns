import json
from math import ceil
from pathlib import Path

import time
import datetime as dt

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
            WaitTimeSeconds=10,
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


def cleanup_s3_objects(bucket_name: str, keys: list[str], region: str) -> None:
    """Delete uploaded S3 objects that were created for a scenario run."""
    if not keys:
        return
    s3 = boto3.client("s3", region_name=region)
    unique_keys = list(dict.fromkeys(keys))
    for start in range(0, len(unique_keys), 1000):
        chunk = unique_keys[start : start + 1000]
        s3.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )


def summarize_lambda_metrics(
    lambda_name: str,
    region: str,
    start_time: dt.datetime,
    end_time: dt.datetime,
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
        Statistics=["Average", "Sum"],
    )
    times = [point["Timestamp"] for point in durations["Datapoints"]]
    return {
        "errors": [point["Sum"] for point in errors["Datapoints"]],
        "durations": [point["Average"] for point in durations["Datapoints"]],
        "total_lambda_time": sum([point["Sum"] for point in durations["Datapoints"]]),
        "start_time": min(times),
        "end_time": max(times),
    }


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
    big_tiles_path: Path,
):

    # clear potential previous run data
    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)

    tile_count = 10**6
    batch_size = 1000
    count = ceil(tile_count / batch_size)
    print(f"creating {count} files.")
    start_time = dt.datetime.now(dt.timezone.utc)
    key_set = set()
    for n in range(count):
        key = f"compare/stress_{n}.parquet"
        key_set.add(key)
        put_parquet(bucket_name, key, big_tiles_path, config)

    failed = []
    source_file_set = set()
    s3_out_list = []
    max_wait_time_s = max(600, 30 * (tile_count / 10**5))  # 5min per 1M
    timeout = time.time()
    while (
        source_file_set != key_set and time.time() - timeout < max_wait_time_s
    ):
        messages = sqs_listen(sqs_out, region, 10)
        for msg in messages:
            body = json.loads(msg["Body"])

            attrs = body["MessageAttributes"]
            status = attrs["status"]["Value"]

            if status == "failed":
                err = attrs["error"]["Value"]
                failed.append(err)
            else:
                s3_out = attrs["s3_output_path"]["Value"]
                s3_out_list.append(s3_out)

            msg_sfs = set(json.loads(attrs["source_files"]["Value"]))
            source_file_set.update(msg_sfs)

    # seconds to remove from time approx:
    end_time = dt.datetime.now(dt.timezone.utc)
    lambda_name = "tns_comp_lambda"


    # collect and print test info
    msgs = []
    pass_fail = True
    missing_sources = sorted(source_file_set.difference(key_set))
    if not missing_sources:
        pass_fail = False
        msgs.append([f"Missing files due to timeout: {missing_sources}"])

    metrics = summarize_lambda_metrics(
        lambda_name, region, start_time, end_time
    )
    if any(metrics["errors"]):
        pass_fail = False
        msgs.append(f"Lambda Errors metrics were non-zero: {metrics['errors']}")
    if not failed:
        pass_fail = False
        msgs.append(f"Errors from SQS messages: {failed}")

    average = sum(metrics["durations"]) / len(metrics["durations"])
    print(f"Average durations: {average}")

    if pass_fail:
        raise AssertionError(json.dumps(msgs))
