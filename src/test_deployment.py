from concurrent.futures import ThreadPoolExecutor
import json
from math import floor
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


def sqs_listen(sqs_arn, region, retries=10):
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
        time.sleep(1)
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
        Statistics=["Average", "Sum", "SampleCount"],
    )
    times = [point["Timestamp"] for point in durations["Datapoints"]]

    return {
        "errors": [point["Sum"] for point in errors["Datapoints"]],
        "avg_durations_ms": [
            point["Average"] for point in durations["Datapoints"]
        ],
        "total_lambda_time_ms": sum(
            [point["Sum"] for point in durations["Datapoints"]]
        ),
        "start_time": min(times) if times else 0,
        "end_time": max(times) if times else 0,
    }


def stress_test_common(
    key_set: list[str],
    cleanup_list: list[str],
    sqs_out: str,
    region: str,
    bucket_name: str,
    start_time: dt.datetime,
    max_wait_time: int,
    lambda_name: str
):
    failed = []
    source_file_set = set()
    s3_out_list = []
    timeout = time.time()
    try:
        while source_file_set != key_set and time.time() - timeout < max_wait_time:
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
                    cleanup_list.append(s3_out)

                msg_sfs = set(json.loads(attrs["source_files"]["Value"]))
                source_file_set.update(msg_sfs)

        # seconds to remove from time approx:
        end_time = dt.datetime.now(dt.timezone.utc)

        # collect and print test info
        msgs = []
        pass_fail = True
        missing_sources = sorted(key_set.difference(source_file_set))
        if missing_sources:
            pass_fail = False
            msgs.append([f"Missing files due to timeout: {missing_sources}"])

        metrics = summarize_lambda_metrics(
            lambda_name, region, start_time, end_time
        )
        if failed:
            pass_fail = False
            msgs.append(f"Errors from SQS messages: {failed}")

        print(metrics)
        if not pass_fail:
            raise AssertionError(json.dumps(msgs))
    except Exception as e:
        cleanup_s3_objects(bucket_name, cleanup_list, region)
        raise e


@pytest.mark.parametrize("env_type", ("prod",), indirect=True)
def test_lambda(
    env_type,
    region: str,
    sqs_out: str,
    sqs_in: str,
    bucket_name: str,
    small_tiles_path: Path,
    small_aois_path: Path,
    aoi_fill: None,
    config: CloudConfig,
):
    """
    Test full process by pushing a file and checking that the response from the
    lambda via SQS matches what's expected.
    """

    # clear potential previous run data
    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)
    try:
        key = f"{config.prefix}/compare/geom.parquet"

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

        source_file = json.loads(attrs["source_files"]["Value"])
        assert len(source_file) == 1
        assert source_file[0] == filepath

        s3_path = attrs["s3_output_path"]["Value"]
        s3_info = config.con.sql(f"select aois from read_parquet('{s3_path}')")
        s3_aois = s3_info.pl().get_column("aois").to_list()
        assert len(s3_aois) == len(states)
        assert set(s3_aois) == set(states)

    except Exception as e:
        clear_sqs(sqs_out, region)
        clear_sqs(sqs_in, region)
        raise e


@pytest.mark.skip(reason="Manually run only.")
@pytest.mark.parametrize("env_type", ("prod",), indirect=True)
def test_many_small_tiles(
    env_type,
    config: CloudConfig,
    bucket_name: str,
    region: str,
    sqs_out: str,
    sqs_in: str,
    aoi_fill: None,
    cities_path: Path,
):
    """
    Test small file performance by pushing files with 1 tile in them and
    querying SQS/Cloudwatch for results.
    """

    # clear potential previous run data
    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)

    try:
        count = 200
        start_time = dt.datetime.now(dt.timezone.utc)
        key_set = set()
        cleanup_list = []
        cities_cmd = f"""
            select id as pk_and_model, geometry
                from read_parquet('{cities_path.as_posix()}')
        """

        cities = config.con.execute(cities_cmd).fetch_df()

        def write_one(city, vsis_path):
            gdf = st.GeoDataFrame(city, geometry_format="wkb", strict=True)
            gdf = gdf.with_columns(st.geom().st.set_srid(4326))
            gdf.st.write_file(path=vsis_path, driver="PARQUET", compression="zstd")

        with ThreadPoolExecutor() as executor:
            for idx in range(count):
                city = cities.loc[idx:idx]
                key = f"{config.prefix}/compare/one_tile_{idx}.parquet"
                vsis_path = f"/vsis3/{bucket_name}/{key}"
                s3_key = f"s3://{bucket_name}/{key}"
                key_set.add(s3_key)
                cleanup_list.append(key)
                executor.submit(write_one, city, vsis_path)
        max_wait_time_s = 300  # 5min per 1M
        stress_test_common(
            key_set,
            cleanup_list,
            sqs_out,
            region,
            bucket_name,
            start_time,
            max_wait_time_s,
        )
    except Exception as e:
        clear_sqs(sqs_out, region)
        clear_sqs(sqs_in, region)
        raise e


# @pytest.mark.skip(reason="Manually run only.")
@pytest.mark.parametrize("env_type", ("prod",), indirect=True)
def test_stress(
    env_type,
    config: CloudConfig,
    bucket_name: str,
    region: str,
    sqs_out: str,
    sqs_in: str,
    big_aoi_fill: None,
    big_tiles_path: Path,
    prefix: str
):
    """
    Stress test the environment by sending 1k files with 1k tiles in them and
    querying SQS/Cloudwatch for results.
    """
    # clear potential previous run data
    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)

    try:

        tile_count = 1074405
        batch_size = 1000
        count = floor(tile_count / batch_size)
        print(f"creating {count} files.")
        start_time = dt.datetime.now(dt.timezone.utc)
        key_set = set()
        cleanup_list = []

        n = 0
        gdf = st.read_file(big_tiles_path)
        gdf = gdf.with_columns(st.geom().st.set_srid(4326))
        def write_file(sl, vsis_path):
            sl.st.write_file(vsis_path, driver="PARQUET", compression="zstd")

        with ThreadPoolExecutor() as executor:
            for sl in gdf.iter_slices(1000):
                key = f"{config.prefix}/compare/stress_{n}.parquet"
                vsis_path = f"/vsis3/{config.bucket}/{key}"
                cleanup_list.append(key)
                s3path = f"s3://{bucket_name}/{key}"
                key_set.add(s3path)
                n = n + 1
                executor.submit(write_file, sl=sl, vsis_path=vsis_path)

        max_wait_time_s = max(600, 30 * (tile_count / 10**5))  # 5min per 1M
        lambda_name = f"{prefix}_tns_comp_lambda"
        stress_test_common(
            key_set,
            cleanup_list,
            sqs_out,
            region,
            bucket_name,
            start_time,
            max_wait_time_s,
            lambda_name
        )
    except Exception as e:
        clear_sqs(sqs_out, region)
        clear_sqs(sqs_in, region)
        raise e
