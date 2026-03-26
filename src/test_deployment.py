import json
import boto3
import datetime
import pandas as pd
from time import sleep

from math import ceil
import polars_st as st

from conftest import put_parquet, clear_sqs
from intersects_lambda import CloudConfig


# def put_polygon(bucket_name, polygon, pk_and_model):
#     gdf = st.GeoDataFrame(
#         data={"pk_and_model": [pk_and_model], "geometry": [polygon.wkb]},
#         strict=True,
#         infer_schema_length=True,
#         geometry_format="wkb",
#     )

#     put_parquet(bucket_name, gdf)


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
            WaitTimeSeconds=5
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


def test_stress(
    config: CloudConfig,
    bucket_name: str,
    region: str,
    sqs_out: str,
    sqs_in: str,
    big_aoi_fill: None,
    big_states_tiles: st.GeoDataFrame,
):

    cloudwatch_client = boto3.client("cloudwatch", region_name=config.region)
    lambda_start_time = datetime.datetime.now(datetime.timezone.utc)

    # clear potential previous run data
    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)

    tile_count = 10**5
    batch_size = 1000
    count = ceil(tile_count / batch_size)
    print(f"creating {count} files.")
    for n in range(count):
        put_parquet(bucket_name, big_states_tiles, n)

    # figure out when lambdas are done creating things
    # arbitrary
    cur_vis = 1
    prev_vis = 0
    prev_empty = 0
    while True:
        end_time = datetime.datetime.now(datetime.timezone.utc)
        start_time = end_time - datetime.timedelta(seconds=30)
        res = cloudwatch_client.get_metric_statistics(
            Namespace="AWS/SQS",
            MetricName="ApproximateNumberOfMessagesVisible",
            Dimensions=[
                {"Name": "QueueName", "Value": "tns_compare_sqs_output"}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=1,
            Statistics=["Sum"],
        )
        if not res["Datapoints"]:
            if prev_empty:
                break
            prev_empty = 1
            sleep(30)
            continue
        df = pd.DataFrame(data=res["Datapoints"])
        cur_vis = df[df.Timestamp == df.Timestamp.max()].iloc[0].Sum.item()
        if cur_vis == prev_vis and prev_vis != 0:
            break
        sleep(30)

    lambda_end_time = datetime.datetime.now(datetime.timezone.utc)
    print("Start Time:", lambda_start_time)
    print("End Time:", lambda_end_time)
    total_time = (lambda_end_time - lambda_start_time).total_seconds()
    print("Total Time: ", total_time)

    # figure out stats on lambdas that were run
    # Errors:
    res = cloudwatch_client.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName="Errors",
        Dimensions=[{"Name": "FunctionName", "Value": "tns_comp_lambda"}],
        StartTime=lambda_start_time,
        EndTime=lambda_end_time,
        Period=60,
        Statistics=["Sum"],
    )
    assert not any([s["Sum"] for s in res["Datapoints"]])

    # Average Durations
    res = cloudwatch_client.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName="Duration",
        Dimensions=[{"Name": "FunctionName", "Value": "tns_comp_lambda"}],
        StartTime=lambda_start_time,
        EndTime=lambda_end_time,
        Period=60,
        Statistics=["Average"],
    )
    print("Average times: ", [s["Average"] for s in res["Datapoints"]])

    msg_count = 0
    failed = []
    while msg_count < count * 50:
        messages = sqs_listen(sqs_out, region, retries=0)
        for msg in messages:
            body = json.loads(msg["Body"])

            attrs = body["MessageAttributes"]
            status = attrs["status"]["Value"]

            if status == "failed":
                failed.append(attrs)

            msg_count += 1
    assert not failed


def test_lambda(
    region: str,
    sqs_out: str,
    sqs_in: str,
    bucket_name: str,
    states_tiles: st.GeoDataFrame,
    states_aois: st.GeoDataFrame,
    aoi_fill: None,
    config: CloudConfig,
):
    # clear potential previous run data
    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)

    filepath = f"s3://{bucket_name}/compare/geom.parquet"
    states = states_aois.to_pandas().pk_and_model.to_list()

    put_parquet(bucket_name, states_tiles)

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
