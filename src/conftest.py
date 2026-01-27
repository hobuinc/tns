import subprocess
import os
import pytest
import json
import boto3
from pathlib import Path
from time import sleep
from shapely import Polygon, geometry, bounds, box, from_geojson

import pandas as pd
import polars_st as st

from db_lambda import delete_if_found, CloudConfig


def clear_sqs(sqs_arn, region):
    sqs = boto3.client("sqs", region_name=region)
    queue_name = sqs_arn.split(":")[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    messages = []
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
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        else:
            break
    return messages


def get_message(action, tf_output, retries=0):
    queue_arn = tf_output[f"db_{action}_sqs_in"]
    aws_region = tf_output["aws_region"]

    sqs = boto3.client("sqs", region_name=aws_region)
    queue_name = queue_arn.split(":")[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    message = sqs.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=1, MessageSystemAttributeNames=["All"]
    )
    try:
        message = message["Messages"][0]
    except KeyError:
        if retries >= 5:
            raise RuntimeError("Failed to fetch SQS message from s3 put")
        sleep(1)
        message = get_message(action, tf_output, retries + 1)

    return message


def put_parquet(action, tf_output, polygon, pk_and_model, amt=1):
    # aws_region = tf_output["aws_region"]
    bucket_name = tf_output["s3_bucket_name"]
    key = f"{action}/geom.parquet"

    # s3 = boto3.client("s3", region_name=aws_region)
    vsis_path = f'/vsis3/{bucket_name}/{key}'
    df_kwargs = {"compression": "zstd",}

    gdf = st.GeoDataFrame(
        data={
            "pk_and_model": [f"{pk_and_model}_{n}" for n in range(amt)],
            "geometry": [from_geojson(polygon).wkb for n in range(amt)],
        },
        strict=True,
        infer_schema_length=True,
        geometry_format='wkb'
    )
    gdf = gdf.with_columns(st.geom().st.set_srid(4326))
    gdf.st.write_file(path=vsis_path, driver="PARQUET", layer="product", **df_kwargs)
    # df_bytes = gdf.to_parquet()
    # return s3.put_object(Body=df_bytes, Bucket=bucket_name, Key=key)


def get_event(message, action, tf_output):
    body = message["Body"]
    handle = message["ReceiptHandle"]
    sqs_arn = tf_output[f"db_{action}_sqs_in"]
    region = tf_output["aws_region"]
    return {
        "Records": [
            {
                "messageId": "",
                "receiptHandle": handle,
                "body": body,
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "",
                    "SenderId": "",
                    "ApproximateFirstReceiveTimestamp": "",
                },
                "messageAttributes": {},
                "md5OfBody": "",
                "eventSource": "aws:sqs",
                "eventSourceARN": sqs_arn,
                "awsRegion": region,
            }
        ]
    }


@pytest.fixture(scope="function")
def states_geoms():
    states_json = json.load(open("./src/geoms.json"))

    def feature_to_wkb(f):
        p = Polygon(f["geometry"]["rings"][0])
        return p.wkb

    states_gdf = st.GeoDataFrame(
        [
            {"pk_and_model": f"raster_{idx}", "geometry": feature_to_wkb(feature)}
            for idx, feature in enumerate(states_json["features"])
        ]
    )
    yield states_gdf

@pytest.fixture(scope="function")
def states_tiles():
    states_json = json.load(open("./src/geoms.json"))

    def feature_to_wkb(f):
        p = Polygon(f["geometry"]["rings"][0])
        return p.wkb

    # make gdf of size 1000
    states_gdf = st.GeoDataFrame(
        [
            {"pk_and_model": f"raster_{idx}_{n}", "geometry": feature_to_wkb(feature)}
            for idx, feature in enumerate(states_json["features"])
            for n in range(20)
        ]
    )

    return states_gdf



@pytest.fixture(scope="session")
def config(tf_output):
    os.environ["AWS_REGION"] = tf_output["aws_region"]
    os.environ["SNS_OUT_ARN"] = tf_output["db_add_sns_out"]
    os.environ["DB_TABLE_NAME"] = tf_output["table_name"]
    yield CloudConfig()


@pytest.fixture(scope="session")
def dynamo(tf_output):
    aws_region = tf_output["aws_region"]
    yield boto3.client("dynamodb", region_name=aws_region)


@pytest.fixture(scope="function")
def cleanup(tf_output, config):
    aois_to_delete = []

    yield aois_to_delete

    for a in aois_to_delete:
        delete_if_found(a, config)


@pytest.fixture(scope="session")
def tf_dir():
    cur_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    tf_dir = cur_dir / ".." / "terraform"
    yield tf_dir


@pytest.fixture(scope="session")
def tf_output(tf_dir):
    tf = subprocess.Popen(
        ["terraform", "output", "--json"],
        cwd=tf_dir,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf8",
    )
    a = tf.communicate()
    output_json = json.loads(a[0])
    key_vals = {k: v["value"] for k, v in output_json.items()}
    yield key_vals


@pytest.fixture(scope="session")
def region(tf_output):
    yield tf_output["aws_region"]


@pytest.fixture(scope="function")
def h3_indices():
    yield ["832a06fffffffff", "832a31fffffffff", "832a04fffffffff"]


@pytest.fixture(scope="function")
def big_geom_h3_indices():
    yield [
        "835a75fffffffff",
        "835a64fffffffff",
        "837ecbfffffffff",
        "835b51fffffffff",
        "835b55fffffffff",
        "835b6dfffffffff",
        "835b43fffffffff",
        "835b46fffffffff",
        "835a54fffffffff",
        "835b29fffffffff",
        "835a70fffffffff",
        "835b4bfffffffff",
        "835a76fffffffff",
        "835a60fffffffff",
        "835b6efffffffff",
        "835a2efffffffff",
        "835b58fffffffff",
        "835b66fffffffff",
        "835b6afffffffff",
        "835a2dfffffffff",
        "835b6bfffffffff",
        "835a73fffffffff",
        "835a0efffffffff",
        "835a2afffffffff",
        "835b60fffffffff",
        "835b5cfffffffff",
        "835b76fffffffff",
        "835b4efffffffff",
        "835a05fffffffff",
        "835b5afffffffff",
        "835a44fffffffff",
        "835a23fffffffff",
        "835b40fffffffff",
        "835a28fffffffff",
        "835a42fffffffff",
        "835b4afffffffff",
        "835a21fffffffff",
        "837eddfffffffff",
        "835a2cfffffffff",
        "837edafffffffff",
        "835b73fffffffff",
        "835b70fffffffff",
        "837669fffffffff",
        "835a08fffffffff",
        "835b4cfffffffff",
        "835b50fffffffff",
        "835b62fffffffff",
        "835b42fffffffff",
        "835b44fffffffff",
        "835a72fffffffff",
        "835b09fffffffff",
        "835b72fffffffff",
        "835b63fffffffff",
        "835b6cfffffffff",
        "835b0dfffffffff",
        "837749fffffffff",
        "835b65fffffffff",
        "835b56fffffffff",
        "835a01fffffffff",
        "837edefffffffff",
        "835a2bfffffffff",
        "835b74fffffffff",
        "837edbfffffffff",
        "837edcfffffffff",
        "835a09fffffffff",
        "835b52fffffffff",
        "835a29fffffffff",
        "835b68fffffffff",
        "837ed9fffffffff",
        "837665fffffffff",
        "835a71fffffffff",
        "835b48fffffffff",
        "835b64fffffffff",
        "835a74fffffffff",
        "835a66fffffffff",
        "835a62fffffffff",
        "835b54fffffffff",
        "835b5dfffffffff",
        "835b69fffffffff",
        "835a46fffffffff",
        "835b59fffffffff",
        "835b5bfffffffff",
        "835a00fffffffff",
        "83766dfffffffff",
        "835b45fffffffff",
        "835b4dfffffffff",
        "835b5efffffffff",
        "837ed8fffffffff",
        "835a25fffffffff",
        "835b41fffffffff",
        "835b53fffffffff",
        "835b71fffffffff",
        "835a55fffffffff",
        "835a0dfffffffff",
        "835a0cfffffffff",
        "83766cfffffffff",
        "837ecafffffffff",
        "835a03fffffffff",
        "835b61fffffffff",
        "835b75fffffffff",
        "837661fffffffff",
    ]


@pytest.fixture(scope="function")
def updated_h3_indices():
    yield ["83281bfffffffff", "8328f4fffffffff"]


@pytest.fixture(scope="function")
def pk_and_model():
    yield "raster_1234"


@pytest.fixture(scope="session")
def db_compare_sqs_in_arn(tf_output):
    yield tf_output["db_compare_sqs_in"]


@pytest.fixture(scope="session")
def db_add_sqs_in_arn(tf_output):
    yield tf_output["db_add_sqs_in"]


@pytest.fixture(scope="session")
def db_delete_sqs_in_arn(tf_output):
    yield tf_output["db_delete_sqs_in"]


@pytest.fixture(scope="function")
def add_message(tf_output, region, geom, pk_and_model):
    put_parquet("add", tf_output, geom, pk_and_model)
    yield get_message("add", tf_output)


@pytest.fixture(scope="function")
def add_event(add_message, tf_output):
    yield get_event(add_message, "add", tf_output)


@pytest.fixture(scope="function")
def add_big_geom_message(tf_output, region, big_geom, pk_and_model):
    put_parquet("add", tf_output, big_geom, pk_and_model)
    yield get_message("add", tf_output)


@pytest.fixture(scope="function")
def add_big_geom_event(add_big_geom_message, tf_output):
    yield get_event(add_big_geom_message, "add", tf_output)


@pytest.fixture(scope="function")
def update_message(tf_output, update_geom, pk_and_model):
    put_parquet("add", tf_output, update_geom, pk_and_model)
    yield get_message("add", tf_output)


@pytest.fixture(scope="function")
def update_event(update_message, tf_output):
    yield get_event(update_message, "add", tf_output)


@pytest.fixture(scope="function")
def comp_message(tf_output, geom, pk_and_model):
    put_parquet("compare", tf_output, geom, pk_and_model, amt=1000)
    yield get_message("compare", tf_output)


@pytest.fixture(scope="function")
def comp_event(comp_message, tf_output):
    yield get_event(comp_message, "compare", tf_output)


@pytest.fixture(scope="function")
def delete_message(tf_output, geom, pk_and_model):
    put_parquet("delete", tf_output, geom, pk_and_model)
    yield get_message("delete", tf_output)


@pytest.fixture(scope="function")
def delete_event(delete_message, tf_output):
    yield get_event(delete_message, "delete", tf_output)


@pytest.fixture(scope="function")
def db_fill(tf_output, pk_and_model, h3_indices, geom, updated_h3_indices):
    db_name = tf_output["table_name"]
    aws_region = tf_output["aws_region"]
    dynamo = boto3.client("dynamodb", region_name=aws_region)
    aoi_name = f"{pk_and_model}_0"

    # make sure that old entries are deleted before using
    for pk in h3_indices:
        key = {
            "pk_and_model": {"S": aoi_name},
            "h3_id": {"S": pk},
        }
        dynamo.delete_item(Key=key, TableName=db_name)
    for pk in updated_h3_indices:
        key = {
            "pk_and_model": {"S": aoi_name},
            "h3_id": {"S": pk},
        }
        dynamo.delete_item(Key=key, TableName=db_name)

    request = {
        f"{db_name}": [
            {
                "PutRequest": {
                    "Item": {
                        "h3_id": {"S": pk},
                        "pk_and_model": {"S": aoi_name},
                        "polygon": {"S": geom},
                    }
                }
            }
            for pk in h3_indices
        ]
    }
    yield dynamo.batch_write_item(RequestItems=request)

    for pk in h3_indices:
        key = {
            "pk_and_model": {"S": aoi_name},
            "h3_id": {"S": pk},
        }
        dynamo.delete_item(Key=key, TableName=db_name)
    for pk in updated_h3_indices:
        key = {
            "pk_and_model": {"S": aoi_name},
            "h3_id": {"S": pk},
        }
        dynamo.delete_item(Key=key, TableName=db_name)


@pytest.fixture(scope="function")
def update_geom():
    yield json.dumps(
        {
            "type": "Polygon",
            "coordinates": [
                [
                    [-123.074979482909725, 44.049714592181111],
                    [-123.075534969143874, 44.062481974419434],
                    [-123.062505348770557, 44.062776048474724],
                    [-123.061952669217845, 44.050008602244745],
                    [-123.074979482909725, 44.049714592181111],
                ]
            ],
        }
    )


@pytest.fixture(scope="function")
def big_geom():
    yield json.dumps(
        {
            "type": "Polygon",
            "coordinates": [
                [
                    [165.260025025527, 4.57486104965216],
                    [172.162002562364, 4.57486104965216],
                    [172.162002562364, 14.6551666259766],
                    [165.260025025527, 14.6551666259766],
                    [165.260025025527, 4.57486104965216],
                ]
            ],
        }
    )


@pytest.fixture(scope="function")
def geom():
    yield json.dumps(
        {
            "type": "Polygon",
            "coordinates": [
                [
                    [-70.493308, 41.279975],
                    [-70.436845, 41.299054],
                    [-70.408171, 41.30899],
                    [-70.394819, 41.319664],
                    [-70.394805, 41.319682],
                    [-70.388153, 41.32578],
                    [-70.384532, 41.333519],
                    [-70.380032, 41.342892],
                    [-70.374466, 41.351584],
                    [-70.36975, 41.357418],
                    [-70.361022, 41.36427],
                    [-70.352415, 41.369206],
                    [-70.346086, 41.372182],
                    [-70.339753, 41.375119],
                    [-70.328182, 41.380159],
                    [-70.315816, 41.383227],
                    [-70.301701, 41.385151],
                    [-70.287685, 41.384672],
                    [-70.271551, 41.381243],
                    [-70.258422, 41.381],
                    [-70.249463, 41.381012],
                    [-70.242163, 41.381703],
                    [-70.23433, 41.383229],
                    [-70.233576, 41.38264],
                    [-70.224936, 41.37512],
                    [-70.221622, 41.371769],
                    [-70.217334, 41.364997],
                    [-70.215115, 41.360275],
                    [-70.208297, 41.358025],
                    [-70.198383, 41.359025],
                    [-70.187287, 41.35838],
                    [-70.174906, 41.35729],
                    [-70.159992, 41.352242],
                    [-70.148699, 41.345216],
                    [-70.13794, 41.346371],
                    [-70.132231, 41.348731],
                    [-70.124461, 41.351327],
                    [-70.119335, 41.352638],
                    [-70.110095, 41.353592],
                    [-70.101618, 41.35345],
                    [-70.097844, 41.353584],
                    [-70.103251, 41.359705],
                    [-70.107199, 41.365213],
                    [-70.111328, 41.370389],
                    [-70.11644, 41.386165],
                    [-70.116541, 41.395132],
                    [-70.112734, 41.408126],
                    [-70.105819, 41.419866],
                    [-70.094412, 41.43081],
                    [-70.078847, 41.438015],
                    [-70.065174, 41.442687],
                    [-70.044272, 41.443801],
                    [-70.025902, 41.441079],
                    [-70.006467, 41.433898],
                    [-69.999458, 41.428432],
                    [-69.994643, 41.425024],
                    [-69.989227, 41.421225],
                    [-69.981775, 41.415208],
                    [-69.974592, 41.408922],
                    [-69.969634, 41.403511],
                    [-69.964065, 41.395852],
                    [-69.957745, 41.38725],
                    [-69.952535, 41.379656],
                    [-69.949535, 41.375119],
                    [-69.943801, 41.366717],
                    [-69.938456, 41.357645],
                    [-69.933859, 41.351049],
                    [-69.929837, 41.345323],
                    [-69.92644, 41.338794],
                    [-69.921579, 41.332398],
                    [-69.91495, 41.324195],
                    [-69.910093, 41.317263],
                    [-69.906211, 41.3108],
                    [-69.902948, 41.305207],
                    [-69.898321, 41.297003],
                    [-69.894885, 41.290372],
                    [-69.893789, 41.286925],
                    [-69.892959, 41.283643],
                    [-69.892445, 41.27909],
                    [-69.8925, 41.276211],
                    [-69.892209, 41.271323],
                    [-69.893025, 41.266637],
                    [-69.894472, 41.25924],
                    [-69.896044, 41.254119],
                    [-69.897743, 41.250122],
                    [-69.89901, 41.245538],
                    [-69.902992, 41.238416],
                    [-69.910319, 41.226849],
                    [-69.915842, 41.220939],
                    [-69.922911, 41.21584],
                    [-69.930953, 41.210543],
                    [-69.942008, 41.20452],
                    [-69.953586, 41.199672],
                    [-69.963739, 41.196393],
                    [-69.973182, 41.193545],
                    [-69.983151, 41.191403],
                    [-69.999457, 41.188305],
                    [-70.012483, 41.187053],
                    [-70.025702, 41.187102],
                    [-70.039705, 41.188192],
                    [-70.051647, 41.190908],
                    [-70.072666, 41.191634],
                    [-70.096057, 41.19053],
                    [-70.109684, 41.189803],
                    [-70.12446, 41.192422],
                    [-70.135855, 41.194649],
                    [-70.16117, 41.20069],
                    [-70.178851, 41.204531],
                    [-70.205762, 41.214219],
                    [-70.217017, 41.218176],
                    [-70.227997, 41.222664],
                    [-70.239982, 41.228463],
                    [-70.285736, 41.242346],
                    [-70.294484, 41.246499],
                    [-70.314298, 41.260532],
                    [-70.33509, 41.272779],
                    [-70.344083, 41.276146],
                    [-70.357554, 41.272596],
                    [-70.369819, 41.270406],
                    [-70.374466, 41.269458],
                    [-70.386671, 41.271282],
                    [-70.396777, 41.273014],
                    [-70.408733, 41.277294],
                    [-70.414493, 41.273953],
                    [-70.429075, 41.268651],
                    [-70.444758, 41.265513],
                    [-70.459851, 41.265253],
                    [-70.475635, 41.27022],
                    [-70.493308, 41.279975],
                ]
            ],
        }
    )


@pytest.fixture()
def big_event():
    yield {
        "Records": [
            {
                "messageId": "4247e223-b0f0-48e0-9859-11c37320c73a",
                "receiptHandle": "AQEBXSyuOIbJXxUE3/fZ2UmZvSEnoGDvzZd46ox1l9L7B0m31IYJyqv9Ch8tkjG714BympNE/kvKCcwvdjb2jDrJ4bGkENz6iJNx5SYnnksOKojWFdKszlpt+CHDYv5zNxWSSjxdq+LiHpfFdawUt1OLT5vjVU/csO70ECFdysHsl64JYA/fwM0R7G1UW+a1mlTV4N7l1o1tI4+bbtA4z5zqW2NJm4rUaoBL5dOtJ5fzomjNZRK+Di9wQbOXBKHXC+gd/NQCh19UofLdkh2X2ZbB9/deW7f11kVosGn8I2SUEg/LsJmYppkK4xcCd1dq0oOCSYWdqi8500MnQIp0K5KP7DOrACORBnBg43g/1c4RQMcieK9MDUMWv47kC2UD8ycxfRxLvg+FkPKM2lGypcvXQA==",
                "body": '{\n  "Type" : "Notification",\n  "MessageId" : "a03cb080-e59a-5f90-934d-d1705f7dea61",\n  "TopicArn" : "arn:aws:sns:us-west-2:068489536557:tns_compare_sns_in",\n  "Subject" : "Amazon S3 Notification",\n  "Message" : "{\\"Records\\":[{\\"eventVersion\\":\\"2.1\\",\\"eventSource\\":\\"aws:s3\\",\\"awsRegion\\":\\"us-west-2\\",\\"eventTime\\":\\"2026-01-26T00:43:18.762Z\\",\\"eventName\\":\\"ObjectCreated:Put\\",\\"userIdentity\\":{\\"principalId\\":\\"AWS:AIDAI4BQDXNYYEOF4QKQA\\"},\\"requestParameters\\":{\\"sourceIPAddress\\":\\"97.127.3.202\\"},\\"responseElements\\":{\\"x-amz-request-id\\":\\"GT9JC1YSS57PYTZ9\\",\\"x-amz-id-2\\":\\"tHStzdZk2PBgDB3FUdhGldRcZH4Zip7jHG5YO3DCO0ytdYmf4Ux3HCL1pKv0oz8wwAlds+bADi0PlOyu9wW74KMDEwcdg0t+\\"},\\"s3\\":{\\"s3SchemaVersion\\":\\"1.0\\",\\"configurationId\\":\\"tf-s3-topic-20260126004022013000000002\\",\\"bucket\\":{\\"name\\":\\"tns-bucket-premade\\",\\"ownerIdentity\\":{\\"principalId\\":\\"A1IA91PUEBL420\\"},\\"arn\\":\\"arn:aws:s3:::tns-bucket-premade\\"},\\"object\\":{\\"key\\":\\"compare/geom.parquet\\",\\"size\\":16371,\\"eTag\\":\\"796850cb80ebe5b047bb7f1a5b9ef20e\\",\\"sequencer\\":\\"006976B8A6AECEE0CB\\"}}}]}",\n  "Timestamp" : "2026-01-26T00:43:19.649Z",\n  "SignatureVersion" : "1",\n  "Signature" : "YAjB8h7Crcxcqaohd8XuGPK835hCyU03HB6fzLCdGNAyZKsWAKNO5LggUqEa285QzjgaF7+qRnrGjP/SIFtsFKFPHUu8UwOU2sAToMc932fjpbU1oD6KTOmiLeF4eXMa9MrF1ug/t9lrAEfv5kJeP0NVE2SX9zf7L9MR4MFjJuU1DjhIcc8tlNQAUc568vRT70Jk67+EhMW3aq/LvaqsXhILi5sgrnYirmtH66MboVYxDDbGlsGd5Yy/pNMx/oo5YqtcQXt8ZGA5B94FYatwsK333zIM6MgjZEm5jDFTIHz0O18L7su3b82Wqa/5s8WJMtCHQAdSIaPISKleWIgpxg==",\n  "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-7506a1e35b36ef5a444dd1a8e7cc3ed8.pem",\n  "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:068489536557:tns_compare_sns_in:39c7dd1d-cfa1-482d-ab68-3159f58c97c6"\n}',
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1769388199681",
                    "SenderId": "AIDAIYLAVTDLUXBIEIX46",
                    "ApproximateFirstReceiveTimestamp": "1769388199696",
                },
                "messageAttributes": {},
                "md5OfBody": "397052706c1edd5be1366feb718aaf91",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-west-2:068489536557:tns_compare_sqs_input",
                "awsRegion": "us-west-2",
            }
        ]
    }
