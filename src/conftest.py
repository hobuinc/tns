import subprocess
import os
from typing import Generator, TypeVar
import pytest
import json
import boto3

from pathlib import Path
from time import sleep

import polars_st as st
from shapely import Polygon

from db_lambda import CloudConfig


TILES_PER_FILE = 1000
T = TypeVar("T")
Fixture = Generator[T, None, None]


def clear_sqs(sqs_arn: str, region: str) -> None:
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
                sqs.delete_message(
                    QueueUrl=queue_url, ReceiptHandle=receipt_handle
                )
        else:
            break
    return


def get_message(
    queue_arn: str, aws_region: str, num_messages: int = 1, retries: int = 0
) -> dict[str, any]:
    sqs = boto3.client("sqs", region_name=aws_region)
    queue_name = queue_arn.split(":")[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    message = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=min(num_messages, 10),
        MessageSystemAttributeNames=["All"],
        WaitTimeSeconds=5
    )
    try:
        messages = message["Messages"]
    except KeyError as e:
        if retries >= 5:
            raise RuntimeError("Failed to fetch SQS message from s3 put") from e
        sleep(1)
        return get_message(queue_arn, aws_region, num_messages, retries + 1)
    return messages


def put_parquet(
    bucket_name, gdf: st.GeoDataFrame, idx: int | None = None
) -> None:
    if idx is not None:
        key = f"compare/geom_{idx}.parquet"
    else:
        key = "compare/geom.parquet"

    vsis_path = f"/vsis3/{bucket_name}/{key}"
    df_kwargs = {
        "compression": "zstd",
        "WRITE_COVERING_BBOX": "yes",
        "USE_PARQUET_GEO_TYPES": "yes",
        "EDGES": "spherical",
    }

    gdf = gdf.with_columns(st.geom().st.set_srid(4326))
    gdf.st.write_file(
        path=vsis_path, driver="PARQUET", layer="product", **df_kwargs
    )


def get_event(
    messages: dict[str, any], sqs_arn: str, region: str
) -> dict[str, list[dict[str, any]]]:
    return {
        "Records": [
            {
                "messageId": "",
                "receiptHandle": message["ReceiptHandle"],
                "body": message["Body"],
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
            for message in messages
        ]
    }


@pytest.fixture(scope="function")
def big_states_tiles() -> Fixture[st.GeoDataFrame]:
    states_json = json.load(open("./src/geoms.json"))

    def feature_to_wkb(f):
        p = Polygon(f["geometry"]["rings"][0])
        return p.wkb

    rng = int(TILES_PER_FILE / 50)
    states_gdf = st.GeoDataFrame(
        [
            {
                "pk_and_model": f"raster_{idx}_{idx2}",
                "geometry": feature_to_wkb(feature),
            }
            for idx, feature in enumerate(states_json["features"])
            for idx2 in range(rng)
        ]
    )
    yield states_gdf


@pytest.fixture(scope="function")
def states_aois() -> Fixture[st.GeoDataFrame]:
    states_json = json.load(open("./src/geoms.json"))

    def feature_to_wkb(f):
        p = Polygon(f["geometry"]["rings"][0])
        return p.wkb

    states_gdf = st.GeoDataFrame(
        [
            {
                "pk_and_model": f"{feature['attributes']['STATE_NAME']}",
                "geometry": feature_to_wkb(feature),
            }
            for idx, feature in enumerate(states_json["features"])
        ]
    )
    yield states_gdf


@pytest.fixture(scope="function")
def states_tiles() -> Fixture[st.GeoDataFrame]:
    states_json = json.load(open("./src/geoms.json"))

    def feature_to_wkb(f):
        p = Polygon(f["geometry"]["rings"][0])
        return p.wkb

    states_gdf = st.GeoDataFrame(
        [
            {
                "pk_and_model": f"raster_{idx}",
                "geometry": feature_to_wkb(feature),
            }
            for idx, feature in enumerate(states_json["features"])
        ]
    )
    yield states_gdf

@pytest.fixture(scope="session", autouse=True)
def env_vars(tf_output: dict[str, str]) -> None:
    os.environ["AWS_REGION"] = tf_output["aws_region"]
    os.environ["SNS_OUT_ARN"] = tf_output["sns_out"]
    os.environ["S3_BUCKET"] = tf_output["s3_bucket_name"]


@pytest.fixture(scope="function")
def config() -> Fixture[CloudConfig]:
    yield CloudConfig()


@pytest.fixture(scope="session")
def tf_dir() -> Fixture[Path]:
    cur_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    tf_dir = cur_dir / ".." / "terraform"
    yield tf_dir


@pytest.fixture(scope="session")
def tf_output(tf_dir: Path) -> Fixture[dict[str, str]]:
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
def region(tf_output: dict[str, str]) -> Fixture[str]:
    yield tf_output["aws_region"]

@pytest.fixture(scope="session")
def sqs_in(tf_output: dict[str, str]) -> Fixture[str]:
    yield tf_output["sqs_in"]

@pytest.fixture(scope="session")
def sqs_out(tf_output: dict[str, str]) -> Fixture[str]:
    yield tf_output["sqs_out"]

@pytest.fixture(scope="session")
def bucket_name(tf_output: dict[str, str]) -> Fixture[str]:
    yield tf_output["s3_bucket_name"]

@pytest.fixture(scope="function")
def pk_and_model() -> Fixture[str]:
    yield "raster_1234"


@pytest.fixture(scope="function")
def messages(
    config: CloudConfig,
    region: str,
    sqs_in: str,
    bucket_name: str,
    states_tiles: st.GeoDataFrame,
) -> Fixture[dict[str, any]]:
    put_parquet(bucket_name, states_tiles)
    messages = get_message(sqs_in, region)
    yield messages


@pytest.fixture(scope="function")
def event(
    messages: dict[str, any], sqs_in: str, region: str
) -> Fixture[dict[str, list[dict[str, any]]]]:
    yield get_event(messages, sqs_in, region)


@pytest.fixture(scope="function")
def big_messages(
    sqs_in: str, region: str, bucket_name: str, big_states_tiles: st.GeoDataFrame
) -> Fixture[list[dict[str, any]]]:
    amt = 10
    for n in range(amt):
        put_parquet(bucket_name, big_states_tiles, n)
    messages = []
    retry = 5
    count = 0
    while len(messages) < amt or count > retry:
        count = count + 1
        try:
            new_messages = get_message(sqs_in, region, amt)
            messages = messages + new_messages
        except RuntimeError:
            break
    yield messages


@pytest.fixture(scope="function")
def big_event(
    big_messages: list, sqs_in: str, region: str
) -> Fixture[dict[str, list[dict[str, any]]]]:
    event = get_event(big_messages, sqs_in, region)
    yield event


@pytest.fixture(scope="function")
def aoi_fill(
    bucket_name, states_aois: st.GeoDataFrame, config: CloudConfig
) -> Fixture[None]:
    key = "subs/subscriptions.parquet"
    vsis_path = f"/vsis3/{bucket_name}/{key}"
    df_kwargs = {
        "compression": "zstd",
        "WRITE_COVERING_BBOX": "yes",
        "USE_PARQUET_GEO_TYPES": "yes",
        "EDGES": "spherical",
    }
    gdf = states_aois.with_columns(st.geom().st.set_srid(4326))
    yield gdf.st.write_file(
        path=vsis_path, driver="PARQUET", layer="product", **df_kwargs
    )
    config.s3.delete_object(Bucket=bucket_name, Key=key)


@pytest.fixture(scope="function")
def big_aoi_fill(
    bucket_name: str,
    config: CloudConfig,
) -> Fixture[None]:
    cur_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    data_path = cur_dir / "../data/" / "aoi_uc.parquet"

    key = "subs/subscriptions.parquet"
    amt = 10000
    data = config.con.sql(f"""
                SELECT PROJECT_ID as pk_and_model, geometry, geometry_bbox
                FROM read_parquet('{data_path}')
                LIMIT {amt}
        """)  # noqa
    sql = f"""
        COPY
            data
            TO 's3://{bucket_name}/{key}' (FORMAT parquet, COMPRESSION zstd)
    """
    config.con.sql(sql)
    config.con.close()
    yield

    config.s3.delete_object(Bucket=bucket_name, Key=key)
