import subprocess
import os
from typing import Generator, TypeVar
import pytest
import json
import boto3

from pathlib import Path
from time import sleep
import polars_st as st

from intersects_lambda import CloudConfig


TILES_PER_FILE = 1000
T = TypeVar("T")
Fixture = Generator[T, None, None]
EventType = dict[str, list[dict[str, any]]]


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
        WaitTimeSeconds=5,
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
    bucket: str,
    key: str,
    filepath: Path,
    config: CloudConfig,
) -> None:
    outpath = f"s3://{bucket}/{key}"
    duck_cmd = f"""
        COPY
            (SELECT * FROM read_parquet('{filepath.as_posix()}')) TO
            '{outpath}' (FORMAT parquet, COMPRESSION zstd)
    """
    config.con.sql(duck_cmd)


def get_event(messages: dict[str, any], sqs_arn: str, region: str) -> EventType:
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
def tf_dir(test_dir: Path) -> Fixture[Path]:
    tf_dir = test_dir / ".." / "terraform"
    yield tf_dir


@pytest.fixture(scope="function")
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


@pytest.fixture(scope="function")
def region(tf_output: dict[str, str]) -> Fixture[str]:
    yield tf_output["aws_region"]


@pytest.fixture(scope="function")
def sqs_in(tf_output: dict[str, str]) -> Fixture[str]:
    yield tf_output["sqs_in"]


@pytest.fixture(scope="function")
def sqs_out(tf_output: dict[str, str]) -> Fixture[str]:
    yield tf_output["sqs_out"]


@pytest.fixture(scope="function")
def sns_out(tf_output: dict[str, str]) -> Fixture[str]:
    yield tf_output["sns_out"]


@pytest.fixture(scope="function")
def bucket_name(tf_output: dict[str, str]) -> Fixture[str]:
    yield tf_output["s3_bucket_name"]


@pytest.fixture(scope="function")
def small_aois_path(test_dir: Path) -> Fixture[str]:
    yield test_dir / "data" / "state_aois.parquet"


@pytest.fixture(scope="function")
def small_tiles_path(test_dir: Path) -> Fixture[str]:
    yield test_dir / "data" / "state_tiles.parquet"


@pytest.fixture(scope="function")
def big_aois_path(test_dir: Path) -> Fixture[str]:
    yield test_dir / "data" / "big_aoi_set.parquet"


@pytest.fixture(scope="function")
def big_tiles_path(test_dir: Path) -> Fixture[str]:
    yield test_dir / "data" / "big_state_tiles.parquet"

@pytest.fixture(scope="function")
def env_vars(tf_output: dict[str, str]) -> None:
    os.environ["AWS_REGION"] = tf_output["aws_region"]
    os.environ["SNS_OUT_ARN"] = tf_output["sns_out"]
    os.environ["S3_BUCKET"] = tf_output["s3_bucket_name"]


@pytest.fixture(scope="function")
def config(region: str, bucket_name: str, sns_out: str) -> Fixture[CloudConfig]:
    yield CloudConfig(region, sns_out, bucket_name)


@pytest.fixture(scope="function")
def test_dir() -> Fixture[Path]:
    yield Path(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="function")
def messages(
    config: CloudConfig,
    region: str,
    sqs_in: str,
    bucket_name: str,
    small_tiles_path: Path,
) -> Fixture[dict[str, any]]:
    key = "compare/geom.parquet"
    put_parquet(bucket_name, key, small_tiles_path, config)
    messages = get_message(sqs_in, region)
    yield messages


@pytest.fixture(scope="function")
def event(
    messages: dict[str, any], sqs_in: str, region: str
) -> Fixture[EventType]:
    yield get_event(messages, sqs_in, region)


@pytest.fixture(scope="function")
def big_messages(
    sqs_in: str,
    region: str,
    bucket_name: str,
    big_tiles_path: Path,
    config: CloudConfig,
) -> Fixture[list[dict[str, any]]]:
    amt = 10
    for n in range(amt):
        key = f"compare/geom_{n}.parquet"
        put_parquet(bucket_name, key, big_tiles_path, config)
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
) -> Fixture[EventType]:
    event = get_event(big_messages, sqs_in, region)
    yield event


@pytest.fixture(scope="function")
def aoi_fill(
    bucket_name: str, small_aois_path: Path, config: CloudConfig
) -> Fixture[None]:
    key = "subs/subscriptions.parquet"
    put_parquet(bucket_name, key, small_aois_path, config)


@pytest.fixture(scope="function")
def big_aoi_fill(
    bucket_name: str,
    big_aois_path: Path,
    config: CloudConfig,
) -> Fixture[None]:
    key = "subs/subscriptions.parquet"
    put_parquet(bucket_name, key, big_aois_path, config)
