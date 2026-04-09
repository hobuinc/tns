import subprocess
import os
from typing import Generator, TypeVar
import pytest
import json
import boto3
import duckdb
import polars_st as st

from pathlib import Path
from time import sleep

from intersects_lambda import CloudConfig


TILES_PER_FILE = 1000
T = TypeVar("T")
Fixture = Generator[T, None, None]
EventType = dict[str, list[dict[str, any]]]


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "skip_by_env(env): fine tests based on terraform env."
    )


def clear_sqs(sqs_arn: str, region: str) -> None:
    """Clear old SQS Messages from queue so tests aren't confused."""
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
    """Receive Message from SQS Queue with retries."""
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
) -> None:
    """Use polars_st to copy a parquet file to S3."""
    outpath = f"/vsis3/{bucket}/{key}"
    local = st.read_file(filepath)
    local = local.with_columns(st.geom().st.set_srid(4326))
    local.st.write_file(
        outpath, driver="PARQUET", compression="zstd", row_group_size=100000
    )


def get_event(messages: dict[str, any], sqs_arn: str, region: str) -> EventType:
    """Recreate SQS Message structure for tests."""
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
    """Terraform directory."""
    tf_dir = test_dir / ".." / "terraform"
    yield tf_dir


@pytest.fixture(scope="function")
def tf_output(tf_dir: Path) -> Fixture[dict[str, str]]:
    """Get terraform output and translate to dictionary."""
    tf = subprocess.Popen(
        ["terraform", "output", "--json"],
        cwd=tf_dir,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf8",
    )
    a = tf.communicate()
    if a[0] == "":
        yield {}
    else:
        output_json = json.loads(a[0])
        key_vals = {k: v["value"] for k, v in output_json.items()}
        yield key_vals


@pytest.fixture(scope="function")
def env(tf_output: dict[str, str]) -> Fixture[str]:
    """Deployment type, determines which tests to run.
    unit means nothing is deployed, only run test_units.
    test means test env is deployed, only run test_lambdas.
    prod means prod env is deployed, only run test_deployment.
    """
    try:
        env = tf_output["env"]
        yield env
    except KeyError:
        yield "unit"


@pytest.fixture(scope="function")
def env_type(request, env):
    req_env = request.param
    if req_env != env:
        pytest.skip(
            f"This test requires environment {env}, but {req_env} is active."
        )


@pytest.fixture(scope="function")
def mem_size(tf_output: dict[str, str]) -> Fixture[str]:
    """Lambda memory size in MB from Terraform output."""
    yield tf_output["lambda_memory_size"]


@pytest.fixture(scope="function")
def prefix(tf_output: dict[str, str]) -> Fixture[str]:
    """AWS Region from Terraform output."""
    yield tf_output["prefix"]


@pytest.fixture(scope="function")
def region(tf_output: dict[str, str]) -> Fixture[str]:
    """AWS Region from Terraform output."""
    yield tf_output["aws_region"]


@pytest.fixture(scope="function")
def sqs_in(tf_output: dict[str, str]) -> Fixture[str]:
    """SQS In ARN from Terraform output."""
    yield tf_output["sqs_in"]


@pytest.fixture(scope="function")
def sqs_out(tf_output: dict[str, str]) -> Fixture[str]:
    """SQS Out ARN from Terraform output."""
    yield tf_output["sqs_out"]


@pytest.fixture(scope="function")
def sns_out(tf_output: dict[str, str]) -> Fixture[str]:
    """SNS Out ARN from Terraform output."""
    yield tf_output["sns_out"]


@pytest.fixture(scope="function")
def bucket_name(tf_output: dict[str, str]) -> Fixture[str]:
    """AWS S3 Bucket name from Terraform output."""
    yield tf_output["s3_bucket_name"]


@pytest.fixture(scope="function")
def small_aois_path(test_dir: Path) -> Fixture[Path]:
    """Parquet of 50 USA States in AOI format."""
    yield test_dir / "data" / "state_aois.parquet"


@pytest.fixture(scope="function")
def small_tiles_path(test_dir: Path) -> Fixture[Path]:
    """Parquet of 50 USA States in Tiles format."""
    yield test_dir / "data" / "state_tiles.parquet"


@pytest.fixture(scope="function")
def big_aois_path(test_dir: Path) -> Fixture[Path]:
    """Parquet of 10,000 sample AOIs in various sizes/shapes."""
    yield test_dir / "data" / "big_aoi_set.parquet"


@pytest.fixture(scope="function")
def big_tiles_path(test_dir: Path) -> Fixture[Path]:
    """Parquet of 50 USA States duplicated to 1000 tiles."""
    yield test_dir / "data" / "big_state_tiles.parquet"

@pytest.fixture(scope="function")
def overture_tiles_path(test_dir: Path) -> Fixture[Path]:
    """Parquet of 50 USA States duplicated to 1000 tiles."""
    yield test_dir / "data" / "overture_set.parquet"

@pytest.fixture(scope="function")
def cities_path(test_dir: Path) -> Fixture[Path]:
    """Parquet of US Cities boundaries from Overture."""
    yield test_dir / "data" / "us_cities.parquet"


@pytest.fixture(scope="function")
def env_vars(tf_output: dict[str, str]) -> None:
    """Push Terraform output to the environment for lamdba code to pull from."""
    os.environ["DEPLOY_PREFIX"] = tf_output["prefix"]
    os.environ["AWS_REGION"] = tf_output["aws_region"]
    os.environ["SNS_OUT_ARN"] = tf_output["sns_out"]
    os.environ["S3_BUCKET"] = tf_output["s3_bucket_name"]
    os.environ["MEMORY_LIMIT"] = str(tf_output["lambda_memory_size"])


@pytest.fixture(scope="function")
def config(
    region: str, bucket_name: str, sns_out: str, prefix: str, mem_size: str
) -> Fixture[CloudConfig]:
    """CloudConfig object made from Terraform output values."""
    yield CloudConfig(region, sns_out, bucket_name, prefix, mem_size)


@pytest.fixture(scope="function")
def test_dir() -> Fixture[Path]:
    """Directory path of this file (conftest.py)."""
    yield Path(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="function")
def messages(
    config: CloudConfig,
    region: str,
    sqs_in: str,
    bucket_name: str,
    small_tiles_path: Path,
) -> Fixture[dict[str, any]]:
    """1 Message grabbed from SQS to craft Events."""
    key = f"{config.prefix}/compare/geom.parquet"
    put_parquet(bucket_name, key, small_tiles_path)
    messages = get_message(sqs_in, region)
    yield messages


@pytest.fixture(scope="function")
def event(
    messages: dict[str, any], sqs_in: str, region: str
) -> Fixture[EventType]:
    """Fake events crafted from real SQS messages."""
    # This is an integration-only fixture that should be skipped if not using
    # the "test" terraform environment
    yield get_event(messages, sqs_in, region)


@pytest.fixture(scope="function")
def big_messages(
    env: str,
    sqs_in: str,
    region: str,
    bucket_name: str,
    big_tiles_path: Path,
    config: CloudConfig,
) -> Fixture[list[dict[str, any]]]:
    """10 Messages grabbed from SQS to craft Events."""
    amt = 10
    for n in range(amt):
        key = f"{config.prefix}/compare/geom_{n}.parquet"
        put_parquet(bucket_name, key, big_tiles_path)
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
    env: str | None, big_messages: list, sqs_in: str, region: str
) -> Fixture[EventType]:
    """Events combined together to replicate lambda EventSourceMapping."""
    event = get_event(big_messages, sqs_in, region)
    yield event


@pytest.fixture(scope="function")
def aoi_fill(
    bucket_name: str, small_aois_path: Path, config: CloudConfig
) -> Fixture[None]:
    """Push small subscriptions parquet file to well known path in S3."""
    key = f"{config.prefix}/subs/subscriptions.parquet"
    put_parquet(bucket_name, key, small_aois_path)


@pytest.fixture(scope="function")
def big_aoi_fill(
    bucket_name: str,
    big_aois_path: Path,
    config: CloudConfig,
) -> Fixture[None]:
    """Push large subscriptions parquet file to well known path in S3."""
    key = f"{config.prefix}/subs/subscriptions.parquet"
    put_parquet(bucket_name, key, big_aois_path)


@pytest.fixture(scope="function")
def stress_945_path(test_dir: Path):
    yield test_dir / "data" / "stress_945.parquet"


@pytest.fixture(scope="function")
def messages_945(
    config: CloudConfig,
    region: str,
    sqs_in: str,
    bucket_name: str,
    stress_945_path: Path,
) -> Fixture[dict[str, any]]:
    """1 Message grabbed from SQS to craft Events."""
    key2 = f"{config.prefix}/compare/stress_945.parquet"
    put_parquet(bucket_name, key2, stress_945_path)
    messages = get_message(sqs_in, region, 2, retries=5)
    yield messages


@pytest.fixture(scope="function")
def event_945(
    messages_945: dict[str, any], sqs_in: str, region: str
) -> Fixture[EventType]:
    """Fake events crafted from real SQS messages."""
    # This is an integration-only fixture that should be skipped if not using
    # the "test" terraform environment
    yield get_event(messages_945, sqs_in, region)


@pytest.fixture(scope="function")
def stress_494_path(test_dir: Path):
    yield test_dir / "data" / "stress_494.parquet"


@pytest.fixture(scope="function")
def stress_496_path(test_dir: Path):
    yield test_dir / "data" / "stress_494.parquet"


@pytest.fixture(scope="function")
def mem_test_messages(
    low_mem_config: CloudConfig,
    region: str,
    sqs_in: str,
    bucket_name: str,
    stress_494_path: Path,
    stress_496_path: Path,
) -> Fixture[dict[str, any]]:
    """1 Message grabbed from SQS to craft Events."""
    key = f"{low_mem_config.prefix}/compare/stress_494.parquet"
    put_parquet(bucket_name, key, stress_494_path)
    key2 = f"{low_mem_config.prefix}/compare/stress_496.parquet"
    put_parquet(bucket_name, key2, stress_496_path)
    messages = get_message(sqs_in, region, 2, retries=5)
    retry = 0
    while len(messages) < 2:
        if retry > 5:
            raise ValueError("Unable to get all messages.")
        retry += 1
        second = get_message(sqs_in, region, 2, retries=5)
        messages = messages + second
    yield messages


@pytest.fixture(scope="function")
def mem_test_event(
    mem_test_messages: dict[str, any], sqs_in: str, region: str
) -> Fixture[EventType]:
    """Fake events crafted from real SQS messages."""
    # This is an integration-only fixture that should be skipped if not using
    # the "test" terraform environment
    yield get_event(mem_test_messages, sqs_in, region)


@pytest.fixture(scope="function")
def low_mem_config(
    region: str, bucket_name: str, sns_out: str, prefix: str
) -> Fixture[CloudConfig]:
    """CloudConfig object made from Terraform output values."""
    yield CloudConfig(region, sns_out, bucket_name, prefix, 3072)
