import json
import os

import boto3
import duckdb
import traceback

from uuid import uuid4, UUID

MAX_MSG_BYTES = 2**10 * 256  # 256KB
EXT_PATH = "/tmp/.duck_extensions"
_DUCKDB_CONNECTION: duckdb.DuckDBPyConnection | None = None


class CloudConfig:
    def __init__(self, region, sns_out_arn, bucket):
        self.region = region
        self.sns_out_arn = sns_out_arn
        self.bucket = bucket

        self.sns = boto3.client("sns", region_name=self.region)
        self.s3 = boto3.client("s3", region_name=self.region)
        self.sqs = boto3.client("sqs", region_name=self.region)

        # Reuse duckdb connection if it's available
        global _DUCKDB_CONNECTION
        if _DUCKDB_CONNECTION is None:
            con = duckdb.connect(config={"memory_limit": "2.5GB"})
            # lambdas can only create files in /tmp
            con.sql(f"SET extension_directory = '{EXT_PATH}';")
            con.execute("INSTALL httpfs; LOAD httpfs")
            con.execute("INSTALL spatial; LOAD spatial")
            con.execute("INSTALL aws; LOAD aws")
            ex_str = f"""
                CREATE SECRET (
                    TYPE S3,
                    REGION '{self.region}',
                    PROVIDER CREDENTIAL_CHAIN)
            """
            con.execute(ex_str)

            __DUCKDB_CONNECTION = con
        self.con = __DUCKDB_CONNECTION
        self.aois_path = f"s3://{self.bucket}/subs/subscriptions.parquet"


def delete_sqs_message(e, config: CloudConfig):
    source_arn = e["eventSourceARN"]
    queue_name = source_arn.split(":")[-1]
    queue_url = config.sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    receipt_handle = e["receiptHandle"]
    return config.sqs.delete_message(
        QueueUrl=queue_url, ReceiptHandle=receipt_handle
    )


def get_data_paths(sqs_event):
    body = json.loads(sqs_event["body"])
    message = json.loads(body["Message"])
    # skip TestEvent
    paths = []
    if "Event" not in message or message["Event"] != "s3:TestEvent":
        for sns_event in message["Records"]:
            s3_info = sns_event["s3"]
            bucket = s3_info["bucket"]["name"]
            key = s3_info["object"]["key"]
            path = f"s3://{bucket}/{key}"
            paths.append(path)
    return paths


def get_pass_res(
    name: UUID, dpaths: list[str], aois: list[str], output_path: str
):
    attrs = {
        "source_files": {
            "DataType": "String",
            "StringValue": json.dumps(dpaths),
        },
        "aoi_list": { # TODO
            "DataType": "String",
            "StringValue": json.dumps(aois),
        },
        "s3_output_path": {"DataType": "String", "StringValue": output_path},
        "status": {"DataType": "String", "StringValue": "succeeded"},
    }
    message = f"{name}"

    res = {
        "MessageAttributes": attrs,
        "Message": message,
        "MessageGroupId": "compare",
    }
    if json.dumps(res).encode().__sizeof__() > MAX_MSG_BYTES:
        split = int(len(aois) / 2)
        res1 = get_pass_res(name, dpaths, aois[:split], output_path)
        res2 = get_pass_res(name, dpaths, aois[split:], output_path)
        return [*res1, *res2]
    return [res]


def get_fail_res(name: UUID, dpaths: list[str], err_str: str):
    res = {
        "MessageAttributes": {
            "source_files": {
                "DataType": "String",
                "StringValue": json.dumps(dpaths),
            },
            "status": {"DataType": "String", "StringValue": "failed"},
            "error": {"DataType": "String", "StringValue": err_str},
        },
        "Message": f"{name}",
        "MessageGroupId": "compare",
    }
    return res


def apply_compare(datapaths: list[str], config: CloudConfig):
    sql = f"""
        SELECT aois.pk_and_model AS aois, list(tiles.pk_and_model) AS tiles
            FROM read_parquet("{config.aois_path}") AS aois
            JOIN read_parquet({datapaths}) AS tiles
            ON ST_Intersects(aois.geometry, tiles.geometry)
            GROUP BY aois.pk_and_model
    """
    ddbi = config.con.sql(sql)
    return ddbi


def push_intersects(
    ddbi: duckdb.DuckDBPyRelation,
    datapaths: list[str],
    config: CloudConfig
):
    name = uuid4()
    base_s3_path = f"{config.bucket}/intersects/{name}.parquet"
    full_s3_path = f"s3://{base_s3_path}"

    aoi_list = ddbi.pl().get_column("aois").to_list()
    config.con.sql(
        f"COPY ddbi TO '{full_s3_path}' (FORMAT parquet, COMPRESSION zstd)"
    )

    return get_pass_res(name, datapaths, aoi_list, full_s3_path)


def get_env_vars(var_name: str):
    env_keys = os.environ.keys()

    if var_name in env_keys:
        val = os.environ[var_name]
        return val
    else:
        raise ValueError(
            f"Required variable {var_name} missing from environment."
        )


# Note: aois in db will have pk_and_model attribute that corresponds with GRiD's
# AOI convention of {ModelPrefix}_{SubscriptionPK}, whereas tiles will also
# have a pk_and_model attribute, but corresponds with GRiD's Tile convention of
# {TileModel}_{TilePK}. All aois will come in in EPSG:4326
def handler(event: dict[str, str], context):
    sns_out = get_env_vars("SNS_OUT_ARN")
    region = get_env_vars("AWS_REGION")

    # catch any config errors that are created. Can only push errors to sns
    # if we successfully got the environment variables beforehand

    try:
        bucket = get_env_vars("S3_BUCKET")
        config = CloudConfig(region, sns_out, bucket)
    except Exception as e:
        sns = boto3.client("sns", region_name=region)
        fail_tb = traceback.format_exc()
        fail_msg = get_fail_res(uuid4(), [], fail_tb)
        sns.publish(TopicArn=sns_out, **fail_msg)
        raise e

    try:
        print("Event:", json.dumps(event))
        data_paths = []
        events = event["Records"]

        # collect data paths, delete messages after processing
        for sqs_event in events:
            data_paths = data_paths + get_data_paths(sqs_event)
        if not data_paths:
            raise ValueError("At least one tile GeoParquet path is required.")

        # process data paths together
        intersects = apply_compare(data_paths, config)
        sns_messages = push_intersects(intersects, data_paths, config)
        for msg in sns_messages:
            config.sns.publish(TopicArn=config.sns_out_arn, **msg)

        # delete sqs messages now that we're done
        for sqs_event in events:
            delete_sqs_message(sqs_event, config)

        return sns_messages
    except Exception as e:
        fail_name = uuid4()
        exc_str = traceback.format_exc()
        fail_msg = get_fail_res(fail_name, data_paths, exc_str)
        config.sns.publish(TopicArn=config.sns_out_arn, **fail_msg)
        raise e
