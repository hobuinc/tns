import json
import os

import boto3
import duckdb
import traceback

from uuid import uuid4, UUID

MAX_MSG_BYTES = 2**10 * 256  # 256KB
EXT_PATH = "/tmp/.duck_extensions"
DDB_PATH = "/tmp/.duckdb"


class CloudConfig:
    def __init__(self):
        env_keys = os.environ.keys()

        if "AWS_REGION" in env_keys:
            self.region = os.environ["AWS_REGION"]
        else:
            raise ValueError(
                "Required variable AWS_REGION missing from environment."
            )

        if "SNS_OUT_ARN" in env_keys:
            self.sns_out_arn = os.environ["SNS_OUT_ARN"]
        else:
            raise ValueError(
                "Required variable SNS_OUT_ARN missing from environment."
            )

        self.sns = boto3.client("sns", region_name=self.region)
        self.s3 = boto3.client("s3", region_name=self.region)
        self.sqs = boto3.client("sqs", region_name=self.region)

        # failures before this can't be published because required info
        # isn't available yet
        try:
            if "S3_BUCKET" in env_keys:
                self.bucket = os.environ["S3_BUCKET"]
            else:
                raise ValueError(
                    "Required variable S3_BUCKET missing from environment."
                )

            con = duckdb.connect(
                database=DDB_PATH, config={"memory_limit": "2.5GB"}
            )
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
            # catch possibility of .duckdb file already made
            try:
                con.execute(ex_str)
            except duckdb.InvalidInputException:
                print("Using previously made duckdb files.")

            self.con = con
            self.aois_path = f"s3://{self.bucket}/subs/subscriptions.parquet"
        except Exception as e:
            fail_tb = traceback.format_exc()
            fail_msg = get_fail_res(uuid4(), [], fail_tb)
            self.sns.publish(TopicArn=self.sns_out_arn, **fail_msg)
            raise e

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.con.close()


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
        "aoi_list": {
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
    name = uuid4()
    base_s3_path = f"{config.bucket}/intersects/{name}.parquet"
    full_s3_path = f"s3://{base_s3_path}"

    # create tracking variables
    sql = f"""
        SELECT aois.pk_and_model AS aois, list(tiles.pk_and_model) AS tiles
            FROM read_parquet("{config.aois_path}") AS aois
            JOIN read_parquet({datapaths}) AS tiles
            ON ST_Intersects(aois.geometry, tiles.geometry)
            GROUP BY aois.pk_and_model
    """
    ddbi = config.con.sql(sql)
    aoi_list = ddbi.pl().get_column("aois").to_list()
    config.con.sql(
        f"COPY ddbi TO '{full_s3_path}' (FORMAT parquet, COMPRESSION zstd)"
    )

    return get_pass_res(name, datapaths, aoi_list, full_s3_path)


# Note: aois in db will have pk_and_model attribute that corresponds with GRiD's
# AOI convention of {ModelPrefix}_{SubscriptionPK}, whereas tiles will also
# have a pk_and_model attribute, but corresponds with GRiD's Tile convention of
# {TileModel}_{TilePK}. All aois will come in in EPSG:4326
def handler(event: dict[str, str], context):
    import time

    print("Event:", json.dumps(event))
    data_paths = []
    with CloudConfig() as config:
        try:
            events = event["Records"]

            # collect data paths, delete messages after processing
            for sqs_event in events:
                data_paths = data_paths + get_data_paths(sqs_event)

            # process data paths together
            start = time.time()
            sns_messages = apply_compare(data_paths, config)
            end = time.time() - start
            print(f"processing time for {len(data_paths)} files: {end}s")
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
