"""
TNS Lambda handler that processes the SNS->SQS Event messages created as a
result of a Tile Parquet being pushed to S3.

This module uses DuckDB to find the intersect between AOI Subscriptions and
ingested Tiles and output the results to S3 and to SNS.
"""

import json
import os

import boto3
import duckdb
import traceback

from uuid import uuid4

MAX_MSG_BYTES = 2**10 * 256  # 256KB


class CloudConfig:
    """Coordinate AWS and DuckDB connections and associated information."""

    def __init__(self, region, sns_out_arn, bucket, prefix, mem_limit):
        self.region = region
        self.sns_out_arn = sns_out_arn
        self.bucket = bucket
        self.prefix = prefix
        sub_key = f"{self.prefix}/subs/subscriptions.parquet"
        self.aois_path = f"s3://{self.bucket}/{sub_key}"

        # mem limit passed in as value of MB (2**20), GB is (2**30), div by
        # 2**10 for value of GB here
        shorter = mem_limit / (2**10)
        self.mem_limit = f"{shorter}GB"

        self.sns = boto3.client("sns", region_name=self.region)
        self.s3 = boto3.client("s3", region_name=self.region)
        self.sqs = boto3.client("sqs", region_name=self.region)
        self.con = None


    def __enter__(self):
        if self.con is not None:
            try:
                self.con.execute('select 1')
                return self
            except duckdb.ConnectionException:
                pass

        con = duckdb.connect()
        # lambdas can only create files in /tmp
        con.execute("LOAD httpfs")
        con.execute("LOAD spatial")
        con.execute("LOAD aws")
        con.execute(f"SET memory_limit='{self.mem_limit}'")
        ex_str = f"""
            CREATE SECRET (
                TYPE S3,
                REGION '{self.region}',
                PROVIDER CREDENTIAL_CHAIN)
        """
        con.execute(ex_str)

        self.con = con
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()
        self.con = None


def delete_sqs_message(e, config: CloudConfig):
    """Remove Message from SQS Queue."""
    source_arn = e["eventSourceARN"]
    queue_name = source_arn.split(":")[-1]
    queue_url = config.sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    receipt_handle = e["receiptHandle"]
    return config.sqs.delete_message(
        QueueUrl=queue_url, ReceiptHandle=receipt_handle
    )


def get_data_paths(sqs_event):
    """Process SQS events and return a list of paths to Tile Parquet in S3."""
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


def get_pass_res(dpaths: list[str], output_path: str):
    """Create SNS success message information on impacted AOIS. If the message
    is too large, recursively split the AOI impact list until it fits."""

    res = {
        "MessageAttributes": {
            "source_files": {
                "DataType": "String",
                "StringValue": json.dumps(dpaths),
            },
            "s3_output_path": {
                "DataType": "String",
                "StringValue": output_path,
            },
            "status": {"DataType": "String", "StringValue": "succeeded"},
        },
        "Message": "succeeded",
    }
    return res


def get_fail_res(dpaths: list[str], err_str: str):
    """Create SNS failed message with error information and source files."""
    res = {
        "MessageAttributes": {
            "source_files": {
                "DataType": "String",
                "StringValue": json.dumps(dpaths),
            },
            "status": {"DataType": "String", "StringValue": "failed"},
            "error": {"DataType": "String", "StringValue": err_str},
        },
        "Message": "failed",
    }
    return res


def apply_compare(datapaths: list[str], config, outpath):
    """Perform DuckDB Intersect on Tiles and Subscriptions and return DuckDB
    Relation object."""
    sql = f"""
        COPY (
            SELECT aois.pk_and_model AS aois, list(tiles.pk_and_model) AS tiles
            FROM read_parquet('{config.aois_path}') AS aois
            JOIN read_parquet({datapaths}) AS tiles
            ON ST_Intersects(aois.geometry, tiles.geometry)
            GROUP BY aois.pk_and_model
        )
        TO '{outpath}'
        (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100_000)
    """
    config.con.execute(sql)
    return get_pass_res(datapaths, outpath)


def get_env_vars(var_name: str):
    """Handle fetching requirend environment variables and crafting error
    messages if they're missing."""
    env_keys = os.environ.keys()

    if var_name in env_keys:
        val = os.environ[var_name]
        return val
    else:
        raise ValueError(
            f"Required variable {var_name} missing from environment."
        )


def handler(event: dict[str, str], context):
    """Base Lambda handler method which coordinates SQS message processing and
    SNS responses in case of errors."""
    sns_out = get_env_vars("SNS_OUT_ARN")
    region = get_env_vars("AWS_REGION")

    config = None
    try:
        bucket = get_env_vars("S3_BUCKET")
        prefix = get_env_vars("DEPLOY_PREFIX")
        mem_limit = get_env_vars("MEMORY_LIMIT")
        mem_limit = int(mem_limit)
        config = CloudConfig(region, sns_out, bucket, prefix, mem_limit)
    except Exception as e:
        sns = boto3.client("sns", region_name=region)
        fail_tb = traceback.format_exc()
        fail_msg = get_fail_res([], fail_tb)
        sns.publish(TopicArn=sns_out, **fail_msg)
        raise e

    data_paths = []
    try:
        with config:
            print("Event:", json.dumps(event))
            events = event["Records"]

            # collect data paths, delete messages after processing
            for sqs_event in events:
                data_paths = data_paths + get_data_paths(sqs_event)
            if not data_paths:
                raise ValueError("At least one tile GeoParquet path is required.")

            # process data paths together
            name = uuid4()
            base_s3_path = f"{config.bucket}/{config.prefix}/intersects/{name}.parquet"
            full_s3_path = f"s3://{base_s3_path}"
            sns_message = apply_compare(data_paths, config, full_s3_path)
            config.sns.publish(TopicArn=config.sns_out_arn, **sns_message)

            # delete sqs messages now that we're done
            for sqs_event in events:
                delete_sqs_message(sqs_event, config)

            # return as list to conform with possibility of needing to split
            return [ sns_message ]
    except duckdb.OutOfMemoryException as e:
        events = event["Records"]
        # if it can't be split further, raise memory error
        if len(events) == 1:
            raise e

        # split into single event runs to see if that alleviates issues
        try:
            split_events = [{"Records": [e]} for e in events]
            sns_messages = [ handler(re, None)[0] for re in split_events]
            return sns_messages
        except Exception as e:
            exc_str = traceback.format_exc()
            fail_msg = get_fail_res(data_paths, exc_str)
            config.sns.publish(TopicArn=config.sns_out_arn, **fail_msg)
            raise e

    except Exception as e:
        exc_str = traceback.format_exc()
        fail_msg = get_fail_res(data_paths, exc_str)
        config.sns.publish(TopicArn=config.sns_out_arn, **fail_msg)
        raise e
