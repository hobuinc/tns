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
import shutil
import glob
import time

from uuid import uuid4

MAX_MSG_BYTES = 2**10 * 256  # 256KB

# Cache duckdb connections across warm-starts
GLOBAL_CONFIG = None
CERT_DEST = "/tmp/cert.pem"


def clean_stale_temp_dirs():
    """
    If a previous container execution suffered a hard crash or a 15-minute Lambda timeout,
    the `__exit__` block wouldn't have run, leaving orphaned files in /tmp.
    This safely sweeps files older than 1 hour when a new container spins up.
    1 hour leaves enough time for 3 retries.
    """
    one_hour_ago = time.time() - 3600
    for leftover in glob.glob("/tmp/duckdb_*"):
        try:
            if os.path.getmtime(leftover) < one_hour_ago:
                if os.path.isdir(leftover):
                    shutil.rmtree(leftover, ignore_errors=True)
                    print(f"Swept orphaned temp directory: {leftover}")
        except Exception:
            # Silently ignore if file is locked or rapidly changed by the OS
            pass

class CloudConfig:
    """Coordinate AWS and DuckDB connections and associated information."""

    def __init__(
        self, region, sns_out_arn, bucket, prefix, mem_limit, cert_path=None,
        s3_endpoint=None
    ):
        self.region = region
        self.sns_out_arn = sns_out_arn
        self.bucket = bucket
        self.prefix = prefix
        sub_key = f"{self.prefix}/subs/subscriptions.parquet"

        self.aois_path = f"s3://{self.bucket}/{sub_key}"
        self.cert_path = cert_path
        self.s3_endpoint = s3_endpoint
        self.cert_dest = None


        # if CA file exists, grab it from S3 and write it to the temp directory
        # then set necessary environment variables and remake the aws clients
        # with it
        if self.cert_path is not None:
            os.environ['REQUESTS_CA_BUNDLE'] = CERT_DEST
            os.environ['AWS_CA_BUNDLE'] = CERT_DEST
            self.cert_dest = CERT_DEST

            # bypass ssl cert checking until we get it copied in
            self.s3 = boto3.client("s3", region_name=self.region, verify=False)
            response = self.s3.get_object(Bucket=self.bucket, Key=self.cert_path)
            cert_content = response["Body"].read()
            with open(self.cert_dest, "wb") as f:
                f.write(cert_content)
            print(
                f"Cert copied from s3://{self.bucket}/{self.cert_path} to "
                f"{self.cert_dest}"
            )
            self.sns = boto3.client(
                "sns", region_name=self.region, verify=self.cert_dest
            )
            self.sqs = boto3.client(
                "sqs", region_name=self.region, verify=self.cert_dest
            )
            # use ssl cert
            self.s3 = boto3.client("s3", region_name=self.region, verify=self.cert_dest)
            self.using_certs = True
        else:
            self.sns = boto3.client("sns", region_name=self.region)
            self.sqs = boto3.client("sqs", region_name=self.region)
            self.s3 = boto3.client("s3", region_name=self.region)
            self.using_certs = False

        # mem limit passed in as value of MB (2**20), GB is (2**30), div by
        # 2**10 for value of GB here
        shorter = mem_limit / (2**10)
        self.mem_limit = f"{shorter}GB"
        self.con = None

        self.con = duckdb.connect()
        self.con.execute("LOAD httpfs")
        self.con.execute("LOAD spatial")
        self.con.execute("LOAD aws")
        self.con.execute(f"SET memory_limit='{self.mem_limit}'")

        # Prevents runaway spatial queries from filling up the entire Lambda /tmp space
        # and crashing the container with a fatal OS Error 28
        self.con.execute("SET max_temp_directory_size='512MB'")

        if self.cert_dest is not None and os.path.exists(self.cert_dest):
            self.con.execute(f"SET ca_cert_file='{self.cert_dest}'")

        if self.s3_endpoint is not None:
            ex_str = f"""
                CREATE SECRET (
                    TYPE S3,
                    REGION '{self.region}',
                    ENDPOINT '{self.s3_endpoint}',
                    PROVIDER CREDENTIAL_CHAIN)
            """
        else:
            ex_str = f"""
                CREATE SECRET (
                    TYPE S3,
                    REGION '{self.region}',
                    PROVIDER CREDENTIAL_CHAIN)
            """
        self.con.execute(ex_str)

    def __enter__(self):
        # Dynamically generate an isolated, static path for this specific execution.
        self.run_id = str(uuid4())
        self.active_tempdir = f"/tmp/duckdb_{self.run_id}"
        os.makedirs(self.active_tempdir, exist_ok=True)
        # Point the warm connection to this isolated folder just before running the query
        self.con.execute(f"SET temp_directory='{self.active_tempdir}'")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Clean up the temp directory after the query finishes to prevent the container
        from running out of disk space during high-throughput warm starts.
        """
        if hasattr(self, 'active_tempdir') and os.path.exists(self.active_tempdir):
            shutil.rmtree(self.active_tempdir, ignore_errors=True)


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
            ON (
                aois.geometry_bbox.xmin <= tiles.geometry_bbox.xmax AND
                aois.geometry_bbox.xmax >= tiles.geometry_bbox.xmin AND
                aois.geometry_bbox.ymin <= tiles.geometry_bbox.ymax AND
                aois.geometry_bbox.ymax >= tiles.geometry_bbox.ymin
            )
            AND ST_Intersects(aois.geometry, tiles.geometry)
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
    val = os.environ.get(var_name)

    if val is not None and val != "":
        print(f"Fetching environment variable: {var_name}.")
        print(f"Value: {val}.")
        return val
    elif var_name == "S3_CERT_PATH" or var_name == "AWS_S3_ENDPOINT":
        return None
    else:
        raise ValueError(
            f"Required variable {var_name} missing from environment."
        )


def handler(event: dict[str, str], context):
    """Base Lambda handler method which coordinates SQS message processing and
    SNS responses in case of errors."""

    global GLOBAL_CONFIG

    sns_out = get_env_vars("SNS_OUT_ARN")
    region = get_env_vars("AWS_REGION")

    config = None
    try:
        # If this is a cold start (first run of the container), initialize config
        if GLOBAL_CONFIG is None:
            clean_stale_temp_dirs() # Run the defensive cleanup once on boot
            bucket = get_env_vars("S3_BUCKET")
            prefix = get_env_vars("DEPLOY_PREFIX")
            mem_limit = get_env_vars("MEMORY_LIMIT")
            s3_endpoint = get_env_vars("AWS_S3_ENDPOINT")

            # on sc/tc, we need a custom certicate to make aws service calls
            cert_path = get_env_vars("S3_CERT_PATH")
            mem_limit = int(mem_limit)
            GLOBAL_CONFIG = CloudConfig(
                region, sns_out, bucket, prefix, mem_limit, cert_path, s3_endpoint
            )
        config = GLOBAL_CONFIG
    except Exception as e:
        # this section won't work in sc/tc because sns won't have
        # the cert allowing them to connect yet
        print(f"ERROR: Lambda processing failed. Error: {e}")
        print(traceback.format_exc())
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
                print("No GeoParquet files found in events."
                      "If the lambda was started by an 's3:TestEvent' then "
                      "this is expected.")

            # process data paths together
            name = uuid4()
            base_s3_path = (
                f"{config.bucket}/{config.prefix}/intersects/{name}.parquet"
            )
            full_s3_path = f"s3://{base_s3_path}"
            sns_message = apply_compare(data_paths, config, full_s3_path)
            config.sns.publish(TopicArn=config.sns_out_arn, **sns_message)

            # delete sqs messages now that we're done
            for sqs_event in events:
                delete_sqs_message(sqs_event, config)

            # return as list to conform with possibility of needing to split
            return [sns_message]
    except duckdb.OutOfMemoryException as e:
        events = event["Records"]
        # if it can't be split further, raise memory error
        if len(events) == 1:
            raise e

        # split into single event runs to see if that alleviates issues
        try:
            split_events = [{"Records": [e]} for e in events]
            sns_messages = [handler(re, context)[0] for re in split_events]
            return sns_messages
        except Exception as e:
            exc_str = traceback.format_exc()
            print(f"ERROR: Lambda processing failed. Error: {exc_str}")
            fail_msg = get_fail_res(data_paths, exc_str)
            config.sns.publish(TopicArn=config.sns_out_arn, **fail_msg)
            raise e

    except Exception as e:
        exc_str = traceback.format_exc()
        print(f"ERROR: Lambda processing failed. Error: {exc_str}")
        fail_msg = get_fail_res(data_paths, exc_str)
        config.sns.publish(TopicArn=config.sns_out_arn, **fail_msg)
        raise e
