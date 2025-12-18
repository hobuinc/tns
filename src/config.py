import os
import boto3


class CloudConfig():
    def __init__(self, sns_out_arn:str=None, dynamo_cfg:dict=None):
        env_keys = os.environ.keys()
        if 'SNS_OUT_ARN' in env_keys:
            self.sns_out_arn = os.environ['SNS_OUT_ARN']
        else:
            self.sns_out_arn = None

        if "DB_TABLE_NAME" in env_keys:
            self.table_name = os.environ["DB_TABLE_NAME"]
        else:
            self.table_name = None

        if "AWS_REGION" in env_keys:
            self.region = os.environ["AWS_REGION"]
        else:
            self.region = "us-west-2"

        if dynamo_cfg is not None:
            self.dynamo = boto3.client("dynamodb", region_name=self.region, config=dynamo_cfg)
        else:
            self.dynamo = boto3.client("dynamodb", region_name=self.region)

        self.sns = boto3.client("sns", region_name=self.region)
        self.s3 = boto3.client("s3", region_name=self.region)
        self.sqs = boto3.client("sqs", region_name=self.region)
