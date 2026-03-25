import os
import boto3
import json
import shutil
from uuid import uuid4
import time
import pytest

from db_lambda import handler, EXT_PATH, DDB_PATH, get_pass_res


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
    return messages


def test_big(tf_output, big_event, big_aoi_fill):
    os.environ["AWS_REGION"] = tf_output["aws_region"]
    os.environ["SNS_OUT_ARN"] = tf_output["sns_out"]

    clear_sqs(tf_output["sqs_in"], tf_output["aws_region"])

    shutil.rmtree(EXT_PATH)
    os.remove(DDB_PATH)

    time1 = time.time()
    aois = handler(big_event, None)
    res_time = time.time() - time1
    assert res_time < 500
    assert len(aois)
    for aoi_res in aois:
        attrs = aoi_res["MessageAttributes"]

        status = attrs["status"]["StringValue"]
        assert status == "succeeded", json.dumps(attrs["error"])

    clear_sqs(tf_output["sqs_in"], tf_output["aws_region"])

    os.remove(DDB_PATH)
    shutil.rmtree(EXT_PATH)


def test_handler(tf_output, event, aoi_fill, config):
    os.environ["AWS_REGION"] = tf_output["aws_region"]
    os.environ["SNS_OUT_ARN"] = tf_output["sns_out"]

    clear_sqs(tf_output["sqs_in"], tf_output["aws_region"])

    aoi_res = handler(event, None)
    assert len(aoi_res) == 1

    aoi_res = aoi_res[0]
    attrs = aoi_res["MessageAttributes"]
    if "error" in attrs.keys():
        print(f"Error in messages: {attrs['error']['StringValue']}")

    aois = json.loads(attrs["aoi_list"]["StringValue"])
    assert len(aois) == 50
    source_files = json.loads(attrs["source_files"]["StringValue"])
    assert len(source_files) == 1
    assert source_files[0] == "s3://tns-bucket-premade/compare/geom.parquet"
    s3_path = attrs["s3_output_path"]["StringValue"]

    s3_info = config.con.sql(f"select aois from read_parquet('{s3_path}')")
    s3_aois = s3_info.pl().get_column("aois").to_list()
    assert len(aois) == len(s3_aois)
    assert set(s3_aois) == set(aois)

    clear_sqs(tf_output["sqs_in"], tf_output["aws_region"])


def test_pass_res():
    paths = ["s3://tns-sample-bucket/tns-sample-path/key.parquet"]

    # basic
    pass_list = ["0123456789" for n in range(15000)]
    res_passes = get_pass_res(uuid4(), paths, pass_list, paths[0])
    assert len(res_passes) == 1

    # splitting
    split_list = ["0123456789" for n in range(20000)]
    res_splits = get_pass_res(uuid4(), paths, split_list, paths[0])
    assert len(res_splits) == 2

    # test that large values don't return nested results
    big_list = ["0123456789" for n in range(10**6)]
    big_splits = get_pass_res(uuid4(), paths, big_list, paths[0])
    types = [not isinstance(n, list) for n in big_splits]
    assert all(types)

def test_failures(sqs_out: str, region: str):
    def get_attrs(msg):
        body = json.loads(msg[0]['Body'])
        return body['MessageAttributes']

    # test bad event creation error catching
    fake_event = {'Records': ['asdf']}
    with pytest.raises(Exception) as e1:
        handler(fake_event, None)
    assert "string indices must be integers" in str(e1)
    msg1 = clear_sqs(sqs_out, region)
    a1 = get_attrs(msg1)
    assert a1['status']['Value'] == 'failed'
    assert 'string indices must be integers' in a1['error']['Value']

    # test cloudconfig failure
    s3_bucket = os.environ.pop('S3_BUCKET')
    with pytest.raises(Exception) as e2:
        handler(fake_event, None)
    os.environ['S3_BUCKET'] = s3_bucket
    assert "KeyError('S3_BUCKET')" in str(e2)
    msg2 = clear_sqs(sqs_out, region)
    a2 = get_attrs(msg2)
    assert a2['status']['Value'] == 'failed'
    assert 'KeyError: \'S3_BUCKET\'' in a2['error']['Value']


    clear_sqs(sqs_out, region)