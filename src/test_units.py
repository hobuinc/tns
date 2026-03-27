import json
from uuid import uuid4

from intersects_lambda import CloudConfig, get_pass_res, get_fail_res


def test_fail_res(bucket_name: str):
    name = uuid4()
    paths = [f"s3://{bucket_name}/tns-sample-path/key.parquet"]
    err_str = "TypeError('You passed in the wrong type, fix that.')"

    msg = get_fail_res(name=name, dpaths=paths, err_str=err_str)
    assert "MessageAttributes" in msg

    attrs = msg['MessageAttributes']
    assert "source_files" in attrs.keys()
    assert "status" in attrs.keys()
    assert "error" in attrs.keys()

    assert attrs["status"]["StringValue"] == "failed"
    assert attrs["error"]["StringValue"] == err_str
    assert json.loads(attrs["source_files"]["StringValue"]) == paths


def test_pass_res(bucket_name: str):
    paths = [f"s3://{bucket_name}/tns-sample-path/key.parquet"]

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

def test_config():
    # set environment variables, which config will pull from
    # then test that cloud config correctly pulls from those
    region = 'us-east-1'
    sns_arn = 'fake-arn::asdf'
    bucket = 'fake-bucket'

    config = CloudConfig(region, sns_arn, bucket)
    assert config.region == region
    assert config.sns_out_arn == sns_arn
    assert config.bucket == bucket
    assert config.aois_path == f"s3://{bucket}/subs/subscriptions.parquet"