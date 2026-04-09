import json
from uuid import uuid4
from pathlib import Path
import polars_st as st
from tempfile import NamedTemporaryFile

from intersects_lambda import (
    CloudConfig,
    get_pass_res,
    get_fail_res,
    apply_compare,
)


def test_compare(small_tiles_path: Path, small_aois_path: Path):
    """Test that apply_compare returns the correct intersections."""

    # make config with fake values and adjust aois_path to local file
    region = "us-west-2"
    sns_out_arn = "fake-sns-arn"
    bucket = "tns-fake-bucket"
    prefix = "fake"
    mem_limit = 5*(2**10)
    config = CloudConfig(region, sns_out_arn, bucket, prefix, mem_limit)
    config.aois_path = small_aois_path.as_posix()

    with config:
        with NamedTemporaryFile() as tempfile:

            # make comparison and confirm
            datapaths = [small_tiles_path.as_posix()]
            res = apply_compare(datapaths, config, tempfile.name)
            attrs = res['MessageAttributes']

            source_files = json.loads(attrs['source_files']['StringValue'])
            assert source_files == datapaths

            assert attrs['s3_output_path']['StringValue']
            assert tempfile.name == attrs['s3_output_path']['StringValue']

            int_pl = st.read_file(tempfile.name).get_column("aois").to_list()

            local_gdf = (st.read_file(small_aois_path).get_column("pk_and_model").to_list())

            assert set(int_pl) == set(local_gdf)


def test_fail_res():
    """Test that failure responses are returned in expected structures."""
    paths = ["s3://fake_bucket/tns-sample-path/key.parquet"]
    err_str = "TypeError('You passed in the wrong type, fix that.')"


    msg = get_fail_res(dpaths=paths, err_str=err_str)
    assert "MessageAttributes" in msg

    attrs = msg["MessageAttributes"]
    assert "source_files" in attrs.keys()
    assert "status" in attrs.keys()
    assert "error" in attrs.keys()

    assert attrs["status"]["StringValue"] == "failed"
    assert attrs["error"]["StringValue"] == err_str
    assert json.loads(attrs["source_files"]["StringValue"]) == paths


def test_pass_res():
    """
    Test that passing responses are returned in expected structures, and that
    when responses are too large from lists of AOIs those responses are split.
    """
    paths = ["s3://fake_bucket/tns-sample-path/key.parquet"]

    res = get_pass_res(paths, paths[0])

    assert "Message" in res
    assert res["Message"] == "succeeded"

    assert 'MessageAttributes' in res
    attrs = res['MessageAttributes']
    assert all(x in attrs for x in ["source_files", "s3_output_path", "status"])

    assert "StringValue" in attrs["source_files"]
    assert "DataType" in attrs["source_files"]
    source_files = json.loads(attrs['source_files']['StringValue'])
    assert source_files == paths


    assert "StringValue" in attrs["s3_output_path"]
    assert "DataType" in attrs["s3_output_path"]
    assert paths[0] == attrs['s3_output_path']['StringValue']

    assert "StringValue" in attrs["status"]
    assert "DataType" in attrs["status"]
    assert attrs['status']['StringValue'] == "succeeded"


def test_config():
    """Test Cloud/DuckDB coordination client work correctly."""
    # set environment variables, which config will pull from
    # then test that cloud config correctly pulls from those
    region = "us-east-1"
    sns_arn = "fake-arn::asdf"
    bucket = "fake-bucket"
    prefix = "fake"

    mem_limit = 5*(2**10)
    config = CloudConfig(region, sns_arn, bucket, prefix, mem_limit)
    assert config.region == region
    assert config.sns_out_arn == sns_arn
    assert config.bucket == bucket
    assert config.aois_path == f"s3://{bucket}/{prefix}/subs/subscriptions.parquet"
    assert config.mem_limit == '5.0GB'
    with config:
        a = config.con.sql('select 1')
        assert a.pl().get_column("1").to_list()[0] == 1

    assert config.con is None
