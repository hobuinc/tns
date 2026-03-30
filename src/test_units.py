import json
from uuid import uuid4
from pathlib import Path
import polars_st as st

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
    config = CloudConfig(region, sns_out_arn, bucket)
    config.aois_path = small_aois_path.as_posix()

    # make comparison and confirm
    datapaths = [small_tiles_path.as_posix()]
    intersects = apply_compare(datapaths, config)
    int_pl = intersects.pl().get_column("aois").to_list()
    assert len(int_pl) == 50

    local_gdf = (
        st.read_file(small_aois_path).get_column("pk_and_model").to_list()
    )

    assert set(int_pl) == set(local_gdf)


def test_fail_res(bucket_name: str):
    """Test that failure responses are returned in expected structures."""
    name = uuid4()
    paths = [f"s3://{bucket_name}/tns-sample-path/key.parquet"]
    err_str = "TypeError('You passed in the wrong type, fix that.')"

    msg = get_fail_res(name=name, dpaths=paths, err_str=err_str)
    assert "MessageAttributes" in msg

    attrs = msg["MessageAttributes"]
    assert "source_files" in attrs.keys()
    assert "status" in attrs.keys()
    assert "error" in attrs.keys()

    assert attrs["status"]["StringValue"] == "failed"
    assert attrs["error"]["StringValue"] == err_str
    assert json.loads(attrs["source_files"]["StringValue"]) == paths


def test_pass_res(bucket_name: str):
    """
    Test that passing responses are returned in expected structures, and that
    when responses are too large from lists of AOIs those responses are split.
    """
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
    """Test Cloud/DuckDB coordination client work correctly."""
    # set environment variables, which config will pull from
    # then test that cloud config correctly pulls from those
    region = "us-east-1"
    sns_arn = "fake-arn::asdf"
    bucket = "fake-bucket"

    config = CloudConfig(region, sns_arn, bucket)
    assert config.region == region
    assert config.sns_out_arn == sns_arn
    assert config.bucket == bucket
    assert config.aois_path == f"s3://{bucket}/subs/subscriptions.parquet"
