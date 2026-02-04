import json
import boto3
import polars_st as st
from time import sleep
from math import ceil
from shapely import from_geojson
from uuid import uuid4

from db_lambda import get_entries_by_aoi


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
            WaitTimeSeconds=10,
        )
        if "Messages" in res.keys():
            messages = res["Messages"]
            for m in messages:
                receipt_handle = m["ReceiptHandle"]
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        else:
            break
    return messages


def put_parquet(action, tf_output, gdf):
    uuid = uuid4()
    bucket_name = tf_output["s3_bucket_name"]
    key = f"{action}/{uuid}.parquet"

    vsis_path = f"/vsis3/{bucket_name}/{key}"
    df_kwargs = {
        "compression": "zstd",
    }
    gdf = gdf.with_columns(st.geom().st.set_srid(4326))
    gdf.st.write_file(path=vsis_path, driver="PARQUET", layer="product", **df_kwargs)


def put_polygon(action, tf_output, polygon, pk_and_model):
    gdf = st.GeoDataFrame(
        data={"pk_and_model": [pk_and_model], "geometry": [polygon.wkb]},
        strict=True,
        infer_schema_length=True,
        geometry_format="wkb",
    )

    put_parquet(action, tf_output, gdf)


def delete_sqs_message(e, sqs_arn, region):
    sqs = boto3.client("sqs", region_name=region)
    queue_name = sqs_arn.split(":")[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    receipt_handle = e["ReceiptHandle"]
    return sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def sns_publish(sns_arn, region, aoi=None, polygon=None):
    sns = boto3.client("sns", region_name=region)
    message_attributes = {}
    if aoi is not None:
        message_attributes["aoi"] = {"DataType": "Number", "StringValue": f"{aoi}"}
    if polygon is not None:
        message_attributes["polygon"] = {
            "DataType": "String",
            "StringValue": f"{polygon}",
        }
    res = sns.publish(
        TopicArn=sns_arn, MessageAttributes=message_attributes, Message=f"{aoi}"
    )
    return res


def sqs_get_messages(sqs_arn, region):
    sqs = boto3.client("sqs", region_name=region)
    queue_name = sqs_arn.split(":")[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    return sqs.receive_message(
        QueueUrl=queue_url,
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=10,
        WaitTimeSeconds=10,
    )


def sqs_listen(sqs_arn, region, retries=5):
    sqs = boto3.client("sqs", region_name=region)
    queue_name = sqs_arn.split(":")[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    messages = []
    retry_count = 0
    while not len(messages):
        res = sqs.receive_message(
            QueueUrl=queue_url,
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
        )
        if "Messages" in res.keys():
            messages = res["Messages"]
            retry_count = 0
        else:
            # set retry to 0 if infinite retries
            if retry_count:
                retry_count += 1
                if retry_count >= retries:
                    raise Exception("Retry count hit.")
    return messages


def test_big(tf_output, pk_and_model, states_tiles, h3_indices, cleanup, states_geoms):
    region = tf_output["aws_region"]

    add_sqs_in = tf_output["db_add_sqs_in"]
    add_sqs_out = tf_output["db_add_sqs_out"]

    comp_sqs_out = tf_output["db_compare_sqs_out"]
    comp_sqs_in = tf_output["db_compare_sqs_in"]

    # clear all of the queues so we don't get any artifacts from previous runs
    clear_sqs(add_sqs_in, region)
    clear_sqs(add_sqs_out, region)
    clear_sqs(comp_sqs_out, region)
    clear_sqs(comp_sqs_in, region)

    put_parquet("add", tf_output, states_geoms)
    cleanup = cleanup + [f"raster_1234_{n}" for n in range(50)]

    tile_count = 10**6
    batch_size = 1000
    count = ceil(tile_count / batch_size)
    for n in range(count):
        put_parquet("compare", tf_output, states_tiles)

    msg_count = 0
    failed = []
    while msg_count < count * 50:
        messages = sqs_listen(comp_sqs_out, region, retries=0)
        for msg in messages:
            body = json.loads(msg["Body"])

            attrs = body["MessageAttributes"]
            status = attrs["status"]["Value"]

            if status == "succeeded":
                assert attrs["aoi_id"]["Value"]
                assert attrs["tiles"]["Value"]
            else:
                failed.append(attrs)

            delete_sqs_message(msg, comp_sqs_out, region)
            msg_count += 1
    assert not failed, print(json.dumps(failed))

    # should be no messages left in the input queue
    sleep(30)  # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(add_sqs_in, region)
    assert "Messages" not in messages


def test_comp(tf_output, pk_and_model, geom, db_fill, cleanup):
    region = tf_output["aws_region"]
    sqs_in = tf_output["db_compare_sqs_in"]
    sqs_out = tf_output["db_compare_sqs_out"]

    clear_sqs(sqs_out, region)
    aoi_name = f"{pk_and_model}_0"
    geom_polygon = from_geojson(geom)
    put_polygon("compare", tf_output, geom_polygon, aoi_name)
    messages = sqs_listen(sqs_out, region)
    cleanup.append(aoi_name)

    for m in messages:
        message = json.loads(m["Body"])
        assert message["MessageAttributes"]["status"]["Value"] == "succeeded", (
            f"Error from SQS {message['MessageAttributes']['error']['Value']}"
        )
        aoi_id = message["MessageAttributes"]["aoi_id"]["Value"]
        tiles = json.loads(message["MessageAttributes"]["tiles"]["Value"])
        assert len(tiles) == 1
        assert tiles[0] == 'raster_1234_0'
        assert aoi_id == aoi_name

    # should be no messages left in the input queue
    sleep(30)  # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert "Messages" not in messages


def test_add(tf_output, pk_and_model, geom, h3_indices, cleanup):
    region = tf_output["aws_region"]
    sqs_in = tf_output["db_add_sqs_in"]
    sqs_out = tf_output["db_add_sqs_out"]

    clear_sqs(sqs_out, region)
    cleanup.append(pk_and_model)
    add_geom = from_geojson(geom)
    put_polygon("add", tf_output, add_geom, pk_and_model)

    messages = sqs_listen(sqs_out, region)
    for msg in messages:
        body = json.loads(msg["Body"])
        message_str = body["Message"]
        assert message_str == f"AOI: {pk_and_model} added"

        attrs = body["MessageAttributes"]

        status = attrs["status"]["Value"]
        assert status == "succeeded"

        aoi = attrs["aoi"]["Value"]
        assert aoi == pk_and_model

        h3s = json.loads(attrs["h3_indices"]["Value"])
        for h in h3s:
            assert h in h3_indices

    # should be no messages left in the input queue
    sleep(30)  # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert "Messages" not in messages


def test_update(
    tf_output,
    db_fill,
    pk_and_model,
    update_geom,
    updated_h3_indices,
    h3_indices,
    cleanup,
    config,
):
    region = tf_output["aws_region"]
    sqs_in = tf_output["db_add_sqs_in"]
    sqs_out = tf_output["db_add_sqs_out"]
    aoi_name = f"{pk_and_model}_0"

    clear_sqs(sqs_out, region)
    cleanup.append(aoi_name)

    og_items = get_entries_by_aoi(aoi_name, config)
    og_h3 = [a["h3_id"]["S"] for a in og_items["Items"]]
    assert len(og_h3) == 3
    for oh in og_h3:
        assert oh in h3_indices

    # update
    geom_polygon = from_geojson(update_geom)
    aoi_name = f"{pk_and_model}_0"
    put_polygon("add", tf_output, geom_polygon, aoi_name)
    messages = sqs_listen(sqs_out, region)
    for msg in messages:
        body = json.loads(msg["Body"])
        message_str = body["Message"]
        assert message_str == f"AOI: {aoi_name} added"

        attrs = body["MessageAttributes"]

        status = attrs["status"]["Value"]
        assert status == "succeeded"

        aoi = attrs["aoi"]["Value"]
        assert aoi == aoi_name

        h3s = json.loads(attrs["h3_indices"]["Value"])
        for h in h3s:
            assert h in updated_h3_indices

    # should be no messages left in the input queue
    sleep(30)  # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert "Messages" not in messages


def test_delete(tf_output, db_fill, geom, pk_and_model, h3_indices, config):
    region = tf_output["aws_region"]
    sqs_in = tf_output["db_delete_sqs_in"]
    sqs_out = tf_output["db_delete_sqs_out"]

    clear_sqs(sqs_out, region)
    aoi_id = f'{pk_and_model}_0'

    og_items = get_entries_by_aoi(aoi_id, config)
    assert og_items["Count"] == 3
    for i in og_items["Items"]:
        assert i["pk_and_model"]["S"] == aoi_id
        assert i["h3_id"]["S"] in h3_indices

    geom_delete = from_geojson(geom)
    put_polygon("delete", tf_output, geom_delete, aoi_id)
    messages = sqs_listen(sqs_out, region)
    for msg in messages:
        body = json.loads(msg["Body"])
        message_str = body["Message"]
        assert message_str == f"AOI: {aoi_id} deleted"

    deleted_items = get_entries_by_aoi(aoi_id, config)
    assert deleted_items["Count"] == 0
    assert len(deleted_items["Items"]) == 0

    # should be no messages left in the input queue
    sleep(30)  # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert "Messages" not in messages
