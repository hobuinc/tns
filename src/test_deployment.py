import json
import boto3
import polars_st as st
from time import sleep
from math import ceil
from shapely import from_geojson

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
    bucket_name = tf_output["s3_bucket_name"]
    key = f"{action}/geom.parquet"

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
    while msg_count < count:
        messages = sqs_listen(comp_sqs_out, region, retries=0)
        for msg in messages:
            body = json.loads(msg["Body"])

            attrs = body["MessageAttributes"]

            status = attrs["status"]["Value"]
            assert status == "succeeded"

            assert attrs["aoi_id"]["Value"]
            assert attrs["tiles"]["Value"]

            delete_sqs_message(msg, comp_sqs_out, region)
            msg_count += 1

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
        aois = json.loads(message["MessageAttributes"]["aois"]["Value"])
        assert len(aois) == 1
        assert aois[0] == aoi_name

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

    og_items = get_entries_by_aoi(pk_and_model, config)
    assert og_items["Count"] == 3
    for i in og_items["Items"]:
        assert i["pk_and_model"]["S"] == f"{pk_and_model}"
        assert i["h3_id"]["S"] in h3_indices

    # sns_publish(sns_in, region, pk_and_model)
    geom_delete = from_geojson(geom)
    put_polygon("delete", tf_output, geom_delete, pk_and_model)
    messages = sqs_listen(sqs_out, region)
    for msg in messages:
        body = json.loads(msg["Body"])
        message_str = body["Message"]
        assert message_str == f"AOI: {pk_and_model} deleted"

    deleted_items = get_entries_by_aoi(pk_and_model, config)
    assert deleted_items["Count"] == 0
    assert len(deleted_items["Items"]) == 0

    # should be no messages left in the input queue
    sleep(30)  # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert "Messages" not in messages


{
    "Records": [
        {
            "messageId": "4f277613-65e6-46f8-ac54-47ffdd536c83",
            "receiptHandle": "AQEBzyhXncUKFOXtidFve3kJF1t+GrFPO3H0YBh2c35mRB96qXrWYiweLjgvmXkgspCOcIk5UT0oJDc4zXPZdduJVECHmznOy0f6jxGLlFiNYizVmxlF+NdWhCV3LLiAzvk3MGtylky1gQI7KMsML0tQ5mWOxIB9zxzEtI8FQ+7ex23DjYCk28HILw8vjkDZqG0w81bVBWxc5ocRhHUob7JkH6FB3XXuxVpWJMtL0kb3egGZ5Gw64jL0aVLSc4D0Say3npRAFr5uEWdL/zcalgH7/zAgN3DJjqRgB3Js8I/U3PXhwGZBjZ2tNVlHvltV/3R/QnfGBsU9oLzSGfWrfoR7W6sN2bWtJ2pMeW37s4PP4zY/57Qt7XrOWJNUbVc9qoHqTkxkC3Lu61UNq6+3VIAwbQ==",
            "body": '{\n  "Type" : "Notification",\n  "MessageId" : "dbd2d9f0-855f-5301-975e-b803afb569cc",\n  "TopicArn" : "arn:aws:sns:us-west-2:068489536557:tns_compare_sns_in",\n  "Subject" : "Amazon S3 Notification",\n  "Message" : "{\\"Records\\":[{\\"eventVersion\\":\\"2.1\\",\\"eventSource\\":\\"aws:s3\\",\\"awsRegion\\":\\"us-west-2\\",\\"eventTime\\":\\"2026-01-27T21:05:45.567Z\\",\\"eventName\\":\\"ObjectCreated:Put\\",\\"userIdentity\\":{\\"principalId\\":\\"AWS:AIDAI4BQDXNYYEOF4QKQA\\"},\\"requestParameters\\":{\\"sourceIPAddress\\":\\"97.127.3.202\\"},\\"responseElements\\":{\\"x-amz-request-id\\":\\"0YS52CXSN9CY9W9X\\",\\"x-amz-id-2\\":\\"ectJ8evaALKBbNaOiuTABbFQS3Y0UppTpKfePPqcLivpHoyS9I/qivVMwxZKhqatwsEgSycvIlnNTofIpB6lMj37qAcn5wQ9\\"},\\"s3\\":{\\"s3SchemaVersion\\":\\"1.0\\",\\"configurationId\\":\\"tf-s3-topic-20260127195844340200000002\\",\\"bucket\\":{\\"name\\":\\"tns-bucket-premade\\",\\"ownerIdentity\\":{\\"principalId\\":\\"A1IA91PUEBL420\\"},\\"arn\\":\\"arn:aws:s3:::tns-bucket-premade\\"},\\"object\\":{\\"key\\":\\"compare/geom.parquet\\",\\"size\\":5564,\\"eTag\\":\\"8914372f98da5982c51f2a6a322a2829\\",\\"sequencer\\":\\"00697928A97C756D4E\\"}}}]}",\n  "Timestamp" : "2026-01-27T21:05:46.462Z",\n  "SignatureVersion" : "1",\n  "Signature" : "KgPpDq31F/Jz+aBATtx1Hs3OM96S1aDPK+eFA7pkhxeawCDys+Wxs48M5SDc8aEBvZlvw+mGh7Uvf/XhDKrUr+prxBivgSSl8rd0HqKU69hbYDeuwIZaIAkm42EN66Bv2kv0lpTh+6qdU74WsdLmDw3uwieb5xO7gG+857xnXw5Yf67FDSieOGytLvlikJLDId+MR+ZqVXfzaSrNn2ojeK4SEjePkeksRPMbz8gWuNWAl0b/h63Eyp4DBwspwaG4rQItqi2678qk0UjmIgZoyiSvcDGyd+zT32y6byJ+KrQoaDcH8Te39wRefORjafBYHJfUMBIgBLBwIk5GZP/VJg==",\n  "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-7506a1e35b36ef5a444dd1a8e7cc3ed8.pem",\n  "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:068489536557:tns_compare_sns_in:07185845-0363-4b58-87c4-1df3fa34c0ea"\n}',
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1769547946503",
                "SenderId": "AIDAIYLAVTDLUXBIEIX46",
                "ApproximateFirstReceiveTimestamp": "1769547946514",
            },
            "messageAttributes": {},
            "md5OfBody": "5b2676042985307070e3879ec7a00469",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-west-2:068489536557:tns_compare_sqs_input",
            "awsRegion": "us-west-2",
        }
    ]
}
