import os
import boto3

from db_lambda import db_add_handler, db_comp_handler, db_delete_handler
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


def test_comp(tf_output, comp_event, pk_and_model, db_fill):
    os.environ["AWS_REGION"] = tf_output["aws_region"]
    os.environ["SNS_OUT_ARN"] = tf_output["db_compare_sns_out"]
    os.environ["DB_TABLE_NAME"] = tf_output["table_name"]
    print("sns")
    print(tf_output["db_compare_sns_out"])

    aois = db_comp_handler(comp_event, None)
    assert aois == 1000
    clear_sqs(tf_output["db_compare_sqs_out"], tf_output["aws_region"])
    clear_sqs(tf_output["db_compare_sqs_in"], tf_output["aws_region"])


def test_add(tf_output, add_event, pk_and_model, h3_indices, config):
    db_add_handler(add_event, None)

    added_items = get_entries_by_aoi(pk_and_model, config)
    assert added_items["Count"] == 3
    for i in added_items["Items"]:
        assert i["pk_and_model"]["S"] == pk_and_model
        assert i["h3_id"]["S"] in h3_indices
    clear_sqs(tf_output["db_add_sqs_out"], tf_output["aws_region"])
    clear_sqs(tf_output["db_add_sqs_in"], tf_output["aws_region"])


def test_update(
    tf_output,
    db_fill,
    update_event,
    pk_and_model,
    updated_h3_indices,
    h3_indices,
    config,
):
    og_items = get_entries_by_aoi(pk_and_model, config)
    for i in og_items["Items"]:
        assert i["pk_and_model"]["S"] == pk_and_model
        assert i["h3_id"]["S"] in h3_indices

    # update
    db_add_handler(update_event, None)

    updated_items = get_entries_by_aoi(pk_and_model, config)
    assert updated_items["Count"] == 2
    for i in updated_items["Items"]:
        assert i["pk_and_model"]["S"] == pk_and_model
        assert i["h3_id"]["S"] in updated_h3_indices
    clear_sqs(tf_output["db_add_sqs_out"], tf_output["aws_region"])
    clear_sqs(tf_output["db_add_sqs_in"], tf_output["aws_region"])


def test_delete(tf_output, db_fill, delete_event, pk_and_model, h3_indices, config):
    og_items = get_entries_by_aoi( pk_and_model, config)
    assert og_items["Count"] == 3
    for i in og_items["Items"]:
        assert i["pk_and_model"]["S"] == pk_and_model
        assert i["h3_id"]["S"] in h3_indices

    db_delete_handler(delete_event, None)

    deleted_items = get_entries_by_aoi(pk_and_model, config)
    assert deleted_items["Count"] == 0
    assert len(deleted_items["Items"]) == 0
    clear_sqs(tf_output["db_delete_sqs_out"], tf_output["aws_region"])
    clear_sqs(tf_output["db_delete_sqs_in"], tf_output["aws_region"])
