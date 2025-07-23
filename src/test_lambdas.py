import os
import boto3

from db_lambda import db_add_handler, db_comp_handler, db_delete_handler
from db_lambda import get_entries_by_aoi

def test_comp(tf_output, comp_event, pk_and_model, db_fill):
    os.environ['AWS_REGION'] = tf_output['aws_region']
    os.environ['SNS_OUT_ARN'] = tf_output['db_compare_sns_out']
    os.environ['DB_TABLE_NAME'] = tf_output['table_name']

    aois = db_comp_handler(comp_event, None)
    assert len(aois) == 1

def test_add(tf_output, add_event, dynamo, pk_and_model, h3_indices):
    os.environ['AWS_REGION'] = tf_output['aws_region']
    os.environ['SNS_OUT_ARN'] = tf_output['db_add_sns_out']
    os.environ['DB_TABLE_NAME'] = tf_output['table_name']
    table_name = tf_output['table_name']

    db_add_handler(add_event, None)

    added_items = get_entries_by_aoi(dynamo, table_name, pk_and_model)
    assert added_items['Count'] == 3
    for i in added_items['Items']:
        assert i['pk_and_model']['S'] == pk_and_model
        assert i['h3_id']['S'] in h3_indices

def test_update(tf_output, db_fill, update_event, pk_and_model, updated_h3_indices, h3_indices):
    os.environ['AWS_REGION'] = tf_output['aws_region']
    os.environ['SNS_OUT_ARN'] = tf_output['db_add_sns_out']
    os.environ['DB_TABLE_NAME'] = tf_output['table_name']
    table_name = tf_output['table_name']

    aws_region = tf_output['aws_region']
    dynamo = boto3.client('dynamodb', region_name=aws_region)

    og_items = get_entries_by_aoi(dynamo, table_name, pk_and_model)
    for i in og_items['Items']:
        assert i['pk_and_model']['S'] == pk_and_model
        assert i['h3_id']['S'] in h3_indices

    # update
    db_add_handler(update_event, None)

    updated_items = get_entries_by_aoi(dynamo, table_name, pk_and_model)
    assert updated_items['Count'] == 2
    for i in updated_items['Items']:
        assert i['pk_and_model']['S'] == pk_and_model
        assert i['h3_id']['S'] in updated_h3_indices

def test_delete(tf_output, db_fill, delete_event, pk_and_model, h3_indices):
    os.environ['AWS_REGION'] = tf_output['aws_region']
    os.environ['SNS_OUT_ARN'] = tf_output['db_delete_sns_out']
    os.environ['DB_TABLE_NAME'] = tf_output['table_name']
    table_name = tf_output['table_name']

    aws_region = tf_output['aws_region']
    dynamo = boto3.client('dynamodb', region_name=aws_region)

    og_items = get_entries_by_aoi(dynamo, table_name, pk_and_model, )
    assert og_items['Count'] == 3
    for i in og_items['Items']:
        assert i['pk_and_model']['S'] == pk_and_model
        assert i['h3_id']['S'] in h3_indices

    db_delete_handler(delete_event, None)

    deleted_items = get_entries_by_aoi(dynamo, table_name, pk_and_model)
    assert deleted_items['Count'] == 0
    assert len(deleted_items['Items']) == 0