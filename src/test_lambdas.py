import subprocess
import os
import pytest
import json
import boto3
from pathlib import Path

from db_lambda import db_add_handler, db_comp_handler, db_delete_handler
from db_lambda import get_entries_by_aoi, delete_if_found

def test_comp(tf_output, comp_event, aoi, db_fill):
    os.environ['AWS_REGION'] = tf_output['aws_region']
    os.environ['SNS_OUT_ARN'] = tf_output['db_comp_sns_out']
    os.environ['DB_TABLE_NAME'] = tf_output['table_name']

    aois = db_comp_handler(comp_event, None)
    assert len(aois) == 1

def test_add(tf_output, add_event, dynamo, aoi, h3_indices):
    os.environ['AWS_REGION'] = tf_output['aws_region']
    os.environ['SNS_OUT_ARN'] = tf_output['db_add_sns_out']
    os.environ['DB_TABLE_NAME'] = tf_output['table_name']
    table_name = tf_output['table_name']

    db_add_handler(add_event, None)

    added_items = get_entries_by_aoi(dynamo, table_name, aoi)
    assert added_items['Count'] == 3
    for i in added_items['Items']:
        assert i['aoi_id']['N'] == f'{aoi}'
        assert i['h3_idx']['S'] in h3_indices

def test_update(tf_output, db_fill, update_event, aoi, updated_h3_indices):
    os.environ['AWS_REGION'] = tf_output['aws_region']
    os.environ['SNS_OUT_ARN'] = tf_output['db_add_sns_out']
    os.environ['DB_TABLE_NAME'] = tf_output['table_name']
    table_name = tf_output['table_name']

    aws_region = tf_output['aws_region']
    dynamo = boto3.client('dynamodb', region_name=aws_region)

    og_items = get_entries_by_aoi(dynamo, table_name, aoi)

    # update
    db_add_handler(update_event, None)

    updated_items = get_entries_by_aoi(dynamo, table_name, aoi)
    assert updated_items['Count'] == 2
    for i in updated_items['Items']:
        assert i['aoi_id']['N'] == f'{aoi}'
        assert i['h3_idx']['S'] in updated_h3_indices

def test_delete(tf_output, db_fill, delete_event, aoi, h3_indices):
    os.environ['AWS_REGION'] = tf_output['aws_region']
    os.environ['SNS_OUT_ARN'] = tf_output['db_delete_sns_out']
    os.environ['DB_TABLE_NAME'] = tf_output['table_name']
    table_name = tf_output['table_name']

    aws_region = tf_output['aws_region']
    dynamo = boto3.client('dynamodb', region_name=aws_region)

    og_items = get_entries_by_aoi(dynamo, table_name, aoi)
    assert og_items['Count'] == 3
    for i in og_items['Items']:
        assert i['aoi_id']['N'] == f'{aoi}'
        assert i['h3_idx']['S'] in h3_indices

    db_delete_handler(delete_event, None)

    deleted_items = get_entries_by_aoi(dynamo, table_name, aoi)
    assert deleted_items['Count'] == 0
    assert len(deleted_items['Items']) == 0