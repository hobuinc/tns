import subprocess
import os
import pytest
import json
import boto3
from pathlib import Path

from db_lambda import db_add_handler, comp_handler, delete_if_found, get_entries_by_aoi

def test_comp(tf_output, comp_event, aoi, db_fill):
    os.environ['AWS_REGION'] = tf_output['aws_region']
    os.environ['SNS_OUT_ARN'] = tf_output['comp_sns_out']
    os.environ['DB_TABLE_NAME'] = tf_output['table_name']

    aois = comp_handler(comp_event, None)
    assert len(aois) == 1

def test_add(tf_output, add_event, comp_event, dynamo, aoi, h3_indices):
    os.environ['AWS_REGION'] = tf_output['aws_region']
    os.environ['SNS_OUT_ARN'] = tf_output['comp_sns_out']
    os.environ['DB_TABLE_NAME'] = tf_output['table_name']
    table_name = tf_output['table_name']

    dynamo_res = db_add_handler(add_event, None)
    assert dynamo_res['ResponseMetadata']['HTTPStatusCode'] == 200

    added_items = get_entries_by_aoi(dynamo, table_name, aoi)
    assert added_items['Count'] == 3
    for i in added_items['Items']:
        assert i['aoi_id']['N'] == f'{aoi}'
        assert i['h3_idx']['S'] in h3_indices

def test_update(tf_output, db_fill, update_event, comp_event, aoi, updated_h3_indices):
    os.environ['AWS_REGION'] = tf_output['aws_region']
    os.environ['SNS_OUT_ARN'] = tf_output['comp_sns_out']
    os.environ['DB_TABLE_NAME'] = tf_output['table_name']
    table_name = tf_output['table_name']

    db_name = tf_output['table_name']
    aws_region = tf_output['aws_region']
    dynamo = boto3.client('dynamodb', region_name=aws_region)

    og_items = get_entries_by_aoi(dynamo, table_name, aoi)

    # update
    dynamo_res = db_add_handler(update_event, None)
    assert dynamo_res['ResponseMetadata']['HTTPStatusCode'] == 200

    updated_items = get_entries_by_aoi(dynamo, table_name, aoi)
    assert updated_items['Count'] == 2
    for i in updated_items['Items']:
        assert i['aoi_id']['N'] == f'{aoi}'
        assert i['h3_idx']['S'] in updated_h3_indices