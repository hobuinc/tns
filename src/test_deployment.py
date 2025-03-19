import json
import boto3

def clear_sqs(sqs_arn, region):
    sqs = boto3.client('sqs', region_name=region)
    queue_name = sqs_arn.split(':')[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    messages = []
    while not len(messages):
        res = sqs.receive_message(QueueUrl=queue_url,
                MessageAttributeNames=['All'], MaxNumberOfMessages=10, WaitTimeSeconds=10)
        if 'Messages' in res.keys():
            messages = res['Messages']
            for m in messages:
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    return messages

def sns_publish(sns_arn, region, aoi=None, polygon=None):
    sns = boto3.client('sns', region_name=region)
    message_attributes = {}
    if aoi is not None:
        message_attributes['aoi'] = {'DataType': 'Number', 'StringValue': f'{aoi}'}
    if polygon is not None:
        message_attributes['polygon'] = {'DataType': 'String', 'StringValue': f'{polygon}'}
    res = sns.publish(TopicArn=sns_arn, MessageAttributes=message_attributes, Message=f"{aoi}")
    return res

def sqs_listen(sqs_arn, region):
    sqs = boto3.client('sqs', region_name=region)
    queue_name = sqs_arn.split(':')[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    messages = []
    while not len(messages):
        res = sqs.receive_message(QueueUrl=queue_url,
                MessageAttributeNames=['All'], MaxNumberOfMessages=10, WaitTimeSeconds=10)
        if 'Messages' in res.keys():
            messages = res['Messages']
    return messages

def test_comp(tf_output, geom, db_fill):
    region = tf_output['aws_region']
    sns_in = tf_output['db_comp_sns_in']
    sqs_out = tf_output['db_comp_sqs_out']
    table_name = tf_output['table_name']
    res = sns_publish(sns_in, region, polygon=geom)
    res = sqs_listen(sqs_out, region)
    message = json.loads(res[0]['Body'])
    aoi = message['MessageAttributes']['aoi']['Value']
    assert aoi == '1234'

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