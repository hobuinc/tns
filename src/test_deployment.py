import json
import boto3
import random
from time import sleep

from db_lambda import get_entries_by_aoi

def delete_sqs_message(e, sqs_arn, region):
    sqs = boto3.client('sqs', region_name=region)
    queue_name= sqs_arn.split(':')[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    receipt_handle = e['ReceiptHandle']
    return sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

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
                receipt_handle=m['ReceiptHandle']
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        else:
            break
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

def sqs_get_messages(sqs_arn, region):
    sqs = boto3.client('sqs', region_name=region)
    queue_name = sqs_arn.split(':')[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    return sqs.receive_message(QueueUrl=queue_url,
            MessageAttributeNames=['All'], MaxNumberOfMessages=10, WaitTimeSeconds=10)

def sqs_listen(sqs_arn, region, retries = 5):
    sqs = boto3.client('sqs', region_name=region)
    queue_name = sqs_arn.split(':')[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    messages = []
    retry_count = 0
    while not len(messages):
        res = sqs.receive_message(QueueUrl=queue_url,
                MessageAttributeNames=['All'], MaxNumberOfMessages=10, WaitTimeSeconds=10)
        if 'Messages' in res.keys():
            messages = res['Messages']
            retry_count = 0
        else:
            retry_count += 1
            if retry_count >= retries:
                raise Exception('Retry count hit.')
    return messages

def test_big(tf_output, dynamo, aoi, geom, h3_indices, cleanup):
    region = tf_output['aws_region']
    sns_in = tf_output['db_add_sns_in']
    sqs_in = tf_output['db_add_sqs_in']
    sqs_out = tf_output['db_add_sqs_out']
    table_name = tf_output['table_name']

    clear_sqs(sqs_out, region)
    count = 5
    for n in range(count):
        sns_publish(sns_in, region, f'{n}', geom)
        cleanup.append(n)

    msg_count = 0
    retry_count = 0
    while msg_count < count:
        messages = sqs_listen(sqs_out, region)
        for msg in messages:
            body = json.loads(msg['Body'])
            message_str = body['Message']
            assert message_str[:4] == 'AOI:'
            assert message_str[-5:] == 'added'

            attrs = body['MessageAttributes']

            status = attrs['status']['Value']
            assert status == 'succeeded'

            aoi = attrs['aoi']['Value']

            h3s = json.loads(attrs['h3_indices']['Value'])
            for h in h3s:
                assert h in h3_indices

            delete_sqs_message(msg, sqs_out, region)
            retry_count = 0
            msg_count += 1

    # should be no messages left in the input queue
    sleep(30) # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert 'Messages' not in messages

def test_comp(tf_output, geom, db_fill, cleanup):
    region = tf_output['aws_region']
    sns_in = tf_output['db_comp_sns_in']
    sqs_in = tf_output['db_comp_sqs_in']
    sqs_out = tf_output['db_comp_sqs_out']
    table_name = tf_output['table_name']

    clear_sqs(sqs_out, region)
    res = sns_publish(sns_in, region, polygon=geom)
    messages = sqs_listen(sqs_out, region)
    cleanup.append('1234')

    for m in messages:
        message = json.loads(m['Body'])
        assert message['MessageAttributes']['status']['Value'] == 'succeeded',  f"Error from SQS {message['MessageAttributes']['error']['Value']}"
        aois = json.loads(message['MessageAttributes']['aois']['Value'])
        assert len(aois) == 1
        assert aois[0] == '1234'

    # should be no messages left in the input queue
    sleep(30) # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert 'Messages' not in messages


def test_add(tf_output, dynamo, aoi, geom, h3_indices, cleanup):
    region = tf_output['aws_region']
    sns_in = tf_output['db_add_sns_in']
    sqs_in = tf_output['db_add_sqs_in']
    sqs_out = tf_output['db_add_sqs_out']
    table_name = tf_output['table_name']

    clear_sqs(sqs_out, region)
    cleanup.append('1234')

    sns_publish(sns_in, region, aoi, geom)
    messages = sqs_listen(sqs_out, region)
    for msg in messages:
        body = json.loads(msg['Body'])
        message_str = body['Message']
        assert message_str == f'AOI: {aoi} added'

        attrs = body['MessageAttributes']

        status = attrs['status']['Value']
        assert status == 'succeeded'

        aoi = attrs['aoi']['Value']
        assert aoi == '1234'

        h3s = json.loads(attrs['h3_indices']['Value'])
        for h in h3s:
            assert h in h3_indices

    # should be no messages left in the input queue
    sleep(30) # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert 'Messages' not in messages

def test_update(tf_output, db_fill, aoi, update_geom, updated_h3_indices, h3_indices, cleanup):
    region = tf_output['aws_region']
    sns_in = tf_output['db_add_sns_in']
    sqs_in = tf_output['db_add_sqs_in']
    sqs_out = tf_output['db_add_sqs_out']
    table_name = tf_output['table_name']

    clear_sqs(sqs_out, region)
    cleanup.append('1234')

    dynamo = boto3.client('dynamodb', region_name=region)

    og_items = get_entries_by_aoi(dynamo, table_name, aoi)
    og_h3 = [a['h3_idx']['S'] for a in og_items['Items']]
    assert len(og_h3) == 3
    for oh in og_h3:
        assert oh in h3_indices

    # update
    sns_publish(sns_in, region, aoi, update_geom)
    messages = sqs_listen(sqs_out, region)
    for msg in messages:
        body = json.loads(msg['Body'])
        message_str = body['Message']
        assert message_str == f'AOI: {aoi} added'

        attrs = body['MessageAttributes']

        status = attrs['status']['Value']
        assert status == 'succeeded'

        aoi = attrs['aoi']['Value']
        assert aoi == '1234'

        h3s = json.loads(attrs['h3_indices']['Value'])
        for h in h3s:
            assert h in updated_h3_indices

    # should be no messages left in the input queue
    sleep(30) # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert 'Messages' not in messages

def test_delete(tf_output, db_fill, aoi, h3_indices):
    region = tf_output['aws_region']
    sns_in = tf_output['db_delete_sns_in']
    sqs_in = tf_output['db_delete_sqs_in']
    sqs_out = tf_output['db_delete_sqs_out']
    table_name = tf_output['table_name']

    clear_sqs(sqs_out, region)

    dynamo = boto3.client('dynamodb', region_name=region)

    og_items = get_entries_by_aoi(dynamo, table_name, aoi)
    assert og_items['Count'] == 3
    for i in og_items['Items']:
        assert i['aoi_id']['N'] == f'{aoi}'
        assert i['h3_idx']['S'] in h3_indices

    sns_publish(sns_in, region, aoi)
    messages = sqs_listen(sqs_out, region)
    for msg in messages:
        body = json.loads(msg['Body'])
        message_str = body['Message']
        assert message_str == f'AOI: {aoi} deleted'

    deleted_items = get_entries_by_aoi(dynamo, table_name, aoi)
    assert deleted_items['Count'] == 0
    assert len(deleted_items['Items']) == 0

    # should be no messages left in the input queue
    sleep(30) # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert 'Messages' not in messages