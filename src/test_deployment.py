import json
import boto3
import random
import pandas as pd
from time import sleep

from db_lambda import get_entries_by_aoi

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

def put_parquet(action, tf_output, polygon, pk_and_model):
    aws_region = tf_output["aws_region"]
    bucket_name = tf_output["s3_bucket_name"]
    key = f"{action}/geom.parquet"

    s3 = boto3.client("s3", region_name=aws_region)

    df = pd.DataFrame(data={"pk_and_model": [pk_and_model], "geometry": [polygon]})
    df_bytes = df.to_parquet()

    return s3.put_object(Body=df_bytes, Bucket=bucket_name, Key=key)

def delete_sqs_message(e, sqs_arn, region):
    sqs = boto3.client('sqs', region_name=region)
    queue_name= sqs_arn.split(':')[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    receipt_handle = e['ReceiptHandle']
    return sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


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

def test_big(tf_output, dynamo, pk_and_model, geom, h3_indices, cleanup):
    region = tf_output['aws_region']
    sqs_in = tf_output['db_add_sqs_in']
    sqs_out = tf_output['db_add_sqs_out']

    clear_sqs(sqs_out, region)
    count = 5
    for n in range(count):
        name = f'raster_{n}'
        put_parquet("add", tf_output, geom, name)
        # sns_publish(sns_in, region, f'{n}', geom)
        cleanup.append(name)

    msg_count = 0
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

            assert attrs['aoi']['Value']

            h3s = json.loads(attrs['h3_indices']['Value'])
            for h in h3s:
                assert h in h3_indices

            delete_sqs_message(msg, sqs_out, region)
            msg_count += 1

    # should be no messages left in the input queue
    sleep(30) # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert 'Messages' not in messages

def test_comp(tf_output, pk_and_model, geom, db_fill, cleanup):
    region = tf_output['aws_region']
    sqs_in = tf_output['db_compare_sqs_in']
    sqs_out = tf_output['db_compare_sqs_out']

    clear_sqs(sqs_out, region)
    put_parquet("compare", tf_output, geom, pk_and_model)
    # res = sns_publish(sns_in, region, polygon=geom)
    messages = sqs_listen(sqs_out, region)
    cleanup.append(pk_and_model)

    for m in messages:
        message = json.loads(m['Body'])
        assert message['MessageAttributes']['status']['Value'] == 'succeeded',  f"Error from SQS {message['MessageAttributes']['error']['Value']}"
        aois = json.loads(message['MessageAttributes']['aois']['Value'])
        assert len(aois) == 1
        assert aois[0] == pk_and_model

    # should be no messages left in the input queue
    sleep(30) # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert 'Messages' not in messages


def test_add(tf_output, dynamo, pk_and_model, geom, h3_indices, cleanup):
    region = tf_output['aws_region']
    sqs_in = tf_output['db_add_sqs_in']
    sqs_out = tf_output['db_add_sqs_out']

    clear_sqs(sqs_out, region)
    cleanup.append(pk_and_model)
    put_parquet("add", tf_output, geom, pk_and_model)

    messages = sqs_listen(sqs_out, region)
    for msg in messages:
        body = json.loads(msg['Body'])
        message_str = body['Message']
        assert message_str == f'AOI: {pk_and_model} added'

        attrs = body['MessageAttributes']

        status = attrs['status']['Value']
        assert status == 'succeeded'

        aoi = attrs['aoi']['Value']
        assert aoi == pk_and_model

        h3s = json.loads(attrs['h3_indices']['Value'])
        for h in h3s:
            assert h in h3_indices

    # should be no messages left in the input queue
    sleep(30) # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert 'Messages' not in messages
    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)

def test_update(tf_output, db_fill, pk_and_model, update_geom, updated_h3_indices, h3_indices, cleanup):
    region = tf_output['aws_region']
    sqs_in = tf_output['db_add_sqs_in']
    sqs_out = tf_output['db_add_sqs_out']
    table_name = tf_output['table_name']

    clear_sqs(sqs_out, region)
    cleanup.append(pk_and_model)

    dynamo = boto3.client('dynamodb', region_name=region)

    og_items = get_entries_by_aoi(dynamo, table_name, pk_and_model)
    og_h3 = [a['h3_id']['S'] for a in og_items['Items']]
    assert len(og_h3) == 3
    for oh in og_h3:
        assert oh in h3_indices

    # update
    put_parquet("add", tf_output, update_geom, pk_and_model)
    messages = sqs_listen(sqs_out, region)
    for msg in messages:
        body = json.loads(msg['Body'])
        message_str = body['Message']
        assert message_str == f'AOI: {pk_and_model} added'

        attrs = body['MessageAttributes']

        status = attrs['status']['Value']
        assert status == 'succeeded'

        aoi = attrs['aoi']['Value']
        assert aoi == pk_and_model

        h3s = json.loads(attrs['h3_indices']['Value'])
        for h in h3s:
            assert h in updated_h3_indices

    # should be no messages left in the input queue
    sleep(30) # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert 'Messages' not in messages
    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)

def test_delete(tf_output, db_fill, geom, pk_and_model, h3_indices):
    region = tf_output['aws_region']
    sqs_in = tf_output['db_delete_sqs_in']
    sqs_out = tf_output['db_delete_sqs_out']
    table_name = tf_output['table_name']

    clear_sqs(sqs_out, region)

    dynamo = boto3.client('dynamodb', region_name=region)

    og_items = get_entries_by_aoi(dynamo, table_name, pk_and_model)
    assert og_items['Count'] == 3
    for i in og_items['Items']:
        assert i['pk_and_model']['S'] == f'{pk_and_model}'
        assert i['h3_id']['S'] in h3_indices

    # sns_publish(sns_in, region, pk_and_model)
    put_parquet("delete", tf_output, geom, pk_and_model)
    messages = sqs_listen(sqs_out, region)
    for msg in messages:
        body = json.loads(msg['Body'])
        message_str = body['Message']
        assert message_str == f'AOI: {pk_and_model} deleted'

    deleted_items = get_entries_by_aoi(dynamo, table_name, pk_and_model)
    assert deleted_items['Count'] == 0
    assert len(deleted_items['Items']) == 0

    # should be no messages left in the input queue
    sleep(30) # the visibility timeout we have to wait out to be sure
    messages = sqs_get_messages(sqs_in, region)
    assert 'Messages' not in messages
    clear_sqs(sqs_out, region)
    clear_sqs(sqs_in, region)