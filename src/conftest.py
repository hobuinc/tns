import subprocess
import os
import pytest
import json
import boto3
from time import sleep
from pathlib import Path


@pytest.fixture(scope='session')
def dynamo(tf_output):
    table_name = tf_output['table_name']
    db_name = tf_output['table_name']
    aws_region = tf_output['aws_region']

    yield boto3.client('dynamodb', region_name=aws_region)

@pytest.fixture(scope='session')
def tf_dir():
    cur_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    tf_dir = cur_dir / '..' / 'terraform'
    yield tf_dir

@pytest.fixture(scope='session')
def tf_output(tf_dir):
    tf = subprocess.Popen(['terraform', 'output', '--json'],
            cwd=tf_dir,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding='utf8')
    a = tf.communicate()
    output_json = json.loads(a[0])
    key_vals = {k:v['value'] for k,v in output_json.items()}
    yield key_vals

@pytest.fixture(scope='session')
def region(tf_output):
    yield tf_output['aws_region']

@pytest.fixture(scope='function')
def h3_indices():
    yield ['832a06fffffffff', '832a31fffffffff', '832a04fffffffff']

@pytest.fixture(scope='function')
def updated_h3_indices():
    yield ['83281bfffffffff', '8328f4fffffffff']

@pytest.fixture(scope='function')
def db_fill(tf_output, aoi, h3_indices, geom, updated_h3_indices):
    db_name = tf_output['table_name']
    aws_region = tf_output['aws_region']
    dynamo = boto3.client('dynamodb', region_name=aws_region)

    # make sure that old entries are deleted before using
    for pk in h3_indices:
        key = {'aoi_id': {'N': f'{aoi}'}, 'h3_idx': {'S': pk}}
        dynamo.delete_item(Key=key, TableName=db_name)
    for pk in updated_h3_indices:
        key = {'aoi_id': {'N': f'{aoi}'}, 'h3_idx': {'S': pk}}
        dynamo.delete_item(Key=key, TableName=db_name)

    request = {
        f'{db_name}': [
            {
                'PutRequest': {
                    'Item': {
                        'h3_idx': {'S': pk},
                        'aoi_id': {'N': f'{aoi}'},
                        'polygon': {'S': geom}
                    }
                }
            }
            for pk in h3_indices
        ]
    }
    yield dynamo.batch_write_item(RequestItems=request)

    for pk in h3_indices:
        key = {'aoi_id': {'N': f'{aoi}'}, 'h3_idx': {'S': pk}}
        dynamo.delete_item(Key=key, TableName=db_name)
    for pk in updated_h3_indices:
        key = {'aoi_id': {'N': f'{aoi}'}, 'h3_idx': {'S': pk}}
        dynamo.delete_item(Key=key, TableName=db_name)


@pytest.fixture(scope='function')
def geom():
    yield json.dumps({"type": "Polygon", "coordinates": [ [ [ -70.493308, 41.279975 ], [ -70.436845, 41.299054 ], [ -70.408171, 41.30899 ], [ -70.394819, 41.319664 ], [ -70.394805, 41.319682 ], [ -70.388153, 41.32578 ], [ -70.384532, 41.333519 ], [ -70.380032, 41.342892 ], [ -70.374466, 41.351584 ], [ -70.36975, 41.357418 ], [ -70.361022, 41.36427 ], [ -70.352415, 41.369206 ], [ -70.346086, 41.372182 ], [ -70.339753, 41.375119 ], [ -70.328182, 41.380159 ], [ -70.315816, 41.383227 ], [ -70.301701, 41.385151 ], [ -70.287685, 41.384672 ], [ -70.271551, 41.381243 ], [ -70.258422, 41.381 ], [ -70.249463, 41.381012 ], [ -70.242163, 41.381703 ], [ -70.23433, 41.383229 ], [ -70.233576, 41.38264 ], [ -70.224936, 41.37512 ], [ -70.221622, 41.371769 ], [ -70.217334, 41.364997 ], [ -70.215115, 41.360275 ], [ -70.208297, 41.358025 ], [ -70.198383, 41.359025 ], [ -70.187287, 41.35838 ], [ -70.174906, 41.35729 ], [ -70.159992, 41.352242 ], [ -70.148699, 41.345216 ], [ -70.13794, 41.346371 ], [ -70.132231, 41.348731 ], [ -70.124461, 41.351327 ], [ -70.119335, 41.352638 ], [ -70.110095, 41.353592 ], [ -70.101618, 41.35345 ], [ -70.097844, 41.353584 ], [ -70.103251, 41.359705 ], [ -70.107199, 41.365213 ], [ -70.111328, 41.370389 ], [ -70.11644, 41.386165 ], [ -70.116541, 41.395132 ], [ -70.112734, 41.408126 ], [ -70.105819, 41.419866 ], [ -70.094412, 41.43081 ], [ -70.078847, 41.438015 ], [ -70.065174, 41.442687 ], [ -70.044272, 41.443801 ], [ -70.025902, 41.441079 ], [ -70.006467, 41.433898 ], [ -69.999458, 41.428432 ], [ -69.994643, 41.425024 ], [ -69.989227, 41.421225 ], [ -69.981775, 41.415208 ], [ -69.974592, 41.408922 ], [ -69.969634, 41.403511 ], [ -69.964065, 41.395852 ], [ -69.957745, 41.38725 ], [ -69.952535, 41.379656 ], [ -69.949535, 41.375119 ], [ -69.943801, 41.366717 ], [ -69.938456, 41.357645 ], [ -69.933859, 41.351049 ], [ -69.929837, 41.345323 ], [ -69.92644, 41.338794 ], [ -69.921579, 41.332398 ], [ -69.91495, 41.324195 ], [ -69.910093, 41.317263 ], [ -69.906211, 41.3108 ], [ -69.902948, 41.305207 ], [ -69.898321, 41.297003 ], [ -69.894885, 41.290372 ], [ -69.893789, 41.286925 ], [ -69.892959, 41.283643 ], [ -69.892445, 41.27909 ], [ -69.8925, 41.276211 ], [ -69.892209, 41.271323 ], [ -69.893025, 41.266637 ], [ -69.894472, 41.25924 ], [ -69.896044, 41.254119 ], [ -69.897743, 41.250122 ], [ -69.89901, 41.245538 ], [ -69.902992, 41.238416 ], [ -69.910319, 41.226849 ], [ -69.915842, 41.220939 ], [ -69.922911, 41.21584 ], [ -69.930953, 41.210543 ], [ -69.942008, 41.20452 ], [ -69.953586, 41.199672 ], [ -69.963739, 41.196393 ], [ -69.973182, 41.193545 ], [ -69.983151, 41.191403 ], [ -69.999457, 41.188305 ], [ -70.012483, 41.187053 ], [ -70.025702, 41.187102 ], [ -70.039705, 41.188192 ], [ -70.051647, 41.190908 ], [ -70.072666, 41.191634 ], [ -70.096057, 41.19053 ], [ -70.109684, 41.189803 ], [ -70.12446, 41.192422 ], [ -70.135855, 41.194649 ], [ -70.16117, 41.20069 ], [ -70.178851, 41.204531 ], [ -70.205762, 41.214219 ], [ -70.217017, 41.218176 ], [ -70.227997, 41.222664 ], [ -70.239982, 41.228463 ], [ -70.285736, 41.242346 ], [ -70.294484, 41.246499 ], [ -70.314298, 41.260532 ], [ -70.33509, 41.272779 ], [ -70.344083, 41.276146 ], [ -70.357554, 41.272596 ], [ -70.369819, 41.270406 ], [ -70.374466, 41.269458 ], [ -70.386671, 41.271282 ], [ -70.396777, 41.273014 ], [ -70.408733, 41.277294 ], [ -70.414493, 41.273953 ], [ -70.429075, 41.268651 ], [ -70.444758, 41.265513 ], [ -70.459851, 41.265253 ], [ -70.475635, 41.27022 ], [ -70.493308, 41.279975 ] ] ] })

@pytest.fixture(scope='function')
def update_geom():
    yield json.dumps({"type": "Polygon", "coordinates": [ [ [ -123.074979482909725, 44.049714592181111 ], [ -123.075534969143874, 44.062481974419434 ], [ -123.062505348770557, 44.062776048474724 ], [ -123.061952669217845, 44.050008602244745 ], [ -123.074979482909725, 44.049714592181111 ] ] ] })

@pytest.fixture(scope='function')
def aoi():
    yield '1234'

@pytest.fixture(scope='session')
def comp_sqs_in_arn(tf_output):
    yield tf_output['db_comp_sqs_in']

@pytest.fixture(scope='session')
def db_add_sqs_in_arn(tf_output):
    yield tf_output['db_add_sqs_in']

@pytest.fixture(scope='session')
def db_delete_sqs_in_arn(tf_output):
    yield tf_output['db_delete_sqs_in']

@pytest.fixture(scope='function')
def add_update_receipt_handle(tf_output, region, geom):
    queue_arn = tf_output['db_add_sqs_in']
    aws_region = tf_output['aws_region']
    sqs = boto3.client('sqs', region_name=aws_region)
    queue_name = queue_arn.split(':')[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    message_res = sqs.send_message(
        QueueUrl = queue_url,
        MessageBody = "Polygon added",
        MessageAttributes = {
            "polygon": {
                "DataType": "String",
                "StringValue": f"{geom}"
            }
        }
    )
    message = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        MessageSystemAttributeNames=['All']
    )
    handle = message['Messages'][0]['ReceiptHandle']
    yield handle

# for information on SNS message attributes \
# https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html
@pytest.fixture(scope='function')
def add_event(region, aoi, geom, add_update_receipt_handle, db_add_sqs_in_arn):
    body = '{\n  "Type" : "Notification",\n  "MessageId" : "57781038-61c3-5c09-acb4-707878414763",\n  "TopicArn" : "arn:aws:sns:us-west-2:068489536557:tns_db_comp_sns_input2025030319513457330000000c",\n  "Message" : "None",\n  "Timestamp" : "2025-03-03T20:55:30.955Z",\n  "SignatureVersion" : "1",\n  "Signature" : "BoJuS+OM69jXkxlM8L2rlPxZjHEt1qOF58h/wwBs0kK4iWdQ3IlT4spkEswZ17FhIl/ncL1gzNfLbzgVxuBDZM//GDd1to9iMaRjrGiz+zxoa7SKK7nQCGFrZrfQ/1083r+QsyjRC24QiG8MEudmn542yMtyUWyRKoh4Ep1AQLgPaBb/tfRtS/MJr7smvg1EE7zbQDoY2+p9Jldp/sTm4zS+pNHRS3qy3M9Ztwd/4VUNhwPaaYMIcvbWlgrvJFdYa3Y8OLRDA6umpJFGqMJ0cWwweAEPKDAzo1hRSTPuOCNrUWQMQZpeHKdNo+iN9DKwh4A2F8ZTu07UmAZtRdDP3A==",\n  "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-9c6465fa7f48f5cacd23014631ec1136.pem",\n  "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:068489536557:tns_db_comp_sns_input2025030319513457330000000c:18d0752b-0408-441d-9c30-05d8746d7185",\n  "MessageAttributes" : {\n "aoi":{"Type":"Number","Value":"'+aoi+'"}, "polygon":{"Type":"String","Value":'+geom+'}\n  }\n}'
    yield {
        "Records": [
            {
                "messageId": "asdfasdfasdfsadf",
                "receiptHandle": add_update_receipt_handle,
                'body': body,
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082649183",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082649185"
                },
                "messageAttributes": { },
                "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                "eventSource": "aws:sqs",
                "eventSourceARN": db_add_sqs_in_arn,
                "awsRegion": region
            },
        ]
    }

@pytest.fixture(scope='function')
def update_event(region, aoi, update_geom, add_update_receipt_handle):
    body='{\n  "Type" : "Notification",\n  "MessageId" : "57781038-61c3-5c09-acb4-707878414763",\n  "TopicArn" : "arn:aws:sns:us-west-2:068489536557:tns_db_comp_sns_input2025030319513457330000000c",\n  "Message" : "None",\n  "Timestamp" : "2025-03-03T20:55:30.955Z",\n  "SignatureVersion" : "1",\n  "Signature" : "BoJuS+OM69jXkxlM8L2rlPxZjHEt1qOF58h/wwBs0kK4iWdQ3IlT4spkEswZ17FhIl/ncL1gzNfLbzgVxuBDZM//GDd1to9iMaRjrGiz+zxoa7SKK7nQCGFrZrfQ/1083r+QsyjRC24QiG8MEudmn542yMtyUWyRKoh4Ep1AQLgPaBb/tfRtS/MJr7smvg1EE7zbQDoY2+p9Jldp/sTm4zS+pNHRS3qy3M9Ztwd/4VUNhwPaaYMIcvbWlgrvJFdYa3Y8OLRDA6umpJFGqMJ0cWwweAEPKDAzo1hRSTPuOCNrUWQMQZpeHKdNo+iN9DKwh4A2F8ZTu07UmAZtRdDP3A==",\n  "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-9c6465fa7f48f5cacd23014631ec1136.pem",\n  "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:068489536557:tns_db_comp_sns_input2025030319513457330000000c:18d0752b-0408-441d-9c30-05d8746d7185",\n  "MessageAttributes" : {\n "aoi":{"Type":"Number","Value":"'+aoi+'"}, "polygon":{"Type":"String","Value":'+update_geom+'}\n  }\n}'
    yield {
        "Records": [
            {
                "messageId": "asdfasdfasdfsadf",
                "receiptHandle": add_update_receipt_handle,
                'body': body,
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082649183",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082649185"
                },
                "messageAttributes": { },
                "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                "eventSource": "aws:sqs",
                "eventSourceARN": db_add_sqs_in_arn,
                "awsRegion": region
            },
        ]
    }

@pytest.fixture(scope='function')
def comp_receipt_handle(tf_output, geom):
    queue_arn = tf_output['db_comp_sqs_in']
    aws_region = tf_output['aws_region']
    sqs = boto3.client('sqs', region_name=aws_region)
    queue_name = queue_arn.split(':')[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    message_res = sqs.send_message(
        QueueUrl = queue_url,
        MessageBody = "Polygon added",
        MessageAttributes = {
            "polygon": {
                "DataType": "String",
                "StringValue": f"{geom}"
            }
        }
    )
    message = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        MessageSystemAttributeNames=['All']
    )
    handle = message['Messages'][0]['ReceiptHandle']
    yield handle

@pytest.fixture(scope='function')
def comp_event(region, geom, comp_sqs_in_arn, comp_receipt_handle):
    body='{\n  "Type" : "Notification",\n  "MessageId" : "57781038-61c3-5c09-acb4-707878414763",\n  "TopicArn" : "arn:aws:sns:us-west-2:068489536557:tns_db_comp_sns_input2025030319513457330000000c",\n  "Message" : "None",\n  "Timestamp" : "2025-03-03T20:55:30.955Z",\n  "SignatureVersion" : "1",\n  "Signature" : "BoJuS+OM69jXkxlM8L2rlPxZjHEt1qOF58h/wwBs0kK4iWdQ3IlT4spkEswZ17FhIl/ncL1gzNfLbzgVxuBDZM//GDd1to9iMaRjrGiz+zxoa7SKK7nQCGFrZrfQ/1083r+QsyjRC24QiG8MEudmn542yMtyUWyRKoh4Ep1AQLgPaBb/tfRtS/MJr7smvg1EE7zbQDoY2+p9Jldp/sTm4zS+pNHRS3qy3M9Ztwd/4VUNhwPaaYMIcvbWlgrvJFdYa3Y8OLRDA6umpJFGqMJ0cWwweAEPKDAzo1hRSTPuOCNrUWQMQZpeHKdNo+iN9DKwh4A2F8ZTu07UmAZtRdDP3A==",\n  "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-9c6465fa7f48f5cacd23014631ec1136.pem",\n  "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:068489536557:tns_db_comp_sns_input2025030319513457330000000c:18d0752b-0408-441d-9c30-05d8746d7185",\n  "MessageAttributes" : {\n    "polygon" : {"Type":"String","Value":'+geom+'}\n  }\n}'
    yield {
        'Records': [
            {
                'messageId': '72823017-6148-4000-ba5a-66effc1ea71b',
                'receiptHandle': comp_receipt_handle,
                'body': body,
                'attributes': {
                    'ApproximateReceiveCount': '2',
                    'SentTimestamp': '1741035331000',
                    'SenderId': 'AIDAIYLAVTDLUXBIEIX46',
                    'ApproximateFirstReceiveTimestamp': '1741035331015'
                },
                'messageAttributes': {},
                'md5OfBody': 'dbc00a6065b95f09871d38de1c25666b',
                'eventSource': 'aws:sqs',
                'eventSourceARN': comp_sqs_in_arn,
                'awsRegion': region
            }
        ]
    }

@pytest.fixture(scope='function')
def delete_receipt_handle(tf_output, geom):
    queue_arn = tf_output['db_delete_sqs_in']
    aws_region = tf_output['aws_region']
    sqs = boto3.client('sqs', region_name=aws_region)
    queue_name = queue_arn.split(':')[-1]
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    message_res = sqs.send_message(
        QueueUrl = queue_url,
        MessageBody = "Polygon added",
        MessageAttributes = {
            "polygon": {
                "DataType": "String",
                "StringValue": f"{geom}"
            }
        }
    )
    message = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        MessageSystemAttributeNames=['All']
    )
    handle = message['Messages'][0]['ReceiptHandle']
    yield handle

@pytest.fixture(scope='function')
def delete_event(region, aoi, delete_receipt_handle, db_delete_sqs_in_arn):
    yield {
        "Records": [
            {
                "messageId": "72823017-6148-4000-ba5a-66effc1ea71b",
                "receiptHandle": delete_receipt_handle,
                'body': '{\n  "Type" : "Notification",\n  "MessageId" : "57781038-61c3-5c09-acb4-707878414763",\n  "TopicArn" : "arn:aws:sns:us-west-2:068489536557:tns_db_comp_sns_input2025030319513457330000000c",\n  "Message" : "None",\n  "Timestamp" : "2025-03-03T20:55:30.955Z",\n  "SignatureVersion" : "1",\n  "Signature" : "BoJuS+OM69jXkxlM8L2rlPxZjHEt1qOF58h/wwBs0kK4iWdQ3IlT4spkEswZ17FhIl/ncL1gzNfLbzgVxuBDZM//GDd1to9iMaRjrGiz+zxoa7SKK7nQCGFrZrfQ/1083r+QsyjRC24QiG8MEudmn542yMtyUWyRKoh4Ep1AQLgPaBb/tfRtS/MJr7smvg1EE7zbQDoY2+p9Jldp/sTm4zS+pNHRS3qy3M9Ztwd/4VUNhwPaaYMIcvbWlgrvJFdYa3Y8OLRDA6umpJFGqMJ0cWwweAEPKDAzo1hRSTPuOCNrUWQMQZpeHKdNo+iN9DKwh4A2F8ZTu07UmAZtRdDP3A==",\n  "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-9c6465fa7f48f5cacd23014631ec1136.pem",\n  "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:068489536557:tns_db_comp_sns_input2025030319513457330000000c:18d0752b-0408-441d-9c30-05d8746d7185",\n  "MessageAttributes" : {\n    "aoi" : {"Type":"Number","Value":"'+aoi+'"}\n  }\n}',
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082649183",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082649185"
                },
                "messageAttributes": { },
                "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                "eventSource": "aws:sqs",
                "eventSourceARN": db_delete_sqs_in_arn,
                "awsRegion": region
            },
        ]
    }