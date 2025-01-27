import os
import boto3
import json
import ast

aws_region = os.environ['AWS_REGION']
sns_out_arn = os.environ["SNS_OUT_ARN"]
sns = boto3.client("sns", region_name=aws_region)

def check_message(msg):

    try:
        msg = json.loads(msg)
    except:
        msg = ast.literal_eval(msg)

    if 'email' not in msg:
        raise AttributeError("Missing attribute 'email' from SNS message.")
    email = msg['email']

    if 'function' in msg:
        fn = msg['function']
        if fn not in ['setFilter', 'addFilter']:
            raise AttributeError(f'"{fn}" is not in set of valid functions '
                    '("setFilter", "addFilter").')
    else:
        fn = 'addFilter'

    if 'aois' in msg:
        aois = msg['aois']
    else:
        aois = None

    return email, fn, aois

def subscribe(email, aois):
    if aois is not None:
        filter_policy = dict(
            FilterPolicyScope='MessageAttributes',
            FilterPolicy=json.dumps({'aoi': aois})
        )
        sns.subscribe(TopicArn=sns_out_arn, Protocol='email',
                Endpoint=email, Attributes=filter_policy)
    else:
        sns.subscribe(TopicArn=sns_out_arn, Protocol='email',
                Endpoint=email)

def set_filter(email, sub_arn, aois):
    sns.set_subscription_attributes(SubscriptionArn=sub_arn,
            AttributeName='FilterPolicy', AttributeValue=flt)

# TODO add add_filter option
# def add_filter()

# handle the filtering and addition of an sns subscription
# message should passed as:
# {'email': '...', function: 'setFilter|(addFilter)', filter: {Filter JSON}}
# addFilter is default function and filter defaults to empty
# see https://docs.aws.amazon.com/sns/latest/dg/attribute-key-matching.html for
# examples
def handler(event, context):
    msg = event["Records"][0]["Sns"]["Message"]

    email, fn, aois = check_message(msg)

    next_token = None
    while(True):
        if next_token is not None:
            subs = sns.list_subscriptions_by_topic(TopicArn=sns_out_arn,
                    NextToken=next_token)
        else:
            subs = sns.list_subscriptions_by_topic(TopicArn=sns_out_arn)
        sub = next((s for s in subs['Subscriptions'] if s['Endpoint'] == email), None)

        if 'NextToken' in subs:
            next_token = subs['NextToken']
        else:
            next_token = None

        if sub is None and next_token is None:
            subscribe(email, aois)
            break
        elif sub is not None:
            sub_arn = next((s for s in subs if s['End'] == value), None)
            set_filter(email, sub_arn, aois)

# e = {'Records': [{'Sns': {'Message': {'email': 'kyle@hobu.co', 'function': 'setFilter' } } } ]}
# e = {'Records': [{'Sns': {'Message': '{\"email\": \"kyle@hobu.co\", \"aois\": [\"asdf\",\"test\"]}' } } ]}
# handler(e, None)