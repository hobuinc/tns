import json
import boto3
import os

from shapely.geometry import Polygon, MultiPolygon
from boto3.dynamodb.conditions import Attr, Key

def delete_sqs_message(e, region):
    sqs = boto3.client('sqs', region_name=region)
    print('deleting from this event', e)
    source_arn = e['eventSourceARN']
    print('source_arn: ', source_arn)
    queue_name= source_arn.split(':')[-1]
    print('queue name:', queue_name)
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    receipt_handle = e['receiptHandle']
    return sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

def cover_polygon_h3(polygon, resolution: int):
    '''
    Return the set of H3 cells at the specified resolution which completely cover the input polygon.
    '''
    import h3
    result_set = set()
    # Hexes for vertices
    vertex_hexes = [h3.latlng_to_cell(t[1], t[0], resolution) for t in list(polygon.exterior.coords)]
    # Hexes for edges (inclusive of vertices)
    for i in range(len(vertex_hexes)-1):
        result_set.update(h3.grid_path_cells(vertex_hexes[i], vertex_hexes[i+1]))
    # Hexes for internal area
    h3_shape = h3.geo_to_h3shape(polygon)
    result_set.update(list(h3.polygon_to_cells(h3_shape, resolution)))
    return result_set

def cover_shape_h3(shape, resolution: int):
    '''
    Return the set of H3 cells at the specified resolution which completely cover the input shape.
    '''
    result_set = set()

    try:
        if isinstance(shape, Polygon):
            result_set = result_set.union(cover_polygon_h3(shape, resolution))  # noqa

        elif isinstance(shape, MultiPolygon):
            result_set = result_set.union(*[
                cover_shape_h3(s, resolution) for s in shape.geoms
            ])
        else:
            raise ValueError(f"{shape.geom_type}, Unsupported geometry_type")

    except Exception as e:
        raise ValueError(f"Error finding indices for geometry.", repr(e))

    return list(result_set)

def get_db_comp(dynamo, polygon, table_name):
    part_keys = cover_shape_h3(polygon, 3)

    aoi_info = {}
    for h3_idx in part_keys:
        res = dynamo.query(
            TableName = table_name,
            KeyConditionExpression = "h3_idx = :h3_val",
            ExpressionAttributeValues = {':h3_val': {'S': h3_idx}}
        )
        for i in res['Items']:
            aname = i['aoi_id']['N']
            if aname not in aoi_info.keys():
                aoi_info[aname] = i['polygon']['S']

    return aoi_info

def get_entries_by_aoi(dynamo, table_name, aoi):
    a =  dynamo.scan(
        TableName=table_name,
        IndexName='aois_index',
        FilterExpression = "aoi_id = :aoi_id",
        ExpressionAttributeValues={
            ':aoi_id': {'N': f'{aoi}'}
        }
    )
    return a

def delete_if_found(dynamo, table_name, aoi):
    scanned = get_entries_by_aoi(dynamo, table_name, aoi)
    if scanned['Count'] == 0 :
        return
    for i in scanned['Items']:
        dynamo.delete_item(TableName = table_name, Key=i)
    return

def db_delete_handler(event, context):
    print('event', event)
    region = os.environ['AWS_REGION']
    table_name = os.environ["DB_TABLE_NAME"]
    sns_out_arn = os.environ["SNS_OUT_ARN"]

    dynamo = boto3.client("dynamodb", region_name=region)
    sns = boto3.client('sns', region_name=region)
    for e in event['Records']:
        try:
            sns_message = json.loads(e['body'])
            msg = sns_message["MessageAttributes"]
            aoi = msg['aoi']['Value']
            delete_if_found(dynamo, table_name, aoi)

            publish_res = sns.publish(TopicArn=sns_out_arn,
                MessageAttributes={
                    'aoi': {
                        'DataType': 'String', 'StringValue': aoi
                    },
                    'status': {
                        'DataType': 'String',
                        'StringValue': 'succeeded'
                    }
                },
                Message=f"AOI: {aoi} deleted")
            print(f'Successful response: {publish_res}')
        except Exception as e:
            publish_res = sns.publish(TopicArn=sns_out_arn,
                MessageAttributes={
                    'aoi': {
                        'DataType': 'String', 'StringValue': aoi
                    },
                    'status': {
                        'DataType': 'String',
                        'StringValue': 'failed'
                    },
                    'error': {
                        'DataType': 'String',
                        'StringValue': f'{e.args}'
                    }
                },
                Message="Failed to deleted aoi {aoi}")
            print(f'Error response: {publish_res}')


def db_add_handler(event, context):
    from shapely import from_geojson
    print('event', event)
    region = os.environ['AWS_REGION']
    table_name = os.environ["DB_TABLE_NAME"]
    sns_out_arn = os.environ["SNS_OUT_ARN"]

    sns = boto3.client("sns", region_name=region)
    dynamo = boto3.client("dynamodb", region_name=region)
    for e in event['Records']:
        try:
            sns_message = json.loads(e['body'])
            msg = sns_message["MessageAttributes"]
            aoi = msg['aoi']['Value']
            polygon_str = msg['polygon']['Value']


            print('sns_message', sns_message)
            print('msg', msg)
            print('aoi', aoi)
            # make sure it's stringified json so that from_geojson accepts it
            if isinstance(polygon_str, dict):
                polygon_str = json.dumps(polygon_str)

            print('polygon_str', polygon_str)
            polygon = from_geojson(polygon_str)
            print('polygon')

            delete_if_found(dynamo, table_name, aoi)

            # create new db entries for aoi-polygon combo
            part_keys = cover_shape_h3(polygon, 3)
            aoi_list = [aoi for s in part_keys]
            keys = zip(part_keys, aoi_list)

            request = {
                f'{table_name}': [
                    {
                        'PutRequest': {
                            'Item': {
                                'h3_idx': {'S': pk},
                                'aoi_id': {'N': aoi},
                                'polygon': {'S': polygon_str}
                            }
                        }
                    }
                    for pk, aoi in keys
                ]
            }
            res = dynamo.batch_write_item(RequestItems=request)

            publish_res = sns.publish(TopicArn=sns_out_arn,
                MessageAttributes={
                    'aoi': {
                        'DataType': 'String',
                        'StringValue': aoi
                    },
                    'h3_indices': {
                        'DataType': 'String.Array',
                        'StringValue': json.dumps(part_keys)
                    },
                    'status': {
                        'DataType': 'String',
                        'StringValue': 'succeeded'
                    }
                },
                Message=f"AOI: {aoi} added")
            print(f'Added AOI response: {publish_res}')
        except Exception as e:
            publish_res = sns.publish(TopicArn=sns_out_arn,
                MessageAttributes={
                    'aoi': {
                        'DataType': 'String',
                        'StringValue': aoi
                    },
                    'status': {
                        'DataType': 'String',
                        'StringValue': 'failed'
                    },
                    'error': {
                        'DataType': 'String',
                        'StringValue': f'{e.args}'
                    }
                },
                Message=f'Error:{e}')
            print(f'Error response: {publish_res}')

def db_comp_handler(event, context):
    from shapely import from_geojson
    try:
        print('event', event)
        region = os.environ['AWS_REGION']
        sns_out_arn = os.environ["SNS_OUT_ARN"]
        table_name = os.environ["DB_TABLE_NAME"]

        sns = boto3.client("sns", region_name=region)
        dynamo = boto3.client("dynamodb", region_name=region)
        sqs = boto3.client('sqs',region_name=region)

        message_id_info = [(e["eventSourceARN"], e['messageId'], e['receiptHandle']) for e in event["Records"]]

        # loop through queue messages, and delete once we're done with them
        for e in event['Records']:
            sns_message = json.loads(e['body'])
            polygon_str = sns_message['MessageAttributes']['polygon']['Value']
            polygon = from_geojson(polygon_str)

            aoi_info = get_db_comp(dynamo, polygon, table_name)

            aoi_impact_list = []
            upoly = Polygon(polygon)
            for k,v in aoi_info.items():
                dbpoly = from_geojson(v)
                if not upoly.disjoint(dbpoly):
                    aoi_impact_list.append(k)

            delete_sqs_message(e, region)

        print('aois found: ', aoi_impact_list)
        publish_res = sns.publish(
            TopicArn=sns_out_arn,
            MessageAttributes={
                'aois': {
                    'DataType': 'String.Array',
                    'StringValue': json.dumps(aoi_impact_list)
                }
            },
            Message=json.dumps(aoi_impact_list)
        )
        print(f'Publish response: {publish_res}')
        return aoi_impact_list
    except Exception as e:
        publish_res = sns.publish(TopicArn=sns_out_arn,
            MessageAttributes={
                'status': {
                    'DataType': 'String',
                    'StringValue': 'failed'
                },
                'error': {
                    'DataType': 'String',
                    'StringValue': f'{e.args}'
                }
            },
            Message=f'Error:{e.args}')