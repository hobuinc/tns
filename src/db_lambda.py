import boto3
import os

import h3
from shapely.geometry import Polygon, MultiPolygon
from shapely import wkt, from_geojson
from boto3.dynamodb.conditions import Attr, Key


def cover_polygon_h3(polygon: Polygon, resolution: int):
    '''
    Return the set of H3 cells at the specified resolution which completely cover the input polygon.
    '''
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

def db_delete(event, context):
    region = os.environ['AWS_REGION']
    table_name = os.environ["DB_TABLE_NAME"]

    dynamo = boto3.client("dynamodb", region_name=region)

    msg = event["Records"][0]["Sns"]["MessageAttributes"]
    aoi = msg['aoi']['Value']
    delete_if_found(dynamo, table_name, aoi)


def db_add_handler(event, context):
    region = os.environ['AWS_REGION']
    table_name = os.environ["DB_TABLE_NAME"]

    dynamo = boto3.client("dynamodb", region_name=region)

    msg = event["Records"][0]["Sns"]["MessageAttributes"]
    aoi = msg['aoi']['Value']
    polygon_str = msg['polygon']['Value']
    polygon = from_geojson(polygon_str)

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
                'DataType': 'String', 'StringValue': aoi
            },
            'h3_indices': {
                'DataType': 'StringArray', 'StringValue': f'{part_keys}'
            },
            'response_code': {
                'DataType': 'Number',
                'StringValue': f'{res['ResponseMetadata']}'
            }
        },
        Message="AOI: {aoi} added")

    return res

def comp_handler(event, context):
    region = os.environ['AWS_REGION']
    sns_out_arn = os.environ["SNS_OUT_ARN"]
    table_name = os.environ["DB_TABLE_NAME"]

    sns = boto3.client("sns", region_name=region)
    dynamo = boto3.client("dynamodb", region_name=region)
    sqs = boto3.client('sqs',region_name=region)

    aoi_impact_data = [ ]
    message_id_info = [(e["eventSourceARN"], e['messageId'], e['receiptHandle']) for e in event["Records"]]

    # loop through queue messages, and delete once we're done with them
    for e in event['Records']:
        polygon_str = e['messageAttributes']['polygon']['Value']
        polygon = from_geojson(polygon_str)

        aoi_info = get_db_comp(dynamo, polygon, table_name)

        aoi_impact_list = []
        upoly = Polygon(polygon)
        for k,v in aoi_info.items():
            dbpoly = from_geojson(v)
            if not upoly.disjoint(dbpoly):
                aoi_impact_list.append(k)

        impact_data = {
            'input_polygon': {
                'DataType': 'String',
                'StringValue': polygon_str
            },
            'impacted_aois': {
                'DataType': 'String',
                'StringListValues': aoi_impact_list
            }
        }

        source_arn = e['eventSourceARN']
        queue_name= source_arn.split(':')[-1]
        queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
        receipt_handle = e['receiptHandle']
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    for aoi in aoi_impact_list:
        res = sns.publish(TopicArn=sns_out_arn,
            MessageAttributes={'aoi': {'DataType': 'String', 'StringValue': aoi}},
            Message="{aoi}")

    return aoi_impact_list