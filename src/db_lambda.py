import boto3
import json
import os

import h3
from shapely.geometry import Polygon, MultiPolygon
from shapely import wkt, from_geojson


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
            aname = i['aoi_name']['S']
            if aname not in aoi_info.keys():
                aoi_info[aname] = i['polygon']['S']

    return aoi_info

def db_add_handler(event, context):
    region = os.environ['AWS_REGION']
    table_name = os.environ["DB_TABLE_NAME"]

    dynamo = boto3.client("dynamodb", region_name=region)

    msg = event["Records"][0]["Sns"]["MessageAttributes"]
    aoi = msg['aoi']['Value']
    polygon_str = msg['polygon']['Value']
    polygon = from_geojson(polygon_str)

    # create new db entries for aoi-polygon combo
    part_keys = cover_shape_h3(polygon, 3)
    # part_keys = [h3.get_base_cell_number(s) for s in sort_keys]
    aoi_list = [aoi for s in part_keys]
    keys = zip(part_keys, aoi_list)

    # join previously created aois to the new one
    # cur_aoi_list = get_db_comp(dynamo, polygon, table_name)
    # for vals in cur_aoi_list:
    #     pk, sk, aois = vals
    #     if sk in sort_keys:
    #         idx = sort_keys.index(sk)
    #         aoi_list[idx] = aoi_list[idx] + aois


    request = {
        f'{table_name}': [
            {
                'PutRequest': {
                    'Item': {
                        'h3_idx': {'S': pk},
                        'aoi_name': {'S': aoi},
                        'polygon': {'S': polygon_str}
                    }
                }
            }
            for pk, aoi in keys
        ]
    }
    res = dynamo.batch_write_item(RequestItems=request)

    return res

def comp_handler(event, context):
    region = os.environ['AWS_REGION']
    sns_out_arn = os.environ["SNS_OUT_ARN"]
    table_name = os.environ["DB_TABLE_NAME"]

    sns = boto3.client("sns", region_name=region)
    dynamo = boto3.client("dynamodb", region_name=region)

    polygon_str = event["Records"][0]["Sns"]["MessageAttributes"]['polygon']['Value']
    polygon = from_geojson(polygon_str)

    aoi_info = get_db_comp(dynamo, polygon, table_name)

    aoi_impact_list = []
    upoly = Polygon(polygon)
    for k,v in aoi_info.items():
        dbpoly = from_geojson(v)
        if not upoly.disjoint(dbpoly):
            aoi_impact_list.append(k)

    for aoi in aoi_impact_list:
        res = sns.publish(TopicArn=sns_out_arn,
            MessageAttributes={'aoi': {'DataType': 'String', 'StringValue': aoi}},
            Message="{aoi}")

    return aoi_impact_list