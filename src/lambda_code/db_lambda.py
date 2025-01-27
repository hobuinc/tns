import boto3
import json
import os

import h3
from shapely.geometry import Polygon, MultiPolygon
from shapely import wkt, from_geojson

region = os.environ['AWS_REGION']
sns = boto3.client("sns", region_name=region)
dynamo = boto3.client("dynamodb", region_name=region)

## Add an h3 partition and sort key to dynamo db

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

def get_db_comp(polygon, table_name):
    sort_keys = cover_shape_h3(polygon, 3)
    part_keys = [h3.get_base_cell_number(hi) for hi in sort_keys]
    key_zip = zip(part_keys, sort_keys)

    aoi_list = []

    request = {
        f'{table_name}': {
            'Keys': [
                {
                    'h3_base_idx': {'N': f'{p}'},
                    'h3_res_3': {'S': s}
                }
                for p,s in key_zip
            ]
        }
    }
    res = dynamo.batch_get_item(RequestItems=request)
    if not res['Responses'][table_name]:
        return []
    aoi_list = [ [v['h3_base_idx']['N'], v['h3_res_3']['S'], v['aoi_list']['SS']] for v in res['Responses'][table_name]]

    return aoi_list


def db_add_handler(event, context):
    sns_out_arn = os.environ["SNS_OUT_ARN"]
    table_name = os.environ["DB_TABLE_NAME"]

    msg = event["Records"][0]["Sns"]["Message"]
    aoi = msg['aoi_name']
    polygon = from_geojson(msg['polygon'])

    # create new db entries for aoi-polygon combo
    sort_keys = cover_shape_h3(polygon, 3)
    part_keys = [h3.get_base_cell_number(s) for s in sort_keys]
    aoi_list = [[aoi] for s in sort_keys]

    # join previously created aois to the new one
    cur_aoi_list = get_db_comp(polygon, table_name)
    for vals in cur_aoi_list:
        pk, sk, aois = vals
        if sk in sort_keys:
            idx = sort_keys.index(sk)
            aoi_list[idx] = aoi_list[idx] + aois


    # TODO, should throw if duplicate AOI found?
    # deduplicate
    aoi_list = [list(set(a)) for a in aoi_list]
    keys = zip(part_keys, sort_keys, aoi_list)

    request = {
        f'{table_name}': [
            {
                'PutRequest': {
                    'Item': {
                        'h3_base_idx': {'N': f'{pk}'},
                        'h3_res_3': {'S': sk},
                        'aoi_list': {'SS': aois}
                    }
                }
            }
            for pk, sk, aois in keys
        ]
    }
    res = dynamo.batch_write_item(RequestItems=request)

    return res

def comp_handler(event, context):
    sns_out_arn = os.environ["SNS_OUT_ARN"]
    table_name = os.environ["DB_TABLE_NAME"]

    polygon_str = event["Records"][0]["Sns"]["Message"]['polygon']
    print(f"Polygon received: {polygon_str}")
    polygon = from_geojson(polygon_str)

    h3_list = get_db_comp(polygon, table_name)

    aoi_list = []
    for b,r,aois in h3_list:
        aoi_list = aoi_list + aois

    aoi_list = list(set(aoi_list))
    for aoi in aoi_list:
        res = sns.publish(TopicArn=sns_out_arn,
            MessageAttributes={'aoi': {'DataType': 'String', 'StringValue': aoi}},
            Message="We've detected an overlap between a newly ingested dataset"
                    f"and your subscribed AOI: {aoi}")
        print('result', res)

    return h3_list

# geom ='{"type": "Polygon", "coordinates": [ [ [ -70.493308, 41.279975 ], [ -70.436845, 41.299054 ], [ -70.408171, 41.30899 ], [ -70.394819, 41.319664 ], [ -70.394805, 41.319682 ], [ -70.388153, 41.32578 ], [ -70.384532, 41.333519 ], [ -70.380032, 41.342892 ], [ -70.374466, 41.351584 ], [ -70.36975, 41.357418 ], [ -70.361022, 41.36427 ], [ -70.352415, 41.369206 ], [ -70.346086, 41.372182 ], [ -70.339753, 41.375119 ], [ -70.328182, 41.380159 ], [ -70.315816, 41.383227 ], [ -70.301701, 41.385151 ], [ -70.287685, 41.384672 ], [ -70.271551, 41.381243 ], [ -70.258422, 41.381 ], [ -70.249463, 41.381012 ], [ -70.242163, 41.381703 ], [ -70.23433, 41.383229 ], [ -70.233576, 41.38264 ], [ -70.224936, 41.37512 ], [ -70.221622, 41.371769 ], [ -70.217334, 41.364997 ], [ -70.215115, 41.360275 ], [ -70.208297, 41.358025 ], [ -70.198383, 41.359025 ], [ -70.187287, 41.35838 ], [ -70.174906, 41.35729 ], [ -70.159992, 41.352242 ], [ -70.148699, 41.345216 ], [ -70.13794, 41.346371 ], [ -70.132231, 41.348731 ], [ -70.124461, 41.351327 ], [ -70.119335, 41.352638 ], [ -70.110095, 41.353592 ], [ -70.101618, 41.35345 ], [ -70.097844, 41.353584 ], [ -70.103251, 41.359705 ], [ -70.107199, 41.365213 ], [ -70.111328, 41.370389 ], [ -70.11644, 41.386165 ], [ -70.116541, 41.395132 ], [ -70.112734, 41.408126 ], [ -70.105819, 41.419866 ], [ -70.094412, 41.43081 ], [ -70.078847, 41.438015 ], [ -70.065174, 41.442687 ], [ -70.044272, 41.443801 ], [ -70.025902, 41.441079 ], [ -70.006467, 41.433898 ], [ -69.999458, 41.428432 ], [ -69.994643, 41.425024 ], [ -69.989227, 41.421225 ], [ -69.981775, 41.415208 ], [ -69.974592, 41.408922 ], [ -69.969634, 41.403511 ], [ -69.964065, 41.395852 ], [ -69.957745, 41.38725 ], [ -69.952535, 41.379656 ], [ -69.949535, 41.375119 ], [ -69.943801, 41.366717 ], [ -69.938456, 41.357645 ], [ -69.933859, 41.351049 ], [ -69.929837, 41.345323 ], [ -69.92644, 41.338794 ], [ -69.921579, 41.332398 ], [ -69.91495, 41.324195 ], [ -69.910093, 41.317263 ], [ -69.906211, 41.3108 ], [ -69.902948, 41.305207 ], [ -69.898321, 41.297003 ], [ -69.894885, 41.290372 ], [ -69.893789, 41.286925 ], [ -69.892959, 41.283643 ], [ -69.892445, 41.27909 ], [ -69.8925, 41.276211 ], [ -69.892209, 41.271323 ], [ -69.893025, 41.266637 ], [ -69.894472, 41.25924 ], [ -69.896044, 41.254119 ], [ -69.897743, 41.250122 ], [ -69.89901, 41.245538 ], [ -69.902992, 41.238416 ], [ -69.910319, 41.226849 ], [ -69.915842, 41.220939 ], [ -69.922911, 41.21584 ], [ -69.930953, 41.210543 ], [ -69.942008, 41.20452 ], [ -69.953586, 41.199672 ], [ -69.963739, 41.196393 ], [ -69.973182, 41.193545 ], [ -69.983151, 41.191403 ], [ -69.999457, 41.188305 ], [ -70.012483, 41.187053 ], [ -70.025702, 41.187102 ], [ -70.039705, 41.188192 ], [ -70.051647, 41.190908 ], [ -70.072666, 41.191634 ], [ -70.096057, 41.19053 ], [ -70.109684, 41.189803 ], [ -70.12446, 41.192422 ], [ -70.135855, 41.194649 ], [ -70.16117, 41.20069 ], [ -70.178851, 41.204531 ], [ -70.205762, 41.214219 ], [ -70.217017, 41.218176 ], [ -70.227997, 41.222664 ], [ -70.239982, 41.228463 ], [ -70.285736, 41.242346 ], [ -70.294484, 41.246499 ], [ -70.314298, 41.260532 ], [ -70.33509, 41.272779 ], [ -70.344083, 41.276146 ], [ -70.357554, 41.272596 ], [ -70.369819, 41.270406 ], [ -70.374466, 41.269458 ], [ -70.386671, 41.271282 ], [ -70.396777, 41.273014 ], [ -70.408733, 41.277294 ], [ -70.414493, 41.273953 ], [ -70.429075, 41.268651 ], [ -70.444758, 41.265513 ], [ -70.459851, 41.265253 ], [ -70.475635, 41.27022 ], [ -70.493308, 41.279975 ] ] ] }'
# write_event = { 'Records': [ { 'Sns': { 'Message': {'aoi_name': 'asdfasdf', 'polygon': geom } } } ] }
# read_event = { 'Records': [ { 'Sns': { 'Message': {'polygon': geom } } } ] }
# db_add_handler(write_event, None)
# comp_handler(read_event, None)