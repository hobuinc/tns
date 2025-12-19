import json
import pandas as pd
import io
import h3
from botocore.config import Config
from config import CloudConfig
from uuid import uuid4
from itertools import islice, batched

from shapely.geometry import Polygon, MultiPolygon
from shapely import from_geojson

def s3_read_parquet(sns_event, config: CloudConfig):
    s3_info = sns_event["s3"]
    bucket = s3_info["bucket"]["name"]
    key = s3_info["object"]["key"]
    file = config.s3.get_object(Bucket=bucket, Key=key)["Body"]
    pq_bytes = io.BytesIO(file.read())
    return pd.read_parquet(pq_bytes)


def delete_sqs_message(e, config: CloudConfig):
    print("deleting from this event", e)
    source_arn = e["eventSourceARN"]
    print("source_arn:", source_arn)
    queue_name = source_arn.split(":")[-1]
    print("queue name:", queue_name)
    queue_url = config.sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    receipt_handle = e["receiptHandle"]
    return config.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def get_pq_df(event, config: CloudConfig):
    pq_dfs = []
    for sqs_event in event["Records"]:
        body = json.loads(sqs_event["body"])
        message = json.loads(body["Message"])
        # skip TestEvent
        if "Event" in message and message["Event"] == "s3:TestEvent":
            delete_sqs_message(sqs_event, config)
            continue
        for sns_event in message["Records"]:
            pq_df = s3_read_parquet(sns_event, config)
            pq_dfs.append(pq_df)
        delete_sqs_message(sqs_event, config)

    return pd.concat(pq_dfs) if pq_dfs else pq_dfs


def cover_polygon_h3(polygon: Polygon|MultiPolygon, resolution: int):
    """
    Return the set of H3 cells at the specified resolution which completely cover the input polygon.
    """

    result_set = set()
    # Hexes for vertices
    vertex_hexes = [
        h3.latlng_to_cell(t[1], t[0], resolution) for t in list(polygon.exterior.coords)
    ]
    # Hexes for edges (inclusive of vertices)
    for i in range(len(vertex_hexes) - 1):
        result_set.update(h3.grid_path_cells(vertex_hexes[i], vertex_hexes[i + 1]))
    # Hexes for internal area
    h3_shape = h3.geo_to_h3shape(polygon)
    result_set.update(list(h3.polygon_to_cells(h3_shape, resolution)))
    return result_set


def cover_shape_h3(shape: Polygon|MultiPolygon, resolution: int):
    """
    Return the set of H3 cells at the specified resolution which completely
    cover the input shape.
    """
    result_set = set()

    try:
        if isinstance(shape, Polygon):
            result_set = result_set.union(cover_polygon_h3(shape, resolution))  # noqa

        elif isinstance(shape, MultiPolygon):
            result_set = result_set.union(
                *[cover_shape_h3(s, resolution) for s in shape.geoms]
            )
        else:
            raise ValueError(f"{shape.geom_type}, Unsupported geometry_type")

    except Exception as e:
        raise ValueError("Error finding indices for geometry.", repr(e))

    return list(result_set)


def get_db_comp(polygon: Polygon|MultiPolygon, config: CloudConfig):
    """Query Dynamo for entries that overlap with a geometry."""
    part_keys = cover_shape_h3(polygon, 3)

    aoi_info = {}
    # for h3_id in part_keys:
    #     res = config.dynamo.query(
    #         TableName=config.table_name,
    #         KeyConditionExpression="h3_id = :h3_val",
    #         ExpressionAttributeValues={":h3_val": {"S": h3_id}},
    #     )
    res = config.dynamo.execute_statement(
        Statement='SELECT * FROM tns_geodata_table'
            f' WHERE h3_id in {part_keys}'
    )
    for i in res["Items"]:
        aname = i["pk_and_model"]["S"]
        if aname not in aoi_info.keys():
            aoi_info[aname] = i["polygon"]["S"]

    return aoi_info


def get_entries_by_aoi(aoi: str, config: CloudConfig):
    """Scan Dynamo for entries with a specific AOI key."""
    a = config.dynamo.scan(
        TableName=config.table_name,
        IndexName="pk_and_model",
        FilterExpression="pk_and_model = :pk_and_model",
        ExpressionAttributeValues={
            ":pk_and_model": {"S": aoi},
        },
    )
    return a


def delete_if_found(aoi: str, config: CloudConfig):
    """Delete entries from dynamo."""
    scanned = get_entries_by_aoi(aoi)
    if scanned["Count"] == 0:
        return
    for i in scanned["Items"]:
        config.dynamo.delete_item(TableName=config.table_name, Key=i)
    return


def apply_delete(df: pd.DataFrame, config: CloudConfig):
    try:
        aoi = df.pk_and_model
        delete_if_found(aoi)

        publish_res = config.sns.publish(
            TopicArn=config.sns_out_arn,
            MessageAttributes={
                "aoi": {"DataType": "String", "StringValue": aoi},
                "status": {
                    "DataType": "String",
                    "StringValue": "succeeded",
                },
            },
            Message=f"AOI: {aoi} deleted",
        )

        print(f"Successful response: {publish_res}")
    except Exception as e:
        publish_res = config.sns.publish(
            TopicArn=config.sns_out_arn,
            MessageAttributes={
                "aoi": {"DataType": "String", "StringValue": aoi},
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            Message="Failed to delete aoi {aoi}",
        )
        print(f"Error response: {publish_res}")


def db_delete_handler(event, context):
    config = CloudConfig()
    print("event", event)
    pq_df = get_pq_df(event, config)
    pq_df.apply(apply_delete, config=config, axis=1)


def apply_add(df: pd.DataFrame, config: CloudConfig):
    try:
        polygon_str = df.geometry
        aoi = df.pk_and_model
        print("polygon_str", polygon_str)
        polygon = from_geojson(polygon_str)
        print("polygon")
        delete_if_found(aoi)
        # create new db entries for aoi-polygon combo
        part_keys = cover_shape_h3(polygon, 3)
        aoi_list = [aoi for s in part_keys]
        keys = zip(part_keys, aoi_list)

        # max length of items in a dynamo batch write call
        for chunk in batched(keys, 25):
            request = {
                f"{config.table_name}": []
            }
            for pk, aoi in chunk:
                request_item = {
                    "PutRequest": {
                        "Item": {
                            "h3_id": {"S": pk},
                            "pk_and_model": {"S": aoi},
                            "polygon": {"S": polygon_str},
                        }
                    }
                }
                request[f"{config.table_name}"].append(request_item)
            config.dynamo.batch_write_item(RequestItems=request)

        publish_res = config.sns.publish(
            TopicArn=config.sns_out_arn,
            MessageAttributes={
                "aoi": {"DataType": "String", "StringValue": aoi},
                "h3_indices": {
                    "DataType": "String.Array",
                    "StringValue": json.dumps(part_keys),
                },
                "status": {"DataType": "String", "StringValue": "succeeded"},
            },
            Message=f"AOI: {aoi} added",
        )
        print(f"Added AOI response: {publish_res}")
    except Exception as e:
        publish_res = config.sns.publish(
            TopicArn=config.sns_out_arn,
            MessageAttributes={
                "aoi": {"DataType": "String", "StringValue": aoi},
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            Message=f"Error:{e}",
        )
        print(f"Error response: {publish_res}")


def db_add_handler(event, context):
    config = CloudConfig()
    print("event", event)
    pq_df = get_pq_df(event, config)

    pq_df.apply(apply_add, config=config, axis=1)

def apply_polygon(df: pd.DataFrame):
    polygon_str = df.geometry
    polygon = from_geojson(polygon_str)
    return cover_shape_h3(polygon, 3)

def apply_compare(df: pd.DataFrame, config: CloudConfig):
    name = uuid4()
    try:
        polygon_str = df.geometry
        polygon = from_geojson(polygon_str)
        aoi_info = get_db_comp(polygon, config)
        aoi_impact_list = []
        upoly = Polygon(polygon)
        for k, v in aoi_info.items():
            dbpoly = from_geojson(v)
            if not upoly.disjoint(dbpoly):
                aoi_impact_list.append(k)
        if not aoi_impact_list:
            return pd.NA
        sns_request_json = {
            "Id": f"{df.pk_and_model}-{name}",
            "MessageAttributes":{
                "tile_id": {
                    "DataType": "String",
                    "StringValue": df.pk_and_model,
                },
                "aois": {
                    "DataType": "String.Array",
                    "StringValue": json.dumps(aoi_impact_list),
                },
                "status": {"DataType": "String", "StringValue": "succeeded"},
            },
            "Message": f"{df.pk_and_model}-{name}",
            'MessageGroupId': 'compare'
        }
    except Exception as e:
        sns_request_json = {
            "Id": f"{df.pk_and_model}-{name}",
            "MessageAttributes":{
                "tile_id": {
                    "DataType": "String",
                    "StringValue": df.pk_and_model,
                },
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            "Message": f"{df.pk_and_model}-{name}",
            'MessageGroupId': 'compare'

        }
    return sns_request_json

def set_dynamo_config():
    return Config(
        retries={"max_attempts": 8, "mode": "adaptive"},
        tcp_keepalive=True,
        read_timeout=30,
        connect_timeout=10
    )

def chunk(iterable, size):
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            return
        yield batch

def db_comp_handler(event, context):
    dynamo_cfg = set_dynamo_config()
    config = CloudConfig(dynamo_cfg)
    pq_df = get_pq_df(event, config)
    # query_items =
    keys_list = pq_df.apply(apply_polygon, axis=1)
    
    for chunk in batched(keys_list, 25):
        exprs = []
        for keys in chunk:
            exprs.append({'Statement':f'SELECT * FROM {config.table_name} WHERE h3_id IN {keys}'})
        config.dynamo.batch_execute_statement(Statements=exprs)

    pq_df_results = pq_df.apply(apply_compare, config=config, axis=1)
    pq_df_results = pq_df_results.dropna()

    sns_messages = pq_df_results.tolist()

    for chunk in batched(sns_messages, 10):
        resp = config.sns.publish_batch(
            TopicArn=config.sns_out_arn,
            PublishBatchRequestEntries=chunk
        )
        if resp.get("Failed"):
            pass
    # return number of tiles which were associated with at least 1 subscription
    return len(pq_df_results.index)