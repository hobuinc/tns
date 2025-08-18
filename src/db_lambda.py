import json
import boto3
import os
import pandas as pd
import io
import h3
from line_profiler import profile
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config
from itertools import islice

from shapely.geometry import Polygon, MultiPolygon
from shapely import from_geojson

# reuse connections and variables
def set_globals(dynamo_cfg=None):
    global SNS
    global DYNAMO
    global SNS_OUT_ARN
    global TABLE_NAME
    global REGION
    global DDB_PROJECTION
    global unique_id_count
    global S3
    SNS_OUT_ARN = os.environ["SNS_OUT_ARN"]
    TABLE_NAME = os.environ["DB_TABLE_NAME"]
    REGION = os.environ["AWS_REGION"]
    SNS = boto3.client("sns", region_name=REGION)
    S3 = boto3.client("s3", region_name=REGION)
    DDB_PROJECTION = "h3_id, pk_and_model"
    unique_id_count = 0
    if dynamo_cfg:
        DYNAMO = boto3.client("dynamodb", region_name=REGION, config=dynamo_cfg)
    else:
        DYNAMO = boto3.client("dynamodb", region_name=REGION)


@profile
def s3_read_parquet(sns_event):
    s3_info = sns_event["s3"]
    bucket = s3_info["bucket"]["name"]
    key = s3_info["object"]["key"]
    file = S3.get_object(Bucket=bucket, Key=key)["Body"]
    pq_bytes = io.BytesIO(file.read())
    return pd.read_parquet(pq_bytes)


@profile
def delete_sqs_message(e):
    sqs = boto3.client("sqs", region_name=REGION)
    print("deleting from this event", e)
    source_arn = e["eventSourceARN"]
    print("source_arn:", source_arn)
    queue_name = source_arn.split(":")[-1]
    print("queue name:", queue_name)
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    receipt_handle = e["receiptHandle"]
    return sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


@profile
def get_pq_df(event):
    pq_dfs = []
    for sqs_event in event["Records"]:
        body = json.loads(sqs_event["body"])
        message = json.loads(body["Message"])
        # skip TestEvent
        if "Event" in message and message["Event"] == "s3:TestEvent":
            delete_sqs_message(sqs_event)
            continue
        for sns_event in message["Records"]:
            pq_df = s3_read_parquet(sns_event)
            pq_dfs.append(pq_df)
        delete_sqs_message(sqs_event)

    return pd.concat(pq_dfs) if pq_dfs else pq_dfs


@profile
def cover_polygon_h3(polygon, resolution: int):
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


@profile
def cover_shape_h3(shape, resolution: int):
    """
    Return the set of H3 cells at the specified resolution which completely cover the input shape.
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
        raise ValueError(f"Error finding indices for geometry.", repr(e))

    return list(result_set)


@profile
def get_db_comp(polygon):
    part_keys = cover_shape_h3(polygon, 3)

    aoi_info = {}
    for h3_id in part_keys:
        res = DYNAMO.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="h3_id = :h3_val",
            ExpressionAttributeValues={":h3_val": {"S": h3_id}},
        )
        for i in res["Items"]:
            aname = i["pk_and_model"]["S"]
            if aname not in aoi_info.keys():
                aoi_info[aname] = i["polygon"]["S"]

    return aoi_info


@profile
def get_entries_by_aoi(aoi):
    a = DYNAMO.scan(
        TableName=TABLE_NAME,
        IndexName="pk_and_model",
        FilterExpression="pk_and_model = :pk_and_model",
        ExpressionAttributeValues={
            ":pk_and_model": {"S": aoi},
        },
    )
    return a


@profile
def delete_if_found(aoi):
    scanned = get_entries_by_aoi(aoi)
    if scanned["Count"] == 0:
        return
    for i in scanned["Items"]:
        DYNAMO.delete_item(TableName=TABLE_NAME, Key=i)
    return


@profile
def apply_delete(df):
    try:
        aoi = df.pk_and_model
        delete_if_found(aoi)

        publish_res = SNS.publish(
            TopicArn=SNS_OUT_ARN,
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
        publish_res = SNS.publish(
            TopicArn=SNS_OUT_ARN,
            MessageAttributes={
                "aoi": {"DataType": "String", "StringValue": aoi},
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            Message="Failed to deleted aoi {aoi}",
        )
        print(f"Error response: {publish_res}")


@profile
def db_delete_handler(event, context):
    set_globals()
    print("event", event)
    pq_df = get_pq_df(event)
    pq_df.apply(apply_delete, axis=1)


@profile
def apply_add(df):
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

        request = {
            f"{TABLE_NAME}": [
                {
                    "PutRequest": {
                        "Item": {
                            "h3_id": {"S": pk},
                            "pk_and_model": {"S": aoi},
                            "polygon": {"S": polygon_str},
                        }
                    }
                }
                for pk, aoi in keys
            ]
        }
        DYNAMO.batch_write_item(RequestItems=request)

        publish_res = SNS.publish(
            TopicArn=SNS_OUT_ARN,
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
        publish_res = SNS.publish(
            TopicArn=SNS_OUT_ARN,
            MessageAttributes={
                "aoi": {"DataType": "String", "StringValue": aoi},
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            Message=f"Error:{e}",
        )
        print(f"Error response: {publish_res}")


@profile
def db_add_handler(event, context):
    set_globals()
    print("event", event)
    pq_df = get_pq_df(event)

    pq_df.apply(apply_add, axis=1)

@profile
def apply_compare(df):
    try:
        print('running apply_compare')
        polygon_str = df.geometry
        polygon = from_geojson(polygon_str)
        aoi_info = get_db_comp(polygon)
        aoi_impact_list = []
        upoly = Polygon(polygon)
        for k, v in aoi_info.items():
            dbpoly = from_geojson(v)
            if not upoly.disjoint(dbpoly):
                aoi_impact_list.append(k)
        if not aoi_impact_list:
            return pd.NA
        global unique_id_count
        sns_request_json = {
            "Id": f"{df.pk_and_model}-{unique_id_count}",
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
            "Message" :json.dumps(aoi_impact_list),
        }
        unique_id_count = unique_id_count + 1
        return sns_request_json
    except Exception as e:
        print(f"encountered exception during apply_compare: {e}")
        SNS.publish(
            TopicArn=SNS_OUT_ARN,
            MessageAttributes={
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            Message=f"Error:{e.args}",
        )
        return pd.NA

@profile
def set_dynamo_config(query_workers):
    return Config(
        retries={"max_attempts": 8, "mode": "adaptive"},
        tcp_keepalive=True,
        read_timeout=30,
        connect_timeout=10
    )

@profile
def chunk(iterable, size):
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            return
        yield batch

@profile
def db_comp_handler(event, context):
    QUERY_WORKERS = 2
    dynamo_cfg = set_dynamo_config(QUERY_WORKERS)
    set_globals(dynamo_cfg)
    pq_df = get_pq_df(event)
    pq_df_results = pq_df.apply(apply_compare, axis=1)
    pq_df_results = pq_df_results.dropna()

    sns_messages = pq_df_results.tolist()

    for batch in chunk(sns_messages, 10):
        resp = SNS.publish_batch(
            TopicArn=SNS_OUT_ARN,
            PublishBatchRequestEntries=batch
        )
        if resp.get("Failed"):
            pass
    # return number of tiles which were associated with at least 1 subscription
    return len(pq_df_results.index)