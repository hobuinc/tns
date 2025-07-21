import json
import boto3
import os
import pandas as pd
import io

from shapely.geometry import Polygon, MultiPolygon
from shapely import from_geojson


def s3_read_parquet(sns_event, s3):
    s3_info = sns_event["s3"]
    bucket = s3_info["bucket"]["name"]
    key = s3_info["object"]["key"]
    file = s3.get_object(Bucket=bucket, Key=key)["Body"]
    pq_bytes = io.BytesIO(file.read())
    return pd.read_parquet(pq_bytes)


def delete_sqs_message(e, region):
    sqs = boto3.client("sqs", region_name=region)
    print("deleting from this event", e)
    source_arn = e["eventSourceARN"]
    print("source_arn:", source_arn)
    queue_name = source_arn.split(":")[-1]
    print("queue name:", queue_name)
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    receipt_handle = e["receiptHandle"]
    return sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def get_pq_df(event):
    region = os.environ["AWS_REGION"]
    s3 = boto3.client("s3")
    pq_dfs = []
    for sqs_event in event["Records"]:
        body = json.loads(sqs_event["body"])
        message = json.loads(body["Message"])
        for sns_event in message["Records"]:
            pq_df = s3_read_parquet(sns_event, s3)
            pq_dfs.append(pq_df)
        delete_sqs_message(sqs_event, region)

    return pd.concat(pq_dfs)


def cover_polygon_h3(polygon, resolution: int):
    """
    Return the set of H3 cells at the specified resolution which completely cover the input polygon.
    """
    import h3

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


def get_db_comp(dynamo, polygon, table_name):
    part_keys = cover_shape_h3(polygon, 3)

    aoi_info = {}
    for h3_id in part_keys:
        res = dynamo.query(
            TableName=table_name,
            KeyConditionExpression="h3_id = :h3_val",
            ExpressionAttributeValues={":h3_val": {"S": h3_id}},
        )
        for i in res["Items"]:
            aname = i["aoi_and_model"]["S"]
            if aname not in aoi_info.keys():
                aoi_info[aname] = i["polygon"]["S"]

    return aoi_info


def get_entries_by_aoi(dynamo, table_name, aoi):
    a = dynamo.scan(
        TableName=table_name,
        IndexName="aoi_and_model",
        FilterExpression="aoi_and_model = :aoi_and_model",
        ExpressionAttributeValues={
            ":aoi_and_model": {"S": aoi},
        },
    )
    return a


def delete_if_found(dynamo, table_name, aoi):
    scanned = get_entries_by_aoi(dynamo, table_name, aoi)
    if scanned["Count"] == 0:
        return
    for i in scanned["Items"]:
        dynamo.delete_item(TableName=table_name, Key=i)
    return


def apply_delete(df):
    region = os.environ["AWS_REGION"]
    table_name = os.environ["DB_TABLE_NAME"]
    sns_out_arn = os.environ["SNS_OUT_ARN"]

    dynamo = boto3.client("dynamodb", region_name=region)
    sns = boto3.client("sns", region_name=region)
    try:
        aoi = df.aoi_and_model
        delete_if_found(dynamo, table_name, aoi)

        publish_res = sns.publish(
            TopicArn=sns_out_arn,
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
        publish_res = sns.publish(
            TopicArn=sns_out_arn,
            MessageAttributes={
                "aoi": {"DataType": "String", "StringValue": aoi},
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            Message="Failed to deleted aoi {aoi}",
        )
        print(f"Error response: {publish_res}")


def db_delete_handler(event, context):
    print("event", event)
    pq_df = get_pq_df(event)
    pq_df.apply(apply_delete, axis=1)


def apply_add(df):
    region = os.environ["AWS_REGION"]
    table_name = os.environ["DB_TABLE_NAME"]
    sns_out_arn = os.environ["SNS_OUT_ARN"]

    dynamo = boto3.client("dynamodb", region_name=region)
    sns = boto3.client("sns", region_name=region)
    try:
        polygon_str = df.geometry
        aoi = df.aoi_and_model
        print("polygon_str", polygon_str)
        polygon = from_geojson(polygon_str)
        print("polygon")
        delete_if_found(dynamo, table_name, aoi)
        # create new db entries for aoi-polygon combo
        part_keys = cover_shape_h3(polygon, 3)
        aoi_list = [aoi for s in part_keys]
        keys = zip(part_keys, aoi_list)

        request = {
            f"{table_name}": [
                {
                    "PutRequest": {
                        "Item": {
                            "h3_id": {"S": pk},
                            "aoi_and_model": {"S": aoi},
                            "polygon": {"S": polygon_str},
                        }
                    }
                }
                for pk, aoi in keys
            ]
        }
        dynamo.batch_write_item(RequestItems=request)

        publish_res = sns.publish(
            TopicArn=sns_out_arn,
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
        publish_res = sns.publish(
            TopicArn=sns_out_arn,
            MessageAttributes={
                "aoi": {"DataType": "String", "StringValue": aoi},
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            Message=f"Error:{e}",
        )
        print(f"Error response: {publish_res}")


def db_add_handler(event, context):
    print("event", event)
    pq_df = get_pq_df(event)

    pq_df.apply(apply_add, axis=1)

def apply_compare(df):
    try:
        region = os.environ["AWS_REGION"]
        sns_out_arn = os.environ["SNS_OUT_ARN"]
        table_name = os.environ["DB_TABLE_NAME"]

        sns = boto3.client("sns", region_name=region)
        dynamo = boto3.client("dynamodb", region_name=region)
        polygon_str = df.geometry
        polygon = from_geojson(polygon_str)

        aoi_info = get_db_comp(dynamo, polygon, table_name)

        aoi_impact_list = []
        upoly = Polygon(polygon)
        for k, v in aoi_info.items():
            dbpoly = from_geojson(v)
            if not upoly.disjoint(dbpoly):
                aoi_impact_list.append(k)

        print("aois found: ", aoi_impact_list)
        publish_res = sns.publish(
            TopicArn=sns_out_arn,
            MessageAttributes={
                "aois": {
                    "DataType": "String.Array",
                    "StringValue": json.dumps(aoi_impact_list),
                },
                "status": {"DataType": "String", "StringValue": "succeeded"},
            },
            Message=json.dumps(aoi_impact_list),
        )
        print(f"Publish response: {publish_res}")
        return aoi_impact_list
    except Exception as e:
        publish_res = sns.publish(
            TopicArn=sns_out_arn,
            MessageAttributes={
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            Message=f"Error:{e.args}",
        )

def db_comp_handler(event, context):
    print("event", event)
    pq_df = get_pq_df(event)
    return pq_df.apply(apply_compare, axis=1)