import json
import pandas as pd
import io
import h3
import os
import boto3
from osgeo import gdal

import traceback
from botocore.config import Config
from uuid import uuid4
from itertools import islice, batched, chain

from shapely.geometry import Polygon, MultiPolygon
from shapely import from_geojson

class CloudConfig():
    def __init__(self, dynamo_cfg:dict=None):
        env_keys = os.environ.keys()
        if 'SNS_OUT_ARN' in env_keys:
            self.sns_out_arn = os.environ['SNS_OUT_ARN']
        else:
            self.sns_out_arn = None

        if "DB_TABLE_NAME" in env_keys:
            self.table_name = os.environ["DB_TABLE_NAME"]
        else:
            self.table_name = None

        if "AWS_REGION" in env_keys:
            self.region = os.environ["AWS_REGION"]
        else:
            self.region = "us-west-2"

        if dynamo_cfg is not None:
            self.dynamo = boto3.client("dynamodb", region_name=self.region, config=dynamo_cfg)
        else:
            self.dynamo = boto3.client("dynamodb", region_name=self.region)

        self.sns = boto3.client("sns", region_name=self.region)
        self.s3 = boto3.client("s3", region_name=self.region)
        self.sqs = boto3.client("sqs", region_name=self.region)


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
        # delete_sqs_message(sqs_event, config)

    return pd.concat(pq_dfs) if pq_dfs else pq_dfs


def cover_polygon_h3(h3_shape: h3.H3Shape, resolution: int):
    """
    Return the set of H3 cells at the specified resolution which completely cover the input polygon.
    """
    return h3.polygon_to_cells_experimental(h3_shape, resolution, 'overlap')


def cover_shape_h3(geojson_dict: dict, resolution: int):
    """
    Return the set of H3 cells at the specified resolution which completely
    cover the input shape.
    """
    result_set = set()

    try:
        # h3 automatically handles Polygon and Multipolygon
        h3_shape = h3.geo_to_h3shape(geojson_dict)
        # get h3 indices
        result_set = set(cover_polygon_h3(h3_shape, resolution))
    except Exception as e:
        raise ValueError("Error finding indices for geometry.", repr(e))

    return list(result_set)


def get_db_comp(geojson_dict: dict, config: CloudConfig):
    """Query Dynamo for entries that overlap with a geometry."""
    part_keys = cover_shape_h3(geojson_dict, 3)
    aoi_info = {}
    res = config.dynamo.execute_statement(
        Statement='SELECT * FROM tns_geodata_table'
            f' WHERE h3_id in {part_keys}'
    )
    print('db_comp_res', res)
    for i in res["Items"]:
        aname = i["pk_and_model"]["S"]
        if aname not in aoi_info.keys():
            aoi_info[aname] = i["polygon"]["S"]

    return aoi_info

def get_entries_by_aoi_test_handler(aoi: str):
    config = CloudConfig()
    return get_entries_by_aoi(aoi, config)

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


def delete_if_found(aoi: str, config: CloudConfig | None = None):
    """Delete entries from dynamo."""
    if config is None:
        config = CloudConfig()
    scanned = get_entries_by_aoi(aoi, config)
    if scanned["Count"] == 0:
        return
    for i in scanned["Items"]:
        i.pop('polygon')
        config.dynamo.delete_item(TableName=config.table_name, Key=i)
    return


def apply_delete(df: pd.DataFrame, config: CloudConfig):
    try:
        aoi = df.pk_and_model
        delete_if_found(aoi, config)

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
        geojson_dict = json.loads(polygon_str)
        aoi = df.pk_and_model
        print("polygon_str", polygon_str)
        delete_if_found(aoi, config)

        # create new db entries for aoi-polygon combo
        part_keys = cover_shape_h3(geojson_dict, 3)
        aoi_list = [aoi for s in part_keys]
        keys = zip(part_keys, aoi_list)

        # max length of items in a dynamo batch write call
        for batch in batched(keys, 25):
            request = {
                f"{config.table_name}": []
            }
            for pk, aoi in batch:
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
        traceback.print_exc()
        publish_res = config.sns.publish(
            TopicArn=config.sns_out_arn,
            MessageAttributes={
                "aoi": {"DataType": "String", "StringValue": aoi},
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            Message="Error.",
        )
        print(f"Error response: {publish_res}")


def db_add_handler(event, context):
    config = CloudConfig()
    print("event", event)
    pq_df = get_pq_df(event, config)

    pq_df.apply(apply_add, config=config, axis=1)

def apply_compare(df: pd.DataFrame, aois: pd.DataFrame, config: CloudConfig):
    name = uuid4()
    filtered_aois = aois[aois.h3_id.isin(df.h3_id)]
    try:
        polygon_str = df.loc[0].geometry
        tile_polygon = from_geojson(polygon_str)
        def aoi_comp(item, tp):
            aoi_polygon = from_geojson(item.polygon)
            if not aoi_polygon.disjoint(tp):
                return item

        aoi_impact_list = filtered_aois.apply(aoi_comp, tp=tile_polygon, axis=1)
        if not aoi_impact_list.any():
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
            "Id": f"{name}",
            "MessageAttributes":{
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            "Message": f"{name}",
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

def apply_polygon(df: pd.DataFrame):
    polygon_str = df.geometry
    polygon = from_geojson(polygon_str)
    h3_ids = cover_shape_h3(polygon, 3)
    # return h3_ids
    newdf = pd.DataFrame(data = [
        {
            'pk_and_model' : df.pk_and_model,
            'geometry': df.geometry,
            'h3_id': h3
        } for h3 in h3_ids
    ])

    return newdf

# Note: aois in db will have pk_and_model attribute that corresponds with GRiD's
# AOI convention of {ModelPrefix}_{SubscriptionPK}, whereas tiles will also
# have a pk_and_model attribute, but corresponds with GRiD's Tile convention of
# {TileModel}_{TilePK}.
# TODO may want to change names to clear on aoi/tile pk_and_model attributes,
# but leaving for now in interest of time.
def db_comp_handler(event, context):
    import tracemalloc
    # create configs
    tracemalloc.start()
    try:
        dynamo_cfg = set_dynamo_config()
        config = CloudConfig(dynamo_cfg)
        print('Event:', event)

        # grab data fraom s3
        pq_df = get_pq_df(event, config)

        #attach keys to respective tiles/geometries
        data = []
        for idx, row in pq_df.iterrows():
            polygon_str = row.geometry
            polygon = from_geojson(polygon_str)
            h3_ids = cover_shape_h3(polygon, 3)
            for h3_id in h3_ids:
                data.append(
                    {
                        'pk_and_model' : row.pk_and_model,
                        'geometry': row.geometry,
                        'h3_id': h3_id
                    }
                )
        new_df = pd.DataFrame(data=data)

        # deduplicate keys
        deduped = list(new_df.h3_id.drop_duplicates())

        # query h3 index
        statement = (f'SELECT * FROM "{config.table_name}"."h3_idx" '
                    f'WHERE h3_id IN {deduped}')
        res = config.dynamo.execute_statement(Statement=statement)

        # get items and make into dataframe for easier manipulation
        aois = res['Items']
        aois_data = [
            {
                'h3_id': i['h3_id']['S'],
                'polygon': i['polygon']['S'],
                'pk_and_model': i['pk_and_model']['S']
            } for i in aois
        ]
        aois_df = pd.DataFrame(data=aois_data).set_index('pk_and_model')
        print('4')
        db_h3_results = [aoi['h3_id']['S'] for aoi in aois]
        print('5')

        # associate matched h3_id with tile id and tile polygon
        print('6')
        tiles = (new_df[new_df.h3_id.isin(db_h3_results)]
            .groupby('pk_and_model', group_keys=False)
            .apply(apply_compare, aois=aois_df, config=config))
        print('7')

        tiles = tiles.dropna()
        print('9')
        sns_messages = tiles.tolist()
        print('10')

        for chunk in batched(sns_messages, 10):
            resp = config.sns.publish_batch(
                TopicArn=config.sns_out_arn,
                PublishBatchRequestEntries=chunk
            )
            if resp.get("Failed"):
                raise ValueError(resp)
            print('10')

        # return number of tiles which were associated with at least 1 subscription
        return len(sns_messages)
    except Exception as e:
        print('Exception found:')
        print(e.args)
        raise e
