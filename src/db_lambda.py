import json
import h3
import os
import boto3
from osgeo import gdal, ogr

import traceback
from botocore.config import Config
from uuid import uuid4
from itertools import batched

gdal.UseExceptions()
H3_RESOLUTION=3
MAX_H3_IDS_SQL=50
SNS_BATCH_LIMIT=10

def set_dynamo_config():
    return Config(
        retries={"max_attempts": 8, "mode": "adaptive"},
        tcp_keepalive=True,
        read_timeout=30,
        connect_timeout=10,
    )


class CloudConfig:
    def __init__(self, dynamo_cfg: dict = None):
        env_keys = os.environ.keys()
        if "SNS_OUT_ARN" in env_keys:
            self.sns_out_arn = os.environ["SNS_OUT_ARN"]
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
            self.dynamo = boto3.client(
                "dynamodb", region_name=self.region, config=dynamo_cfg
            )
        else:
            self.dynamo = boto3.client("dynamodb", region_name=self.region)

        self.sns = boto3.client("sns", region_name=self.region)
        self.s3 = boto3.client("s3", region_name=self.region)
        self.sqs = boto3.client("sqs", region_name=self.region)


def delete_sqs_message(e, config: CloudConfig):
    print("deleting from this event", e)
    source_arn = e["eventSourceARN"]
    print("source_arn:", source_arn)
    queue_name = source_arn.split(":")[-1]
    print("queue name:", queue_name)
    queue_url = config.sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    receipt_handle = e["receiptHandle"]
    return config.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def get_gdal_layers(event, config: CloudConfig):
    datasets = []
    filenames = []
    for sqs_event in event["Records"]:
        body = json.loads(sqs_event["body"])
        message = json.loads(body["Message"])
        # skip TestEvent
        if "Event" in message and message["Event"] == "s3:TestEvent":
            delete_sqs_message(sqs_event, config)
            continue
        for sns_event in message["Records"]:
            s3_info = sns_event["s3"]
            bucket = s3_info["bucket"]["name"]
            key = s3_info["object"]["key"]
            vsis_path = f"/vsis3/{bucket}/{key}"

            dataset = gdal.OpenEx(vsis_path)

            datasets.append(dataset)
            filenames.append(vsis_path)

        delete_sqs_message(sqs_event, config)

    return zip(datasets, filenames)


def cover_shape_h3(geojson_dict, resolution: int):
    """
    Return the set of H3 cells at the specified resolution which completely
    cover the input shape.
    """

    # h3 automatically handles Polygon and Multipolygon
    h3_shape = h3.geo_to_h3shape(geojson_dict)
    # get h3 indices
    # TODO check on overlap, could we do bbox_overlap
    cells = h3.polygon_to_cells_experimental(h3_shape, resolution, "overlap")

    return cells


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
        i.pop("polygon")
        config.dynamo.delete_item(TableName=config.table_name, Key=i)
    return


def apply_delete(feature: ogr.Feature, filename: str, config: CloudConfig):
    try:
        aoi = feature.pk_and_model
        delete_if_found(aoi, config)

        publish_res = config.sns.publish(
            TopicArn=config.sns_out_arn,
            MessageAttributes={
                "source_file": {"DataType": "String", "StringValue": filename},
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
                "source_file": {"DataType": "String", "StringValue": filename},
                "aoi": {"DataType": "String", "StringValue": aoi},
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            Message="Failed to delete aoi {aoi}",
        )
        print(f"Error response: {publish_res}")


def db_delete_handler(event, context):
    config = CloudConfig()
    print("Event:", json.dumps(event))
    datasets = get_gdal_layers(event, config)
    for ds, filename in datasets:
        layer = ds.GetLayer()
        for feature in layer:
            apply_delete(feature, filename, config)


def apply_add(feature: ogr.Feature, filename:str, config: CloudConfig):
    try:
        geometry = feature.geometry()
        geojson_str = geometry.ExportToJson()
        geojson_dict = json.loads(geojson_str)
        aoi = feature.pk_and_model
        print("polygon_str", geojson_str)
        delete_if_found(aoi, config)

        # create new db entries for aoi-polygon combo
        part_keys = cover_shape_h3(geojson_dict, 3)
        aoi_list = [aoi for s in part_keys]
        keys = zip(part_keys, aoi_list)

        # max length of items in a dynamo batch write call
        for batch in batched(keys, 25):
            request = {f"{config.table_name}": []}
            for pk, aoi in batch:
                request_item = {
                    "PutRequest": {
                        "Item": {
                            "h3_id": {"S": pk},
                            "pk_and_model": {"S": aoi},
                            "polygon": {"S": geojson_str},
                        }
                    }
                }
                request[f"{config.table_name}"].append(request_item)
            config.dynamo.batch_write_item(RequestItems=request)

        publish_res = config.sns.publish(
            TopicArn=config.sns_out_arn,
            MessageAttributes={
                "aoi": {"DataType": "String", "StringValue": aoi},
                "source_file": {"DataType": "String", "StringValue": filename},
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
                "source_file": {"DataType": "String", "StringValue": filename},
                "status": {"DataType": "String", "StringValue": "failed"},
                "error": {"DataType": "String", "StringValue": f"{e.args}"},
            },
            Message="Error.",
        )
        print(f"Error response: {publish_res}")


def db_add_handler(event, context):
    config = CloudConfig()
    print("Event:", json.dumps(event))
    datasets = get_gdal_layers(event, config)
    for ds, filename in datasets:
        layer = ds.GetLayer()
        for feature in layer:
            apply_add(feature, filename, config)


# Note: aois in db will have pk_and_model attribute that corresponds with GRiD's
# AOI convention of {ModelPrefix}_{SubscriptionPK}, whereas tiles will also
# have a pk_and_model attribute, but corresponds with GRiD's Tile convention of
# {TileModel}_{TilePK}. All aois will come in in EPSG:4326
# TODO may want to change names to clear on aoi/tile pk_and_model attributes,
# but leaving for now in interest of time.
def db_comp_handler(event, context):
    # create configs
    dynamo_cfg = set_dynamo_config()
    config = CloudConfig(dynamo_cfg)
    print("Event:", json.dumps(event))

    # grab data fraom s3
    datasets = get_gdal_layers(event, config)

    # iterate gdal dataset layers
    # create list of h3_ids from geometry
    # use h3_ids to query dynamo
    # filter layers by polygons produced from dynamo
    sns_messages = []
    for ds, filename in datasets:
        # iterate tiles and find h3 cover
        try:
            # create tracking variables
            aois_affected_map: dict[str, list] = {}
            h3_ids = []
            layer = ds.GetLayer()
            for feature in layer:
                geometry = feature.geometry()
                if geometry is not None:
                    polygon_str = feature.geometry().ExportToJson()
                    # TODO double check this needs to be a polygon
                    # polygon = from_geojson(polygon_str)
                    h3_ids = h3_ids + cover_shape_h3(json.loads(polygon_str), H3_RESOLUTION)
            deduped = list(set(h3_ids))

            # query h3 index
            aoi_poly_map = {}
            # limit of 50 in IN method
            for dd_batched in batched(deduped, MAX_H3_IDS_SQL):
                statement = (
                    f'SELECT * FROM "{config.table_name}"."h3_idx" '
                    f"WHERE h3_id IN {dd_batched}"
                )
                res = config.dynamo.execute_statement(Statement=statement)
                for aoi in res["Items"]:
                    ogr_geom = ogr.CreateGeometryFromJson(aoi["polygon"]["S"])
                    aoi_poly_map[aoi["pk_and_model"]["S"]] = ogr_geom

            # create ogr geometry from polygons
            for aoi_pk, geom in aoi_poly_map.items():
                # TODO check if gdal takes reference to this
                # if so, use gdal Clone
                layer.SetSpatialFilter(geom)
                tile_pks = [feature.pk_and_model for feature in layer]
                if aoi_pk in aois_affected_map:
                    aois_affected_map[aoi_pk] = aois_affected_map[aoi_pk] + tile_pks
                else:
                    aois_affected_map[aoi_pk] = tile_pks

            if aois_affected_map:
                print(f'AOIs found. Pushing to SNS Topic {config.sns_out_arn}.')
            # maximum number of tiles that can be implicated here is 10k in the end
            # should not exceed the sqs limits
            for aoi_pk, tile_list in aois_affected_map.items():
                name = f"{aoi_pk}-{uuid4()}"
                sns_request_json = {
                    "Id": name,
                    "MessageAttributes": {
                        "source_file": {"DataType": "String", "StringValue": filename},
                        "aoi_id": {
                            "DataType": "String",
                            "StringValue": aoi_pk,
                        },
                        "tiles": {
                            "DataType": "String.Array",
                            "StringValue": json.dumps(tile_list),
                        },
                        "status": {"DataType": "String", "StringValue": "succeeded"},
                    },
                    "Message": name,
                    "MessageGroupId": "compare",
                }
                sns_messages.append(sns_request_json)
        except Exception as e:
            name = uuid4()
            sns_request_json = {
                "Id": f"{name}",
                "MessageAttributes": {
                    "source_file": {"DataType": "String", "StringValue": filename},
                    "status": {"DataType": "String", "StringValue": "failed"},
                    "error": {"DataType": "String", "StringValue": f"{e.args}"},
                },
                "Message": f"{name}",
                "MessageGroupId": "compare",
            }
            sns_messages.append(sns_request_json)


    for batch in batched(sns_messages, SNS_BATCH_LIMIT):
        resp = config.sns.publish_batch(
            TopicArn=config.sns_out_arn, PublishBatchRequestEntries=batch
        )
        if resp.get("Failed"):
            raise ValueError(resp)
        else:
            print("SNS response:", resp)

    # return number of tiles which were associated with at least 1 subscription
    return sns_messages
