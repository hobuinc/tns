resource aws_dynamodb_table geodata_table {
    name = "tns_geodata_table"
    billing_mode = "PAY_PER_REQUEST"
    hash_key = "h3_idx"
    range_key = "aoi_id"

    attribute {
        name = "h3_idx"
        type = "S"
    }

    attribute {
        name = "aoi_id"
        type = "N"
    }

    global_secondary_index {
        name = "aois_index"
        hash_key = "aoi_id"
        non_key_attributes = ["geometry"]
        projection_type = "INCLUDE"
    }

}

output table_name {
    value = aws_dynamodb_table.geodata_table.name
}

output table_arn {
    value = aws_dynamodb_table.geodata_table.arn
}