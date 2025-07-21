resource aws_dynamodb_table geodata_table {
    name = "tns_geodata_table"
    billing_mode = "PAY_PER_REQUEST"
    hash_key = "h3_id"
    range_key = "aoi_and_model"

    attribute {
        name = "h3_id"
        type = "S"
    }

    attribute {
        name = "aoi_and_model"
        type = "S"
    }

    global_secondary_index {
        name = "aoi_and_model"
        hash_key = "aoi_and_model"
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