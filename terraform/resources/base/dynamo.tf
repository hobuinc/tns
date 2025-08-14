resource aws_dynamodb_table geodata_table {
    name = "tns_geodata_table"
    billing_mode = "PAY_PER_REQUEST"
    hash_key = "h3_id"
    range_key = "pk_and_model"

    attribute {
        name = "h3_id"
        type = "S"
    }

    attribute {
        name = "pk_and_model"
        type = "S"
    }

    global_secondary_index {
        name = "pk_and_model"
        hash_key = "pk_and_model"
        non_key_attributes = ["geometry"]
        projection_type = "INCLUDE"
    }

    global_secondary_index {
        name = "h3_idx"
        hash_key = "h3_id"
        non_key_attributes = ["pk_and_model"]
        projection_type = "INCLUDE"
    }

}

output table_name {
    value = aws_dynamodb_table.geodata_table.name
}

output table_arn {
    value = aws_dynamodb_table.geodata_table.arn
}