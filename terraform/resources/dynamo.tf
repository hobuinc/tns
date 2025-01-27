resource aws_dynamodb_table geodata_table {
    name = "tns_geodata_table"
    billing_mode = "PAY_PER_REQUEST"
    hash_key = "h3_base_idx"
    range_key = "h3_res_3"

    attribute {
        name = "h3_base_idx"
        type = "N"
    }

    attribute {
        name = "h3_res_3"
        type = "S"
    }

}

output table_name {
    value = aws_dynamodb_table.geodata_table.name
}