#!/bin/bash

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
source "$SCRIPT_DIR/../../../../scripts/pixi_env"

eventfilename=$1

FUNCTION_NAME=$(cat ../terraform/terraform.tfstate | tns_pixi_exec jq '.outputs.info_lambda_name.value // empty' -r)

if [ -z "$AWS_ACCESS_KEY_ID" ]
then
    echo "AWS_ACCESS_KEY_ID must be set in environment!"
    exit 1;
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ]
then
    echo "AWS_SECRET_ACCESS_KEY must be set in environment!"
    exit 1;
fi

tns_pixi_exec aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --invocation-type RequestResponse \
    --payload fileb://$eventfilename \
   response.json
