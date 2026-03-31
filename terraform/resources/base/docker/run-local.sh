#!/bin/bash

# ./run-local.sh /var/task/python-entry.sh pdal_lambda.ecr.info.handler

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
source "$SCRIPT_DIR/../../../../scripts/pixi_env"

entrypoint="$1"
command="$2"

CONTAINER=$(cd ../../../ && tns_pixi_exec terraform output --json | tns_pixi_exec jq '.container.value' -r)

REGION=$AWS_DEFAULT_REGION
if [ -z "$REGION" ]
then
    echo "AWS_DEFAULT_REGION must be set!"
    exit 1;
fi

LOCALPORT=9000
REMOTEPORT=8080

KEY_ID=$(tns_pixi_exec aws --profile "$AWS_DEFAULT_PROFILE" configure get aws_access_key_id)
SECRET_ID=$(tns_pixi_exec aws --profile "$AWS_DEFAULT_PROFILE" configure get aws_secret_access_key)
echo $KEY_ID
echo $SECRET_ID


echo "Starting container $CONTAINER"

if [ -z "$entrypoint" ]
then
    echo "executing default entrypoint using $command"
    echo "docker run -p $LOCALPORT:$REMOTEPORT -e AWS_DEFAULT_REGION=$REGION -e AWS_ACCESS_KEY_ID=${KEY_ID} -e AWS_SECRET_ACCESS_KEY=${SECRET_ID} $CONTAINER $command"

    docker run -p $LOCALPORT:$REMOTEPORT \
        --platform linux/x86_64 \
        -e AWS_DEFAULT_REGION=$REGION \
        -e AWS_ACCESS_KEY_ID=${KEY_ID} \
        -e AWS_SECRET_ACCESS_KEY=${SECRET_ID} \
        $CONTAINER "$command"
else
    echo "executing with $entrypoint and command '$command'"
    docker run -p $LOCALPORT:$REMOTEPORT \
        -e AWS_DEFAULT_REGION=$REGION \
        -e AWS_ACCESS_KEY_ID=$KEY_ID \
        -e AWS_SECRET_ACCESS_KEY=$SECRET_ID \
        -t -i \
        -v $(pwd):/data \
        --entrypoint=$entrypoint \
        $CONTAINER "$command"
fi
