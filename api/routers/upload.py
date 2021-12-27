from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from requests import HTTPError
from aws_dynamodb_parser import parse

from api.util import get_today_string, timestamp_jst
from api.schema import (
    ReqUploadStatusPost,
    ReqUploadStatusPut,
    ResUploadStatus,
    UploadStatus,
    UploadFile,
    ReqFile,
)

from api.vimeoapi import create_vimeo_client, get_upload_url
from api.dynamodb import create_dynamodb_client

router = APIRouter()
vimeo_client = create_vimeo_client()
client = create_dynamodb_client()

#
# routes
#


# OK
@router.get("/upload/status", response_model=List[UploadStatus])
def get_upload_status():
    """Get upload status from DynamoDB (tody only)"""

    input = create_query_input()
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        # append index
        for item in items:
            item["id"] = item["PK"]
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/upload", response_model=UploadFile)
def post_upload_file(file: ReqFile):
    """Get upload URL from vimeo"""

    try:
        client_response = get_upload_url(vimeo_client, file)
        return client_response

    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/upload/status", response_model=ResUploadStatus)
def post_upload_status(req_status: ReqUploadStatusPost):
    """Post upload status to DynamoDB"""

    created_at = timestamp_jst()
    input = create_put_item_input(req_status, created_at)
    try:
        client.put_item(**input)
        return {
            "uri": req_status.uri,
            "timestamp": created_at,
            "status": req_status.status,
        }

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.put("/upload/status", response_model=ResUploadStatus)
def put_upload_status(req_status: ReqUploadStatusPut):
    """Put upload status to DynamoDB"""

    input = create_update_item_input(req_status)
    try:
        client.update_item(**input)

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
def create_query_input():
    return {
        "TableName": "primary_table",
        "IndexName": "GSI-1-SK",
        "KeyConditionExpression": "#key = :key",
        "FilterExpression": "begins_with(#today, :today)",
        "ScanIndexForward": False,
        "ExpressionAttributeNames": {"#key": "indexKey", "#today": "SK"},
        "ExpressionAttributeValues": {
            ":key": {"S": "Status"},
            ":today": {"S": get_today_string()},
        },
    }


# OK
def create_put_item_input(status, created_at):
    return {
        "TableName": "primary_table",
        "Item": {
            "PK": {"S": status.uri},
            "SK": {"S": created_at},
            "id": {"S": status.uri},
            "indexKey": {"S": "Status"},
            "name": {"S": status.name},
            "filename": {"S": status.filename},
            "createdAt": {"S": created_at},
            "createdUser": {"S": status.user},
            "status": {"S": status.status},
        },
    }


# OK
def create_update_item_input(status):
    return {
        "TableName": "primary_table",
        "Key": {"PK": {"S": status.uri}, "SK": {"S": status.timestamp}},
        "UpdateExpression": "SET #status = :status",
        "ExpressionAttributeNames": {"#status": "status"},
        "ExpressionAttributeValues": {":status": {"S": status.status}},
    }
