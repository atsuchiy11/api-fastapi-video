from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from aws_dynamodb_parser import parse

from api.util import timestamp_jst
from api.schema import ReqThreadDelete, ReqThreadPost, ReqThreadPut, Thread
from api.dynamodb import create_dynamodb_client

router = APIRouter()
client = create_dynamodb_client()

#
# routes
#


# OK
@router.get("/thread/{video_id}", response_model=List[Thread])
def get_thread(video_id):
    """Get threads for video from DynamoDB"""

    input = create_query_input(video_id)
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        items.sort(key=lambda x: x["createdAt"], reverse=True)
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/thread")
def post_thread(req_thread: ReqThreadPost):
    """Post thread for video to DynamoDB"""

    input = create_put_item_input(req_thread)
    try:
        res = client.put_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.put("/thread")
def put_thread(req_thread: ReqThreadPut):
    """Put thread for video to DynamoDB"""

    input = create_update_item_input(req_thread)
    try:
        res = client.update_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


#
# utilities
#
def create_query_input(video_id):
    return {
        "TableName": "primary_table",
        "KeyConditionExpression": "#PK = :PK",
        "FilterExpression": "#key = :key And #invalid = :invalid",
        # "ScanIndexForward": False,
        "ExpressionAttributeNames": {
            "#PK": "PK",
            "#key": "indexKey",
            "#invalid": "invalid",
        },
        "ExpressionAttributeValues": {
            ":PK": {"S": f"/videos/{video_id}"},
            ":key": {"S": "Thread"},
            ":invalid": {"BOOL": False},
        },
    }


def create_put_item_input(thread):
    if thread.thread:
        thread_post = f"{thread.thread}_{timestamp_jst()}"
        created_at = thread.thread
    else:
        thread_post = f"{timestamp_jst()}_{timestamp_jst()}"
        created_at = timestamp_jst()

    return {
        "TableName": "primary_table",
        "Item": {
            "PK": {"S": thread.video},
            "SK": {"S": thread_post},
            "indexKey": {"S": "Thread"},
            "createdAt": {"S": created_at},
            "createdUser": {"S": thread.user},
            "body": {"S": thread.body},
            "invalid": {"BOOL": False},
        },
    }


def create_update_item_input(thread):
    if thread.body:
        return {
            "TableName": "primary_table",
            "Key": {"PK": {"S": thread.video}, "SK": {"S": thread.id}},
            "UpdateExpression": "SET #body = :body",
            "ExpressionAttributeNames": {"#body": "body"},
            "ExpressionAttributeValues": {
                ":body": {"S": thread.body},
            },
        }
    else:
        return {
            "TableName": "primary_table",
            "Key": {"PK": {"S": thread.video}, "SK": {"S": thread.id}},
            "UpdateExpression": "SET #invalid = :invalid",
            "ExpressionAttributeNames": {"#invalid": "invalid"},
            "ExpressionAttributeValues": {
                ":invalid": {"BOOL": thread.invalid},
            },
        }


# スレッドは削除しない。無効フラグを立てて非表示にする
@router.delete("/thread")
def delete_thread(req_thread: ReqThreadDelete):
    """Delete thread for video from DynamoDB"""

    input = {
        "TableName": "primary_table",
        "Key": {"PK": {"S": req_thread.video}, "SK": {"S": req_thread.id}},
    }
    try:
        res = client.delete_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
