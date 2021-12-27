from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from aws_dynamodb_parser import parse

import uuid
from api.util import timestamp_jst
from api.schema import Likes, ReqLikePost, ReqLikeDelete
from api.dynamodb import create_dynamodb_client

router = APIRouter()
client = create_dynamodb_client()

#
# routes
#


# OK
@router.get("/like/{video_id}", response_model=Likes)
def get_likes(video_id):
    """Get like for video from DynamoDB"""

    input = create_query_input(video_id)
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))

        good = [item for item in items if item.get("like")]
        bad = [item for item in items if not item.get("like")]
        return {"good": good, "bad": bad}

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/like")
def post_like(req_like: ReqLikePost):
    """Post like for video to DynamoDB"""

    input = create_put_item_input(req_like)
    try:
        res = client.put_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.delete("/like")
def delete_like(req_like: ReqLikeDelete):
    """Delete like for video from DynamoDB"""

    input = {
        "TableName": "primary_table",
        "Key": {"PK": {"S": req_like.video}, "SK": {"S": req_like.id}},
    }
    try:
        res = client.delete_item(**input)
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
        "FilterExpression": "#key = :key",
        "ExpressionAttributeNames": {"#PK": "PK", "#key": "indexKey"},
        "ExpressionAttributeValues": {
            ":PK": {"S": f"/videos/{video_id}"},
            ":key": {"S": "Like"},
        },
    }


def create_put_item_input(like):
    id = str(uuid.uuid1())[:8]
    return {
        "TableName": "primary_table",
        "Item": {
            "PK": {"S": like.video},
            "SK": {"S": "like-" + id},
            "indexKey": {"S": "Like"},
            "createdAt": {"S": timestamp_jst()},
            "createdUser": {"S": like.user},
            "like": {"BOOL": like.like},
        },
    }
