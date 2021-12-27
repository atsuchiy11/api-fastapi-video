from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from aws_dynamodb_parser import parse

from api.util import timestamp_jst
from api.schema import Favorite, ReqFavorite
from api.dynamodb import create_dynamodb_client


router = APIRouter()
client = create_dynamodb_client()

#
# routes
#


# OK
@router.get("/favorite/{user_id}", response_model=List[Favorite])
def get_favorite(user_id):
    """Get favorite videos for each user from DynamoDB"""

    input = create_query_input(user_id)
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/favorite")
def post_favorite(req_favorite: ReqFavorite):
    """Post favorite video to DynamoDB"""

    input = create_put_item_input(req_favorite)
    try:
        res = client.put_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.delete("/favorite")
def delete_favorite(req_favorite: ReqFavorite):
    """Delete favorite video from DynamoDB"""

    input = {
        "TableName": "primary_table",
        "Key": {"PK": {"S": req_favorite.user}, "SK": {"S": req_favorite.video}},
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


def create_query_input(user_id):
    return {
        "TableName": "primary_table",
        "KeyConditionExpression": "#PK = :PK",
        "FilterExpression": "#key = :key",
        "ExpressionAttributeNames": {"#PK": "PK", "#key": "indexKey"},
        "ExpressionAttributeValues": {
            ":PK": {"S": user_id},
            ":key": {"S": "Favorite"},
        },
    }


def create_put_item_input(favorite):
    return {
        "TableName": "primary_table",
        "Item": {
            "PK": {"S": favorite.user},
            "SK": {"S": favorite.video},
            "indexKey": {"S": "Favorite"},
            "createdAt": {"S": timestamp_jst()},
        },
    }
