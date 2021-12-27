from typing import List, Optional
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from aws_dynamodb_parser import parse

import uuid
from api.util import get_today_string
from api.schema import UserHistory, ReqHistory
from api.dynamodb import create_dynamodb_client

router = APIRouter()
client = create_dynamodb_client()

#
# routes
#


# OK
@router.get("/history/{user_id}", response_model=List[UserHistory])
def get_history(user_id, limit: Optional[int] = 30):
    """Get histories for each user from DynamoDB"""

    input = create_query_input(user_id, limit)
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        # 念のため
        items.sort(key=lambda x: x["createdAt"], reverse=True)
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.get("/histories")
def get_histories():
    """Get history of all users today"""

    input = {
        "TableName": "primary_table",
        "IndexName": "GSI-1-SK",
        "KeyConditionExpression": "#key = :key And begins_with(#today, :today)",
        "ScanIndexForward": False,
        "ExpressionAttributeNames": {"#key": "indexKey", "#today": "createdAt"},
        "ExpressionAttributeValues": {
            ":key": {"S": "History"},
            ":today": {"S": get_today_string()},
        },
    }
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        # items.sort(key=lambda x: x["createdAt"], reverse=True)
        return {"count": len(items)}

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/history")
def post_history(req_history: ReqHistory):
    """Post history to dynamoDB"""

    input = create_put_item_input(req_history)
    try:
        res = client.put_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


#
# utilities
#
def create_put_item_input(history):
    id = str(uuid.uuid1())[:8]
    return {
        "TableName": "primary_table",
        "Item": {
            "PK": {"S": history.user},
            "SK": {"S": "H-" + id},
            "indexKey": {"S": "History"},
            "createdAt": {"S": history.createdAt},
            "videoUri": {"S": history.video},
            "parse": {"N": str(history.parse)},
            "finishedAt": {"S": history.finishedAt},
            "referrer": {"S": history.referrer},
        },
    }


def create_query_input(user_id, limit):
    return {
        "TableName": "primary_table",
        "KeyConditionExpression": "#PK = :PK",
        "FilterExpression": "#key = :key",
        "ScanIndexForward": False,
        "Limit": limit,
        "ExpressionAttributeNames": {"#PK": "PK", "#key": "indexKey"},
        "ExpressionAttributeValues": {
            ":PK": {"S": user_id},
            ":key": {"S": "History"},
        },
    }
