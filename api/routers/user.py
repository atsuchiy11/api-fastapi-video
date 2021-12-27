from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from aws_dynamodb_parser import parse

from api.util import timestamp_jst, get_today_string
from api.schema import User, ReqUser
from api.dynamodb import (
    create_dynamodb_client,
    create_dynamodb_resource,
    # get_item_from_db,
)

router = APIRouter()
client = create_dynamodb_client()
db_resource = create_dynamodb_resource()

#
# routes
#


# OK
@router.get("/users/login")
def get_user_count():
    """Get login users count today"""

    input = {
        "TableName": "primary_table",
        "IndexName": "GSI-1-SK",
        "KeyConditionExpression": "#key = :key And begins_with(#today, :today)",
        "ScanIndexForward": False,
        "ExpressionAttributeNames": {"#key": "indexKey", "#today": "createdAt"},
        "ExpressionAttributeValues": {
            ":key": {"S": "User"},
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
@router.get("/user/{user_id}", response_model=User)
def get_user(user_id):
    """Get user from DynamoDB"""

    input = {
        "TableName": "primary_table",
        "Key": {"PK": {"S": user_id}, "SK": {"S": user_id}},
    }
    try:
        res = client.get_item(**input)
        item = parse(res.get("Item", []))
        return item

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.get("/users", response_model=List[User])
def get_users():
    """Get users from dynamoDB"""

    input = create_query_input()
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        for i, item in enumerate(items, 1):
            item["id"] = i
        # items.sort(key=lambda x: x["SortKey"])
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/user")
def post_user(req_user: ReqUser):
    """Post user to dynamoDB"""

    input = create_put_item_input(req_user)
    try:
        res = client.put_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.put("/user")
def put_user(req_user: ReqUser):
    """Put user to dynamoDB"""

    input = create_update_item_input(req_user)
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


# OK
def create_query_input():
    return {
        "TableName": "primary_table",
        "IndexName": "GSI-1-SK",
        "KeyConditionExpression": "#key = :value",
        "ScanIndexForward": False,
        "ExpressionAttributeNames": {"#key": "indexKey"},
        "ExpressionAttributeValues": {":value": {"S": "User"}},
    }


# OK
def create_put_item_input(user):
    return {
        "TableName": "primary_table",
        "Item": {
            "PK": {"S": user.PK},
            "SK": {"S": user.PK},
            "indexKey": {"S": "User"},
            "createdAt": {"S": timestamp_jst()},
            "updatedAt": {"S": timestamp_jst()},
            "name": {"S": user.name},
            "image": {"S": user.image},
            "acl": {"S": "user"},
        },
    }


# OK
def create_update_item_input(user):
    input = {
        "TableName": "primary_table",
        "Key": {
            "PK": {"S": user.PK},
            "SK": {"S": user.PK},
        },
        "UpdateExpression": "SET #login = :login",
        "ExpressionAttributeNames": {"#login": "createdAt"},
        "ExpressionAttributeValues": {":login": {"S": timestamp_jst()}},
    }
    updated = False

    if user.name is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #name = :name"
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = {"S": user.name}
        updated = True
    if user.image is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #image = :image"
        input["ExpressionAttributeNames"]["#image"] = "image"
        input["ExpressionAttributeValues"][":image"] = {"S": user.image}
        updated = True
    if user.acl is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #acl = :acl"
        input["ExpressionAttributeNames"]["#acl"] = "acl"
        input["ExpressionAttributeValues"][":acl"] = {"S": user.acl}
        updated = True
    if updated:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #update = :update"
        )
        input["ExpressionAttributeNames"]["#update"] = "updatedAt"
        input["ExpressionAttributeValues"][":update"] = {"S": timestamp_jst()}
    return input
