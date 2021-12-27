from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from aws_dynamodb_parser import parse

import uuid
from api.util import timestamp_jst
from api.schema import Category, ReqCategoryPost, ReqCategoryPut
from api.dynamodb import (
    # create_delete_item_transact,
    create_dynamodb_client,
    # transaction,
)
from api.util import merge_categories

router = APIRouter()
client = create_dynamodb_client()


#
# routes
#

# OK
@router.get("/categories", response_model=List[Category])
def get_categories():
    """Get categories from dynamoDB"""

    input = create_query_input()
    try:
        categories = client.query(**input)
        items = parse(categories.get("Items", []))
        items.sort(key=lambda x: x["SK"])

        res = merge_categories(items)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/category")
def post_category(req_category: ReqCategoryPost):
    """Post category to DynamoDB"""

    input = create_put_item_input(req_category)
    try:
        res = client.put_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.put("/category")
def put_category(req_category: ReqCategoryPut):
    """Put category to DynamoDB"""

    input = create_update_item_input(req_category)
    try:
        res = client.update_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.delete("/category/{category_id}")
def delete_category(category_id):
    """delete category meta"""

    input = create_delete_item_input(category_id)
    try:
        res = client.delete_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
# 任意のカテゴリを含む動画一覧を取得する
@router.get("/videos/{category_id}")
def get_videos_contain_any_category(category_id):
    """get videos contain any tag"""

    input = {
        "TableName": "primary_table",
        "IndexName": "GSI-1-SK",
        "KeyConditionExpression": "#key = :key",
        "FilterExpression": "contains(#category, :category)",
        "ScanIndexForward": False,
        "ExpressionAttributeNames": {"#key": "indexKey", "#category": "categoryId"},
        "ExpressionAttributeValues": {
            ":key": {"S": "Video"},
            ":category": {"S": category_id},
        },
    }
    try:
        res = client.query(**input)
        items = parse(res.get("Items", {}))
        return items

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
        "ExpressionAttributeNames": {"#key": "indexKey"},
        "ExpressionAttributeValues": {":value": {"S": "Category"}},
    }


# OK
def create_put_item_input(category):
    id = str(uuid.uuid1())[:8]
    return {
        "TableName": "primary_table",
        "Item": {
            "PK": {"S": "C-" + id},
            "SK": {"S": "C-" + id},
            "indexKey": {"S": "Category"},
            "name": {"S": category.name},
            "parentId": {"S": category.parentId},
            "description": {"S": category.description},
            "note": {"S": category.note},
            "invalid": {"BOOL": False},
            "createdAt": {"S": timestamp_jst()},
            "createdUser": {"S": category.user},
            "updatedAt": {"S": timestamp_jst()},
            "updatedUser": {"S": category.user},
        },
    }


# OK
def create_update_item_input(category):
    input = {
        "TableName": "primary_table",
        "Key": {"PK": {"S": category.PK}, "SK": {"S": category.PK}},
        "UpdateExpression": "SET #date = :date, #user = :user",
        "ExpressionAttributeNames": {
            "#date": "updatedAt",
            "#user": "updatedUser",
        },
        "ExpressionAttributeValues": {
            ":date": {"S": timestamp_jst()},
            ":user": {"S": category.user},
        },
    }
    if category.name is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #name = :name"
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = {"S": category.name}
    if category.description is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #description = :description"
        )
        input["ExpressionAttributeNames"]["#description"] = "description"
        input["ExpressionAttributeValues"][":description"] = {"S": category.description}
    if category.note is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #note = :note"
        input["ExpressionAttributeNames"]["#note"] = "note"
        input["ExpressionAttributeValues"][":note"] = {"S": category.note}
    if category.invalid is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #invalid = :invalid"
        )
        input["ExpressionAttributeNames"]["#invalid"] = "invalid"
        input["ExpressionAttributeValues"][":invalid"] = {"BOOL": category.invalid}
    if category.parentId is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #parentId = :parentId"
        )
        input["ExpressionAttributeNames"]["#parentId"] = "parentId"
        input["ExpressionAttributeValues"][":parentId"] = {"S": category.parentId}
    return input


# OK
def create_delete_item_input(category_id):
    return {
        "TableName": "primary_table",
        "Key": {"PK": {"S": category_id}, "SK": {"S": category_id}},
    }
