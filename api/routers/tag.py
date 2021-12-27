from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from aws_dynamodb_parser import parse

import uuid
from api.util import timestamp_jst
from api.schema import ReqTagDelete, ReqTagPost, ReqTagPut, Tag
from api.dynamodb import create_dynamodb_client

router = APIRouter()
client = create_dynamodb_client()


#
# routes
#

# OK
@router.get("/tags", response_model=List[Tag])
def get_tags():
    """Get tags from dynamoDB"""
    input = create_query_input()
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        # append index
        for i, item in enumerate(items, 1):
            item["id"] = i
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        err = err.response.json()
        err_message = err.get("error", "System error occurred.")
        raise HTTPException(status_code=404, detail=err_message)


# OK
@router.post("/tag")
def post_tag(req_tag: ReqTagPost):
    """Post tag to DynamoDB"""

    try:
        # check duplicate
        tags = get_tags()
        duplicate = [tag for tag in tags if tag["name"] == req_tag.name]
        if duplicate:
            return {"duplicate": True}

        input = create_put_item_input(req_tag)
        res = client.put_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.put("/tag")
def put_tag(req_tag: ReqTagPut):
    """Put tag to DynamoDB"""

    input = create_update_item_input(req_tag)
    try:
        res = client.update_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.delete("/tag")
def delete_tag(req_tag: ReqTagDelete):
    """delete tag meta and video/tag relations"""

    items = []
    # remove tag meta
    items.append({"Delete": create_delete_item_input(req_tag.PK)})

    try:
        # update video meta
        videos = _get_videos_contain_any_tag(req_tag.PK)
        items.extend(
            _create_input_tag_remove_from_video(
                videos=videos, PK=req_tag.PK, user=req_tag.user
            )
        )
        # transaction
        res = client.transact_write_items(
            ReturnConsumedCapacity="INDEXES", TransactItems=items
        )
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
        "ExpressionAttributeValues": {":value": {"S": "Tag"}},
    }


# OK
def create_put_item_input(tag):
    id = str(uuid.uuid1())[:8]
    return {
        "TableName": "primary_table",
        "Item": {
            "PK": {"S": "T-" + id},
            "SK": {"S": "T-" + id},
            "indexKey": {"S": "Tag"},
            "name": {"S": tag.name},
            "description": {"S": tag.description},
            "note": {"S": tag.note},
            "invalid": {"BOOL": False},
            "createdAt": {"S": timestamp_jst()},
            "createdUser": {"S": tag.user},
            "updatedAt": {"S": timestamp_jst()},
            "updatedUser": {"S": tag.user},
        },
    }


# OK
def create_update_item_input(tag):
    input = {
        "TableName": "primary_table",
        "Key": {"PK": {"S": tag.PK}, "SK": {"S": tag.PK}},
        "UpdateExpression": "SET #date = :date, #user = :user",
        "ExpressionAttributeNames": {
            "#date": "updatedAt",
            "#user": "updatedUser",
        },
        "ExpressionAttributeValues": {
            ":date": {"S": timestamp_jst()},
            ":user": {"S": tag.user},
        },
    }
    if tag.name is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #name = :name"
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = {"S": tag.name}
    if tag.description is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #description = :description"
        )
        input["ExpressionAttributeNames"]["#description"] = "description"
        input["ExpressionAttributeValues"][":description"] = {"S": tag.description}
    if tag.note is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #note = :note"
        input["ExpressionAttributeNames"]["#note"] = "note"
        input["ExpressionAttributeValues"][":note"] = {"S": tag.note}
    if tag.invalid is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #invalid = :invalid"
        )
        input["ExpressionAttributeNames"]["#invalid"] = "invalid"
        input["ExpressionAttributeValues"][":invalid"] = {"BOOL": tag.invalid}
    return input


# OK
def create_delete_item_input(tag_id):
    return {
        "TableName": "primary_table",
        "Key": {"PK": {"S": tag_id}, "SK": {"S": tag_id}},
    }


# OK
# 公開するのはありかも。。
def _get_videos_contain_any_tag(tag_id):
    """get videos contain any tag"""
    input = {
        "TableName": "primary_table",
        "IndexName": "GSI-1-SK",
        "KeyConditionExpression": "#key = :key",
        "FilterExpression": "contains(#tag, :tag)",
        "ScanIndexForward": False,
        "ExpressionAttributeNames": {"#key": "indexKey", "#tag": "tagIds"},
        "ExpressionAttributeValues": {":key": {"S": "Video"}, ":tag": {"S": tag_id}},
    }
    try:
        res = client.query(**input)
        items = parse(res.get("Items", {}))
        return items
    except ClientError as err:
        raise err

    except BaseException as err:
        raise err


# OK
def _create_input_tag_remove_from_video(videos, PK, user):
    """remove tag from video meta"""

    new_videos = []
    for video in videos:
        tag_ids = video.get("tagIds", [])
        tag_ids.remove(PK)
        # escape empty
        if len(tag_ids) <= 0:
            tag_ids = [""]
        input = {
            "TableName": "primary_table",
            "Key": {
                "PK": {"S": video["PK"]},
                "SK": {"S": video["PK"]},
            },
            "UpdateExpression": "SET updatedAt = :date, "
            + "updatedUser = :user, tagIds = :tags",
            "ExpressionAttributeValues": {
                ":date": {"S": timestamp_jst()},
                ":user": {"S": user},
                ":tags": {"SS": tag_ids},
            },
        }
        new_videos.append({"Update": input})
    return new_videos
