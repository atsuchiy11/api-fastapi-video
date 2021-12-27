from api.routers.video import get_video
from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from aws_dynamodb_parser import parse

import inspect
import uuid
from api.util import timestamp_jst
from api.schema import (
    Path,
    ReqPathDeleteTransact,
    ReqPathPost,
    ReqPathPutTransact,
    _ReqVideoOrder,
)
from api.dynamodb import (
    create_dynamodb_client,
)
from api.util import merge_paths_and_videos
import time

router = APIRouter()
client = create_dynamodb_client()

#
# routes
#


# OK
@router.get("/path/{path_id}", response_model=Path)
def get_path(path_id):
    """Get learning path from DynamoDB"""

    input = {
        "TableName": "primary_table",
        "Key": {"PK": {"S": path_id}, "SK": {"S": path_id}},
    }
    try:
        res = client.get_item(**input)
        path = parse(res.get("Item", {}))

        paths = []
        paths.append(path)
        # videos = get_videos_by_paths_from_db(client)
        videos = get_paths_videos()
        client_response = merge_paths_and_videos(paths, videos)
        return client_response[0]

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
# 遅い？？->DynamoDBの性能の問題
@router.get("/paths", response_model=List[Path])
def get_paths():
    """Get learning paths and video orders from DynamoDB"""

    start = time.time()
    print(inspect.currentframe().f_code.co_name)

    try:
        paths = get_paths_db()
        videos = get_paths_videos()
        res = merge_paths_and_videos(paths, videos)

        print("/path/path", time.time() - start)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        print(err)
        raise HTTPException(status_code=404, detail=str(err))


# OK
# 遅い？？->DynamoDBの性能の問題
@router.get("/paths/paths")
def get_paths_db():
    """Get learning paths fron DynamoDB"""

    start = time.time()
    print(inspect.currentframe().f_code.co_name)

    input = create_query_input()
    try:
        res = client.query(**input)
        paths = parse(res.get("Items", []))

        print("/path/path", time.time() - start)
        return paths

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        print(err)
        raise HTTPException(status_code=404, detail=str(err))


# OK
# 遅い？？->DynamoDBの性能の問題
@router.get("/paths/videos")
def get_paths_videos():
    """Get videos & playback orders included in learning paths"""

    start = time.time()
    print(inspect.currentframe().f_code.co_name)

    input = {
        "TableName": "primary_table",
        "IndexName": "GSI-1-SK",
        "KeyConditionExpression": "#key = :value",
        "FilterExpression": "attribute_not_exists(#attr)",
        "ExpressionAttributeNames": {"#key": "indexKey", "#attr": "invalid"},
        "ExpressionAttributeValues": {":value": {"S": "Video"}},
    }
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        items.sort(key=lambda x: x["PK"])

        print("/path/path", time.time() - start)
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        print(err)
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/path")
def post_path(req_path: ReqPathPost):
    """Post learning path to DynamoDB"""

    input = create_put_item_input(req_path)
    try:
        res = client.put_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


#
# [備忘]ここだけ飛び抜けてカオス
# 再生リスト更新でやること
# ・再生リストのメタデータ更新
# ・動画のメタデータ更新（リストIDの追加 or 削除）
# ・再生順の更新（追加 or 更新 or 削除）
# をトランザクションで実行する
#

# OK
@router.put("/path")
def put_path(req_path: ReqPathPutTransact):
    """Put learning path and relations to DynamoDB"""

    print("request_body", req_path)
    items = []

    # 再生リストのメタ情報を更新する
    items.append({"Update": create_update_item_input(req_path)})
    print("path meta setted")

    # 再生リストに動画が追加された場合、動画のメタデータに再生リストIDを追加する
    items.extend(
        _create_input_path_append_to_video(
            appended=req_path.appended, PK=req_path.PK, user=req_path.user
        )
    )
    # 再生リストから動画が削除された場合、動画のメタデータから再生リストIDを削除する
    items.extend(
        _create_input_path_remove_from_video(
            removed=req_path.removed, PK=req_path.PK, user=req_path.user
        )
    )
    # 再生リストから削除された動画の再生順を削除する
    items.extend(
        _create_input_remove_video_order(removed=req_path.removed, PK=req_path.PK)
    )
    # 再生リストの再生順を更新する
    items.extend(_create_input_video_orders(orders=req_path.orders, PK=req_path.PK))

    try:
        res = client.transact_write_items(
            ReturnConsumedCapacity="INDEXES", TransactItems=items
        )
        return res

    except ClientError as err:
        print(err)
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        print(err)
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.delete("/path")
def delete_path(req_path: ReqPathDeleteTransact):
    """Delete learning path and relations from DynamoDB"""

    items = []
    # remove path meta
    items.append({"Delete": create_delete_item_input(req_path.PK)})

    try:
        # update video meta
        videos = _get_videos_contain_any_path(req_path.PK)
        items.extend(
            __create_input_path_remove_from_video(
                videos=videos, PK=req_path.PK, user=req_path.user
            )
        )
        # remove video order
        orders = _get_orders_contain_any_path(req_path.PK)
        items.extend(__create_input_remove_video_order(orders))

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
        "ExpressionAttributeValues": {":value": {"S": "LearningPath"}},
    }


# OK
def create_put_item_input(path):
    id = str(uuid.uuid1())[:8]
    return {
        "TableName": "primary_table",
        "Item": {
            "PK": {"S": "L-" + id},
            "SK": {"S": "L-" + id},
            "indexKey": {"S": "LearningPath"},
            "name": {"S": path.name},
            "description": {"S": path.description},
            "note": {"S": path.note},
            "invalid": {"BOOL": False},
            "createdAt": {"S": timestamp_jst()},
            "createdUser": {"S": path.user},
            "updatedAt": {"S": timestamp_jst()},
            "updatedUser": {"S": path.user},
        },
    }


# OK
def create_update_item_input(path):
    input = {
        "TableName": "primary_table",
        "Key": {"PK": {"S": path.PK}, "SK": {"S": path.PK}},
        "UpdateExpression": "SET #date = :date, #user = :user",
        "ExpressionAttributeNames": {
            "#date": "updatedAt",
            "#user": "updatedUser",
        },
        "ExpressionAttributeValues": {
            ":date": {"S": timestamp_jst()},
            ":user": {"S": path.user},
        },
    }
    if path.name is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #name = :name"
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = {"S": path.name}
    if path.description is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #description = :description"
        )
        input["ExpressionAttributeNames"]["#description"] = "description"
        input["ExpressionAttributeValues"][":description"] = {"S": path.description}
    if path.note is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #note = :note"
        input["ExpressionAttributeNames"]["#note"] = "note"
        input["ExpressionAttributeValues"][":note"] = {"S": path.note}
    if path.invalid is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #invalid = :invalid"
        )
        input["ExpressionAttributeNames"]["#invalid"] = "invalid"
        input["ExpressionAttributeValues"][":invalid"] = {"BOOL": path.invalid}
    return input


# OK
def create_delete_item_input(path_id):
    return {
        "TableName": "primary_table",
        "Key": {"PK": {"S": path_id}, "SK": {"S": path_id}},
    }


# OK
# update
def _create_input_path_append_to_video(appended, PK, user):
    """update path to video meta"""

    videos = []
    for uri in appended:
        video_id = uri.split("/")[2]
        video = get_video(video_id)
        path_ids = video["learningPathIds"]
        # escape empty
        if path_ids[0] == "":
            path_ids.remove("")
        path_ids.append(PK)
        input = {
            "TableName": "primary_table",
            "Key": {
                "PK": {"S": video["PK"]},
                "SK": {"S": video["PK"]},
            },
            "UpdateExpression": "SET updatedAt = :date, "
            + "updatedUser = :user, learningPathIds = :paths",
            "ExpressionAttributeValues": {
                ":date": {"S": timestamp_jst()},
                ":user": {"S": user},
                ":paths": {"SS": path_ids},
            },
        }
        videos.append({"Update": input})
    print("append video meta setted")
    return videos


# OK
# update
def _create_input_path_remove_from_video(removed, PK, user):
    """remove path from video meta"""

    videos = []
    for uri in removed:
        video_id = uri.split("/")[2]
        video = get_video(video_id)
        path_ids = video.get("learningPathIds", [])
        path_ids.remove(PK)
        # escape empty
        if len(path_ids) <= 0:
            path_ids = [""]
        input = {
            "TableName": "primary_table",
            "Key": {
                "PK": {"S": video["PK"]},
                "SK": {"S": video["PK"]},
            },
            "UpdateExpression": "SET updatedAt = :date, "
            + "updatedUser = :user, learningPathIds = :paths",
            "ExpressionAttributeValues": {
                ":date": {"S": timestamp_jst()},
                ":user": {"S": user},
                ":paths": {"SS": path_ids},
            },
        }
        videos.append({"Update": input})
    print("remove video meta setted")
    return videos


# OK
# updte
def _create_input_video_orders(orders, PK):
    """appendd or update video orders"""

    new_orders = []
    for order in orders:

        # 現在の動画の再生順を取得
        current_order = _get_order({"PK": PK, "uri": order.uri})

        # 再生順がある場合は、動画の再生順を更新する
        if current_order:
            input = {
                "TableName": "primary_table",
                "Key": {"PK": {"S": PK}, "SK": {"S": order.uri}},
                "UpdateExpression": "SET #order = :order",
                "ExpressionAttributeNames": {"#order": "order"},
                "ExpressionAttributeValues": {":order": {"N": str(order.order)}},
            }
            new_orders.append({"Update": input})

        # 再生順がない場合は、動画の再生順を追加する
        else:
            input = {
                "TableName": "primary_table",
                "Item": {
                    "PK": {"S": PK},
                    "SK": {"S": order.uri},
                    "indexKey": {"S": "Video"},
                    "createdAt": {"S": str(uuid.uuid1())[:8]},
                    "order": {"N": str(order.order)},
                },
            }
            new_orders.append({"Put": input})
    print("append or update video order setted")
    return new_orders


# OK
# update
def _get_order(req_order: _ReqVideoOrder):
    """Get video order"""

    input = {
        "TableName": "primary_table",
        "Key": {"PK": {"S": req_order["PK"]}, "SK": {"S": req_order["uri"]}},
    }
    try:
        res = client.get_item(**input)
        item = parse(res.get("Item", {}))
        return item

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
# update
def _create_input_remove_video_order(removed, PK):
    """remove video order"""

    orders = []
    for uri in removed:
        input = {
            "TableName": "primary_table",
            "Key": {"PK": {"S": PK}, "SK": {"S": uri}},
        }
        orders.append({"Delete": input})
    print("remove video order setted")
    return orders


# OK
# delete
def _get_videos_contain_any_path(path_id):
    """get videos contain any learning path"""
    input = {
        "TableName": "primary_table",
        "IndexName": "GSI-1-SK",
        "KeyConditionExpression": "#key = :key",
        "FilterExpression": "contains(#path, :path)",
        "ScanIndexForward": False,
        "ExpressionAttributeNames": {"#key": "indexKey", "#path": "learningPathIds"},
        "ExpressionAttributeValues": {":key": {"S": "Video"}, ":path": {"S": path_id}},
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
# delete
def __create_input_path_remove_from_video(videos, PK, user):
    """remove path from video meta"""

    new_videos = []
    for video in videos:
        path_ids = video.get("learningPathIds", [])
        path_ids.remove(PK)
        # escape empty
        if len(path_ids) <= 0:
            path_ids = [""]
        input = {
            "TableName": "primary_table",
            "Key": {
                "PK": {"S": video["PK"]},
                "SK": {"S": video["PK"]},
            },
            "UpdateExpression": "SET updatedAt = :date, "
            + "updatedUser = :user, learningPathIds = :paths",
            "ExpressionAttributeValues": {
                ":date": {"S": timestamp_jst()},
                ":user": {"S": user},
                ":paths": {"SS": path_ids},
            },
        }
        new_videos.append({"Update": input})
    print("remove video meta setted")
    return new_videos


# OK
# delete
def _get_orders_contain_any_path(path_id):
    input = {
        "TableName": "primary_table",
        "KeyConditionExpression": "#key = :key",
        "FilterExpression": "attribute_exists(#attr)",
        "ScanIndexForward": False,
        "ExpressionAttributeNames": {"#key": "PK", "#attr": "order"},
        "ExpressionAttributeValues": {":key": {"S": path_id}},
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
# delete
def __create_input_remove_video_order(orders):
    """remove video order"""

    remove_orders = []
    for order in orders:
        input = {
            "TableName": "primary_table",
            "Key": {"PK": {"S": order["PK"]}, "SK": {"S": order["SK"]}},
        }
        remove_orders.append({"Delete": input})
    print("remove video order setted")
    return remove_orders
