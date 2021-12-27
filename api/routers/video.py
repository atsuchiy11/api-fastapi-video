from typing import List

from api.util import timestamp_jst
from fastapi import APIRouter, HTTPException, File, UploadFile
from botocore.exceptions import ClientError
from requests import HTTPError
from aws_dynamodb_parser import parse

from pathlib import Path  # 一時ファイルのパス取得
import tempfile  # 一時ファイルの作成
import shutil  # ファイルオブジェクトのコピー

import concurrent.futures
import requests
import asyncio
import math
import time

import inspect
import glob
import os

from api.schema import (
    ReqVideoPost,
    ReqVideoPut,
    ReqVimeoPut,
    VideoFilter,
    VideoVimeo,
    VideoDB,
    Video,
)
from api.vimeoapi import (
    create_vimeo_client,
    get_total,
    get_videos_from_vimeo,
    get_upload_status,
    get_videos_from_vimeo_async,
    upload_thumbnail,
    get_video_from_vimeo,
)
from api.dynamodb import (
    create_dynamodb_client,
    create_dynamodb_resource,
)
from api.util import merge_videos

router = APIRouter()
vimeo_client = create_vimeo_client()
client = create_dynamodb_client()
db_resource = create_dynamodb_resource()

#
# routes
#


# OK
@router.get("/video/{video_id}", response_model=VideoDB)
def get_video(video_id):
    """Get a specific video from DynamoDB"""

    uri = "/videos/" + video_id
    input = {
        "TableName": "primary_table",
        "Key": {"PK": {"S": uri}, "SK": {"S": uri}},
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
@router.get("/vimeo/video/{video_id}", response_model=VideoVimeo)
def get_vimeo_video(video_id):
    """Get a specific video from VimeoAPI"""

    try:
        res = get_video_from_vimeo(vimeo_client, video_id)
        return res
    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/videos", response_model=List[VideoDB])
def get_videos(filter: VideoFilter, open: bool = True):
    """Get videos from DynamoDB filtered by tags, categories, plyalist, title"""

    print(inspect.currentframe().f_code.co_name)

    input = create_query_input(filter=filter, open=open)
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        print(len(items))
        return items

    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/video")
def post_video_to_db(req_video: ReqVideoPost):
    """Post video to DynamoDB"""

    input = create_put_item_input(req_video)
    try:
        res = client.put_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.put("/video")
def put_video(req_video: ReqVideoPut):
    """Put video to dynamoDB"""

    input = create_update_item_input(req_video)
    try:
        res = client.update_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
# 用途拡張してもいいかも
@router.put("/vimeo/video")
def put_vimeo_video(req_video: ReqVimeoPut):
    """Update title to vimeo"""

    try:
        res = vimeo_client.patch(
            f"https://api.vimeo.com/videos/{req_video.PK}",
            data={"name": req_video.name},
        )
        res_parsed = res.json()
        return {
            "uri": res_parsed["uri"],
            "name": res_parsed["name"],
        }
    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.get("/video/status/")
def get_transcode_status(video_id: str):
    """Get video upload status from vimeo"""

    try:
        res = get_upload_status(vimeo_client, video_id)
        return res
    except Exception as err:
        print(err)
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/video/thumbnail/{video_id}")
def post_thumbnail_vimeo(video_id: int, image: UploadFile = File(...)):
    """Upload video thumbnail to vimeo"""

    suffix = Path(image.filename).suffix
    try:
        # create temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as fp:
            shutil.copyfileobj(image.file, fp)
            tmp_path = Path(fp.name)
            res = upload_thumbnail(vimeo_client, video_id, tmp_path)
            # remove temp files
            for p in glob.glob("/tmp/" + "*"):
                if os.path.isfile(p):
                    os.remove(p)
            return res

    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


###


#
# utilities
#

# OK
def create_query_input(filter=None, open=True):
    # open=Falseなら非公開も全て取得する
    input = {
        "TableName": "primary_table",
        "IndexName": "GSI-1-SK",
        "KeyConditionExpression": "#key = :value",
        # remove video relations
        "FilterExpression": "attribute_exists(#invalid)",
        "ScanIndexForward": False,
        "ExpressionAttributeNames": {"#key": "indexKey", "#invalid": "invalid"},
        "ExpressionAttributeValues": {
            ":value": {"S": "Video"},
        },
    }
    if open:
        input["FilterExpression"] = "#invalid = :invalid"
        input["ExpressionAttributeValues"][":invalid"] = {"BOOL": False}

    # filter condition
    if filter.categoryId:
        input["FilterExpression"] = (
            input.get("FilterExpression") + " And contains(#category, :category)"
        )
        input["ExpressionAttributeNames"]["#category"] = "categoryId"
        input["ExpressionAttributeValues"][":category"] = {"S": filter.categoryId}

    if filter.tagId:
        input["FilterExpression"] = (
            input.get("FilterExpression") + " And contains(#tag, :tag)"
        )
        input["ExpressionAttributeNames"]["#tag"] = "tagIds"
        input["ExpressionAttributeValues"][":tag"] = {"S": filter.tagId}

    if filter.learningPathId:
        input["FilterExpression"] = (
            input.get("FilterExpression") + " And contains(#path, :path)"
        )
        input["ExpressionAttributeNames"]["#path"] = "learningPathIds"
        input["ExpressionAttributeValues"][":path"] = {"S": filter.learningPathId}

    if filter.name:
        input["FilterExpression"] = (
            input.get("FilterExpression") + " And contains(#name, :name)"
        )
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = {"S": filter.name}

    return input


# OK
def create_put_item_input(video):
    input = {
        "TableName": "primary_table",
        "Item": {
            "PK": {"S": video.PK},
            "SK": {"S": video.PK},
            "indexKey": {"S": "Video"},
            "invalid": {"BOOL": False},
            "createdAt": {"S": timestamp_jst()},
            "createdUser": {"S": video.user},
            "updatedAt": {"S": timestamp_jst()},
            "updatedUser": {"S": video.user},
            "description": {"S": video.description},
            "categoryId": {"S": video.categoryId},
            "tagIds": {"SS": video.tagIds},
            "learningPathIds": {"SS": [""]},
            "note": {"S": video.note},
        },
    }
    # vimeo params
    if video.uri is not None:
        input["Item"]["uri"] = {"S": video.uri}
    if video.thumbnail is not None:
        input["Item"]["thumbnail"] = {"S": video.thumbnail}
    if video.plays is not None:
        input["Item"]["plays"] = {"N": str(video.plays)}
    if video.name is not None:
        input["Item"]["name"] = {"S": video.name}
    if video.duration is not None:
        input["Item"]["duration"] = {"N": str(video.duration)}
    if video.html is not None:
        input["Item"]["html"] = {"S": video.html}

    return input


# OK
def create_update_item_input(video):
    input = {
        "TableName": "primary_table",
        "Key": {
            "PK": {"S": video.PK},
            "SK": {"S": video.PK},
        },
        "UpdateExpression": "SET #date = :date, #user = :user",
        "ExpressionAttributeNames": {
            "#date": "updatedAt",
            "#user": "updatedUser",
        },
        "ExpressionAttributeValues": {
            ":date": {"S": timestamp_jst()},
            ":user": {"S": video.user},
        },
    }
    if video.categoryId is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #category = :category"
        )
        input["ExpressionAttributeNames"]["#category"] = "categoryId"
        input["ExpressionAttributeValues"][":category"] = {"S": video.categoryId}

    if video.tagIds is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #tags = :tags"
        input["ExpressionAttributeNames"]["#tags"] = "tagIds"
        input["ExpressionAttributeValues"][":tags"] = {"SS": video.tagIds}

    if video.learningPathIds is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #paths = :paths"
        input["ExpressionAttributeNames"]["#paths"] = "learningPathIds"
        input["ExpressionAttributeValues"][":paths"] = {"SS": video.learningPathIds}
        pass

    if video.description is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #description = :description"
        )
        input["ExpressionAttributeNames"]["#description"] = "description"
        input["ExpressionAttributeValues"][":description"] = {"S": video.description}

    if video.note is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #note = :note"
        input["ExpressionAttributeNames"]["#note"] = "note"
        input["ExpressionAttributeValues"][":note"] = {"S": video.note}

    if video.invalid is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #invalid = :invalid"
        )
        input["ExpressionAttributeNames"]["#invalid"] = "invalid"
        input["ExpressionAttributeValues"][":invalid"] = {"BOOL": video.invalid}

    # vimeo params
    if video.uri is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #uri = :uri"
        input["ExpressionAttributeNames"]["#uri"] = "uri"
        input["ExpressionAttributeValues"][":uri"] = {"S": video.uri}

    if video.thumbnail is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #thumbnail = :thumbnail"
        )
        input["ExpressionAttributeNames"]["#thumbnail"] = "thumbnail"
        input["ExpressionAttributeValues"][":thumbnail"] = {"S": video.thumbnail}

    if video.plays is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #plays = :plays"
        input["ExpressionAttributeNames"]["#plays"] = "plays"
        input["ExpressionAttributeValues"][":plays"] = {"N": str(video.plays)}

    if video.name is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #name = :name"
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = {"S": video.name}

    if video.duration is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #duration = :duration"
        )
        input["ExpressionAttributeNames"]["#duration"] = "duration"
        input["ExpressionAttributeValues"][":duration"] = {"N": str(video.duration)}

    if video.html is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #html = :html"
        input["ExpressionAttributeNames"]["#html"] = "html"
        input["ExpressionAttributeValues"][":html"] = {"S": video.html}

    return input


#
# 不要だけど残す
#


@router.get("/vimeo/videos", response_model=VideoVimeo)
def get_vimeo_videos(all=False, page=1):
    """Get videos from VimeoAPI"""
    try:
        res = get_videos_from_vimeo(vimeo_client, all, page)
        return res
    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.get("/db/videos", response_model=List[VideoDB])
def get_db_videos(all: bool = False):
    """Get videos from DynamoDB"""

    start = time.time()

    # all=Falseなら非公開（invalid=True）は取得しない
    input = create_query_input(all=all)
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        print(time.time() - start)
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# 検索条件をリクエストボディで受け取りたいのでGETじゃなくてPOSTにする
# オーバーライドとかは一回考えない


@router.post("/db/videos", response_model=List[VideoDB])
def get_db_videos_filtered(filter: VideoFilter, all: bool = False):
    """Get videos from DynamoDB"""

    # all=Falseなら非公開（invalid=True）は取得しない
    input = create_query_input(all=all, filter=filter)
    print(input)
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


#
# Multi Process
# 1回のAPIの通信速度(上限100件)はVimeo依存なのでこれ以上のパフォーマンスは出せない
# 複数回コールする必要があるときはマルチプロセスにすることでかなり向上する。
#


@router.get("/vimeo/videos/async", response_model=VideoVimeo)
async def get_async_vimeo_videos_multi_process(all: bool = False, page: int = 1):
    """Get videos from vimeo by multi process"""
    # start = time.time()

    chunk = 100
    videos = {"total": 0, "data": []}
    try:
        times = 1
        # 特定のページだけ取得する場合（チャンクは100）
        if page >= times + 1:
            times = page
        # 全取得する場合は事前に総数を取得する（プリリクエスト）
        if all:
            pre = get_total(vimeo_client)
            times = math.ceil(pre["total"] / chunk)  # iter回数

        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            tasks = [
                loop.run_in_executor(
                    executor, get_videos_from_vimeo_async, vimeo_client, chunk, i
                )
                for i in range(page, times + 1)
            ]
            data = await asyncio.gather(*tasks)
            # merge data
            for lot in data:
                videos["data"] += lot["data"]
                videos["total"] = lot["total"]

            # print(time.time() - start)
            return videos

    except HTTPError as err:
        raise err
    except requests.exceptions.RequestException as err:
        raise err


@router.get("/videos/async", response_model=List[Video])
async def get_videos_muliprocess(all: bool = False, page: int = 1):
    """Get videos merged VimeoAPI & DynamoDB"""

    start = time.time()
    print(inspect.currentframe().f_code.co_name)

    try:
        vimeo_response = await get_async_vimeo_videos_multi_process(all, page)
        db_response = get_db_videos(all=all)
        res = merge_videos(vimeo_response, db_response, all)

        print(time.time() - start)
        return res

    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except ClientError as err:
        print(err)
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# def syncFn1(n):
#     """sync fucntion"""
#     print(f"start:fn({n})")
#     print(f"finish:fn{n}")
#     return f"finish:fn{n}"


# def syncFn2(n):
#     """sync fucntion"""
#     print(f"start:fn({n})")
#     print(f"finish:fn{n}")
#     return f"finish:fn{n}"


# @router.get("/dammy")
# async def get_dammy():
#     """Dammy Multi Process"""
#     loop = asyncio.get_running_loop()
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         task1 = loop.run_in_executor(executor, syncFn1, 1)
#         task2 = loop.run_in_executor(executor, syncFn2, 2)
#         res1 = await task1
#         res2 = await task2
#         print("task finished")
#         return {"res1": res1, "res2": res2}
