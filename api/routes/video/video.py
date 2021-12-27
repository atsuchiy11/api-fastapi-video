from fastapi import APIRouter, HTTPException, UploadFile, File
from botocore.exceptions import ClientError
from requests import HTTPError
from typing import List
from pathlib import Path
import urllib.parse
import tempfile
import shutil
import glob
import os

import math
import asyncio
import concurrent.futures
from functools import reduce

from api.util import document_it
from api.client import DynamoDB, VimeoAPI
from .schema import (
    ReqVideoPost,
    ReqVideoPut,
    ReqVimeoPut,
    VideoDB,
    VideoFilter,
    VideoVimeo,
)
import api.routes.video.input as video_input

router = APIRouter()
db = DynamoDB()
table = db.table
vimeo = VimeoAPI()


@router.get("/video/{video_id}", response_model=VideoDB)
@document_it
def get_video_from_db(video_id):
    """Get a specific video from DynamoDB"""

    try:
        input = video_input.get_item(video_id)
        res = table.get_item(**input)
        item = res.get("Item", {})
        return item
    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.get("/vimeo/video/{video_id}", response_model=VideoVimeo)
@document_it
def get_video_from_vimeo(video_id):
    """Get a specific video from Vimeo"""

    params = {
        "fields": "uri,name,duration,stats,privacy,embed.html,pictures.sizes",
    }
    query_params = urllib.parse.urlencode(params)
    try:
        res = vimeo.client.get(f"/videos/{video_id}?{query_params}")
        res.raise_for_status()
        data = vimeo.sort(res.json())
        return data
    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/videos", response_model=List[VideoDB])
@document_it
def get_videos(filter: VideoFilter, open: bool = True):
    """Get videos from DynamoDB filtered by tags, categories, playlist, title"""

    try:
        input = video_input.query(filter, open)
        res = table.query(**input)
        items = res.get("Items", [])
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/video")
@document_it
def post_video_to_db(req_video: ReqVideoPost):
    """Post video to DynamoDB"""

    try:
        input = video_input.put_item(video=req_video)
        res = table.put_item(Item=input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.put("/video")
@document_it
def put_video_to_db(req_video: ReqVideoPut):
    """Put video to DynamoDB"""

    try:
        input = video_input.update_item(video=req_video)
        res = table.update_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.put("/vimeo/video")
@document_it
def put_video_to_vimeo(req_video: ReqVimeoPut):
    """Update title to Vimeo"""

    try:
        res = vimeo.client.patch(
            f"/videos/{req_video.PK}", data={"name": req_video.name}
        )
        res_json = res.json()
        return {"uri": res_json.get("uri", ""), "name": res_json.get("name", "")}

    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.get("/videos/status")
@document_it
def get_transcode_status(video_id: str):
    """Get video upload status from Vimeo"""

    query = "fields=transcode.status"
    try:
        res = vimeo.client.get(f"/videos/{video_id}?{query}")
        res.raise_for_status()
        res_json = res.json()
        return {"transcode_status": res_json["transcode"]["status"]}

    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/video/thumbnail/{video_id}")
@document_it
def post_thumbnail_to_vimeo(video_id: str, image: UploadFile = File(...)):
    """Upload video thumbnail to Vimeo"""

    suffix = Path(image.filename).suffix
    try:
        # create temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as fp:
            shutil.copyfileobj(image.file, fp)
            tmp_path = Path(fp.name)

            res = vimeo.client.upload_picture(
                f"/videos/{video_id}", tmp_path, activate=True
            )
            # remove temp file
            for p in glob.glob("/tmp/" + "*"):
                if os.path.isfile(p):
                    os.remove(p)
            return res

    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


#
# ここから非公開API
#


@router.get("/vimeo/videos", response_model=List[VideoVimeo])
async def get_videos_from_vimeo(all: bool = False, page: int = 1):
    """Get videos from Vimeo by page"""

    chunk = 100

    def set_iter_times(page):
        times = 1
        # get specific page
        if page >= times + 1:
            times = page
        # get all page
        if all:
            pre = vimeo.get_total()
            times = math.ceil(pre["total"] / chunk)
        return times

    try:
        times = set_iter_times(page)
        loop = asyncio.get_running_loop()

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            tasks = [
                loop.run_in_executor(executor, vimeo.get_videos_by_page, chunk, i)
                for i in range(page, times + 1)
            ]
            data = await asyncio.gather(*tasks)
            merged = reduce(lambda a, b: a + b, data)
            print("total:", len(merged))
            return merged

    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
