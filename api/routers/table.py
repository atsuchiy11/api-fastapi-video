from typing import List
from api.schema import VideoTableRow
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from requests import HTTPError
from api.util import merge_table_for_video

from api.routers.user import get_users
from api.routers.path import get_paths
from api.routers.tag import get_tags
from api.routers.category import get_categories

# from api.routers.video import get_videos
from api.routers.video import get_videos_muliprocess

import inspect
import time

router = APIRouter()

#
# routes
#


# クライアント側で処理するので不要
# 型定義は欲しいので残す
@router.get("/table/videos", response_model=List[VideoTableRow])
async def get_table_videos():
    """Get videos merged VimeoAPI & DynamoDB for table"""
    start = time.time()
    print(inspect.currentframe().f_code.co_name)
    try:
        # videos = get_videos(all=True)
        videos = await get_videos_muliprocess(all=True)
        categories = get_categories()
        tags = get_tags()
        paths = get_paths()
        users = get_users()
        table_data = merge_table_for_video(
            videos=videos, categories=categories, tags=tags, paths=paths, users=users
        )
        print(time.time() - start)
        return table_data

    except HTTPError as err:
        raise HTTPException(status_code=404, detail=str(err))

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        print(err)
        raise HTTPException(status_code=404, detail=str(err))
