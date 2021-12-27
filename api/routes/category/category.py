from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError

from api.util import document_it
from api.routes.video.video import get_videos
from api.routes.video.schema import VideoFilter
from api.client import DynamoDB
from .schema import Category, ReqCategoryPost, ReqCategoryPut
import api.routes.category.input as category_input

router = APIRouter()
db = DynamoDB()
table = db.table


@router.get("/categories", response_model=List[Category])
@document_it
def get_categories():
    """Get categories from DynamoDB"""

    try:
        input = category_input.query()
        res = table.query(**input)
        items = res.get("Items", [])
        items.sort(key=lambda x: x["SK"])

        items = db.merge_categories(items)
        return items
    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/category")
@document_it
def post_category(req_category: ReqCategoryPost):
    """Post category to DynamoDB"""

    try:
        input = category_input.put_item(req_category)
        res = table.put_item(Item=input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.put("/category")
@document_it
def put_category(req_category: ReqCategoryPut):
    """Put category to DynamoDB"""

    try:
        input = category_input.update_item(req_category)
        res = table.update_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.delete("/category/{category_id}")
def delete_category(category_id):
    """Delete category from Dynamo"""

    # いずれかの動画が紐づいているカテゴリは削除できない
    # 関連する動画の確認はクライアント側で処理してる（どっちでやるべきか）
    # 一応サーバ側でも判定する

    try:
        filter = {"categoryId": category_id}
        videos = get_videos(filter=VideoFilter(**filter), open=False)
        if videos:
            return {"relations": True}

        input = category_input.delete_item(category_id)
        res = table.delete_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
