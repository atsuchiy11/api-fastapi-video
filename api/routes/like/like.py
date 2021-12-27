from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError

from api.util import document_it
from api.client import DynamoDB
from .schema import Likes, ReqLikePost, ReqLikeDelete
import api.routes.like.input as like_input

router = APIRouter()
db = DynamoDB()
table = db.table


@router.get("/like/{video_id}", response_model=Likes)
@document_it
def get_likes(video_id):
    """Get likes for video from DynamoDB"""

    try:
        input = like_input.query(video_id)
        res = table.query(**input)
        items = res.get("Items", [])

        good = [item for item in items if item.get("like")]
        bad = [item for item in items if not item.get("like")]
        return {"good": good, "bad": bad}

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/like")
@document_it
def post_like(req_like: ReqLikePost):
    """Post like for video to DynamoDB"""

    try:
        input = like_input.put_item(req_like)
        res = table.put_item(Item=input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.delete("/like")
@document_it
def delete_like(req_like: ReqLikeDelete):
    """Delete like for video from DynamoDB"""

    try:
        input = like_input.delete_item(req_like)
        res = table.delete_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
