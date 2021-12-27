from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError

from api.util import document_it
from api.client import DynamoDB
from .schema import Thread, ReqThreadPost, ReqThreadPut
import api.routes.thread.input as thread_input

router = APIRouter()
db = DynamoDB()
table = db.table


@router.get("/thread/{video_id}", response_model=List[Thread])
@document_it
def get_thread(video_id):
    """Get threads for video from DynamoDB"""

    try:
        input = thread_input.query(video_id)
        res = table.query(**input)
        items = res.get("Items", [])
        # items.sort(key=lambda x: x["createdAt"], reverse=True)
        return items
    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/thread")
@document_it
def post_thread(req_thread: ReqThreadPost):
    """Post thread for video to DynamoDB"""

    try:
        input = thread_input.put_item(req_thread)
        res = table.put_item(Item=input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.put("/thread")
@document_it
def put_thread(req_thread: ReqThreadPut):
    """Put thread for video to DynamoDB"""

    try:
        input = thread_input.update_item(req_thread)
        res = table.update_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
