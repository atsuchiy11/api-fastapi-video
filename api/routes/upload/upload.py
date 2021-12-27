from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from typing import List

from api.util import document_it, timestamp_jst
from api.client import DynamoDB
from .schema import UploadStatus, ReqUploadStatusPost, ResUploadStatus
import api.routes.upload.input as upload_input

router = APIRouter()
db = DynamoDB()
table = db.table


@router.get("/upload/status", response_model=List[UploadStatus])
@document_it
def get_upload_status():
    """Get upload status from DynamoDB (today only)"""

    try:
        input = upload_input.query()
        res = table.query(**input)
        items = res.get("Items", {})
        items = [{**item, **{"id": item["PK"]}} for i, item in enumerate(items, 1)]
        return items
    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/upload/status", response_model=ResUploadStatus)
@document_it
def post_upload_status(req_status: ReqUploadStatusPost):
    """Post upload status to DynamoDB"""

    created_at = timestamp_jst()
    try:
        input = upload_input.put_item(req_status, created_at)
        table.put_item(Item=input)
        return {
            "uri": req_status.uri,
            "timestamp": created_at,
            "status": req_status.status,
        }
    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.put("/upload/status", response_model=ResUploadStatus)
@document_it
def put_upload_status(req_status: ResUploadStatus):
    """Put upload status to DynamoDB"""

    try:
        input = upload_input.update_item(req_status)
        table.update_item(**input)
        return {
            "uri": req_status.uri,
            "timestamp": req_status.timestamp,
            "status": req_status.status,
        }

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
