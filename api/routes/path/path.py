from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError

from api.util import document_it
from api.client import DynamoDB
from .schema import Path, ReqPathPost, ReqPathPutTransact, ReqPathDeleteTransact
import api.routes.path.input as path_input

router = APIRouter()
db = DynamoDB()
table = db.table


@router.get("/path/{path_id}", response_model=Path)
@document_it
def get_path(path_id):
    """Get specific learningPath from DynamoDB"""

    try:
        input = path_input.get_item(path_id)
        res = table.get_item(**input)
        item = res.get("Item", {})

        videos = get_videos_contains_path()
        paths = db.merge_paths(paths=[item], videos=videos)
        path, *_ = paths
        return path
    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.get("/paths", response_model=List[Path])
@document_it
def get_paths():
    """Get learning paths and video orders from DynamoDB"""

    try:
        paths = get_paths_from_db()
        videos = get_videos_contains_path()

        res = db.merge_paths(paths, videos)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.get("/paths/paths")
@document_it
def get_paths_from_db():
    """Get learning paths from DynamoDB"""

    try:
        input = path_input.query_paths()
        res = table.query(**input)
        items = res.get("Items", [])
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.get("/paths/videos")
@document_it
def get_videos_contains_path():
    """Get videos & playback orders included in learning paths"""

    try:
        input = path_input.query_videos()
        res = table.query(**input)
        items = res.get("Items", [])
        items.sort(key=lambda x: x["PK"])
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/path")
@document_it
def post_path(req_path: ReqPathPost):
    """Post learning path to DynamoDB"""

    try:
        input = path_input.put_item(path=req_path)
        res = table.put_item(Item=input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.put("/path")
@document_it
def put_path_to_db(req_path: ReqPathPutTransact):
    """Put learning path to DynamoDB"""

    try:
        transact_items = path_input.transact_update_path(path=req_path)
        # return transact_items
        res = db.client.transact_write_items(
            ReturnConsumedCapacity="INDEXES", TransactItems=transact_items
        )
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.delete("/path")
@document_it
def delete_path(req_path: ReqPathDeleteTransact):
    """Delete learning path and relations from DynamoDB"""

    try:
        transact_items = path_input.transact_remove_path(path=req_path)
        # return transact_items
        res = db.client.transact_write_items(
            ReturnConsumedCapacity="INDEXES", TransactItems=transact_items
        )
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
