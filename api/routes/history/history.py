from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError

from api.util import document_it
from api.client import DynamoDB
from .schema import UserHistory, ReqHistory
import api.routes.history.input as history_input

router = APIRouter()
db = DynamoDB()
table = db.table


@router.get("/history/{user_id}", response_model=List[UserHistory])
@document_it
def get_history(user_id, limit: int = 30):
    """Get histories for each user from DynamoDB (limit 30)"""

    try:
        input = history_input.query_by_user(user_id, limit=limit)
        res = table.query(**input)
        items = res.get("Items", [])
        items.sort(key=lambda x: x["createdAt"], reverse=True)
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.get("/histories")
@document_it
def get_histories():
    """Get histories today"""

    try:
        input = history_input.query_all()
        res = table.query(**input)
        items = res.get("Items", [])
        return {"count": len(items)}

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/history")
@document_it
def post_history(req_history: ReqHistory):
    """Post history to DynamoDB"""

    try:
        input = history_input.put_item(req_history)
        res = table.put_item(Item=input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
