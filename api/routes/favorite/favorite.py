from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError

from api.util import document_it
from api.client import DynamoDB
from .schema import Favorite, ReqFavorite
import api.routes.favorite.input as favorite_input

router = APIRouter()
db = DynamoDB()
table = db.table


@router.get("/favorite/{user_id}", response_model=List[Favorite])
@document_it
def get_favorite(user_id):
    """Get favorite videos for each user from DynamoDB"""

    try:
        input = favorite_input.query(user_id)
        res = table.query(**input)
        items = res.get("Items", [])
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/favorite")
@document_it
def post_favorite(req_favorite: ReqFavorite):
    """Post favorite video to DynamoDB"""

    try:
        input = favorite_input.put_item(req_favorite)
        res = table.put_item(Item=input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.delete("/favorite")
@document_it
def delete_favorite(req_favorite: ReqFavorite):
    """Delete favorite video from DynamoDB"""

    try:
        input = favorite_input.delete_item(req_favorite)
        res = table.delete_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
