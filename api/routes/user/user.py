from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError

from api.util import document_it
from api.client import DynamoDB
from .schema import User, ReqUser
import api.routes.user.input as user_input

router = APIRouter()
db = DynamoDB()
table = db.table


@router.get("/users/login")
@document_it
def get_user_count():
    """Get login users count today"""

    try:
        input = user_input.login_count()
        res = table.query(**input)
        items = res.get("Items", [])
        return {"count": len(items)}

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.get("/user/{user_id}", response_model=User)
@document_it
def get_user(user_id):
    """Get user from DynamoDB"""

    try:
        input = user_input.get_item(user_id)
        res = table.get_item(**input)
        item = res.get("Item", {})
        return item

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.get("/users", response_model=List[User])
@document_it
def get_users():
    """Get users from DynamoDB"""

    try:
        input = user_input.query()
        res = table.query(**input)
        items = res.get("Items", {})
        # append index
        items = [{**item, **{"id": i}} for i, item in enumerate(items, 1)]
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/user")
@document_it
def post_user(req_user: ReqUser):
    """Post user to DynamoDB"""

    try:
        input = user_input.put_item(user=req_user)
        res = table.put_item(Item=input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.put("/user")
@document_it
def put_user(req_user: ReqUser):
    """Put user to DynamoDB"""

    try:
        input = user_input.update_item(user=req_user)
        res = table.update_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
