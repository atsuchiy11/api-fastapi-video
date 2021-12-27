from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError

from api.util import document_it
from api.client import DynamoDB
from .schema import Tag, ReqTagPost, ReqTagPut, ReqTagDelete
import api.routes.tag.input as tag_input


router = APIRouter()
db = DynamoDB()
table = db.table


@router.get("/tag/{tag_id}", response_model=Tag)
@document_it
def get_tag(tag_id):
    """Get a specific tag from DynamoDB"""

    try:
        input = dict(Key={"PK": tag_id, "SK": tag_id})
        res = table.get_item(**input)
        item = res.get("Item", {})
        item = {**item, **{"id": 1}}
        return item

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.get("/tags", response_model=List[Tag])
@document_it
def get_tags():
    """Get tags from DynamoDB"""

    try:
        input = tag_input.query()
        res = table.query(**input)
        items = res.get("Items", [])
        # append index
        items = [{**item, **{"id": i}} for i, item in enumerate(items, 1)]
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/tag")
@document_it
def post_tag(req_tag: ReqTagPost):
    """Post tag to DynamoDB"""

    try:
        # check duplicate
        # queryにname検索をつければいいけどどうしよう。。
        tags = get_tags()
        duplicate = [tag for tag in tags if tag["name"] == req_tag.name]
        if duplicate:
            return {"duplicate": True}

        input = tag_input.put_item(tag=req_tag)
        res = table.put_item(Item=input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.put("/tag")
@document_it
def put_tag(req_tag: ReqTagPut):
    """Put tag to DynamoDB"""

    try:
        input = tag_input.update_item(tag=req_tag)
        res = table.update_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.delete("/tag")
@document_it
def delete_tag(req_tag: ReqTagDelete):
    """Delete tag from DynamoDB"""

    try:
        transact_items = tag_input.transact_remove_tag(tag=req_tag)
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
