from typing import List
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr


from api.util import document_it
from api.client import DynamoDB
from .schema import ReqOrder, Order

router = APIRouter()
db = DynamoDB()
table = db.table


@router.post("/order", response_model=Order)
@document_it
def get_order(req_order: ReqOrder):
    """Get order meta from DynamoDB"""

    try:
        input = dict(Key={"PK": req_order.PK, "SK": req_order.uri})
        res = table.get_item(**input)
        item = res.get("Item", {})
        return item

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.get("/orders/{path_id}", response_model=List[Order])
@document_it
def get_orders_contain_any_path(path_id):
    """Get orders contain any path from DynamoDB"""

    try:
        input = dict(
            KeyConditionExpression=Key("PK").eq(path_id),
            FilterExpression=Attr("order").exists(),
        )
        res = table.query(**input)
        items = res.get("Items", [])
        items.sort(key=lambda x: x["order"])
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
