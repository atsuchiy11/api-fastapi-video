from boto3.dynamodb.conditions import Key, Attr
from api.util import get_today_string
from decimal import Decimal
import uuid


def query_by_user(user_id, limit):
    """input get histories for each user from DynamoDB"""

    return dict(
        KeyConditionExpression=Key("PK").eq(user_id),
        FilterExpression=Attr("indexKey").eq("History"),
        ScanIndexForward=False,
        Limit=limit,
    )


def query_all():
    """input get histories today"""

    return dict(
        IndexName="GSI-1-SK",
        KeyConditionExpression=Key("indexKey").eq("History")
        & Key("createdAt").begins_with(get_today_string()),
        ScanIndexForward=False,
    )


def put_item(history):
    """input post history to DynamoDB"""

    # float-decimal問題が後々顕在化するかも。。

    id = f"H-{str(uuid.uuid1())[:8]}"
    return dict(
        PK=history.user,
        SK=id,
        indexKey="History",
        createdAt=history.createdAt,
        videoUri=history.video,
        parse=Decimal(history.parse),
        finishedAt=history.finishedAt,
        referrer=history.referrer,
    )
