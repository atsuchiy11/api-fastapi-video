from collections import defaultdict
from boto3.dynamodb.conditions import Key
from api.util import get_today_string, timestamp_jst


def login_count():
    """input get login users count today"""

    return dict(
        IndexName="GSI-1-SK",
        KeyConditionExpression=Key("indexKey").eq("User")
        & Key("createdAt").begins_with(get_today_string()),
        ScanIndexForward=False,
    )


def get_item(user_id):
    """input get user from DynamoDB"""

    return dict(Key={"PK": user_id, "SK": user_id})


def query():
    """input get users from DynamoDB"""

    return dict(IndexName="GSI-1-SK", KeyConditionExpression=Key("indexKey").eq("User"))


def put_item(user):
    """input post user to DynamoDB"""

    return dict(
        PK=user.PK,
        SK=user.PK,
        indexKey="User",
        createdAt=timestamp_jst(),
        updatedAt=timestamp_jst(),
        name=user.name,
        image=user.image,
        acl="user",
    )


def update_item(user):
    """input put user to DynamoDB"""

    # 元がちょっと変？？(createdAtとupdatedAtが逆？)

    input = defaultdict(
        dict,
        Key={"PK": user.PK, "SK": user.PK},
        UpdateExpression="SET createdAt=:date",
        ExpressionAttributeValues={":date": timestamp_jst()},
    )
    if user.name is not None:
        input["UpdateExpression"] += ", #name=:name"
        # name is reserved
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = user.name
    if user.image is not None:
        input["UpdateExpression"] += ", image=:image"
        input["ExpressionAttributeValues"][":image"] = user.image
    if user.acl is not None:
        input["UpdateExpression"] += ", acl=:acl"
        input["ExpressionAttributeValues"][":acl"] = user.acl
    return input
