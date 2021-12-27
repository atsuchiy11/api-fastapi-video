from collections import defaultdict
from boto3.dynamodb.conditions import Key
from api.util import timestamp_jst
import uuid


def query(active):
    """input get banners from DynamoDB"""

    # バナー1つで運用しているのでフラグ管理は本来不要

    input = defaultdict(
        dict, IndexName="GSI-1-SK", KeyConditionExpression=Key("indexKey").eq("Banner")
    )
    if not active:
        input["FilterExpression"] = Key("invalid").eq(False)
    return input


def put_item(banner):
    """input post banner to DynamoDB"""

    id = f"B-{str(uuid.uuid1())[:8]}"
    return dict(
        PK=id,
        SK=id,
        indexKey="Banner",
        name=banner.name,
        descriiption=banner.description,
        image=banner.image,
        note=banner.note,
        invalid=False,
        createdAt=timestamp_jst(),
        createdUser=banner.user,
        updatedAt=timestamp_jst(),
        updatedUser=banner.user,
    )


def update_item(banner):
    """input put banner to DynamoDB"""

    input = defaultdict(
        dict,
        Key={"PK": banner.PK, "SK": banner.PK},
        UpdateExpression="SET updatedAt=:date, updatedUser=:user",
        ExpressionAttributeValues={":date": timestamp_jst(), ":user": banner.user},
    )
    if banner.name is not None:
        input["UpdateExpression"] += ", #name=:name"
        # name is reserved
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = banner.name
    if banner.description is not None:
        input["UpdateExpression"] += ", description=:description"
        input["ExpressionAttributeValues"][":description"] = banner.description
    if banner.image is not None:
        input["UpdateExpression"] += ", image=:image"
        input["ExpressionAttributeValues"][":image"] = banner.image
    if banner.link is not None:
        input["UpdateExpression"] += ", link=:link"
        input["ExpressionAttributeValues"][":link"] = banner.link
    if banner.note is not None:
        input["UpdateExpression"] += ", note=:note"
        input["ExpressionAttributeValues"][":note"] = banner.note
    if banner.invalid is not None:
        input["UpdateExpression"] += ", invalid=:invalid"
        input["ExpressionAttributeValues"][":invalid"] = banner.invalid

    return input
