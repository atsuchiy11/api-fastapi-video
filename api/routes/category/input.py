from collections import defaultdict
from boto3.dynamodb.conditions import Key
from api.util import timestamp_jst
import uuid


def query():
    """input get categories from DynamoDB"""

    return dict(
        IndexName="GSI-1-SK", KeyConditionExpression=Key("indexKey").eq("Category")
    )


def put_item(category):
    """input post category to DynamoDB"""

    id = f"C-{str(uuid.uuid1())[:8]}"
    return dict(
        PK=id,
        SK=id,
        indexKey="Category",
        name=category.name,
        parentId=category.parentId,
        description=category.description,
        note=category.note,
        invalid=False,
        createdAt=timestamp_jst(),
        createdUser=category.user,
        updatedAt=timestamp_jst(),
        updatedUser=category.user,
    )


def update_item(category):
    """input put category to DynamoDB"""

    input = defaultdict(
        dict,
        Key={"PK": category.PK, "SK": category.PK},
        UpdateExpression="SET updatedAt=:date, updatedUser=:user",
        ExpressionAttributeValues={":date": timestamp_jst(), ":user": category.user},
    )
    if category.name is not None:
        input["UpdateExpression"] += ", #name=:name"
        # name is reserved
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = category.name
    if category.description is not None:
        input["UpdateExpression"] += ", description=:description"
        input["ExpressionAttributeValues"][":description"] = category.description
    if category.note is not None:
        input["UpdateExpression"] += ", note=:note"
        input["ExpressionAttributeValues"][":note"] = category.note
    if category.invalid is not None:
        input["UpdateExpression"] += ", invalid=:invalid"
        input["ExpressionAttributeValues"][":invalid"] = category.invalid
    if category.parentId is not None:
        input["UpdateExpression"] += ", parentId=:parentId"
        input["ExpressionAttributeValues"][":parentId"] = category.parentId
    return input


def delete_item(category_id):
    """input delete category from DynamoDB"""

    return dict(Key={"PK": category_id, "SK": category_id})
