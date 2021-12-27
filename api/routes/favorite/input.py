from boto3.dynamodb.conditions import Key, Attr

from api.util import timestamp_jst

# from api.util import timestamp_jst


def query(user_id):
    """input get favorite videos for each user from DynamoDB"""

    return dict(
        KeyConditionExpression=Key("PK").eq(user_id),
        FilterExpression=Attr("indexKey").eq("Favorite"),
    )


def put_item(favorite):
    """input post favorite video to DynamoDB"""

    return dict(
        PK=favorite.user,
        SK=favorite.video,
        indexKey="Favorite",
        createdAt=timestamp_jst(),
    )


def delete_item(favorite):
    """input delete favorite video from DynamoDB"""

    return dict(Key={"PK": favorite.user, "SK": favorite.video})
