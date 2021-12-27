from boto3.dynamodb.conditions import Key, Attr
import uuid

from api.util import timestamp_jst


def query(video_id):
    """input get likes for video from DynamoDB"""

    return dict(
        KeyConditionExpression=Key("PK").eq(f"/videos/{video_id}"),
        FilterExpression=Attr("indexKey").eq("Like"),
    )


def put_item(like):
    """input post like for video to DynamoDB"""

    id = f"like-{str(uuid.uuid1())[:8]}"
    return dict(
        PK=like.video,
        SK=id,
        indexKey="Like",
        createdAt=timestamp_jst(),
        createdUser=like.user,
        like=like.like,
    )


def delete_item(like):
    """input delete like for video from DynamoDB"""

    return dict(Key={"PK": like.video, "SK": like.id})
