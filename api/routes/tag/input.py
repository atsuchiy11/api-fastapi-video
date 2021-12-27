from collections import defaultdict
from boto3.dynamodb.conditions import Key
from api.routes.video.video import get_videos
from api.routes.video.schema import VideoFilter
from api.util import timestamp_jst
import uuid


def query():
    """input get tags from DynamoDB"""

    return dict(IndexName="GSI-1-SK", KeyConditionExpression=Key("indexKey").eq("Tag"))


def put_item(tag):
    """input post tag to DynamoDB"""

    id = f"T-{str(uuid.uuid1())[:8]}"
    return dict(
        PK=id,
        SK=id,
        indexKey="Tag",
        name=tag.name,
        description=tag.description,
        note=tag.note,
        invalid=False,
        createdAt=timestamp_jst(),
        createdUser=tag.user,
        updatedAt=timestamp_jst(),
        updatedUser=tag.user,
    )


def update_item(tag):
    """input put tag to DynamoDB"""

    input = defaultdict(
        dict,
        Key={"PK": tag.PK, "SK": tag.PK},
        UpdateExpression="SET updatedAt=:date, updatedUser=:user",
        ExpressionAttributeValues={":date": timestamp_jst(), ":user": tag.user},
    )
    if tag.name is not None:
        input["UpdateExpression"] += ", #name=:name"
        # name is reserved
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = tag.name
    if tag.description is not None:
        input["UpdateExpression"] += ", description=:description"
        input["ExpressionAttributeValues"][":description"] = tag.description
    if tag.note is not None:
        input["UpdateExpression"] += ", note=:note"
        input["ExpressionAttributeValues"][":note"] = tag.note
    if tag.invalid is not None:
        input["UpdateExpression"] += ", invalid=:invalid"
        input["ExpressionAttributeValues"][":invalid"] = tag.invalid
    return input


def transact_remove_tag(tag):
    """input transact update to DynamoDB"""

    def delete_item(tag_id):
        """input delete tag from DynamoDB"""

        input = dict(
            TableName="primary_table", Key={"PK": {"S": tag_id}, "SK": {"S": tag_id}}
        )
        return {"Delete": input}

    def remove_tag_from_video(tag_id, user):
        """input remove tag from video"""

        filter = {"tagId": tag_id}
        videos = get_videos(filter=VideoFilter(**filter), open=False)

        new_videos = []
        for video in videos:
            tag_ids = video.get("tagIds", [])
            tag_ids = list(tag_ids)
            tag_ids.remove(tag_id)
            # escape empty
            if len(tag_ids) <= 0:
                tag_ids = [""]

            input = dict(
                TableName="primary_table",
                Key={"PK": {"S": video["PK"]}, "SK": {"S": video["PK"]}},
                UpdateExpression="SET updatedAt=:date, "
                + "updatedUser=:user, tagIds=:tags",
                ExpressionAttributeValues={
                    ":date": {"S": timestamp_jst()},
                    ":user": {"S": user},
                    ":tags": {"SS": tag_ids},
                },
            )
            new_videos.append({"Update": input})
        return new_videos

    transact_items = []
    transact_items.append(delete_item(tag.PK))
    transact_items.extend(remove_tag_from_video(tag.PK, tag.user))
    return transact_items
