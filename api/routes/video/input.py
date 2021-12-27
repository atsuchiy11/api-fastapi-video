from collections import defaultdict
from boto3.dynamodb.conditions import Key, Attr
from api.util import timestamp_jst


def get_item(video_id):
    """input specified video from DynamoDB"""

    uri = f"/videos/{video_id}"
    return dict(Key={"PK": uri, "SK": uri})


def query(filter, open):
    """input videos from DynamoDB"""

    input = dict(
        IndexName="GSI-1-SK",
        KeyConditionExpression=Key("indexKey").eq("Video"),
        FilterExpression=Attr("invalid").exists(),
        ScanIndexForward=False,
    )
    # open=Trueなら非公開も全て取得する
    if open:
        input["FilterExpression"] &= Key("invalid").eq(False)
    # filtering
    if filter.categoryId:
        input["FilterExpression"] &= Attr("categoryId").contains(filter.categoryId)
    if filter.tagId:
        input["FilterExpression"] &= Attr("tagIds").contains(filter.tagId)
    if filter.learningPathId:
        input["FilterExpression"] &= Attr("learningPathIds").contains(
            filter.learningPathId
        )
    if filter.name:
        input["FilterExpression"] &= Attr("name").contains(filter.name)
    return input


def put_item(video):
    """input post video to DynamoDB"""

    input = dict(
        PK=video.PK,
        SK=video.PK,
        indexKey="Video",
        invalid=False,
        createdAt=timestamp_jst(),
        createdUser=video.user,
        updatedAt=timestamp_jst(),
        updatedUser=video.user,
        description=video.description,
        categoryId=video.categoryId,
        tagIds=video.tagIds,
        learningPathIds=[""],
        note=video.note,
    )
    # vimeo params
    if video.uri is not None:
        input["uri"] = video.uri
    if video.thumbnail is not None:
        input["thumbnail"] = video.thumbnail
    if video.plays is not None:
        input["plays"] = video.plays
    if video.name is not None:
        input["name"] = video.name
    if video.duration is not None:
        input["duration"] = video.duration
    if video.html is not None:
        input["html"] = video.html
    return input


def update_item(video):
    """input put video to DynamoDB"""

    input = defaultdict(
        dict,
        Key={"PK": video.PK, "SK": video.PK},
        UpdateExpression="SET updatedAt=:date, updatedUser=:user",
        ExpressionAttributeValues={":date": timestamp_jst(), ":user": video.user},
    )
    if video.categoryId is not None:
        input["UpdateExpression"] += ", categoryId=:category"
        input["ExpressionAttributeValues"][":category"] = video.categoryId
    if video.tagIds is not None:
        input["UpdateExpression"] += ", tagIds=:tags"
        input["ExpressionAttributeValues"][":tags"] = video.tagIds
    if video.learningPathIds is not None:
        input["UpdateExpression"] += ", learningPathIds=:paths"
        input["ExpressionAttributeValues"][":paths"] = video.learningPathIds
    if video.description is not None:
        input["UpdateExpression"] += ", description=:description"
        input["ExpressionAttributeValues"][":description"] = video.description
    if video.note is not None:
        input["UpdateExpression"] += ", note=:note"
        input["ExpressionAttributeValues"][":note"] = video.note
    if video.invalid is not None:
        input["UpdateExpression"] += ", invalid=:invalid"
        input["ExpressionAttributeValues"][":invalid"] = video.invalid
    # vimeo params
    if video.uri is not None:
        input["UpdateExpression"] += ", uri=:uri"
        input["ExpressionAttributeValues"][":uri"] = video.uri
    if video.thumbnail is not None:
        input["UpdateExpression"] += ", thumbnail=:thumbnail"
        input["ExpressionAttributeValues"][":thumbnail"] = video.thumbnail
    if video.plays is not None:
        input["UpdateExpression"] += ", plays=:plays"
        input["ExpressionAttributeValues"][":plays"] = str(video.plays)
    if video.name is not None:
        input["UpdateExpression"] += ", #name=:name"
        # name is reserved
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = video.name
    if video.duration is not None:
        input["UpdateExpression"] += ", #duration=:duration"
        # duration is reserved
        input["ExpressionAttributeNames"]["#duration"] = "duration"
        input["ExpressionAttributeValues"][":duration"] = str(video.duration)
    if video.html is not None:
        input["UpdateExpression"] += ", html=:html"
        input["ExpressionAttributeValues"][":html"] = video.html
    return input
