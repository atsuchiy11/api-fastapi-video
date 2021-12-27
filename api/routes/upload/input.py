from boto3.dynamodb.conditions import Key, Attr
from api.util import get_today_string


def query():
    """input get upload status from DynamoDB"""

    return dict(
        IndexName="GSI-1-SK",
        KeyConditionExpression=Key("indexKey").eq("Status"),
        # FilterExpression=Attr("SK").begins_with("2021-12-16"),
        FilterExpression=Attr("SK").begins_with(get_today_string()),
        ScanIndexForward=False,
    )


def put_item(status, created_at):
    """input post upload status to DynamoDB"""

    # なんでidがuriなのか。。

    return dict(
        id=status.uri,
        PK=status.uri,
        SK=created_at,
        indexKey="Status",
        name=status.name,
        filename=status.filename,
        createdAt=created_at,
        createdUser=status.user,
        status=status.status,
    )


def update_item(status):
    """input put upload status to DynamoDB"""

    return dict(
        Key={"PK": status.uri, "SK": status.timestamp},
        UpdateExpression="SET #status=:status",
        # status is reserved
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":status": status.status},
    )


# {
#   "uri": "string",
#   "timestamp": "2021-12-17 11:46:39",
#   "status": "アップロード中"
# }
