from boto3.dynamodb.conditions import Attr, Key
from api.util import timestamp_jst


def query(video_id):
    """input get threads for video from DynamoDB"""

    return dict(
        KeyConditionExpression=Key("PK").eq(f"/videos/{video_id}"),
        FilterExpression=Attr("indexKey").eq("Thread") & Attr("invalid").eq(False),
    )


def put_item(thread):
    """input post thread for video to DynamoDB"""

    # 親スレッドがあれば日付をつけてPKにする
    if thread.thread:
        # replay
        thread_post = f"{thread.thread}_{timestamp_jst()}"
        created_at = thread.thread
    else:
        # new thread
        thread_post = f"{timestamp_jst()}_{timestamp_jst()}"
        created_at = timestamp_jst()

    return dict(
        PK=thread.video,
        SK=thread_post,
        indexKey="Thread",
        createdAt=created_at,
        createdUser=thread.user,
        body=thread.body,
        invalid=False,
    )


# スレッドは削除しない。無効フラグを立てて非表示にする
def update_item(thread):
    """input put thread for video to DynamoDB"""

    if thread.body:
        return dict(
            Key={"PK": thread.video, "SK": thread.id},
            UpdateExpression="SET body=:body",
            ExpressionAttributeValues={":body": thread.body},
        )
    else:
        return dict(
            Key={"PK": thread.video, "SK": thread.id},
            UpdateExpression="SET invalid=:invalid",
            ExpressionAttributeValues={":invalid": thread.invalid},
        )
