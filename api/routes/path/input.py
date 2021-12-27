from collections import defaultdict
from boto3.dynamodb.conditions import Key, Attr

from api.routes.order.order import get_order, get_orders_contain_any_path
from api.routes.video.video import get_video_from_db, get_videos
from api.routes.video.schema import VideoFilter
from api.routes.order.schema import ReqOrder
from api.util import timestamp_jst
import uuid


def get_item(path_id):
    """input specified path from DynamoDB"""

    return dict(Key={"PK": path_id, "SK": path_id})


def query_paths():
    """input learning paths from DynamoDB"""

    return dict(
        IndexName="GSI-1-SK",
        KeyConditionExpression=Key("indexKey").eq("LearningPath"),
        ScanIndexForward=False,
    )


def query_videos():
    """input videos & playback orders from DynamoDB"""

    return dict(
        IndexName="GSI-1-SK",
        KeyConditionExpression=Key("indexKey").eq("Video"),
        FilterExpression=Attr("invalid").not_exists(),
    )


def put_item(path):
    """input post learning path to DynamoDB"""

    id = f"L-{str(uuid.uuid1())[:8]}"
    return dict(
        PK=id,
        SK=id,
        indexKey="LearningPath",
        name=path.name,
        description=path.description,
        note=path.note,
        invalid=False,
        createdAt=timestamp_jst(),
        createdUser=path.user,
        updatedAt=timestamp_jst(),
        updatedUser=path.user,
    )


def transact_update_path(path):
    """input transact update to DynamoDB"""

    # transact_write_itemsはclientAPIなので注意

    def update_path(path):
        """input put learning path to DynamoDB"""

        input = defaultdict(
            dict,
            TableName="primary_table",
            Key={"PK": {"S": path.PK}, "SK": {"S": path.PK}},
            UpdateExpression="SET updatedAt=:date, updatedUser=:user",
            ExpressionAttributeValues={
                ":date": {"S": timestamp_jst()},
                ":user": {"S": path.user},
            },
        )
        if path.name is not None:
            input["UpdateExpression"] += ", #name=:name"
            # name is reserved
            input["ExpressionAttributeNames"]["#name"] = "name"
            input["ExpressionAttributeValues"][":name"] = {"S": path.name}
        if path.description is not None:
            input["UpdateExpression"] += ", description=:description"
            input["ExpressionAttributeValues"][":description"] = {"S": path.description}
        if path.note is not None:
            input["UpdateExpression"] += ", note=:note"
            input["ExpressionAttributeValues"][":note"] = {"S": path.note}
        if path.invalid is not None:
            input["UpdateExpression"] += ", invalid=:invalid"
            input["ExpressionAttributeValues"][":invalid"] = {"BOOL": path.invalid}
        return {"Update": input}

    def update_path_to_video(appended, path_id, user):
        """input put video path to DynamoDB"""

        def get_videos(appended):
            for uri in appended:
                video_id = uri.split("/")[2]
                video = get_video_from_db(video_id)
                yield video

        def generate_input(video, path_id, user):
            path_ids = video.get("learningPathIds", [])
            # escape empty
            # なぜか集合のケースがあるので明示的にリストにする
            path_ids = list(path_ids)
            if not path_ids[0]:
                path_ids.remove("")
            path_ids.append(path_id)

            input = dict(
                TableName="primary_table",
                Key={"PK": {"S": video["PK"]}, "SK": {"S": video["PK"]}},
                UpdateExpression="SET updatedAt=:date, updatedUser=:user"
                + ", learningPathIds=:paths",
                ExpressionAttributeValues={
                    ":date": {"S": timestamp_jst()},
                    ":user": {"S": user},
                    ":paths": {"SS": path_ids},
                },
            )
            return {"Update": input}

        it_videos = get_videos(appended)
        it_inputs = (generate_input(video, path_id, user) for video in it_videos)
        return it_inputs

    def remove_path_from_video(removed, path_id, user):
        """input remove video path from DynamoDB"""

        def get_videos(removed):
            for uri in removed:
                video_id = uri.split("/")[2]
                video = get_video_from_db(video_id)
                yield video

        def generate_input(video, path_id, user):
            path_ids = video.get("learningPathIds", [])
            path_ids = list(path_ids)
            if path_id in path_ids:
                path_ids.remove(path_id)
            # escape empty
            if len(path_ids) <= 0:
                path_ids = [""]

            input = dict(
                TableName="primary_table",
                Key={"PK": {"S": video["PK"]}, "SK": {"S": video["PK"]}},
                UpdateExpression="SET updatedAt=:date, updatedUser=:user"
                + ", learningPathIds=:paths",
                ExpressionAttributeValues={
                    ":date": {"S": timestamp_jst()},
                    ":user": {"S": user},
                    ":paths": {"SS": path_ids},
                },
            )
            return {"Update": input}

        it_videos = get_videos(removed)
        it_inputs = (generate_input(video, path_id, user) for video in it_videos)
        return it_inputs

    def update_video_order(orders, path_id):
        """input append or update video orders"""

        def get_orders(orders, path_id):
            for order in orders:
                req_order = {"PK": path_id, "uri": order.uri}
                current_order = get_order(req_order=ReqOrder(**req_order))
                if current_order:
                    yield True, order
                else:
                    yield False, order

        def generate_input(order_with_update_or_append, path_id):
            is_update, order = order_with_update_or_append
            if is_update:
                input = dict(
                    TableName="primary_table",
                    Key={"PK": {"S": path_id}, "SK": {"S": order.uri}},
                    UpdateExpression="SET #order=:order",
                    # order is reserved
                    ExpressionAttributeNames={"#order": "order"},
                    ExpressionAttributeValues={":order": {"N": str(order.order)}},
                )
                return {"Update": input}
            else:
                input = dict(
                    PK={"S": path_id},
                    SK={"S": order.uri},
                    indexKey={"S": "Video"},
                    createdAt={"S": str(uuid.uuid1())[:8]},
                    order={"N": str(order.order)},
                )
                item = {"TableName": "primary_table", "Item": input}
                return {"Put": item}

        it_orders = get_orders(orders, path_id)
        it_inputs = (generate_input(order, path_id) for order in it_orders)
        return it_inputs

    def remove_video_order(removed, path_id):
        """input remove video orders"""

        inputs = [
            dict(
                TableName="primary_table", Key={"PK": {"S": path_id}, "SK": {"S": uri}}
            )
            for uri in removed
        ]
        orders = [{"Delete": input} for input in inputs]
        return orders

    transact_items = []

    # 再生リストのメタ情報を更新する
    transact_items.append(update_path(path))

    # 再生リストに動画が追加された場合、動画のメタデータに再生リストIDを追加する
    transact_items.extend(
        update_path_to_video(appended=path.appended, path_id=path.PK, user=path.user)
    )
    # 再生リストから動画が削除された場合、動画のメタデータから再生リストIDを削除する
    transact_items.extend(
        remove_path_from_video(removed=path.removed, path_id=path.PK, user=path.user)
    )
    # 再生リストから削除された動画の再生順を削除する
    transact_items.extend(remove_video_order(removed=path.removed, path_id=path.PK))
    # 再生リストの更新順を更新する
    transact_items.extend(update_video_order(orders=path.orders, path_id=path.PK))

    return transact_items


def transact_remove_path(path):
    """input transact update to DynamoDB"""

    def delete_path(path_id):
        input = dict(
            TableName="primary_table",
            Key={
                "PK": {"S": path_id},
                "SK": {"S": path_id},
            },
        )
        return {"Delete": input}

    def remove_path_from_video(path_id, user):
        """remove path from video"""

        filter = {"learningPathId": path_id}
        videos = get_videos(filter=VideoFilter(**filter), open=False)

        new_videos = []
        for video in videos:
            path_ids = video.get("learningPathIds", [])
            path_ids = list(path_ids)
            path_ids.remove(path_id)
            # escape empty
            if len(path_ids) <= 0:
                path_ids = [""]

            input = dict(
                TableName="primary_table",
                Key={
                    "PK": {"S": video["PK"]},
                    "SK": {"S": video["PK"]},
                },
                UpdateExpression="SET updateAt=:date, "
                + "updatedUser = :user, learningPathIds = :paths",
                ExpressionAttributeValues={
                    ":date": {"S": timestamp_jst()},
                    ":user": {"S": user},
                    ":paths": {"SS": path_ids},
                },
            )
            new_videos.append({"Update": input})
        return new_videos

    def remove_video_order(path_id):
        """remove video order"""

        orders = get_orders_contain_any_path(path_id)
        remove_orders = []
        for order in orders:
            input = dict(
                TableName="primary_table",
                Key={
                    "PK": {"S": order["PK"]},
                    "SK": {"S": order["SK"]},
                },
            )
            remove_orders.append({"Delete": input})
        return remove_orders

    transact_items = []
    # delete path meta
    transact_items.append(delete_path(path.PK))
    # update video meta
    transact_items.extend(remove_path_from_video(path.PK, path.user))
    # remove video order
    transact_items.extend(remove_video_order(path.PK))
    return transact_items
