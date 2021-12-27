from datetime import datetime, timezone
import pytz
from pprint import pprint
from functools import wraps


dammy_videmo = {
    "indexKey": "",
    "PK": "",
    "SK": "",
    "createdAt": "",
    "createdUser": "",
    "updatedAt": "",
    "updatedUser": "",
    "invalid": False,
    "note": "This video is in Vimeo, but not in database",
    "description": "",
    "learningPathIds": [],
    "tagIds": [],
    "categoryId": "",
}
dammy_vimeo = {
    "uri": "",
    "duration": 0,
    "stats": {"plays": 0},
    "privacy": {"view": "none"},
}

#
# 閲覧用(all=False)と管理用(all=True)で返すデータを変える
# 閲覧用データ(all=False)
# ・DBなくてVimeoにある ->Skip
# ・DBにあってVimeoにない ->Skip
# 管理用データ(all=True)
# ・DBになくてVimeoにある ->取得してアラート
# ・DBにあってVimeoにない ->取得してアラート


def merge_videos(vimeo_response, db_response, all):
    """Merge videos from vimeo & videos from db"""

    vimeo_data = vimeo_response.get("data", [])
    records = []
    # マージ(DBを正としてループを回す->DBレコードは全て取得される)
    for item in db_response:
        item["match"] = False
        for data in vimeo_data:
            if item.get("PK", None) == data.get("uri", None):
                item["match"] = True
                item.update(data)
                records.append(item)
        # DBにあってVimeoにない
        if not item["match"]:
            records.append(item)

    # VimeoにあってDBにない
    for data in vimeo_data:
        data["match"] = False
        for item in db_response:
            if data.get("uri", None) == item.get("PK", None):
                data["match"] = True
        if not data["match"]:
            records.append(data)

    # DBにあってVimeoにない動画は除外する（バグる）
    if not all:
        excluded = [item for item in records if item["match"]]
        return excluded
    else:
        return records


def merge_paths_and_videos(paths, videos):
    """Merge paths and videos, playback orders"""
    for i, path in enumerate(paths, 1):
        path["id"] = i
        path["videos"] = []
        for video in videos:
            if path.get("PK") == video.get("PK"):
                _d = {}
                _d["uri"] = video.get("SK", None)
                _d["order"] = video.get("order", 0)
                path["videos"].append(_d)
        path["videos"].sort(key=lambda x: x["order"])
    return paths


# Vimeoから取得するパラメータ: uri,thumbnail,name,duration,plays
# レスポンスでエラーにせず（値は返す）、管理画面でエラーをキャッチする


def merge_table_for_video(videos, categories, tags, paths, users):
    """Create table data for video"""
    rows = []
    for i, video in enumerate(videos, 1):
        row = {}
        row["id"] = i
        row["match"] = video.get("match", True)
        row["uri"] = video.get("uri", "")
        row["invalid"] = video.get("invalid", True)

        if (thumbnail := video.get("thumbnail", None)) is not None:
            row["thumbnail"] = thumbnail.get("link", "")
        else:
            row["thumbnail"] = ""

        row["name"] = video.get("name", "")
        row["description"] = video.get("description", "")
        # find category
        secondary = [c for c in categories if c["PK"] == video.get("categoryId", None)]
        if not secondary:
            row["primary"] = ""
            row["secondary"] = ""
        else:
            primary = [c for c in categories if c["PK"] == secondary[0]["parentId"]]
            row["primary"] = primary[0]["name"]
            row["secondary"] = secondary[0]["name"]
        # find tags
        if not video.get("tagIds", None):
            row["tags"] = []
        else:
            tag_names = [
                t["name"] for t in tags for id in video["tagIds"] if id == t["PK"]
            ]
            row["tags"] = tag_names

        # find learning paths
        if not video.get("learningPathIds", None):
            row["paths"] = []
        else:
            path_names = [
                p["name"]
                for p in paths
                for id in video["learningPathIds"]
                if id == p["PK"]
            ]
            row["paths"] = path_names

        row["note"] = video.get("note", "")
        row["duration"] = video.get("duration", 0)

        if (stats := video.get("stats", None)) is not None:
            row["plays"] = stats.get("plays", 0)
        else:
            row["plays"] = 0

        # find user
        if not video.get("createdUser", None):
            row["createdUser"] = ""
            row["updatedUser"] = ""
        else:
            created_user = [u["name"] for u in users if video["createdUser"] == u["PK"]]
            updated_user = [u["name"] for u in users if video["updatedUser"] == u["PK"]]
            row["createdUser"] = created_user[0]
            row["updatedUser"] = updated_user[0]

        row["createdAt"] = video.get("createdAt", "")
        row["updatedAt"] = video.get("updatedAt", "")

        rows.append(row)
    return rows


def merge_categories(categories):
    """Merge parent and child categories"""
    for i, category in enumerate(categories, 1):
        category["id"] = i
        if (primary := category.get("parentId", None)) != "C999":
            for _category in categories:
                if _category.get("PK", None) == primary:
                    category["parent"] = _category["name"]
    return categories


paths = [
    {
        "IndexKey": "LearningPath",
        "PartitionKey": "L002",
        "PathCreatedAt": "20210802102536",
        "PathCreatedUser": "k-hase@prime-x.co.jp",
        "PathDescription": "中途社員入社時に受ける研修たち",
        "PathInvalid": False,
        "PathName": "キャリア入社社員入社時",
        "PathNote": "No Remarks",
        "PathUpdatedAt": "20210802102536",
        "PathUpdatedUser": "k-hase@prime-x.co.jp",
        "SortKey": "L002",
    },
    {
        "IndexKey": "LearningPath",
        "PartitionKey": "L001",
        "PathCreatedAt": "20210801093512",
        "PathCreatedUser": "k-hase@prime-x.co.jp",
        "PathDescription": "新入社員入社時に受ける研修たち",
        "PathInvalid": False,
        "PathName": "新入社員入社時",
        "PathNote": "No Remarks",
        "PathUpdatedAt": "20210803210541",
        "PathUpdatedUser": "k-hase@prime-x.co.jp",
        "SortKey": "L001",
    },
]

videos = [
    {
        "IndexKey": "Video",
        "PartitionKey": "L001",
        "PlaybackOrder": 2,
        "SortKey": "/videos/561734799",
    },
    {
        "IndexKey": "Video",
        "PartitionKey": "L001",
        "PlaybackOrder": 1,
        "SortKey": "/videos/564506284",
    },
    {
        "IndexKey": "Video",
        "PartitionKey": "L002",
        "PlaybackOrder": 2,
        "SortKey": "/videos/561734799",
    },
    {
        "IndexKey": "Video",
        "PartitionKey": "L002",
        "PlaybackOrder": 1,
        "SortKey": "/videos/543097654",
    },
]


def document_it(func):
    """Information this function (not async)"""

    def wrapper(*args, **kwargs):
        print("=========== Running function:", func.__name__, "===========")
        print("Positional arguments:", args)
        print("Keyword arguments:", kwargs, "\n")
        return func(*args, **kwargs)

    original_wrapper_annotations = wrapper.__annotations__
    wraps(func)(wrapper)
    wrapper.__annotations__.update(original_wrapper_annotations)
    return wrapper


def timestamp_jst():
    """Return timestamp(JST) formated yyyy-mm-dd hh:mm:ss"""

    now = datetime.now(tz=timezone.utc)
    tokyo = pytz.timezone("Asia/Tokyo")
    jst_now = tokyo.normalize(now.astimezone(tokyo))
    return jst_now.strftime("%Y-%m-%d %H:%M:%S")


def get_today_string():
    now = datetime.now(tz=timezone.utc)
    tokyo = pytz.timezone("Asia/Tokyo")
    jst_now = tokyo.normalize(now.astimezone(tokyo))
    return jst_now.strftime("%Y-%m-%d")


def generate_id(id, *, digit=4):
    """Return zero filled item ID"""
    num = int(id[1:]) + 1
    initial = id[0]
    return initial + str(num).zfill(digit)


if __name__ == "__main__":
    """Run locally, but need test data"""

    # res = merge_paths_and_videos(paths, videos)
    # res = timestamp_jst()
    # res = id_generate("H", num=1, digit=4)
    # res = generate_id("H0")
    res = get_today_string()
    pprint(res)
