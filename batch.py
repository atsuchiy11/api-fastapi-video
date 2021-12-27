import os
import vimeo
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from requests import HTTPError
from aws_dynamodb_parser import parse
from dotenv import load_dotenv
import urllib.parse
import concurrent.futures
import asyncio
import math
import time

from dataclasses import dataclass
from functools import reduce

from pprint import pprint
import pickle

load_dotenv(override=True)


#
#
#


def create_vimeo_client():
    """create Vimeo client"""

    return vimeo.VimeoClient(
        token=os.environ.get("VIMEO_TOKEN_PROD"),
        key=os.environ.get("VIMEO_KEY_PROD"),
        secret=os.environ.get("VIMEO_SECRET_PROD"),
    )


def create_dynamodb_client():
    """Create DynamoDB client"""

    return boto3.client(
        "dynamodb",
        region_name=os.environ.get("REGION"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def create_dynamodb_resource():
    """Create DynamoDB resource"""

    return boto3.resource(
        "dynamodb",
        region_name=os.environ.get("REGION"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def create_query_input(open=True):
    input = {
        "TableName": "primary_table",
        "IndexName": "GSI-1-SK",
        "KeyConditionExpression": "#key = :value",
        # remove video relations
        "FilterExpression": "attribute_exists(#invalid)",
        "ScanIndexForward": False,
        "ExpressionAttributeNames": {"#key": "indexKey", "#invalid": "invalid"},
        "ExpressionAttributeValues": {
            ":value": {"S": "Video"},
        },
    }
    if open:
        input["FilterExpression"] = "#invalid = :invalid"
        input["ExpressionAttributeValues"][":invalid"] = {"BOOL": False}

    return input


def query(client, open=True):
    """DBから動画一覧を取得"""

    print("get videos from DynamoDB")
    start = time.time()

    input = create_query_input(open)
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))

        print(time.time() - start)
        return items

    except ClientError as err:
        print(err)
    except BaseException as err:
        print(err)


def get_videos_from_vimeo(client, chunk, page):
    """Vimeo動画を取得（100件/1APIコール）"""

    params = {
        "page": page,
        "per_page": chunk,
        "fields": "uri,name,duration,stats,privacy,embed.html,pictures.sizes",
    }
    query_params = urllib.parse.urlencode(params)

    try:
        res = client.get(f"/me/videos?{query_params}")
        res_json = res.json()
        return res_json

    except ClientError as err:
        print(err)
    except BaseException as err:
        print(err)


def get_total(client):
    """Vimeo側の動画総数を取得"""

    params = {"fields": "total"}
    query_params = urllib.parse.urlencode(params)
    try:
        res = client.get(f"/me/videos?{query_params}")
        # res.rasie_for_status()
        res_json = res.json()
        return res_json

    except HTTPError as err:
        print(err)
    except BaseException as err:
        print(err)


async def getter(client):
    """マルチスレッドでVimeo動画を全取得"""

    print("get videos from vimeo")
    start = time.time()

    chunk = 100
    pre = get_total(client)
    times = math.ceil(pre["total"] / chunk)

    videos = []

    try:
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            tasks = [
                loop.run_in_executor(executor, get_videos_from_vimeo, client, chunk, i)
                for i in range(1, times + 1)
            ]
            data = await asyncio.gather(*tasks)
            # merge data
            for lot in data:
                videos += lot["data"]

            print(time.time() - start)
            return videos

    except HTTPError as err:
        print(err)
    except BaseException as err:
        print(err)


def sort(vimeo_res):
    """VimeoレスポンスをDB用のデータ形式に整形する"""

    sorted = []
    for data in vimeo_res:
        d = {}
        d["uri"] = data["uri"]
        d["uri"] = data["uri"]
        d["name"] = data["name"]
        d["duration"] = data["duration"]
        d["plays"] = data["stats"]["plays"]
        d["privacy"] = data["privacy"]["view"]
        d["html"] = data["embed"]["html"]

        pictures = data["pictures"]["sizes"]
        *_, max_size = pictures
        d["thumbnail"] = max_size["link"]
        sorted.append(d)
    return sorted


def diff(db_res, vimeo_res):
    """DBとVimeoの差分を取る"""

    diff = []

    for item in db_res:
        is_found = False
        for data in vimeo_res:
            is_diff = False
            if item.get("PK") == data.get("uri"):
                is_found = True
                for key, value in data.items():
                    if item[key] != value:
                        item[key] = value
                        is_diff = True
                if is_diff:
                    diff.append(item)
        # DBにあってVimeoにない場合はフラグを立てる
        if not is_found:
            item["match"] = False
            diff.append(item)

    return diff


def merge(db_res, vimeo_res):
    """
    DBのレスポンスにVimeoのレスポンスを付与する
    Vimeo固有のパラメータ
    ・URI
    ・タイトル
    ・再生時間
    ・再生回数
    ・埋め込みタグ
    ・サムネURL
    """

    for item in db_res:
        for data in vimeo_res:
            if item.get("PK") == data.get("uri"):
                for key, value in data.items():
                    item[key] = value
    return db_res


def bulk_update(db, items):
    """マージしたデータでDB一括更新（厳密には置換）"""

    print("bulk update to DynamoDB")
    start = time.time()

    try:
        table = db.Table("primary_table")
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

        print(time.time() - start)
        return {"status": "finished"}
    except ClientError as err:
        print(err)
    except BaseException as err:
        print(err)


def make_pickle(data, name):
    """DB/Vimeoレスポンスをpickle化する"""

    with open(name, "wb") as f:
        pickle.dump(data, f)


#
#
#


@dataclass
class Auth:
    region_name: str = os.environ.get("REGION")
    aws_access_key_id: str = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = os.environ.get("AWS_SECRET_ACCESS_KEY")
    vimeo_token: str = os.environ.get("VIMEO_TOKEN_PROD")
    vimeo_key: str = os.environ.get("VIMEO_KEY_PROD")
    vimeo_secret: str = os.environ.get("VIMEO_SECRET_PROD")

    def create_dynamodb_resource(self):
        return boto3.resource(
            "dynamodb",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def create_vimeo_client(self):
        return vimeo.VimeoClient(
            token=self.vimeo_token, key=self.vimeo_key, secret=self.vimeo_secret
        )


class DynamoDB(Auth):
    def __init__(self):
        super().__init__()
        self.resource = self.create_dynamodb_resource()
        self.table = self.resource.Table("primary_table")

    def query(self, open):
        print("get videos from DynamoDB")
        start = time.time()

        input = dict(
            IndexName="GSI-1-SK",
            KeyConditionExpression=Key("indexKey").eq("Video"),
            FilterExpression=Attr("invalid").exists(),
            ScanIndexForward=False,
        )
        if open:
            input["FilterExpression"] &= Key("invalid").eq(False)
        try:
            res = self.table.query(**input)
            items = res.get("Items", [])

            print(time.time() - start)
            return items
        except ClientError as err:
            print(err)
        except BaseException as err:
            print(err)

    def diff(self, db_res, vimeo_res):
        diff = []

        for item in db_res:
            is_found = False
            for data in vimeo_res:
                is_diff = False
                if item.get("PK") == data.get("uri"):
                    is_found = True
                    for key, value in data.items():
                        if item[key] != value:
                            item[key] = value
                            is_diff = True
                    if is_diff:
                        diff.append(item)
            # DBにあってVimeoにない場合はフラグを立てる
            if not is_found:
                item["match"] = False
                diff.append(item)
        return diff

    def bulk_update(self, items):
        print("bulk update to DynamoDB")
        start = time.time()

        try:
            with self.table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)

            print(time.time() - start)
            return {"status": "finished"}
        except ClientError as err:
            print(err)
        except BaseException as err:
            print(err)


class VimeoAPI(Auth):
    def __init__(self):
        super().__init__()
        self.client = self.create_vimeo_client()

    def sort(self, json):
        data = dict(
            uri=json.get("uri", None),
            name=json.get("name", None),
            duration=json.get("duration", 0),
            plays=json.get("stats").get("plays", 0),
            html=json.get("embed").get("html"),
        )
        pictures = json.get("pictures").get("sizes")
        *_, max_size = pictures
        data["thumbnail"] = max_size.get("link")
        return data

    def get_total(self):
        params = {"fields": "total"}
        query_params = urllib.parse.urlencode(params)
        try:
            res = self.client.get(f"/me/videos?{query_params}")
            res.raise_for_status()
            return res.json()
        except HTTPError as err:
            raise err
        except BaseException as err:
            raise err

    def get_videos_by_page(self, chunk, page):
        params = {
            "page": page,
            "per_page": chunk,
            "fields": "uri,name,duration,stats,privacy,embed.html,pictures.sizes",
        }
        query_params = urllib.parse.urlencode(params)
        try:
            res = self.client.get(f"/me/videos?{query_params}")
            res.raise_for_status()
            res_json = res.json()
            data = [self.sort(d) for d in res_json.get("data", [])]
            return data
        except HTTPError as err:
            raise err
        except BaseException as err:
            raise err

    async def get_videos(self):
        print("get videos from DynamoDB")
        start = time.time()

        chunk = 100
        try:
            pre = self.get_total()
            times = math.ceil(pre["total"] / chunk)
            loop = asyncio.get_running_loop()

            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                tasks = [
                    loop.run_in_executor(executor, self.get_videos_by_page, chunk, i)
                    for i in range(1, times + 1)
                ]
                data = await asyncio.gather(*tasks)
                merged = reduce(lambda a, b: a + b, data)

                print(time.time() - start)
                return merged
        except HTTPError as err:
            raise err
        except BaseException as err:
            raise err


if __name__ == "__main__":

    db_client = create_dynamodb_client()
    vimeo_client = create_vimeo_client()
    db_resource = create_dynamodb_resource()

    db = DynamoDB()
    v = VimeoAPI()

    try:

        """pickleがない場合はAPIをコールする"""
        db_res = db.query(open=False)
        print(len(db_res))

        vimeo_res = asyncio.run(v.get_videos())
        print(len(vimeo_res))

        data_diff = db.diff(db_res, vimeo_res)
        print(len(data_diff))
        if len(data_diff) > 0:
            pprint(data_diff)

        # res = db.bulk_update(items=data_diff)
        # print(res)

        # vimeo_res = asyncio.run(getter(vimeo_client))
        # print("vimeo_total:", len(vimeo_res))
        # make_pickle(vimeo_res, "vimeo.pickle")

        # db_res = query(db_client, open=False)
        # print("db_total:", len(db_res))
        # make_pickle(db_res, "db.pickle")

        """pickleがある場合は読み込む"""

        # f_db = open("db.pickle", "rb")
        # db_res = pickle.load(f_db)

        # f_vimeo = open("vimeo.pickle", "rb")
        # vimeo_res = pickle.load(f_vimeo)

        # vimeo_res_sorted = sort(vimeo_res)

        """差分更新"""
        # db_res_diff = diff(db_res, vimeo_res_sorted)
        # pprint(db_res_diff)
        # res = bulk_update(db=db_resource, items=db_res_diff)

        """全更新"""
        # merged_items = merge(db_res=db_res, vimeo_res=vimeo_res_sorted)
        # pprint(merged_items[0])
        # res = bulk_update(db=db_resource, items=merged_items)
        # print(res)

    except HTTPError as err:
        print(err)
    except ClientError as err:
        print(err)
    except BaseException as err:
        print(err)
