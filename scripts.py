import vimeo
import os
import boto3
import uuid
from dotenv import load_dotenv
from pprint import pprint
from botocore.exceptions import ClientError

from api.util import timestamp_jst
import asyncio
import concurrent.futures

load_dotenv(override=True)


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


def create_s3_resource():
    """Create S3 resource"""
    return boto3.resource(
        "s3",
        region_name=os.environ.get("REGION"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def create_s3_client():
    """Create S3 resource"""
    return boto3.client(
        "s3",
        region_name=os.environ.get("REGION"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def create_input_tags(bulk_input):
    # id = "T-" + str(uuid.uuid1())[:8]
    ids = ["T-" + str(uuid.uuid1())[:8] for _ in range(0, len(bulk_input))]
    return [
        {
            "PK": id,
            "SK": id,
            "indexKey": "Tag",
            "createdAt": timestamp_jst(),
            "createdUser": "a2-tsuchiya@prime-x.co.jp",
            "updatedAt": timestamp_jst(),
            "updatedUser": "a2-tsuchiya@prime-x.co.jp",
            "invalid": False,
            "note": "No Remarks",
            "name": input,
            "description": "ツールチップに表示されるのでちゃんと入力してねw",
        }
        for input, id in zip(bulk_input, ids)
    ]


def create_input_categories(bulk_input):
    ids = ["C-" + str(uuid.uuid1())[:8] for _ in range(0, len(bulk_input))]
    return [
        {
            "PK": id,
            "SK": id,
            "indexKey": "Category",
            "createdAt": timestamp_jst(),
            "createdUser": "a2-tsuchiya@prime-x.co.jp",
            "updatedAt": timestamp_jst(),
            "updatedUser": "a2-tsuchiya@prime-x.co.jp",
            "invalid": False,
            "note": "No Remarks",
            "name": child,
            "description": "後で埋めてくださいw",
            "parentId": parent,
        }
        for (parent, child), id in zip(bulk_input, ids)
    ]


def create_input_paths(bulk_input):
    ids = ["L-" + str(uuid.uuid1())[:8] for _ in range(0, len(bulk_input))]
    return [
        {
            "PK": id,
            "SK": id,
            "indexKey": "LearningPath",
            "createdAt": timestamp_jst(),
            "createdUser": "a2-tsuchiya@prime-x.co.jp",
            "updatedAt": timestamp_jst(),
            "updatedUser": "a2-tsuchiya@prime-x.co.jp",
            "invalid": False,
            "note": "No Remarks",
            "name": input,
            "description": "後で埋めてくださいw",
        }
        for input, id in zip(bulk_input, ids)
    ]


def bulk_import(db, items):
    """Put items"""
    try:
        table = db.Table("primary_table")
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
        return {"status": "finished"}
    except ClientError as err:
        raise err
    except BaseException as err:
        raise err


def generate_uuid():
    for _ in range(1, 5):
        print("C-" + str(uuid.uuid1())[:8])


def file_upload(s3):
    try:
        bucket = s3.Bucket("px-ad-img")
        bucket.upload_file(
            Filename="./03-flat-red.png",
            Key="03-flat-red.png",
            ExtraArgs={"ContentType": "image/png"},
        )
    except ClientError as err:
        raise err
    except BaseException as err:
        raise err


# inputだけを個別に生成して全部ここに投げるようなロジック


def transact_demo(client):
    table_name = "primary_table"
    try:
        res = client.transact_write_items(
            ReturnConsumedCapacity="INDEXES",
            TransactItems=[
                {
                    "Delete": {
                        "TableName": table_name,
                        "Key": {"PK": {"S": "T-802ae9ad"}, "SK": {"S": "T-802ae9ad"}},
                    },
                },
                {
                    "Update": {
                        "TableName": table_name,
                        "Key": {
                            "PK": {"S": "/videos/601553817"},
                            "SK": {"S": "/videos/601553817"},
                        },
                        "UpdateExpression": "SET tagIds = :tagIds",
                        "ExpressionAttributeValues": {":tagIds": {"SS": [""]}},
                    }
                },
            ],
        )
        pprint(res)
    except ClientError as err:
        raise err
    except BaseException as err:
        raise err


def _generate_id(type, digit=8):
    """generate prefixed unique ID"""
    if type == "tag":
        return ("T-" + str(uuid.uuid1())[:digit], "Tag")


#
# シングルスレッド並行処理とマルチプロセス並列処理を理解する
#


async def asyncFn1(n):
    """async fucntion"""
    print(f"start:fn({n})")
    await asyncio.sleep(3)
    print(f"finish:fn{n}")
    return f"finish:fn{n}"


async def asyncFn2(n):
    """async fucntion"""
    print(f"start:fn({n})")
    await asyncio.sleep(1)
    print(f"finish:fn{n}")
    return f"finish:fn{n}"


def syncFn1(n):
    """sync fucntion"""
    print(f"start:fn({n})")
    print(f"finish:fn{n}")
    return f"finish:fn{n}"


def syncFn2(n):
    """sync fucntion"""
    print(f"start:fn({n})")
    print(f"finish:fn{n}")
    return f"finish:fn{n}"


async def get_async():
    """Single Thread"""
    task1 = asyncio.create_task(asyncFn1(1))
    task2 = asyncio.create_task(asyncFn2(2))
    res1 = await task1
    res2 = await task2
    print("tasks finished")
    return {"res1": res1, "res2": res2}


async def get_async_loop():
    """Single Thread Loop"""
    tasks = [asyncio.create_task(asyncFn1(i)) for i in range(1, 3)]
    res = await asyncio.gather(*tasks)
    print("tasks finished")
    return {"res": res}


async def get_async_multiprocess():
    """Multi Process"""
    loop = asyncio.get_running_loop()
    with concurrent.futures.ProcessPoolExecutor() as pool:
        task1 = loop.run_in_executor(pool, syncFn1, 1)
        task2 = loop.run_in_executor(pool, syncFn2, 2)
        res1 = await task1
        res2 = await task2
        print("task finished")
        return {"res1": res1, "res2": res2}


async def get_async_multiprocess_loop():
    loop = asyncio.get_running_loop()
    with concurrent.futures.ProcessPoolExecutor() as pool:
        tasks = [loop.run_in_executor(pool, syncFn1, i) for i in range(1, 3)]
        res = await asyncio.gather(*tasks)
        return {"res": res}


if __name__ == "__main__":
    """Run locally"""

    resources = create_dynamodb_resource()

    # asyncio.run(get_async())
    # asyncio.run(get_async_loop())
    # asyncio.run(get_async_multiprocess())
    # asyncio.run(get_async_multiprocess_loop())

    # client = create_dynamodb_client()
    # resources = create_dynamodb_resource()
    # s3 = create_s3_resource()
    # try:
    #     # transact_demo(client)
    #     id, key = _generate_id("tag")
    #     pprint(id)
    #     pprint(key)
    # except ClientError as err:
    #     print(err)
    # except BaseException as err:
    #     pprint(err)
