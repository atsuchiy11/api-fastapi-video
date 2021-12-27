from typing import List
from fastapi import APIRouter, HTTPException, File, UploadFile
from botocore.exceptions import ClientError
from aws_dynamodb_parser import parse

import uuid
from api.util import timestamp_jst
from api.schema import Banner, BannerImage, ReqBannerPost, ReqBannerPut
from api.dynamodb import create_dynamodb_client, create_s3_resource

from pathlib import Path
import tempfile
import shutil
import glob
import os

router = APIRouter()
client = create_dynamodb_client()
s3 = create_s3_resource()


#
# routes
#

# OK
@router.get("/banners", response_model=List[Banner])
def get_banners(active: bool = False):
    """Get banners from DynamoDB"""

    input = create_query_input(active)
    try:
        res = client.query(**input)
        items = parse(res.get("Items", []))
        for i, item in enumerate(items, 1):
            item["id"] = i
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/banner")
def post_banner(req_banner: ReqBannerPost):
    """Post banner to DynamoDB"""

    input = create_put_item_input(req_banner)
    try:
        res = client.put_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.post("/banner/image", response_model=BannerImage)
def post_banner_image(image: UploadFile = File(...)):
    """Upload banner image to S3 bucket"""

    public_url = "https://px-ad-img.s3.ap-northeast-1.amazonaws.com/"
    suffix = Path(image.filename).suffix
    timestamp = timestamp_jst()
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as fp:
            shutil.copyfileobj(image.file, fp)
            filename = Path(fp.name)
            _key = image.filename.replace(suffix, "")
            key = f"{_key}_{timestamp}{suffix}"
            content_type = image.content_type

            # upload
            bucket = s3.Bucket("px-ad-img")
            bucket.upload_file(
                Filename=str(filename), Key=key, ExtraArgs={"ContentType": content_type}
            )
            # remove temp file
            for p in glob.glob("/tmp/" + "*"):
                if os.path.isfile(p):
                    os.remove(p)
            # return {"url": f"{public_url}{key}_{timestamp_jst()}{suffix}"}
            return {"url": f"{public_url}{key}"}

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


# OK
@router.put("/banner")
def put_banner(req_banner: ReqBannerPut):
    """Put banner to DynamoDB"""

    input = create_update_item_input(req_banner)
    try:
        res = client.update_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


#
# utilities
#

# OK
def create_query_input(active):
    if active:
        input = {
            "TableName": "primary_table",
            "IndexName": "GSI-1-SK",
            "KeyConditionExpression": "#key = :key",
            "FilterExpression": "#invalid = :invalid",
            "ExpressionAttributeNames": {"#key": "indexKey", "#invalid": "invalid"},
            "ExpressionAttributeValues": {
                ":key": {"S": "Banner"},
                ":invalid": {"BOOL": False},
            },
        }
    else:
        input = {
            "TableName": "primary_table",
            "IndexName": "GSI-1-SK",
            "KeyConditionExpression": "#key = :key",
            "ExpressionAttributeNames": {"#key": "indexKey"},
            "ExpressionAttributeValues": {":key": {"S": "Banner"}},
        }
    return input


# OK
def create_put_item_input(banner):
    id = str(uuid.uuid1())[:8]
    return {
        "TableName": "primary_table",
        "Item": {
            "PK": {"S": "B-" + id},
            "SK": {"S": "B-" + id},
            "indexKey": {"S": "Banner"},
            "name": {"S": banner.name},
            "description": {"S": banner.description},
            "image": {"S": banner.image},
            "note": {"S": banner.note},
            "invalid": {"BOOL": False},
            "createdAt": {"S": timestamp_jst()},
            "createdUser": {"S": banner.user},
            "updatedAt": {"S": timestamp_jst()},
            "updatedUser": {"S": banner.user},
        },
    }


# OK
def create_update_item_input(banner):
    input = {
        "TableName": "primary_table",
        "Key": {"PK": {"S": banner.PK}, "SK": {"S": banner.PK}},
        "UpdateExpression": "SET #date = :date, #user = :user",
        "ExpressionAttributeNames": {
            "#date": "updatedAt",
            "#user": "updatedUser",
        },
        "ExpressionAttributeValues": {
            ":date": {"S": timestamp_jst()},
            ":user": {"S": banner.user},
        },
    }
    if banner.name is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #name = :name"
        input["ExpressionAttributeNames"]["#name"] = "name"
        input["ExpressionAttributeValues"][":name"] = {"S": banner.name}
    if banner.description is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #description = :description"
        )
        input["ExpressionAttributeNames"]["#description"] = "description"
        input["ExpressionAttributeValues"][":description"] = {"S": banner.description}
    if banner.image is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #image = :image"
        input["ExpressionAttributeNames"]["#image"] = "image"
        input["ExpressionAttributeValues"][":image"] = {"S": banner.image}
    if banner.link is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #link = :link"
        input["ExpressionAttributeNames"]["#link"] = "link"
        input["ExpressionAttributeValues"][":link"] = {"S": banner.link}
    if banner.note is not None:
        input["UpdateExpression"] = input.get("UpdateExpression") + ", #note = :note"
        input["ExpressionAttributeNames"]["#note"] = "note"
        input["ExpressionAttributeValues"][":note"] = {"S": banner.note}
    if banner.invalid is not None:
        input["UpdateExpression"] = (
            input.get("UpdateExpression") + ", #invalid = :invalid"
        )
        input["ExpressionAttributeNames"]["#invalid"] = "invalid"
        input["ExpressionAttributeValues"][":invalid"] = {"BOOL": banner.invalid}
    return input
