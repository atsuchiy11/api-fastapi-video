from typing import List
from fastapi import APIRouter, HTTPException, UploadFile, File
from botocore.exceptions import ClientError

from api.util import document_it, timestamp_jst
from api.client import DynamoDB
from .schema import Banner, ReqBannerPost, ReqBannerPut
import api.routes.banner.input as banner_input

from pathlib import Path
import tempfile
import shutil

# import glob
# import os

router = APIRouter()
db = DynamoDB()
table = db.table
s3 = db.s3


@router.get("/banners", response_model=List[Banner])
@document_it
def get_banners(active: bool = False):
    """Get banners from DynamoDB"""

    try:
        input = banner_input.query(active)
        res = table.query(**input)
        items = res.get("Items", {})
        items = [{**item, **{"id": i}} for i, item in enumerate(items, 1)]
        return items

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/banner")
@document_it
def post_banner(req_banner: ReqBannerPost):
    """Post banner to DynamoDB"""

    try:
        input = banner_input.put_item(req_banner)
        res = table.put_item(Item=input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.post("/banner/image")
@document_it
def post_banner_image(image: UploadFile = File(...)):
    """Upload banner image to S3 bucket"""

    public_url = "https://px-ad-img.s3.ap-northeast-1.amazonaws.com/"
    suffix = Path(image.filename).suffix
    timestamp = timestamp_jst()

    try:
        with tempfile.NamedTemporaryFile(delete=True, suffix=suffix) as fp:
            shutil.copyfileobj(image.file, fp)
            input_filename = Path(fp.name)
            print(dir(fp))
            name = image.filename.replace(suffix, "")
            output_filename = f"{name}_{timestamp}{suffix}"

            # upload
            bucket = s3.Bucket("px-ad-img")
            bucket.upload_file(
                Filename=str(input_filename),
                Key=output_filename,
                ExtraArgs={"ContentType": image.content_type},
            )
            # remove temp file
            # for p in glob.glob("/tmp/" + "*"):
            #     if os.path.isfile(p):
            #         os.remove(p)
            return {"url": f"{public_url}{output_filename}"}

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))


@router.put("/banner")
@document_it
def put_banner(req_banner: ReqBannerPut):
    """Put banner to DynamoDB"""

    try:
        input = banner_input.update_item(req_banner)
        res = table.update_item(**input)
        return res

    except ClientError as err:
        err_message = err.response["Error"]["Message"]
        raise HTTPException(status_code=404, detail=err_message)

    except BaseException as err:
        raise HTTPException(status_code=404, detail=str(err))
