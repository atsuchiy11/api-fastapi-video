import boto3

# from botocore.exceptions import ClientError
# from aws_dynamodb_parser import parse
import os

# from os.path import join, dirname
from dotenv import load_dotenv

# env_path = join(dirname(__file__), "../.env")
load_dotenv(override=True)


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


# def get_videos_by_paths_from_db(client):
#     """Get videos & playback orders included in learning paths"""
#     input = {
#         "TableName": "primary_table",
#         "IndexName": "GSI-1-SK",
#         "KeyConditionExpression": "#key = :value",
#         "FilterExpression": "attribute_not_exists(#attr)",
#         "ExpressionAttributeNames": {"#key": "indexKey", "#attr": "invalid"},
#         "ExpressionAttributeValues": {":value": {"S": "Video"}},
#     }
#     try:
#         res = client.query(**input)
#         items = parse(res.get("Items", []))
#         items.sort(key=lambda x: x["PK"])
#         return items
#     except ClientError as err:
#         print(err)
#         raise err
#     except BaseException as err:
#         print(err)
#         raise err
