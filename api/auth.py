import os
import boto3
import vimeo

from dotenv import load_dotenv
from dataclasses import dataclass

load_dotenv(override=True)


@dataclass
class Auth:
    """Generate DynamoDB & Vimeo client"""

    region_name: str = os.environ.get("REGION")
    aws_access_key_id: str = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = os.environ.get("AWS_SECRET_ACCESS_KEY")
    vimeo_token: str = os.environ.get("VIMEO_TOKEN_PROD")
    vimeo_key: str = os.environ.get("VIMEO_KEY_PROD")
    vimeo_secret: str = os.environ.get("VIMEO_SECRET_PROD")

    def create_dynamodb_client(self):
        """Generate DynamoDB client"""

        return boto3.client(
            "dynamodb",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def create_dynamodb_resoure(self):
        """Generate DynamoDB resource"""

        return boto3.resource(
            "dynamodb",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def create_s3_resource(self):
        """Create S3 resource"""

        return boto3.resource(
            "s3",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def create_vimeo_client(self):
        """Generate VimeoAPI client"""

        return vimeo.VimeoClient(
            token=self.vimeo_token, key=self.vimeo_key, secret=self.vimeo_secret
        )
