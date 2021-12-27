from pydantic import BaseModel


class UploadStatus(BaseModel):
    """Video upload status response body from DynamoDB"""

    id: int
    PK: str
    SK: str
    indexKey: str
    createdAt: str
    createdUser: str
    name: str
    filename: str
    status: str


class ResUploadStatus(BaseModel):
    """Video upload status response body from DynamoDB"""

    uri: str
    timestamp: str
    status: str


class ReqUploadStatusPost(BaseModel):
    """Post upload status request body to DynamoDB"""

    uri: str
    user: str
    name: str
    filename: str
    status: str
