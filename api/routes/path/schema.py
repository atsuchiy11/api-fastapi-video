from typing import List
from pydantic import BaseModel


class VideoOrder(BaseModel):
    """Video order contain any learning path"""

    uri: str
    order: int = 0


class Path(BaseModel):
    """LearningPath from DynamoDB"""

    id: int
    indexKey: str
    PK: str
    SK: str
    createdAt: str
    createdUser: str
    updatedAt: str
    updatedUser: str
    invalid: bool
    note: str
    name: str
    description: str
    videos: List[VideoOrder]


class ReqPathPost(BaseModel):
    """post learning path request body to DynamoDB"""

    name: str
    description: str
    note: str = ""
    user: str


class ReqPathPut(BaseModel):
    """put learning path request body to DynamoDB"""

    PK: str
    user: str
    name: str = None
    description: str = None
    note: str = None
    invalid: str = None


class ReqPathPutTransact(ReqPathPut):
    """put learning path request body to DynamoDB (transaction)"""

    appended: List[str] = []
    removed: List[str] = []
    orders: List[VideoOrder] = []


class ReqPathDeleteTransact(BaseModel):
    """delete learning path request body from DynamoDB"""

    PK: str
    user: str
