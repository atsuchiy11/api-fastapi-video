from typing import List
from pydantic import BaseModel


class VideoVimeo(BaseModel):
    """Video from Vimeo"""

    uri: str = ""
    name: str = ""
    duration: int = 0
    plays: int = 0
    html: str = ""
    thumbnail: str = ""


class VideoDB(VideoVimeo):
    """Video from DynamoDB"""

    indexKey: str = ""
    PK: str = ""
    SK: str = ""
    createdAt: str = ""
    createdUser: str = ""
    updatedAt: str = ""
    updatedUser: str = ""
    invalid: bool = False
    note: str = ""
    description: str = ""
    learningPathIds: List[str] = []
    tagIds: List[str] = []
    categoryId: str = ""
    match: bool = True


class VideoFilter(BaseModel):
    """video search request body from DynamoDB"""

    categoryId: str = ""
    tagId: str = ""
    learningPathId: str = ""
    name: str = ""


class ReqVideoPost(BaseModel):
    """post video request body to DynamoDB"""

    PK: str
    user: str
    invalid: bool = False
    note: str = "No Remarks"
    description: str
    learningPathIds: List[str] = None
    tagIds: List[str] = None
    categoryId: str
    # vimeo params
    uri: str = None
    thumbnail: str = None
    plays: int = 0
    name: str = None
    duration: int = 0
    html: str = None


class ReqVideoPut(BaseModel):
    """put video request body to DynamoDB"""

    PK: str
    user: str
    invalid: bool = False
    note: str = None
    description: str = None
    learningPathIds: List[str] = None
    tagIds: List[str] = None
    categoryId: str = None
    # vimeo params
    uri: str = None
    thumbnail: str = None
    plays: int = 0
    name: str = None
    duration: int = 0
    html: str = None


class ReqVimeoPut(BaseModel):
    """put video request body to Vimeo (name only)"""

    PK: str
    name: str
