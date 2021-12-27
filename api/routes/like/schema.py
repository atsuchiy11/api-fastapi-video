from typing import List
from pydantic import BaseModel


class Like(BaseModel):
    """Like for video"""

    indexKey: str
    PK: str
    SK: str
    createdAt: str
    createdUser: str
    like: bool


class Likes(BaseModel):
    """Likes for video"""

    good: List[Like]
    bad: List[Like]


class ReqLikePost(BaseModel):
    """Post like request body"""

    video: str
    user: str
    like: bool


class ReqLikeDelete(BaseModel):
    """Delete like request body"""

    video: str
    id: str
