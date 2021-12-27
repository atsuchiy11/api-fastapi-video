from pydantic import BaseModel


class Favorite(BaseModel):
    """Favorite from dynamoDB"""

    indexKey: str
    PK: str
    SK: str
    createdAt: str


class ReqFavorite(BaseModel):
    """Favorite request body"""

    user: str
    video: str
