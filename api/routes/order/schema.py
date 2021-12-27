from pydantic import BaseModel


class Order(BaseModel):
    """playback order of videos in playlist"""

    PK: str
    SK: str
    indexKey: str
    createdAt: str
    order: int


class ReqOrder(BaseModel):
    """get order meta request body"""

    PK: str
    uri: str
