from pydantic import BaseModel


class UserHistory(BaseModel):
    """History response body from dynamoDB"""

    indexKey: str
    PK: str
    SK: str
    createdAt: str
    videoUri: str
    parse: float = 0
    finishedAt: str = None
    referrer: str = None


class ReqHistory(BaseModel):
    """History request body"""

    user: str
    video: str
    createdAt: str
    parse: float = 0
    finishedAt: str = ""
    referrer: str = ""
