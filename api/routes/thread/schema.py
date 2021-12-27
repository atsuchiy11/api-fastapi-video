from pydantic import BaseModel


class Thread(BaseModel):
    """Thread response body"""

    indexKey: str
    PK: str
    SK: str
    createdAt: str
    createdUser: str
    body: str


class ReqThreadPost(BaseModel):
    """Post thread request body"""

    video: str
    user: str
    body: str
    thread: str = None  # 親スレッドのdate


class ReqThreadPut(BaseModel):
    """Put thread request body"""

    video: str
    id: str
    body: str = None
    invalid: bool = False
