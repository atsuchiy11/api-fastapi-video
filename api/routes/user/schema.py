from pydantic import BaseModel


class User(BaseModel):
    """User response body"""

    id: int
    indexKey: str
    PK: str
    SK: str
    createdAt: str
    updatedAt: str
    name: str
    image: str = ""
    acl: str = "user"


class ReqUser(BaseModel):
    """Post & Put user request body"""

    PK: str
    name: str = None
    image: str = None
    acl: str = None
