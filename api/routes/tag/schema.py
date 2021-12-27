from pydantic import BaseModel


class Tag(BaseModel):
    """tag response body"""

    id: str
    indexKey: str
    PK: str
    SK: str
    createdAt: str
    createdUser: str
    updatedAt: str
    updatedUser: str
    invalid: bool
    note: str = None
    name: str
    description: str


class ReqTagPost(BaseModel):
    """Post tag request body"""

    user: str
    name: str
    description: str = ""
    note: str = ""


class ReqTagPut(BaseModel):
    """Put tag request body"""

    PK: str
    user: str
    name: str = None
    description: str = None
    note: str = None
    invalid: bool = None


class ReqTagDelete(BaseModel):
    """Delete tag request body"""

    PK: str
    user: str
