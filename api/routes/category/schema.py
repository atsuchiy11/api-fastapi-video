from pydantic import BaseModel


class Category(BaseModel):
    """Category response body"""

    id: int
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
    description: str = None
    parentId: str
    parent: str = ""


class ReqCategoryPost(BaseModel):
    """Post category request body"""

    user: str
    name: str
    description: str
    note: str = ""
    parentId: str = "C999"


class ReqCategoryPut(BaseModel):
    """Put category request body"""

    PK: str
    user: str
    name: str = None
    parentId: str = None
    description: str = None
    note: str = None
    invalid: bool = None
