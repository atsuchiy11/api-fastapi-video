from pydantic import BaseModel


class Banner(BaseModel):
    """Banner response body"""

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
    description: str
    image: str = None
    link: str = None


class BannerImage(BaseModel):
    """Banner image response body"""

    url: str


class ReqBannerPost(BaseModel):
    """Post banner request body"""

    user: str
    name: str
    description: str
    image: str
    link: str = ""
    note: str = ""


class ReqBannerPut(BaseModel):
    """Put banner request body"""

    PK: str
    user: str
    name: str = None
    description: str = None
    image: str = None
    link: str = None
    note: str = None
    invalid: bool = None
