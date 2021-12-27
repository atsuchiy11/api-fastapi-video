from typing import List, Optional
from pydantic import BaseModel

#
# 採用
#


# @types videos


class VideoVimeo(BaseModel):
    """Video from Vimeo"""

    uri: str = ""
    name: str = ""
    duration: int = 0
    plays: int = 0
    html: str = ""
    thumbnail: str = ""


class VideoFilter(BaseModel):
    """Video search request body"""

    categoryId: str = ""
    tagId: str = ""
    learningPathId: str = ""
    name: str = ""


# OK
class VideoDB(BaseModel):
    """Video from DynamoDB"""

    indexKey: str = ""
    PK: str = ""
    SK: str = ""
    createdAt: str = ""
    createdUser: str = ""
    updatedAt: str = ""
    updatedUser: str = ""
    invalid: bool = True
    note: str = ""
    description: str = ""
    learningPathIds: List[str] = []
    tagIds: List[str] = []
    categoryId: str = ""
    match: bool = True
    # 最後必須にしてもいいかも
    uri: str = ""
    thumbnail: str = ""
    plays: int = 0
    name: str = ""
    duration: int = 0
    html: str = ""


# OK
class VideoTableRow(BaseModel):
    """Video table row"""

    id: int
    match: bool
    uri: str = ""
    invalid: bool = True
    thumbnail: str = ""
    name: str = ""
    description: str = ""
    primary: str = ""
    secondary: str = ""
    tags: List[str] = []
    paths: List[str] = []
    note: str = ""
    duration: int = 0
    plays: int = 0
    createdAt: str = ""
    createdUser: str = ""
    updatedAt: str = ""
    updatedUser: str = ""


# OK
class ReqVideoPost(BaseModel):
    """Post video request body"""

    # PK=SK
    PK: str
    user: str
    invalid: bool = False
    note: str = "No Remarks"
    description: str
    learningPathIds: List[str] = [""]
    tagIds: List[str] = [""]
    categoryId: str
    # vimeo params
    uri: str = None
    thumbnail: str = None
    plays: int = 0
    name: str = None
    duration: int = 0
    html: str = None


# OK
class ReqVideoPut(BaseModel):
    """Put video request body"""

    PK: str
    user: str
    invalid: bool = None
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


# 拡張するかも
class ReqVimeoPut(BaseModel):
    """Put video to vimeo request body (name only)"""

    PK: str
    name: str


# @types category

# OK
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


# @types tag

# OK
class Tag(BaseModel):
    """Tag response body"""

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


# @types path

# OK
class VideoOrder(BaseModel):
    """Video order contain learning path"""

    uri: str
    order: int = 0


# OK
class Path(BaseModel):
    """Learning Path response body"""

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
    videos: List[VideoOrder] = []


# @types user

# OK
class User(BaseModel):
    """User response body"""

    id: int = ""
    indexKey: str = ""
    PK: str = ""
    SK: str = ""
    createdAt: str = ""
    updatedAt: str = ""
    name: str = ""
    image: str = ""
    acl: str = "user"


# OK
class ReqUser(BaseModel):
    """Post & Put user request body"""

    PK: str
    # user: str
    name: Optional[str] = None
    image: Optional[str] = None
    acl: Optional[str] = None


# @types upload


# OK
class UploadStatus(BaseModel):
    """Video upload status response body from DynamoDB"""

    PK: str
    SK: str
    id: str
    indexKey: str
    createdAt: str
    createdUser: str
    name: str
    filename: str
    status: str


# OK
class UploadFile(BaseModel):
    """File upload URL for vimeo"""

    uri: str
    name: str
    type: str
    description: str
    link: str
    upload_link: str


# OK
class ResUploadStatus(BaseModel):
    """Response upload status"""

    uri: str
    timestamp: str
    status: str


# OK
class ReqFile(BaseModel):
    """File upload URL request body"""

    name: str
    description: str
    size: int


# OK
class ReqUploadStatusPost(BaseModel):
    """Post upload status request body"""

    uri: str
    user: str
    name: str
    filename: str
    status: str


# OK
class ReqUploadStatusPut(BaseModel):
    """Put upload status request body"""

    uri: str
    timestamp: str
    status: str


# @types history

# OK
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


# OK
class ReqHistory(BaseModel):
    """History request body"""

    user: str
    video: str
    createdAt: str
    parse: float = 0
    finishedAt: str = ""
    referrer: str = ""


# @types favorite

# OK
class Favorite(BaseModel):
    """Favorite from dynamoDB"""

    indexKey: str
    PK: str
    SK: str
    createdAt: str


# OK
class ReqFavorite(BaseModel):
    """Favorite request body"""

    user: str
    video: str


# @types likes


# OK
class Like(BaseModel):
    """Like for video"""

    indexKey: str
    PK: str
    SK: str
    createdAt: str
    createdUser: str
    like: bool


# OK
class Likes(BaseModel):
    """Likes for video"""

    good: List[Like]
    bad: List[Like]


# OK
class ReqLikePost(BaseModel):
    """Post like request body"""

    video: str
    user: str
    like: bool


# OK
class ReqLikeDelete(BaseModel):
    """Delete like request body"""

    video: str
    id: str


# @types thread

# OK
class Thread(BaseModel):
    """Thread response body"""

    indexKey: str
    PK: str
    SK: str
    createdAt: str
    createdUser: str
    body: str


# OK
class ReqThreadPost(BaseModel):
    """Post thread request body"""

    video: str
    user: str
    body: str
    thread: str = None


# OK
class ReqThreadPut(BaseModel):
    """Put thread request body"""

    video: str
    id: str
    body: str = None
    invalid: bool = False


############################


# 削除しない
class ReqThreadDelete(BaseModel):
    """Delete thread request body"""

    video: str
    id: str


class ReqOrder(BaseModel):
    """Order request body"""

    path: str
    video: str
    order: int = 0


#
# 整理整頓する
#

#
# Video
#


class StatsVimeo(BaseModel):
    """Stats from Vimeo"""

    plays: int


class PrivacyVimeo(BaseModel):
    """Privacy from Vimeo"""

    view: str


class ThumbnailVimeo(BaseModel):
    """Thumbnail from Vimeo"""

    width: int
    height: int
    link: str
    link_with_play_button: str = None


class DataVideoVimeo(BaseModel):
    """Video rawdata from Vimeo"""

    uri: str = ""
    name: str = ""
    duration: int = ""
    stats: StatsVimeo = ""
    privacy: PrivacyVimeo = None
    html: str = ""
    thumbnail: ThumbnailVimeo = ""


# class VideoVimeo(BaseModel):
#     """Video from Vimeo"""

#     total: int
#     data: List[DataVideoVimeo]


class Video(DataVideoVimeo, VideoDB):
    """Video response body"""

    pass


#
# User
#


#
# (Learning) Path
#


class ReqPathPost(BaseModel):
    """Post learning path request body"""

    name: str
    description: str
    note: str = ""
    user: str


class ReqPathPut(BaseModel):
    """Put learningh path request body"""

    PK: str
    user: str
    name: str = None
    description: str = None
    note: str = None
    invalid: bool = None


class ReqPathPutTransact(ReqPathPut):

    appended: List[str] = []
    removed: List[str] = []
    orders: List[VideoOrder] = []


class ReqPathDeleteTransact(BaseModel):

    PK: str
    user: str


class _ReqVideoOrder(BaseModel):

    PK: str
    uri: str


#
# Category
#


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


#
# Tag
#


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


#
# Banner
#
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
