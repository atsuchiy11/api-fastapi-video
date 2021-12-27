from fastapi import FastAPI
from mangum import Mangum
from starlette.middleware.cors import CORSMiddleware

from .routes.video import video
from .routes.path import path
from .routes.order import order
from .routes.tag import tag
from .routes.category import category
from .routes.user import user
from .routes.banner import banner
from .routes.upload import upload
from .routes.favorite import favorite
from .routes.like import like
from .routes.history import history
from .routes.thread import thread

app = FastAPI(
    title="Prime Studio API v2",
    description="API specifications for Prime Studio",
    version="2.0.0",
)
app.include_router(video.router)
app.include_router(path.router)
app.include_router(order.router)
app.include_router(tag.router)
app.include_router(category.router)
app.include_router(user.router)
app.include_router(banner.router)
app.include_router(upload.router)
app.include_router(favorite.router)
app.include_router(like.router)
app.include_router(history.router)
app.include_router(thread.router)

# Allow domain
origins = [
    "http://localhost",
    "http://localhost:3000",
    "https://localhost",
    "https://localhost:3000",
    "https://prime-stream.vercel.app",
    "https://prime-studio.vercel.app",
]
# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

handler = Mangum(app)
