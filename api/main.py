# from typing import Optional
# from fastapi import FastAPI, HTTPException, Header
# from starlette.middleware.cors import CORSMiddleware
# from mangum import Mangum

# # Router
# from .routers import (
#     video,
#     path,
#     tag,
#     category,
#     user,
#     history,
#     favorite,
#     like,
#     thread,
#     banner,
#     table,
#     upload,
# )

# # util
# from .util import timestamp_jst

# app = FastAPI(
#     title="Prime Studio API",
#     description="API specifications for Prime Studio",
#     version="1.0.0",
# )

# app.include_router(video.router)
# app.include_router(path.router)
# app.include_router(tag.router)
# app.include_router(category.router)
# app.include_router(user.router)
# app.include_router(history.router)
# app.include_router(favorite.router)
# app.include_router(like.router)
# app.include_router(thread.router)
# app.include_router(banner.router)
# app.include_router(table.router)
# app.include_router(upload.router)


# # Allow domain
# origins = [
#     "http://localhost",
#     "http://localhost:3000",
#     "https://localhost",
#     "https://localhost:3000",
#     "https://prime-stream.vercel.app",
#     "https://prime-studio.vercel.app",
# ]
# # CORS
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# @app.get("/hello")
# async def read_root(user_agent: Optional[str] = Header(None)):
#     """getting startd FastAPI"""
#     try:
#         return {
#             "Hello": "World",
#             "timestamp": timestamp_jst(),
#             "user_agent": user_agent,
#         }
#     except BaseException as err:
#         err = err.response.json()
#         err_message = err.get("error", "System error occurred.")
#         raise HTTPException(status_code=404, detail=err_message)


# handler = Mangum(app)
