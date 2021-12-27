import os
from os.path import join, dirname
from dotenv import load_dotenv
from pydantic import BaseSettings

env_path = join(dirname(__file__), "../.env")
load_dotenv(env_path)


class VimeoSettings(BaseSettings):
    vimeo_key: str = os.environ.get("VIMEO_KEY")
    vimeo_secret: str = os.environ.get("VIMEO_SECRET")
    vimeo_token: str = os.environ.get("VIMEO_TOKEN")


settings = VimeoSettings()
