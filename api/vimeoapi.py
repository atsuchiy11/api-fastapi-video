import vimeo
import requests
import urllib.parse
from requests import HTTPError
import os

# from os.path import join, dirname
from dotenv import load_dotenv
from pprint import pprint

# env_path = join(dirname(__file__), "../.env")
load_dotenv(override=True)


def create_vimeo_client():
    """create Vimeo client"""
    return vimeo.VimeoClient(
        token=os.environ.get("VIMEO_TOKEN_PROD"),
        key=os.environ.get("VIMEO_KEY_PROD"),
        secret=os.environ.get("VIMEO_SECRET_PROD"),
    )


def about_me(client):
    res = client.get("/me")
    pprint(res.json())


# OK
def get_video_from_vimeo(client, video_id):
    """Get specified video from VimeoAPI"""

    params = {
        "fields": "uri,name,duration,stats,privacy,embed.html,pictures.sizes",
    }
    query_params = urllib.parse.urlencode(params)
    try:
        res = client.get(f"/videos/{video_id}?{query_params}")
        res.raise_for_status()
        res_json = res.json()

        # sort
        data = {}
        data["uri"] = res_json.get("uri", None)
        data["name"] = res_json.get("name", None)
        data["duration"] = res_json.get("duration", 0)
        data["stats"] = res_json.get("stats").get("plays", 0)
        # data["privacy"] = res_json.get("privacy").get("view", None)
        data["html"] = res_json.get("embed").get("html")

        pictures = res_json.get("pictures").get("sizes")
        *_, max_size = pictures
        data["thumbnail"] = max_size.get("link")

        return data
    except HTTPError as err:
        raise err
    except requests.exceptions.RequestException as err:
        raise err


# OK
def get_upload_url(client, file):
    """Get upload URL for vimeo"""
    preset_id = 120971641
    try:
        # get upload URL
        res = client.post(
            "/me/videos",
            data={
                "name": file.name,
                "description": file.description,
                "locale": "ja",
                "privacy": {"download": False, "view": "disable"},
                "upload": {"approach": "tus", "size": str(file.size)},
            },
        )
        res_parsed = res.json()
        pprint(res_parsed)
        res_filtered = {}
        res_filtered["uri"] = res_parsed.get("uri", "")
        res_filtered["name"] = res_parsed.get("name", "")
        res_filtered["type"] = res_parsed.get("type", "")
        res_filtered["description"] = res_parsed.get("description", "")
        res_filtered["link"] = res_parsed.get("link", "")
        if (upload := res_parsed.get("upload", None)) is None:
            res_filtered["upload_link"] = ""
        else:
            res_filtered["upload_link"] = upload.get("upload_link", "")

        # allow domain
        client.put(res_parsed["uri"] + "/privacy/domains/localhost:3000")
        client.put(res_parsed["uri"] + "/privacy/domains/prime-studio.vercel.app")
        client.patch(res_parsed["uri"], data={"privacy": {"embed": "whitelist"}})

        # append embed presets
        client.put(res_parsed["uri"] + f"/presets/{preset_id}")

        return res_filtered
    except Exception as err:
        print(err)


# OK
def get_upload_status(client, video_id):
    """Get status for upload video to vimeo"""

    uri = f"/videos/{video_id}"
    res = client.get(uri + "?fields=transcode.status").json()

    if res["transcode"]["status"] == "complete":
        print("Your video finished transcoding.")
    elif res["transcode"]["status"] == "in_progress":
        print("Your video is still transcoding.")
    else:
        print("Your video encountered an error during transcoding.")

    return {"transcode_status": res["transcode"]["status"]}


###


def get_total(client):
    params = {"fields": "total"}
    query_params = urllib.parse.urlencode(params)
    try:
        res = client.get(f"/me/videos?{query_params}")
        # res.rasie_for_status()
        res_json = res.json()
        return res_json
    except HTTPError as err:
        raise err
    except requests.exceptions.RequestException as err:
        raise err


def get_videos_from_vimeo_async(client, chunk, page):
    params = {
        "page": page,
        "per_page": chunk,
        "fields": "uri,name,duration,stats,privacy,embed.html,pictures.sizes",
    }
    query_params = urllib.parse.urlencode(params)
    print(f"getting videos: {page}")
    try:
        res = client.get(f"/me/videos?{query_params}")
        # res.rasie_for_status()
        res_json = res.json()
        print(f"got videos: {page}")

        api_response = {}
        api_response["total"] = res_json.get("total", None)
        api_response["data"] = []
        for d in res_json.get("data", []):
            _d = {}
            _d["uri"] = d.get("uri", None)
            _d["name"] = d.get("name", None)
            _d["duration"] = d.get("duration", None)
            _d["stats"] = d.get("stats", {})
            _d["privacy"] = {}
            _d["privacy"]["view"] = d.get("privacy", {}).get("view", None)
            _d["html"] = d.get("embed", {}).get("html", None)
            _d["thumbnail"] = {}
            if (pictures := d.get("pictures", {}).get("sizes", None)) is not None:
                *_, max_size = pictures
                _d["thumbnail"] = max_size

            api_response["data"].append(_d)
        return api_response

    except HTTPError as err:
        raise err
    except requests.exceptions.RequestException as err:
        raise err


def get_videos_from_vimeo(client, all, page):
    """Get videos from VimeoAPI"""

    per_page = 100
    videos = {"total": 0, "data": []}

    def getter(page, per_page, videos):
        params = {
            "page": page,
            "per_page": per_page,
            "fields": "uri,name,duration,stats,privacy,embed.html,pictures.sizes",
        }
        query_params = urllib.parse.urlencode(params)

        try:
            res = client.get(f"/me/videos?{query_params}")
            res.raise_for_status()
            res_json = res.json()
            videos["data"] += res_json["data"]
            videos["total"] = res_json["total"]

            # 再帰しない
            if not all:
                return videos
            # 再帰する
            if int(res_json.get("total", 0)) - (per_page * int(page)) > 0:
                page = int(page) + 1
                query_params = urllib.parse.urlencode(params)
                getter(page, per_page, videos)
            return videos

        except HTTPError as err:
            raise err
        except requests.exceptions.RequestException as err:
            raise err

    try:
        vimeo_json = getter(page, per_page, videos)
    except HTTPError as err:
        raise err
    except requests.exceptions.RequestException as err:
        raise err

    api_response = {}
    api_response["total"] = vimeo_json.get("total", None)
    api_response["data"] = []
    for d in vimeo_json.get("data", []):
        _d = {}
        _d["uri"] = d.get("uri", None)
        _d["name"] = d.get("name", None)
        _d["duration"] = d.get("duration", None)
        _d["stats"] = d.get("stats", {})
        _d["privacy"] = {}
        _d["privacy"]["view"] = d.get("privacy", {}).get("view", None)
        _d["html"] = d.get("embed", {}).get("html", None)
        _d["thumbnail"] = {}
        if (pictures := d.get("pictures", {}).get("sizes", None)) is not None:
            *_, max_size = pictures
            _d["thumbnail"] = max_size

        api_response["data"].append(_d)
    return api_response


def upload_thumbnail(client, video_id, tmp_path):
    """Upload thumbnail to Vimeo"""
    try:
        res = client.upload_picture(f"/videos/{video_id}", tmp_path, activate=True)
        return res
    except HTTPError as err:
        print("HTTPError", err)
        raise err
    except requests.exceptions.RequestException as err:
        print(err)
        raise err
    except BaseException as err:
        print("BaseException", err)
        raise err
    except Exception as err:
        print("Exception", err)
        raise (err)


if __name__ == "__main__":
    """Run locally"""
    client = create_vimeo_client()
    try:
        # get_videos_from_vimeo(client)
        upload_thumbnail(client)
    except BaseException as err:
        raise err
