from requests import HTTPError
import urllib.parse
from .auth import Auth


class DynamoDB(Auth):
    """Generate DynamoDB client"""

    def __init__(self):
        super().__init__()
        self.client = self.create_dynamodb_client()
        self.resource = self.create_dynamodb_resoure()
        self.s3 = self.create_s3_resource()
        self.table = self.resource.Table("primary_table")

    def merge_paths(self, paths, videos):
        """merge learning paths & video orders"""

        for i, path in enumerate(paths, 1):
            path["id"] = i
            orders = [
                {"uri": video.get("SK", None), "order": video.get("order", 0)}
                for video in videos
                if path.get("PK") == video.get("PK")
            ]
            path["videos"] = orders
            path["videos"].sort(key=lambda x: x["order"])
        return paths

    def merge_categories(self, categories):
        """merge parent and child categories"""

        def find_parent_name(category):
            if (parent_id := category.get("parentId", None)) == "C999":
                return ""
            else:
                for category in categories:
                    if category.get("PK", None) == parent_id:
                        return category["name"]
                return ""

        merged = [
            {**category, **{"id": i}, **{"parent": find_parent_name(category)}}
            for i, category in enumerate(categories, 1)
        ]
        return merged


class VimeoAPI(Auth):
    """Generate VimeoAPI client"""

    def __init__(self):
        super().__init__()
        self.client = self.create_vimeo_client()

    def sort(self, json):
        """Sort vimeo response"""

        data = dict(
            uri=json.get("uri", None),
            name=json.get("name", None),
            duration=json.get("duration", 0),
            plays=json.get("stats").get("plays", 0),
            html=json.get("embed").get("html"),
        )
        pictures = json.get("pictures").get("sizes")
        *_, max_size = pictures
        data["thumbnail"] = max_size.get("link")
        return data

    def get_total(self):
        """Get video count from Vimeo"""

        query = "fields=total"
        try:
            res = self.client.get(f"/me/videos?{query}")
            res.raise_for_status()
            return res.json()
        except HTTPError as err:
            raise err
        except BaseException as err:
            raise err

    def get_videos_by_page(self, chunk, page):
        """Get videos from Vimeo by page"""

        params = {
            "page": page,
            "per_page": chunk,
            "fields": "uri,name,duration,stats,privacy,embed.html,pictures.sizes",
        }
        query_params = urllib.parse.urlencode(params)
        try:
            res = self.client.get(f"/me/videos?{query_params}")
            res.raise_for_status()
            res_json = res.json()
            data = [self.sort(d) for d in res_json.get("data", [])]
            return data

        except HTTPError as err:
            raise err
        except BaseException as err:
            raise err
