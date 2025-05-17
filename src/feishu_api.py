import requests


class FeishuAPI:
    def __init__(self, app_id: str, app_secret: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self.base_url = "https://open.feishu.cn/open-apis"
        self.access_token = self._get_access_token()

    def _get_access_token(self) -> str:
        url = f"{self.base_url}/auth/v3/app_access_token/internal"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        data = {"app_id": self.app_id, "app_secret": self.app_secret}
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()["app_access_token"]

    def request(self, method: str, url: str, params=None, body=None) -> dict:
        url = f"{self.base_url}/{url.lstrip('/')}"
        headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
        response = requests.request(method, url, headers=headers, params=params, json=body)
        try:
            response.raise_for_status()
        except Exception as e:
            print(response.text)
            raise Exception(response.text) from e
        result = response.json()
        if result["code"] != 0:
            print(response.text)
            raise Exception(response.text)
        return result.get("data") or {}

    def paginate(self, method: str, url: str, params=None, body=None, page_size=100) -> list:
        items = []
        page_token = None
        while True:
            _params = {**(params or {}), "page_size": page_size}
            if page_token is not None:
                _params["page_token"] = page_token
            result = self.request(method, url, params=_params, body=body)
            items.extend(result.get("items") or [])
            has_more = result.get("has_more")
            page_token = result.get("page_token")
            if not has_more or not page_token:
                break
        return items

    def get_wiki_app_token(self, node_token: str) -> str:
        """获取知识库节点信息"""
        result = self.request("GET", "/wiki/v2/spaces/get_node", {"token": node_token})
        return result["node"]["obj_token"]
