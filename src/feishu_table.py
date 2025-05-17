import re
from typing import Any, Dict, List, Tuple

from .feishu_api import FeishuAPI

B64P = r"[A-Za-z0-9+/]+={0,2}"
FEISHU_BI_TABLE_URL_PATTERN = f"https?://[^/]*feishu[^/]*/wiki/({B64P}).*?table=({B64P}).*?"


class FeishuBiTable:
    """飞书多维表格"""

    def __init__(self, app_id: str, app_secret: str, bi_table_url: str):
        self.feishu_api = FeishuAPI(app_id, app_secret)
        node_token, self.table_id = self.parse_feishu_bi_table_url(bi_table_url)
        self.app_token = self.feishu_api.get_wiki_app_token(node_token)

    @staticmethod
    def parse_feishu_bi_table_url(url: str) -> tuple[str, str]:
        """解析飞书多维表格URL"""
        m = re.match(FEISHU_BI_TABLE_URL_PATTERN, url)
        if not m:
            raise ValueError("无法解析飞书多维表格URL")
        return m.group(1), m.group(2)

    def list_records(self) -> List[Dict[str, Any]]:
        """获取多维表格记录"""
        url = f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/search"
        return self.feishu_api.paginate("POST", url, body={}, page_size=500)

    def get_record(self, rid: str) -> Dict[str, Any]:
        """获取多维表格记录"""
        pass

    def create_record(self, record: Dict[str, Any]) -> str:
        """创建多维表格记录, 返回记录ID"""
        url = f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        body = {"fields": record}
        return self.feishu_api.request("POST", url, body=body)["record"]["record_id"]

    def update_record(self, rid: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """更新多维表格记录"""
        url = f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/{rid}"
        body = {"fields": record}
        return self.feishu_api.request("PUT", url, body=body)["record"]

    def batch_update_records(self, records: List[Tuple[str, Dict[str, Any]]]) -> list:
        """批量更新多维表格记录"""
        url = f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_update"
        _records = [{"record_id": rid, "fields": record} for rid, record in records]
        body = {"records": _records}
        return self.feishu_api.request("POST", url, body=body)["records"]

    def list_fields(self) -> List[Dict[str, Any]]:
        """获取多维表格列"""
        url = f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        return self.feishu_api.paginate("GET", url)

    def create_field(self, field: Dict[str, Any]):
        """创建多维表格列"""
        url = f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        return self.feishu_api.request("POST", url, body=field)["field"]
