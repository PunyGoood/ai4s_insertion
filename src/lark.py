import os
import time
from datetime import datetime
from pytz import timezone
import requests
import dotenv
from pymongo import errors
import json
from .feishu_sync import FeishuBiTableSync
from .feishu_table import FeishuBiTable
from .mongodb import get_collection
from .record_db import RecordDatabase
import logging
import pandas as pd
import numpy as np
import ast
import json
import math
import logging
from typing import List, Dict, Any
logging.basicConfig(level=logging.DEBUG)

APP_ID="cli_a880af1659b0d013"
APP_SECRET="6LRmHThxkRlfIE2HEvNvpcI7SfkdPMZ3"

#https://aicarrier.feishu.cn/wiki/RV7JwNDPLiwUXrk5rfLc7gNwn9a?table=tbllob6N3JsFsPw9&view=vewnVm2Nmj
#https://aicarrier.feishu.cn/wiki/RV7JwNDPLiwUXrk5rfLc7gNwn9a?table=tblQV9fpL1IDwu5K&view=vewOC2wJP2

LARK_TAB_ID_ais="tblQV9fpL1IDwu5K"
LARK_VIEW_ID_ais="vewOC2wJP2"
LARK_TAB_ID_xsk="tbllob6N3JsFsPw9"
LARK_VIEW_ID_xsk="vewnVm2Nmj"

app_token="I2vVbMSTbavpxksuqnacSKdCnqg"
#app_token="Z9ErbJHAhajGrbsstwScuytonQg"

_token_cache = {
    "token": None,
    "expire_at": 0
}

def ipupdate(source_df,field_types):

    update_records=[]

    for _,row in source_df.iterrows():
        fields={}
        for col,val in row.items():
            ftype = field_types.get(col)
            if pd.isna(val) or (isinstance(val, (list, np.ndarray)) and len(val)==0):
                continue
            
            if ftype == "text":
                fields[col] = str(val).strip()
            elif ftype == "url":
                if isinstance(val, dict):
                    fields[col] = {
                        "text": val.get("text","").strip(),
                        "link": val.get("link","").strip()
                    }
                else:
                    fields[col] = {"text": "", "link": str(val).strip()}
            elif ftype == "multi_select":
                if isinstance(val, list):
                    fields[col] = [str(x).strip() for x in val]
                else:
                    fields[col] = [x.strip() for x in str(val).split(",") if x.strip()]
            elif ftype == "datetime":
                fields[col] = int(float(val) * 1000)        
        update_records.append({
            "record_id":row.get("record_id"),
            "fields":fields
        })

    with open('update_records_output.txt', 'w', encoding='utf-8') as file:
        for record in update_records:
            file.write(f"{record}\n")


    tenant_access_token = get_valid_tenant_access_token()
    # 3. 分批调用 batch_update 接口
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{LARK_TAB_ID_xsk}/records/batch_update"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    batch_size = 1000
    success, fail = 0, 0
    for i in range(0, len(update_records), batch_size):
        batch = update_records[i : i + batch_size]
        payload = {"records": batch}
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 200 and resp.json().get("code") == 0:
            success += len(batch)
            logging.info(f"第 {i//batch_size+1} 批更新成功 {len(batch)} 条")
        else:
            fail += len(batch)
            logging.error(f"第 {i//batch_size+1} 批更新失败：HTTP {resp.status_code}，响应：{resp.text}")

    logging.info(f"批量更新完成：成功 {success} 条，失败 {fail} 条")

def convert_to_dataframe(records):

    try:
        logging.info("开始将记录转换为DataFrame...")

        # 准备数据
        data = []
        
        for record in records:
            

            row = {}
            row['record_id'] = record.get('record_id')
            fields = record.get('fields', {})

            for key, value in fields.items():
                if key != 'record_id':  
                    row[key] = value
            data.append(row)
                
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        logging.error(f"转换为DataFrame时发生异常: {str(e)}")
        return pd.DataFrame()


def get_valid_tenant_access_token():
    """
    获取有效的 tenant_access_token，自动处理过期
    """
    now = time.time()

    if _token_cache["token"] is None or now > _token_cache["expire_at"] - 60:
        _token_cache["token"] = get_tenant_access_token()
        _token_cache["expire_at"] = now + 6000
    return _token_cache["token"]


def get_tenant_access_token():
    token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    token_payload = {
        "app_id": APP_ID,
        "app_secret": APP_SECRET
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(token_url, headers=headers, json=token_payload)
        token_data = response.json()
        tenant_access_token = token_data.get("tenant_access_token")
        logging.info(f"成功获取访问令牌{tenant_access_token}")
        return tenant_access_token
    except Exception as e:
        logging.error(f"获取令牌时发生异常: {str(e)}")
        return None

def get_bitable_datas(tenant_access_token, app_token, table_id, view_id, page_token='', page_size=20):

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size={page_size}&page_token={page_token}&user_id_type=user_id"
    payload_dict = {}
    if view_id:
        payload_dict["view_id"] = view_id
    payload = json.dumps(payload_dict)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {tenant_access_token}'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    
    return response.json()

def get_records_from_table(table_id, view_id, page_size=100):
    tenant_access_token = get_valid_tenant_access_token()
    

    page_token = ''
    page_size = 500
    has_more = True
    feishu_datas = []
    while has_more:
        response = get_bitable_datas(tenant_access_token, app_token, table_id, view_id, page_token, page_size)
        if response['code'] == 0:
            page_token = response['data'].get('page_token')
            has_more = response['data'].get('has_more')

            feishu_datas.extend(response['data'].get('items'))
        else:
            print(response)
            raise Exception(response['msg'])
            
        
    return feishu_datas


def update_bitable_record(table_id, record_id, fields):
    """
    更新飞书多维表格中的记录
    
    Args:
        table_id (str): 表格ID
        record_id (str): 记录ID
        fields (dict): 要更新的字段和值
    
    Returns:
        bool: 更新是否成功
    """
    tenant_access_token = get_valid_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {"fields": fields}
    response = requests.put(url, headers=headers, json=payload)
    
    if response.status_code == 200 and response.json().get("code") == 0:
        logging.info(f"成功更新记录 {record_id}")
        return True
    else:
        logging.error(f"更新记录失败：{response.text}")
        return False

def create_bitable_record(table_id, fields):
    """
    在飞书多维表格中创建新记录
    
    Args:
        table_id (str): 表格ID
        fields (dict): 要创建的字段和值
    
    Returns:
        bool: 创建是否成功
    """
    tenant_access_token = get_valid_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {"fields": fields}
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200 and response.json().get("code") == 0:
        logging.info(f"成功创建记录")
        return True
    else:
        logging.error(f"创建记录失败：{response.text}")
        return False

def sanitize_for_json(obj: Any) -> Any:
    """递归清理 NaN 和无穷值，确保 JSON 兼容"""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        logging.warning(f"发现 JSON 不合规的浮点值: {obj}，替换为 None")
        return None  # 或 ""，根据飞书 API 字段要求
    return obj

def save_records_to_json(records: List[Dict[str, Any]], filename: str) -> None:
    """保存记录到 JSON 文件"""
    try:
        # 清理记录
        cleaned_records = [sanitize_for_json(record) for record in records]
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(cleaned_records, f, ensure_ascii=False, indent=2)
        logging.info(f"记录已保存到 {filename}")
    except Exception as e:
        logging.error(f"保存 JSON 文件失败: {str(e)}")

def load_records_from_json(filename: str) -> List[Dict[str, Any]]:
    """从 JSON 文件加载记录"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            records = json.load(f)
        logging.info(f"从 {filename} 加载记录成功，记录数: {len(records)}")
        return records
    except Exception as e:
        logging.error(f"加载 JSON 文件失败: {str(e)}")
        return []