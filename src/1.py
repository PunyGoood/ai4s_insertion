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
import asyncio
import aiohttp
from typing import List, Dict, Any
from .globals import id_data,id_lock,increment_id
from .lark import(
    ipupdate,
    get_valid_tenant_access_token,
    get_tenant_access_token,
    get_bitable_datas,
    get_records_from_table,
    update_bitable_record,
    create_bitable_record,
    sanitize_for_json,
    save_records_to_json,
    load_records_from_json
)

from .lark import(
    get_records_from_table_async,
    update_bitable_record_async
)

logging.basicConfig(level=logging.DEBUG)

from .linshi import get_records_from_table_1,get_records_from_table_2,update_tmp_record_status,update_all_tmp_records,extract_domain
    
#https://aicarrier.feishu.cn/wiki/SFrswlREwijtqDkHv6scbdWBnue?table=tblGz3Ls6CCGHlJP&view=vewTRw3wmL
LARK_TAB_ID_xsk="tblGz3Ls6CCGHlJP"
LARK_VIEW_ID_xsk="vewTRw3wmL"

#https://aicarrier.feishu.cn/wiki/ON7uwDVWaiC74LklNcbcoafqnMi?table=tblOV8p2VwpKqF4I&view=vewfUuYgQL
LARK_TAB_ID_tmp="tblOV8p2VwpKqF4I"
LARK_VIEW_ID_tmp="vewfUuYgQL"

app_token_1="Z9ErbJHAhajGrbsstwScuytonQg"
app_token_2="MIR0bNf1Eao09mswwMwcVrYJndd"

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
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token_2}/tables/{table_id}/records/{record_id}"
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


tmp_records=get_records_from_table_2(LARK_TAB_ID_tmp, LARK_VIEW_ID_tmp)

print(tmp_records)

for tmp_record in tmp_records:
    record_id=tmp_record.get('record_id')
    fields=tmp_record.get('fields', {})
    interface_people=tmp_record.get('fields', {}).get('接口人',[])
    if interface_people:
        person_id = interface_people[0].get('id')
        person_name = interface_people[0].get('name', '')
        print(person_id)  # 输出: da714327
        
        interface_person = [
            {
                "id": person_id,
                "name": person_name
            }
        ]
    else:
        interface_person = []
        print("未找到接口人信息")
    
    fujian=(
                        fields.get('fujian', [])[0].get('text', '')
                        
                        if isinstance(fields.get('fujian'), list) 
                        and fields.get('fujian') 
                        and len(fields.get('fujian')) > 0
                        and isinstance(fields.get('fujian')[0], dict)
                        and 'text' in fields.get('fujian')[0]
                        else ''
                    )
    #print("fujian",fujian)

    fields = {
        '接口人': [
            {
                "id": "ou_26e9a6fcbcc1b996fb8614bf7beda10f"
            },
            {
                "id": "ou_06594f55f5089ef3390c2ed3a23a384e"
            }
        ]
        #'fujian': [{'link': 'https://aicarrier.feishu.cn/docx/EhZpdwkz8o6AOfxQRD2c7uaznsg', 'mentionType': 'Docx', 'text': '语料库表单说明', 'token': 'EhZpdwkz8o6AOfxQRD2c7uaznsg', 'type': 'mention'}]
    }

    update_bitable_record(LARK_TAB_ID_tmp, record_id, fields)