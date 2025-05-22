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
# 假设这些函数也有异步版本，如果没有，需要创建
from .lark import(
    get_records_from_table_async,
    update_bitable_record_async
)

logging.basicConfig(level=logging.DEBUG)
    
#https://aicarrier.feishu.cn/wiki/SFrswlREwijtqDkHv6scbdWBnue?table=tblGz3Ls6CCGHlJP&view=vewTRw3wmL
LARK_TAB_ID_xsk="tblGz3Ls6CCGHlJP"
LARK_VIEW_ID_xsk="vewTRw3wmL"

#https://aicarrier.feishu.cn/wiki/ON7uwDVWaiC74LklNcbcoafqnMi?table=tbl5Ny0ghsOxDJcN&view=vewfUuYgQL
LARK_TAB_ID_tmp="tbl5Ny0ghsOxDJcN"
LARK_VIEW_ID_tmp="vewfUuYgQL"

app_token_1="Z9ErbJHAhajGrbsstwScuytonQg"
app_token_2="MIR0bNf1Eao09mswwMwcVrYJndd"

def extract_domain(url):
    """提取URL中的域名（一级或二级域名）"""
    # 移除协议部分
    if "://" in url:
        url = url.split("://")[1]
    print(url)
    
    # 特殊处理github和huggingface
    if any(site in url.lower() for site in ["github", "huggingface","kaggle"]):
        # 保留路径信息，但移除查询参数
        if "?" in url:
            url = url.split("?")[0]
        return url  # 返回包含路径的URL
    
    # 对于其他网站，移除路径部分
    if "/" in url:
        url = url.split("/")[0]
    print(url)
    
    # 分割域名部分
    parts = url.split(".")
    
    # 如果域名部分少于3个，可能是一级域名（如example.com）
    if len(parts) <= 2:
        return url
    
    # 否则取最后两个或三个部分作为域名（如sub.example.com）
    return ".".join(parts[-3:]) if parts[-2] in ["co", "com", "org", "net", "edu", "gov"] else ".".join(parts[-2:])

def normalize_url(raw_link):
    # 1. 移除可能存在的协议头（http://, https://, //）
    if raw_link.startswith(("http://", "https://", "//")):
        # 找到 "://" 或 "//" 之后的部分
        raw_link = raw_link.split("://")[-1] if "://" in raw_link else raw_link[2:]
    
    # 2. 移除开头的 "www."（如果有的话，避免重复）
    if raw_link.startswith("www."):
        raw_link = raw_link[4:]
    
    # 3. 移除末尾的斜杠 "/"
    raw_link = raw_link.rstrip("/")
    
    # 4. 拼接成最终的 "https://www." 格式
    normalized_url = f"https://www.{raw_link}"
    
    return normalized_url

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
            
def get_records_from_table_1(table_id, view_id, page_size=100):
    tenant_access_token = get_valid_tenant_access_token()
    

    page_token = ''
    page_size = 500
    has_more = True
    feishu_datas = []
    while has_more:
        response = get_bitable_datas(tenant_access_token, app_token_1, table_id, view_id, page_token, page_size)
        if response['code'] == 0:
            page_token = response['data'].get('page_token')
            has_more = response['data'].get('has_more')

            feishu_datas.extend(response['data'].get('items'))
        else:
            print(response)
            raise Exception(response['msg'])
            
        
    return feishu_datas

def get_records_from_table_2(table_id, view_id, page_size=100):
    tenant_access_token = get_valid_tenant_access_token()
    

    page_token = ''
    page_size = 500
    has_more = True
    feishu_datas = []
    while has_more:
        response = get_bitable_datas(tenant_access_token, app_token_2, table_id, view_id, page_token, page_size)
        if response['code'] == 0:
            page_token = response['data'].get('page_token')
            has_more = response['data'].get('has_more')

            feishu_datas.extend(response['data'].get('items'))
        else:
            print(response)
            raise Exception(response['msg'])
            
        
    return feishu_datas

def update_tmp_record_status(record_id, found_in_xsk, xsk_record_id=None):
    """更新tmp表中记录的'线索库是否收录'字段和'线索库记录ID'字段"""
    fields = {
        '线索库是否收录': "是" if found_in_xsk else "否",
    }
    
    # 如果找到了对应的线索库记录，添加记录ID
    if found_in_xsk and xsk_record_id:
        fields['数据ID（若线索库收录）'] = xsk_record_id
    
    return update_bitable_record(LARK_TAB_ID_tmp, record_id, fields)

def update_all_tmp_records(results):
    """批量更新所有tmp记录的状态，添加降速功能"""
    for i, result in enumerate(results):
        record_id = result['tmp_record_id']
        found_in_xsk = result['found_in_xsk']
        xsk_record_id = result.get('xsk_record_id')
        update_tmp_record_status(record_id, found_in_xsk, xsk_record_id)
        
        
    
    return True

def main():
    #xsk_records = get_records_from_table_1(LARK_TAB_ID_xsk, LARK_VIEW_ID_xsk)
    tmp_records=get_records_from_table_2(LARK_TAB_ID_tmp, LARK_VIEW_ID_tmp)

    #save_records_to_json(xsk_records, 'xsk_records.json')
    xsk_records = load_records_from_json('xsk_records.json')
    
    # 创建结果列表
    results = []
    
    # 预处理xsk记录中的域名，提高查找效率
    xsk_domains = {}
    for xsk_record in xsk_records:
        xsk_link = xsk_record.get('fields', {}).get('数据链接', {}).get('link', '')
        if xsk_link:
            xsk_domain = extract_domain(xsk_link)
            if xsk_domain not in xsk_domains:
                xsk_domains[xsk_domain] = []
            xsk_domains[xsk_domain].append(xsk_record)
    
    for tmp_record in tmp_records:
        tmp_link = tmp_record.get('fields', {}).get('数据链接', {}).get('link', '')
        normalized_tmp_link = normalize_url(tmp_link)
        tmp_domain = extract_domain(normalized_tmp_link)
        
        # 检查域名是否存在于xsk_records中
        found = False
        matching_record = None
        
        if tmp_domain in xsk_domains:
            found = True
            # 如果有多个匹配记录，取第一个
            matching_record = xsk_domains[tmp_domain][0]
        
        # 将结果添加到列表中
        result = {
            'tmp_record_id': tmp_record.get('record_id'),
            'tmp_link': normalized_tmp_link,
            'tmp_domain': tmp_domain,
            'found_in_xsk': found
        }
        
        if found and matching_record:
            result['xsk_link'] = matching_record.get('fields', {}).get('数据链接', {}).get('link', '')
            # 添加xsk记录ID
           
            result['xsk_record_id'] = matching_record.get('fields', {}).get('数据ID', [])[0]['text']
            
        
        results.append(result)
    
    # 保存结果到JSON文件
    with open('link_check_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    
    print(f"检查完成，共处理 {len(tmp_records)} 条记录，结果已保存到 link_check_results.json")
    
    # 更新tmp表中的"线索库是否收录"字段
    print("开始更新飞书表格中的'线索库是否收录'字段和'线索库记录ID'字段...")
    update_all_tmp_records(results)
    print("更新完成！")

if __name__ == "__main__":
    main()
