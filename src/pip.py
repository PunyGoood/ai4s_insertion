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

async def process_record(xsk_record):
    fields = xsk_record.get('fields', {})

    try:
        # 先获取数据链接字段，默认为空字典
        data_link = fields.get('数据链接', {})
        # 检查是否是字典且包含link键
        if isinstance(data_link, dict):
            xsk_link = data_link.get('link', '')
        else:
            xsk_link = ''
    except Exception as e:
        logging.error(f"处理记录时出错: {e}")
        return

    raw_link = xsk_link
    link = normalize_url(raw_link)

    rid = xsk_record.get('record_id')

    url = {
        "link": link,
        "text": link,
    }

    fields_xsk = {
        '数据链接': url
    }
    
    await update_bitable_record_async(LARK_TAB_ID_xsk, rid, fields_xsk)
    logging.debug(f"已更新记录 {rid} 的链接为 {link}")

async def main():
    # 异步获取记录
    xsk_records = await get_records_from_table_async(LARK_TAB_ID_xsk, LARK_VIEW_ID_xsk)
    
    # 如果需要保存和加载记录，可以保留这部分
    save_records_to_json(xsk_records, 'xsk_records.json')
    xsk_records = load_records_from_json('xsk_records.json')
    
    # 创建任务列表
    tasks = []
    
    # 使用信号量限制并发请求数量
    semaphore = asyncio.Semaphore(5)  # 限制最大并发数为5
    
    async def process_with_rate_limit(record):
        async with semaphore:
            await process_record(record)
            # 添加延迟，避免请求过快
            await asyncio.sleep(0.5)  # 每个请求后等待0.5秒
    
    # 创建限速后的任务列表
    for record in xsk_records:
        tasks.append(process_with_rate_limit(record))
    
    # 并发执行所有任务（但受信号量限制）
    await asyncio.gather(*tasks)
    
    logging.info(f"已完成处理 {len(tasks)} 条记录")

if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())

    # test_urls 部分保持不变，但已注释掉
    # test_urls = [
    #     "example.com",           # -> https://www.example.com
    #     "http://example.com",    # -> https://www.example.com
    #     "https://example.com",   # -> https://www.example.com
    #     "//example.com",         # -> https://www.example.com
    #     "www.example.com",       # -> https://www.example.com
    #     "example.com/",          # -> https://www.example.com
    #     "http://www.example.com",# -> https://www.example.com
    #     "https://www.example.com/", # -> https://www.example.com
    # ]

    # for url in test_urls:
    #     print(f"Input: {url} -> Output: {normalize_url(url)}")