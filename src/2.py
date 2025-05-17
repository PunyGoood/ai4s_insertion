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
logging.basicConfig(level=logging.DEBUG)


# TZ = timezone("Asia/Shanghai")
# coll_seq_ids = get_collection("seq_ids")
# coll_seq_ids.create_index([("name", 1)], unique=True)

APP_ID="cli_a880af1659b0d013"
APP_SECRET="6LRmHThxkRlfIE2HEvNvpcI7SfkdPMZ3"

LARK_TAB_ID_ais="tblq61scZ0rMd27q"
LARK_VIEW_ID_ais="vewjzbGWbm"
LARK_TAB_ID_xsk="tblFT4Xo7kcVjRx0"
LARK_VIEW_ID_xsk="vewnVm2Nmj"

app_token="Z9ErbJHAhajGrbsstwScuytonQg"



    
if __name__ == "__main__":
    dotenv.load_dotenv()

    app_id = os.getenv("FEISHU_APP_ID")
    if not app_id:
        raise ValueError("FEISHU_APP_ID 未设置")

    app_secret = os.getenv("FEISHU_APP_SECRET")
    if not app_secret:
        raise ValueError("FEISHU_APP_SECRET 未设置")

    
    while(True):

        ais_records=get_records_from_table(LARK_TAB_ID_ais, LARK_VIEW_ID_ais)
        xsk_records = get_records_from_table(LARK_TAB_ID_xsk, LARK_VIEW_ID_xsk)


        
        save_records_to_json(ais_records, 'ais_records.json')
        save_records_to_json(xsk_records, 'xsk_records.json')

        ais_records = load_records_from_json('ais_records.json')
        xsk_records = load_records_from_json('xsk_records.json')

        links = {}
    
        for ais_record in ais_records:  
            fields = ais_record.get('fields', {})  # 获取 fields 字典
            if fields.get('数据链接') and '内部需求-AI4S' in fields.get('线索来源类型') :
                link=fields.get('数据链接')['link']
                if(link):
                    links[link]=fields
             
        matching_records = []
        not_found_records = []


    for xsk_record in xsk_records:
        fields = xsk_record.get('fields', {})

        ###这里要保证非空
        xsk_link =xsk_record.get('fields', {}).get('数据链接', {})['link']
        # print("链接如下")
        # print(xsk_link)
        # print(type(xsk_link))
        #xsk_link = fields.get('数据链接', {})[0].get('link', '')
       
        if xsk_link and xsk_link in links :
            print(xsk_link)
            matching_records.append(xsk_record)
            
    print(f"找到 {len(matching_records)} 条匹配记录")
    for record in matching_records:
        print(f"数据名称: {record.get('fields', {}).get('数据名称', [])[0].get('text', '')}")
        print(f"链接: {record.get('fields', {}).get('数据链接', {})['link']}")
        print("---")


    
    for xsk_record in xsk_records:
        
        xsk_link = xsk_record.get('fields', {}).get('数据链接', {})['link']
        if xsk_link and xsk_link in links:
            print(xsk_link)
            print(xsk_record)

            rid = xsk_record.get('fields', {})['rid']['value'][0]['text']

            print(rid)

            #更新线索次数
            current_count = xsk_record.get('fields', {}).get('来源-线索次数',0)
            existing_type = xsk_record.get('来源-类型', [])
            new_type = existing_type if '内部需求-AI4S' in existing_type else ['内部需求-AI4S']

            print(new_type)
            

            fields = {
                '来源-线索次数': current_count+1 ,
                '来源-类型': ['内部需求-AI4S'],
                '来源-（内部）需求团队': ['AI4生命科学团队']
            }
            update_bitable_record(LARK_TAB_ID_xsk, rid, fields)
            
        else:
            not_found_records.append(xsk_record)



    for ais_link, ais_record in links.items():
        
        found = False
        for xsk_record in xsk_records:
            fields = xsk_record.get('fields', {})
            xsk_link = xsk_record.get('fields', {}).get('数据链接', {})['link']
  
            if ais_link == xsk_link:
                found = True
                break
                
            
        if not found:
            print(f"未找到的链接: {ais_link}")

            print(ais_record)

            rid = ais_record['rid']['value'][0]['text']

            print(rid)

            ##在此之前正确
        

            # new_record = {
            #     '数据名称': ais_record.get('数据名称', [])[0].get('text', '') if isinstance(ais_record.get('数据名称', []), list) and ais_record.get('数据名称', []) and isinstance(ais_record.get('数据名称', [])[0], dict) else '',
            #     '数据链接': ais_record.get('数据链接', {}).copy(),
            #     '数据简介': ais_record.get('数据简介', [])[0].get('text', '') if isinstance(ais_record.get('数据简介', []), list) and ais_record.get('数据简介', []) and isinstance(ais_record.get('数据简介', [])[0], dict) else ais_record.get('数据简介', ''),
            #     '一级领域': ais_record.get('一级领域', []),
            #     '二级领域': ais_record.get('二级领域', []),
            #     '国家/地区': ais_record.get('国家/地区', []),
            #     '预估大小': ais_record.get('预估大小', [])[0].get('text', '') if isinstance(ais_record.get('预估大小', []), list) and ais_record.get('预估大小', []) and isinstance(ais_record.get('预估大小', [])[0], dict) else ais_record.get('预估大小', ''),
            #     '来源-（内部）需求团队': ['AI4生命科学团队'],
            #     '备注': ais_record.get('备注', [])[0].get('text', '') if isinstance(ais_record.get('备注', []), list) and ais_record.get('备注', []) and isinstance(ais_record.get('备注', [])[0], dict) else ais_record.get('备注', ''),
            #     '来源-类型': ais_record.get('线索来源类型', ais_record.get('来源-类型', [])),
            #     '来源-线索次数': 1
            # }

            fields=ais_record.get('fields', {})

            print("打印")

            _=datetime.fromtimestamp(ais_record.get('提交日期')/1000).month
            month= str(_).zfill(2)
            print(month)


            
            id="xsk_2025"+str(month)+str(increment_id())
            print(type(id))

            new_record = {
                '数据ID': id,
                '数据名称': ais_record.get('数据名称', [])[0].get('text', '') if isinstance(ais_record.get('数据名称', []), list) and ais_record.get('数据名称', []) and isinstance(ais_record.get('数据名称', [])[0], dict) else '',
                '数据链接': ais_record.get('数据链接', {}),
                '数据简介': ais_record.get('数据简介', [])[0].get('text', '') if isinstance(ais_record.get('数据简介', []), list) and ais_record.get('数据简介', []) and isinstance(ais_record.get('数据简介', [])[0], dict) else ais_record.get('数据简介', ''),
                
                '预估大小（GB）': ais_record.get('预估大小（GB）', [])[0].get('text', ''),
                '来源-（内部）需求团队': ['AI4生命科学团队'],
                '备注': ais_record.get('备注', [])[0].get('text', ''),
                '来源-类型': ais_record.get('线索来源类型', ais_record.get('来源-类型', [])),
                '来源-线索次数': 1
            }
            print(new_record)
            for key, value in new_record.items():
                print(f"字段 '{key}': 类型是 {type(value)}")
            # new_record = sanitize_for_json(new_record)
        
            create_bitable_record(LARK_TAB_ID_xsk, new_record)

            fields = {
                    '数据ID': id,
                    
                }
            update_bitable_record(LARK_TAB_ID_ais, rid, fields)





    

    
    


