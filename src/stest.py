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


LARK_TAB_ID_ais="tblQV9fpL1IDwu5K"
LARK_VIEW_ID_ais="vewOC2wJP2"
LARK_TAB_ID_xsk="tbllob6N3JsFsPw9"
LARK_VIEW_ID_xsk="vewnVm2Nmj"



if __name__ == "__main__":
    dotenv.load_dotenv()

    
    while(True):

        ais_records=get_records_from_table(LARK_TAB_ID_ais, LARK_VIEW_ID_ais)
        xsk_records = get_records_from_table(LARK_TAB_ID_xsk, LARK_VIEW_ID_xsk)


        
        save_records_to_json(ais_records, 'ais_records.json')
        save_records_to_json(xsk_records, 'xsk_records.json')

        ais_records = load_records_from_json('ais_records.json')
        xsk_records = load_records_from_json('xsk_records.json')

        links = {}


        for ais_record in ais_records:  # 直接遍历列表
            logging.info(f"ais_record: {ais_record}")
            logging.info("ais_record 字段类型:")
            fields = ais_record.get('fields', {})  # 获取 fields 字典
            for key, value in fields.items():
                logging.info(f"  {key}: {value}, 类型: {type(value)}")

            print(ais_record.get('fields', {}).get('完成输入'))

        
        for ais_record in ais_records:  
            fields = ais_record.get('fields', {})  
            if fields.get('数据链接') and '内部需求-AI4S' in fields.get('线索来源类型') and ais_record.get('fields', {}).get('完成输入'):
                link=fields.get('数据链接')['link']
                link=link.strip().rstrip('/')
                print("打印links的link")
                print(link)
                if(link):
                    links[link]=ais_record
                
        
        matching_records = []
        not_found_records = []

        for xsk_record in xsk_records:
            fields = xsk_record.get('fields', {})

            ###这里要保证非空
            xsk_link =xsk_record.get('fields', {}).get('数据链接', {})['link']
        
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

                    #rid = xsk_record.get('fields', {})['rid']['value'][0]['text']
                    rid=xsk_record.get('record_id')
                    print(rid)


                    #更新线索次数
                    current_count = xsk_record.get('fields', {}).get('来源-线索次数',0)
                    existing_type = xsk_record.get('来源-类型', [])
                    new_type = existing_type if '内部需求-AI4S' in existing_type else ['内部需求-AI4S']

                    print(new_type)
                    

                    fields_xsk = {
                        '来源-线索次数': current_count+1 ,
                        '来源-类型': ['内部需求-AI4S'],
                        '来源-内部需求团队': ['AI4生命科学团队']
                    }
                    update_bitable_record(LARK_TAB_ID_xsk, rid, fields_xsk)

                    id=xsk_record.get('fields', {}).get('数据ID',[])[0]['text']
                    raw_date = xsk_record.get('fields', {}).get('入库日期')

                    if raw_date:
                        from datetime import datetime
                        date_obj = datetime.fromtimestamp(raw_date / 1000)
                        formatted_date = date_obj.strftime('%Y%m%d')
                    else:
                        formatted_date = '未知日期'

                    stat=xsk_record.get('fields', {}).get('获取状态','暂无')
                    condi = f"线索{id}在{formatted_date}录入，当前采集状态是：{stat}"
                    
                    print(condi)

                    fields_ais= {
                        '数据ID':id,
                        '线索在数据库情况':condi,
                        
                    }
                    ais_record=links[xsk_link]
                    print(ais_record)
                    rid=ais_record.get('record_id')
                    update_bitable_record(LARK_TAB_ID_ais, rid, fields_ais)
                    
                else:
                    not_found_records.append(xsk_record)


        for ais_link, ais_record in links.items():
            
            print(ais_link, ais_record)
            found = False
            for xsk_record in xsk_records:
                fields = xsk_record.get('fields', {})
                xsk_link = xsk_record.get('fields', {}).get('数据链接', {})['link']
    
                if ais_link == xsk_link:
                    found = True
                    break
                    
                
            if not found:
                print(f"未找到的链接: {ais_link}")

                

                rid = ais_record.get('record_id')

                print(rid)
                  

                fields=ais_record.get('fields', {})

                print("打印")

                timestamp = ais_record.get('fields', {}).get('提交日期', 0)
                month_num = datetime.fromtimestamp(timestamp / 1000).month
                # 格式化为两位数（01-12）
                month = f"{month_num:02d}" 
                print(month)


                
                id="xsk_2025"+str(month)+str(increment_id())

                print(fields.get('数据名称', [])[0].get('text', ''))

                new_record = {
                    '数据ID': id,
                    '数据名称': fields.get('数据名称', [])[0].get('text', '') ,
                    '数据链接': fields.get('数据链接', {}),
                    '数据简介': fields.get('数据简介', [])[0].get('text', '') if isinstance(ais_record.get('数据简介', []), list) and ais_record.get('数据简介', []) and isinstance(ais_record.get('数据简介', [])[0], dict) else ais_record.get('数据简介', ''),
                    
                    '预估大小（GB）': fields.get('预估大小（GB）'),
                    '来源-内部需求团队': ['AI4生命科学团队'],
                    '备注': fields.get('备注', [])[0].get('text', ''),
                    '来源-类型': ['内部需求-AI4S'],
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


        time.sleep(5)
