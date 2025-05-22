import os
import time
from datetime import datetime
from pytz import timezone
import requests
import dotenv
from pymongo import errors
import json
import logging
import pandas as pd
import numpy as np
import ast
import math
from typing import List, Dict, Any

from .feishu_sync import FeishuBiTableSync
from .feishu_table import FeishuBiTable
from .mongodb import get_collection
from .record_db import RecordDatabase
from .globals import id_data, id_lock, increment_id
from .lark import (
    ipupdate,
    get_valid_tenant_access_token,
    get_tenant_access_token,
    get_bitable_datas,
    get_records_from_table,
    update_bitable_record,
    create_bitable_record,
    sanitize_for_json,
    save_records_to_json,
    load_records_from_json,
    normalize_url
)

# 设置日志级别
logging.basicConfig(level=logging.DEBUG)

# 飞书表格常量
LARK_TAB_ID_ais = "tblQV9fpL1IDwu5K"
LARK_VIEW_ID_ais = "vewOC2wJP2"
LARK_TAB_ID_xsk = "tbllob6N3JsFsPw9"
LARK_VIEW_ID_xsk = "vewnVm2Nmj"


if __name__ == "__main__":
    # 加载已处理的ID
    PROCESSED_IDS_FILE = "processed_ids.json"
    if os.path.exists(PROCESSED_IDS_FILE):
        with open(PROCESSED_IDS_FILE, "r", encoding="utf-8") as f:
            processed_ids = set(json.load(f))  # 从JSON加载并转为set
    else:
        processed_ids = set()  # 文件不存在则初始化空集合
    
    while True:
        # 获取飞书表格数据
        ais_records = get_records_from_table(LARK_TAB_ID_ais, LARK_VIEW_ID_ais)
        xsk_records = get_records_from_table(LARK_TAB_ID_xsk, LARK_VIEW_ID_xsk)
        
        # 保存和加载记录
        save_records_to_json(ais_records, 'ais_records.json')
        save_records_to_json(xsk_records, 'xsk_records.json')
        ais_records = load_records_from_json('ais_records.json')
        xsk_records = load_records_from_json('xsk_records.json')

        links = {}

        # 打印记录信息
        for ais_record in ais_records:  
            logging.info(f"ais_record: {ais_record}")
            logging.info("ais_record 字段类型:")
            fields = ais_record.get('fields', {})  
            for key, value in fields.items():
                logging.info(f"  {key}: {value}, 类型: {type(value)}")

            print(ais_record.get('fields', {}).get('完成输入'))
        
        # 收集需要处理的记录
        for ais_record in ais_records:  
            fields = ais_record.get('fields', {})  
            if (fields.get('数据链接') and ais_record.get('fields', {}).get('完成输入')):
                link = fields.get('数据链接')['link']
                link = normalize_url(link)
                
                try:
                    ais_id = ais_record.get('fields', {}).get('数据ID', [])[0]['text']
                except (KeyError, IndexError, TypeError):
                    ais_id = None

                print(ais_id)

                # 只处理未处理过的记录
                if link and (ais_id == None or ais_id not in processed_ids):
                    links[link] = ais_record
                    print(link)
                
        print("processed_ids")
        print(processed_ids)

        # 查找匹配记录
        matching_records = []
        not_found_records = []

        for xsk_record in xsk_records:
            try:
                # 获取数据链接字段
                data_link = xsk_record.get('fields', {}).get('数据链接', {})
                # 检查是否是字典且包含link键
                if isinstance(data_link, dict):
                    xsk_link = data_link.get('link', '')
                else:
                    xsk_link = ''
            except Exception as e:
                logging.warning(f"处理数据链接时出错: {e}")
                xsk_link = ''
    
            # 确保xsk_link是字符串且不为空
            xsk_link = normalize_url(xsk_link) if xsk_link else ''
        
            if xsk_link and xsk_link in links:
                print(xsk_link)
                matching_records.append(xsk_record)
                
        print(f"找到 {len(matching_records)} 条匹配记录")
        for record in matching_records:
            print(f"链接: {record.get('fields', {}).get('数据链接', {})['link']}")
            print("---")

        #########################################################################################
        ##############################对已有的记录操作############################################
        #########################################################################################

        for xsk_record in xsk_records:
            try:
                data_link = xsk_record.get('fields', {}).get('数据链接', {})
                if isinstance(data_link, dict):
                    xsk_link = data_link.get('link', '')
                else:
                    xsk_link = ''
            except Exception as e:
                logging.warning(f"处理数据链接时出错: {e}")
                xsk_link = ''
    
            # 确保xsk_link是字符串且不为空
            xsk_link = normalize_url(xsk_link) if xsk_link else ''
            xsk_id = xsk_record.get('fields', {}).get('数据ID', [])[0]['text']

            if xsk_link and xsk_link in links:
                # 获取记录ID
                rid = xsk_record.get('record_id')

                # 更新线索次数
                current_count = xsk_record.get('fields', {}).get('来源-线索次数', 0)

                # 更新来源类型
                existing_type = xsk_record.get('fields', {}).get('来源-类型', [])
                
                # 确保 existing_type 是列表
                if not isinstance(existing_type, list):
                    existing_type = []
                
                print("existing_type")
                print(existing_type)

                # 添加新类型（如果不存在）
                if '内部需求-AI4S' not in existing_type:
                    new_type = existing_type + ['内部需求-AI4S']
                else:
                    new_type = existing_type
                
                # 处理外部需求方
                ais_record = links[xsk_link]
                existing_external_demanders = xsk_record.get('fields', {}).get('外部需求方', [])
                if not isinstance(existing_external_demanders, list):
                    existing_external_demanders = []

                ais_external_demanders = ais_record.get('fields', {}).get('外部需求方', [])
                if not isinstance(ais_external_demanders, list):
                    ais_external_demanders = []
                
                if existing_external_demanders is not []:
                    new_external_demanders = existing_external_demanders + ais_external_demanders
                

                # 更新XSK记录
                fields_xsk = {
                    '来源-线索次数': current_count + 1,
                    '来源-类型': new_type,
                    '来源-（内部）需求团队': ['AI4生命科学团队'],
                    '外部需求方': new_external_demanders
                }
                update_bitable_record(LARK_TAB_ID_xsk, rid, fields_xsk)

                # 获取ID和日期信息
                id = xsk_record.get('fields', {}).get('数据ID', [])[0]['text']
                raw_date = xsk_record.get('fields', {}).get('入库日期')

                if raw_date:
                    date_obj = datetime.fromtimestamp(raw_date / 1000)
                    formatted_date = date_obj.strftime('%Y%m%d')
                else:
                    formatted_date = '未知日期'

                stat = xsk_record.get('fields', {}).get('获取状态', '暂无')
                condi = f"线索{id}在{formatted_date}录入，当前采集状态是：{stat}"
                
                print(condi)

                # 更新AIS记录
                fields_ais = {
                    '数据ID': id,
                    '进入线索库时间': formatted_date,
                    '在线索库的采集状态': stat,
                }
                ais_record = links[xsk_link]
                rid = ais_record.get('record_id')
                update_bitable_record(LARK_TAB_ID_ais, rid, fields_ais)

                # 更新已处理ID列表
                processed_ids.add(xsk_id)
                with open(PROCESSED_IDS_FILE, "w", encoding="utf-8") as f:
                    json.dump(list(processed_ids), f)
                
            else:
                not_found_records.append(xsk_record)

        #########################################################################################
        ##############################对不存在的记录操作############################################
        #########################################################################################

        for ais_link, ais_record in links.items():

            ais_link = normalize_url(ais_link) if ais_link else ''
            found = False
            
            # 检查是否已存在匹配记录
            for xsk_record in xsk_records:
                try:
                    xsk_link = xsk_record.get('fields', {}).get('数据链接', {})['link']
                    xsk_link = normalize_url(xsk_link) if xsk_link else ''
                    if ais_link == xsk_link:
                        found = True
                        break
                except (KeyError, TypeError):
                    continue
                    
            if not found:
                print(f"未找到的链接: {ais_link}")
                rid = ais_record.get('record_id')

                
                fields = ais_record.get('fields', {})

                # 生成月份信息
                timestamp = fields.get('提交日期', 0)
                month_num = datetime.fromtimestamp(timestamp / 1000).month
                month = f"{month_num:02d}" 
                print(month)

                # 生成新ID
                id = "xsk_2025" + str(month) + str(increment_id())

                # 处理备注字段
                if remarks := fields.get('备注'):
                    remark = remarks[0].get('text', '') if remarks and isinstance(remarks, list) else ''
                else:
                    remark = ''

                # 处理来源-事件ID
                issue_id = fields.get('来源-事件ID', '')
                issue_id = [issue_id] if issue_id else []

                # 创建新记录
                new_record = {
                    '数据ID': id,
                    '数据名称': (fields.get('数据名称', [])[0].get('text', '') 
                        if fields.get('数据名称') and len(fields['数据名称']) > 0 
                        else ''),
                    '数据链接': fields.get('数据链接', {}),
                    '数据简介': (
                        fields.get('数据简介', [])[0].get('text', '')
                        if isinstance(fields.get('数据简介'), list) 
                        and fields.get('数据简介') 
                        and len(fields.get('数据简介')) > 0
                        and isinstance(fields.get('数据简介')[0], dict)
                        and 'text' in fields.get('数据简介')[0]
                        else ''
                    ),
                    '一级领域': fields.get('一级领域', ''),
                    '二级领域': fields.get('二级领域', ''),
                    '数据类型': fields.get('数据类型', ''),
                    '国家/地区': fields.get('国家/地区', []),
                    '预估数据规模': (
                        fields.get('预估数据规模', [])[0].get('text', '')
                        if isinstance(fields.get('预估数据规模'), list) 
                        and fields.get('预估数据规模') 
                        and len(fields.get('预估数据规模')) > 0
                        and isinstance(fields.get('预估数据规模')[0], dict)
                        and 'text' in fields.get('预估数据规模')[0]
                        else ''
                    ),
                    '预估大小（GB）': fields.get('预估大小（GB）'),
                    '来源-（内部）需求团队': fields.get('来源-（内部）需求团队', []),
                    '备注': remark,
                    '来源-类型': fields.get('来源-类型', []),
                    '来源-合作方': fields.get('来源-合作方', []),
                    '来源-事件ID': issue_id,
                    '来源-（外部）提供单位': fields.get('来源-（外部）提供单位', []),
                    '来源-存储方': fields.get('来源-存储方', []),
                    '来源-线索次数': 1,
                    '外部需求方': fields.get('外部需求方', [])
                }
                
                print(new_record)
                for key, value in new_record.items():
                    print(f"字段 '{key}': 类型是 {type(value)}")
                
                # 创建新记录
                create_bitable_record(LARK_TAB_ID_xsk, new_record)

                # 更新AIS记录
                fields = {
                    '数据ID': id,                    
                }
                update_bitable_record(LARK_TAB_ID_ais, rid, fields)

                # 更新已处理ID列表
                processed_ids.add(id)
                with open(PROCESSED_IDS_FILE, "w", encoding="utf-8") as f:
                    json.dump(list(processed_ids), f)

        # 打印统计信息
        print(f"找到 {len(matching_records)} 条匹配记录")
        print(f"新增 {len(not_found_records)} 条记录")
        # time.sleep(5)


