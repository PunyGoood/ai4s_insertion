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
            raise Exception(response['msg'])
        
    return feishu_datas



# def new_seq_id(name: str) -> int:
#     while True:
#         seq_obj = coll_seq_ids.find_one({"name": name})
#         if seq_obj is None:
#             try:
#                 coll_seq_ids.insert_one({"name": name, "seq_id": 1})
#             except errors.DuplicateKeyError:
#                 continue
#             return 1
#         seq_id = seq_obj["seq_id"]
#         result = coll_seq_ids.update_one(
#             {"name": name, "seq_id": seq_id},
#             {"$set": {"seq_id": seq_id + 1}},
#         )
#         if result.modified_count == 1:
#             return seq_id + 1

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
    
if __name__ == "__main__":
    dotenv.load_dotenv()

    app_id = os.getenv("FEISHU_APP_ID")
    if not app_id:
        raise ValueError("FEISHU_APP_ID 未设置")

    app_secret = os.getenv("FEISHU_APP_SECRET")
    if not app_secret:
        raise ValueError("FEISHU_APP_SECRET 未设置")



    # ais_url = "https://aicarrier.feishu.cn/wiki/SFrswlREwijtqDkHv6scbdWBnue?table=tblq61scZ0rMd27q&view=vewjzbGWbm"
    # ais_table = FeishuBiTable(app_id, app_secret, ais_url)
    # ais_db = RecordDatabase("ais")
    # ais_sync = FeishuBiTableSync(ais_table, ais_db)

    # xsk_url="https://aicarrier.feishu.cn/wiki/SFrswlREwijtqDkHv6scbdWBnue?table=tblFT4Xo7kcVjRx0&view=vewnVm2Nmj"
    # xsk_table=FeishuBiTable(app_id, app_secret, xsk_url)
    # xsk_db=RecordDatabase("xsk")
    # xsk_sync=FeishuBiTableSync(xsk_table,xsk_db)
    



    ais_records=get_records_from_table(LARK_TAB_ID_ais, LARK_VIEW_ID_ais)
    xsk_records = get_records_from_table(LARK_TAB_ID_xsk, LARK_VIEW_ID_xsk)


    
    save_records_to_json(ais_records, 'ais_records.json')
    save_records_to_json(xsk_records, 'xsk_records.json')

    ais_records = load_records_from_json('ais_records.json')
    xsk_records = load_records_from_json('xsk_records.json')

    
    for ais_record in ais_records:  # 直接遍历列表
        logging.info(f"ais_record: {ais_record}")
        logging.info("ais_record 字段类型:")
        fields = ais_record.get('fields', {})  # 获取 fields 字典
        for key, value in fields.items():
            logging.info(f"  {key}: {value}, 类型: {type(value)}")


    links = {}
    
    for ais_record in ais_records:  # 遍历列表
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
       
        if xsk_link and xsk_link in links:
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










    # for xsk_record in xsk_records:
    #     if '数据链接' in xsk_record:
    #         xsk_link = xsk_record['数据链接'].get('link')
    #         if xsk_link in links:
    #             print(xsk_link)
    #             matching_records.append(xsk_record)


    #             #更新线索次数
    #             current_count = xsk_record.get('来源-线索次数', 0)
    #             existing_type = xsk_record.get('来源-类型', [])
    #             new_type = existing_type if '内部需求-AI4S' in existing_type else ['内部需求-AI4S']

    #             print(new_type)
    #             # 获取当前记录的版本号
    #             current_record = xsk_db.get_record(xsk_record['rid'])
    #             if current_record:
    #                 ver = current_record.get('ver', 0)
    #             else:
    #                 ver = 0
                
    #             xsk_db.update_record({
    #                 'rid': xsk_record['rid'],  # 将rid作为字典的一部分传递
    #                 'ver': ver,  # 传递当前版本号
    #                 '来源-线索次数': current_count + 1,
    #                 '来源-类型': new_type,
    #                 '来源-（内部）需求团队': ['AI4生命科学团队']
                    
    #             })

    #             updated_record = xsk_db.get_record(xsk_record['rid'])
    #             print("更新后的记录:")
    #             print(f"数据名称: {updated_record.get('数据名称', 'N/A')}")
    #             print(f"来源-线索次数: {updated_record.get('来源-线索次数', 'N/A')}")
    #             print(f"来源-类型: {updated_record.get('来源-类型', 'N/A')}")
    #             print("---")

    #             fields = {
    #                 '来源-线索次数': updated_record.get('来源-线索次数'),
    #                 '来源-类型': updated_record.get('来源-类型'),
    #                 '来源-（内部）需求团队': updated_record.get('来源-（内部）需求团队')
    #             }
    #             xsk_table.update_record(rid=xsk_record['rid'], record=fields)

    #             #xsk_sync.sync()
                
    #         else:
    #             not_found_records.append(xsk_record)
    
    # # 处理未找到匹配的记录
    # for link, ais_record in links.items():
    #     # 检查这个AIS记录是否已经在XSK中
    #     found = False
    #     for xsk_record in xsk_records:
    #         if '数据链接' in xsk_record and xsk_record['数据链接'].get('link') == link:
    #             found = True
    #             break
        
    #     if not found:
    #         print(f"未找到的链接: {link}")
            
    #         new_record = {
    #             '数据名称': ais_record.get('数据名称', ''),
    #             '数据链接': ais_record.get('数据链接', {}),
    #             '数据简介': ais_record.get('数据简介',''),
    #             '一级领域': ais_record.get('一级领域',[]),
    #             '二级领域': ais_record.get('二级领域',[]),
    #             '国家/地区': ais_record.get('国家/地区',[]),
    #             '预估大小': ais_record.get('预估大小',''),
    #             '来源-（内部）需求团队': ['AI4生命科学团队'],
    #             '备注': ais_record.get('备注', ''),
    #             '来源-类型': ['内部需求-AI4S'],
    #             '来源-线索次数': 1
   
    #         }
    #         # 插入到数据库
    #         updated_record = xsk_db.update_record(new_record)
    #         # 插入到飞书多维表格

    #         xsk_table.create_record(new_record)
 
                

    # print(f"找到 {len(matching_records)} 条匹配记录")
    # print(f"新增 {len(not_found_records)} 条记录")

    

    
    


