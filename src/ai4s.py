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


    

    df_ais = convert_to_dataframe( ais_records)
    df_ais.to_csv('ais_records.csv', index=False, encoding='utf-8-sig')

    df_xsk = convert_to_dataframe(xsk_records)
    df_xsk.to_csv('xsk_records.csv', index=False, encoding='utf-8-sig')

    df_ais = pd.read_csv('ais_records.csv', encoding='utf-8')
    df_xsk = pd.read_csv('xsk_records.csv', encoding='utf-8')

    ais_records = df_ais.to_dict('records')
    xsk_records = df_xsk.to_dict('records')



    links = {}
    #print(type(links))
    for record in ais_records:
        if record.get('数据链接') and '内部需求-AI4S' in (record.get('线索来源类型', []) or []):
            link = record['数据链接']
        
            parsed_dict = ast.literal_eval(link)
        # 提取实际的链接
            actual_link = parsed_dict['link']
            print(actual_link)
            if link:
                links[actual_link] = record

    matching_records = []
    not_found_records = []

    print("准备打印")

    for _, xsk_record in df_xsk.iterrows():
        xsk_link = xsk_record['数据链接']
        try:
            # 处理数据链接可能是字符串形式的字典
            if isinstance(xsk_link, str):
                parsed_link = ast.literal_eval(xsk_link)
                if isinstance(parsed_link, dict):
                    link = parsed_link.get('link', '')
                elif isinstance(parsed_link, list) and len(parsed_link) > 0:
                    link = parsed_link[0].get('link', '')
            else:
                link = xsk_link.get('link', '') if isinstance(xsk_link, dict) else ''
            
            print(link)
            if link and link in links:
                print(1)
                matching_records.append(xsk_record.to_dict())
                
        except (ValueError, SyntaxError) as e:
            logging.error(f"解析数据链接时出错: {str(e)}, 链接值: {xsk_link}")
            continue
            
    
    print(links)

    print(f"找到 {len(matching_records)} 条匹配记录")
    for record in matching_records:
        print(f"数据名称: {record.get('数据名称', 'N/A')}")
        print(f"链接: {record.get('数据链接', 'N/A')}")
        print("---")





    for xsk_record in xsk_records:
        if '数据链接' in xsk_record:
            xsk_link = xsk_record['数据链接'] 
            if isinstance(xsk_link, str):
                parsed_link = ast.literal_eval(xsk_link)
                if isinstance(parsed_link, dict):
                    link = parsed_link.get('link', '')
                elif isinstance(parsed_link, list) and len(parsed_link) > 0:
                    link = parsed_link[0].get('link', '')
            else:
                link = xsk_link.get('link', '') if isinstance(xsk_link, dict) else ''
            
            print(link)
            if link and link in links:
                print(1)
                
                matching_records.append(xsk_record)

                #更新线索次数
                current_count = xsk_record.get('来源-线索次数', 0)
                existing_type = xsk_record.get('来源-类型', [])
                new_type = existing_type if '内部需求-AI4S' in existing_type else ['内部需求-AI4S']

                print(new_type)
                print(type(new_type))
                # 更新记录

                xsk_record.update({
                    '来源-线索次数': current_count + 1,
                    '来源-类型': ast.literal_eval(new_type),
                    '来源-（内部）需求团队': ['AI4生命科学团队']
                })

                print("更新后的记录:")
                print(xsk_record)
                print(f"数据名称: {xsk_record.get('数据名称', 'N/A')}")
                print(f"来源-线索次数: {xsk_record.get('来源-线索次数', 'N/A')}")
                print(f"来源-类型: {xsk_record.get('来源-类型', 'N/A')}")
                print("---")

                fields = {
                    '来源-线索次数': xsk_record.get('来源-线索次数'),
                    '来源-类型': xsk_record.get('来源-类型'),
                    '来源-（内部）需求团队': ['AI4生命科学团队']
                }
                update_bitable_record(LARK_TAB_ID_xsk, xsk_record['record_id'], fields)


                    
            else:
                not_found_records.append(xsk_record)



# 处理未找到匹配的记录
    # for link, ais_record in links.items():
    # # 检查这个AIS记录是否已经在XSK中
    #     found = False
    #     for xsk_record in xsk_records:
    #         if '数据链接' in xsk_record:
    #             xsk_link = xsk_record['数据链接']
    #             # 处理数据链接可能是字典或字符串的情况
    #             if isinstance(xsk_link, dict):
    #                 xsk_link = xsk_link.get('link', '')
    #             elif isinstance(xsk_link, str):
    #                 xsk_link = xsk_link.strip()
                
    #             if xsk_link == link:
    #                 found = True
    #                 break
        
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
            
            

    #         create_bitable_record(LARK_TAB_ID_xsk, new_record)










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

    

    
    


