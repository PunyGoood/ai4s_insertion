import os
import time
from datetime import datetime
from pytz import timezone

import dotenv
from pymongo import errors

from .feishu_sync import FeishuBiTableSync
from .feishu_table import FeishuBiTable
from .mongodb import get_collection
from .record_db import RecordDatabase

TZ = timezone("Asia/Shanghai")
coll_seq_ids = get_collection("seq_ids")
coll_seq_ids.create_index([("name", 1)], unique=True)


def new_seq_id(name: str) -> int:
    while True:
        seq_obj = coll_seq_ids.find_one({"name": name})
        if seq_obj is None:
            try:
                coll_seq_ids.insert_one({"name": name, "seq_id": 1})
            except errors.DuplicateKeyError:
                continue
            return 1
        seq_id = seq_obj["seq_id"]
        result = coll_seq_ids.update_one(
            {"name": name, "seq_id": seq_id},
            {"$set": {"seq_id": seq_id + 1}},
        )
        if result.modified_count == 1:
            return seq_id + 1


if __name__ == "__main__":
    dotenv.load_dotenv()

    app_id = os.getenv("FEISHU_APP_ID")
    if not app_id:
        raise ValueError("FEISHU_APP_ID 未设置")

    app_secret = os.getenv("FEISHU_APP_SECRET")
    if not app_secret:
        raise ValueError("FEISHU_APP_SECRET 未设置")



    partner_url = "https://aicarrier.feishu.cn/wiki/SFrswlREwijtqDkHv6scbdWBnue?table=tblof4wKxcVqezjQ&view=vewwQgboyF"
    partner_table = FeishuBiTable(app_id, app_secret, partner_url)
    partner_db = RecordDatabase("partner")
    partner_sync = FeishuBiTableSync(partner_table, partner_db)

    xsk_url="https://aicarrier.feishu.cn/wiki/SFrswlREwijtqDkHv6scbdWBnue?table=tblGz3Ls6CCGHlJP&view=vewnVm2Nmj"
    xsk_table=FeishuBiTable(app_id, app_secret, xsk_url)
    xsk_db=RecordDatabase("xsk")
    xsk_sync=FeishuBiTableSync(xsk_table,xsk_db)
    

    xsk_sync.sync()



    partner_sync.sync()





#操作逻辑
    partner_records=partner_db.list_records()
    xsk_records = xsk_db.list_records()

    links = []
    for record in partner_records:
        if '数据链接' in record:
            link = record['数据链接'].get('link')
            if link:
                links.append(link)

    print(links)
    
    matching_records = []
    not_found_records = []
    for xsk_record in xsk_records:
        if '数据链接' in xsk_record:
            xsk_link = xsk_record['数据链接'].get('link')
            if xsk_link in links:
                matching_records.append(xsk_record)

    print(f"找到 {len(matching_records)} 条匹配记录")
    for record in matching_records:
        print(f"数据名称: {record.get('数据名称', 'N/A')}")
        print(f"链接: {record.get('数据链接', {}).get('link', 'N/A')}")
        print("---")


    for xsk_record in xsk_records:
        if '数据链接' in xsk_record:
            xsk_link = xsk_record['数据链接'].get('link')
            if xsk_link in links:
                matching_records.append(xsk_record)
                # 更新线索次数
                current_count = xsk_record.get('来源-线索次数', 0)
                xsk_db.update_record(xsk_record['rid'], {
                    '来源-线索次数': current_count + 1,
                    '线索来源类型': links[xsk_link].get('线索来源类型', '未知')
                })
                # 更新飞书多维表格
                xsk_table.update_record(xsk_record['rid'], {
                    '来源-线索次数': current_count + 1,
                    '线索来源类型': links[xsk_link].get('线索来源类型', '未知')
                })
            else:
                not_found_records.append(xsk_record)

    # 处理未找到的记录
    for link, partner_record in links.items():
        found = False
        for xsk_record in xsk_records:
            if xsk_record.get('数据链接', {}).get('link') == link:
                found = True
                break
        
        if not found:
            # 创建新记录
            new_record = {
                '数据名称': partner_record.get('数据名称', ''),
                '数据链接': partner_record.get('数据链接', {}),
                '数据存储方': partner_record.get('数据存储方', ''),
                '线索来源类型': partner_record.get('线索来源类型', '未知'),
                '来源-线索次数': 1
            }
            # 插入到数据库
            record_id = xsk_db.insert_record(new_record)
            # 插入到飞书多维表格
            xsk_table.insert_record(new_record)

    print(f"找到 {len(matching_records)} 条匹配记录")
    print(f"新增 {len(not_found_records)} 条记录")

    

    
    


