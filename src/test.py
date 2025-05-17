import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import dotenv
from pymongo import errors

from .feishu_sync import FeishuBiTableSync
from .feishu_table import FeishuBiTable
from .mongodb import get_collection
from .record_db import RecordDatabase

TZ = ZoneInfo("Asia/Shanghai")

coll_seq_ids = get_collection("seq_ids")
coll_seq_ids.create_index([("name", 1)], unique=True)


def new_seq_id() -> int:
    result = coll_seq_ids.find_one_and_update(
        {"_id": "global_counter"},  # 使用固定ID
        {"$inc": {"seq_id": 1}},    # 自增
        upsert=True,                # 如果不存在则创建
        return_document=True        # 返回更新后的文档
    )
    return result["seq_id"]


if __name__ == "__main__":
    dotenv.load_dotenv()

    app_id = os.getenv("FEISHU_APP_ID")
    if not app_id:
        raise ValueError("FEISHU_APP_ID 未设置")

    app_secret = os.getenv("FEISHU_APP_SECRET")
    if not app_secret:
        raise ValueError("FEISHU_APP_SECRET 未设置")

    # test_url = "https://aicarrier.feishu.cn/wiki/SFrswlREwijtqDkHv6scbdWBnue?table=tblGz3Ls6CCGHlJP&view=vewnVm2Nmj"
    # test_url = "https://aicarrier.feishu.cn/wiki/VkSPwfjmViWachkzlR9cps1bn1e?table=tbl8XA0T63COIqkZ&view=veweTAhDU4"

    xsk_url = "https://aicarrier.feishu.cn/wiki/VIxfwyHTviqQOrkz5uGcadAHnKf?table=tblT4mvSK8yxkEBw&view=vewnVm2Nmj"
    xsk_table = FeishuBiTable(app_id, app_secret, xsk_url)
    xsk_db = RecordDatabase("records")
    xsk_sync = FeishuBiTableSync(xsk_table, xsk_db)

    collect_url = "https://aicarrier.feishu.cn/wiki/TQ5Ewrc6QiAxCkk7LwScEZwGnkb?table=tbl5ztY8vnq7cH7g&view=vewdpbtgF0"
    collect_table = FeishuBiTable(app_id, app_secret, collect_url)
    collect_db = RecordDatabase("collect")
    collect_sync = FeishuBiTableSync(collect_table, collect_db)

    # path, task_time, task_status, start_time, end_time, file_count, file_size, message
    coll_scan_tasks = get_collection("scan_tasks")

    while True:
        begin = time.time()

        xsk_sync.sync()
        print(f"xsk_sync: {time.time() - begin}")
        time.sleep(3)

        collect_sync.sync()
        print(f"collect_sync: {time.time() - begin}")
        time.sleep(3)

        xsk_records = xsk_db.list_records()
        xsk_record_by_link = {}
        for record in xsk_records:
            if not record.get("数据ID"):
                ctime = record["ctime"] / 1000
                local_date = datetime.fromtimestamp(ctime, TZ)
                date_id = local_date.strftime("%Y%m")
                seq_id = new_seq_id()
                record["数据ID"] = f"xsk_{date_id}{seq_id:06d}"
                xsk_db.update_record(record)
                print(".", end="", flush=True)

            data_link_link = (record.get("数据链接") or {}).get("link")
            data_link = (record.get("数据链接") or {}).get("text")
            xsk_record_by_link[data_link] = record

        collect_records = collect_db.list_records()
        for collect_record in collect_records:
            # no need in final program
            data_id = collect_record.get("线索ID")
            data_link = collect_record.get("链接")
            if not data_id and data_link and data_link in xsk_record_by_link:
                data_id = xsk_record_by_link[data_link]["数据ID"]
                collect_record["线索ID"] = data_id
                collect_db.update_record(collect_record)
                print(",", end="", flush=True)

            data_path = collect_record.get("入库地址")
            scan_time = collect_record.get("扫描时间") or 0
            now = int(time.time() * 1000)
            TWO_DAYS = 2 * 24 * 60 * 60 * 1000
            if data_path and now - scan_time > TWO_DAYS:
                # find scan task with maximum time
                scan_task = coll_scan_tasks.find_one({"path": data_path}, sort=[("task_time", -1)])
                if not scan_task or now - scan_task["task_time"] > TWO_DAYS:
                    coll_scan_tasks.insert_one({"path": data_path, "task_time": now, "task_status": "pending"})

                # scanner will update scan task status
                # TODO: write table when status changed.
        time.sleep(5)


# scanner checks new scan tasks
# start scan task in bigdata cluster
# get scan result and write to db
