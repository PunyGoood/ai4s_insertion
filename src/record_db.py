import copy
import hashlib
import json
import time
import uuid
from typing import List

from pymongo import ASCENDING, errors

from .feishu_consts import META_FIELD_NAMES
from .mongodb import get_collection


class VerMismatchError(Exception):
    pass


class RecordDatabase:
    def __init__(self, coll_name: str):
        ver_coll_name = f"ver_{coll_name}"
        self.coll = get_collection(coll_name)
        self.ver_coll = get_collection(ver_coll_name)
        self.coll.create_index([("rid", ASCENDING)], unique=True)
        self.ver_coll.create_index([("rid", ASCENDING), ("ver", ASCENDING)], unique=True)

    @staticmethod
    def calc_hash(record: dict) -> str:
        keys = [k for k in record.keys() if k not in META_FIELD_NAMES]
        keys.sort()
        record = {k: record[k] for k in keys if record[k] is not None}
        record_str = json.dumps(record, ensure_ascii=False)
        return hashlib.md5(record_str.encode("utf-8")).hexdigest()

    @staticmethod
    def temp_rid():
        temp_ts = hex(int(time.time()))[2:]
        temp_rand = str(uuid.uuid4())[:8]
        return f"temp_{temp_ts}{temp_rand}"

    @staticmethod
    def is_temp_rid(rid: str) -> bool:
        return rid.startswith("temp_")

    def list_records(self) -> List[dict]:
        """列出数据库记录"""
        lst = []
        for record in self.coll.find():
            record.pop("_id", None)
            lst.append(record)
        return lst

    def get_record(self, rid: str) -> dict | None:
        """获取数据库记录"""
        record = self.coll.find_one({"rid": rid})
        if record is not None:
            record.pop("_id", None)
        return record

    def get_ver_record(self, rid: str, ver: int) -> dict:
        """获取数据库记录历史"""
        assert ver >= 0
        if ver == 0:
            return {"rid": rid, "ver": ver}
        record = self.ver_coll.find_one({"rid": rid, "ver": ver})
        assert record is not None
        record.pop("_id", None)
        return record

    def update_record(self, record: dict, hash=None) -> dict:
        """更新数据库, 返回更新后的记录"""
        record = copy.deepcopy(record)
        rid = record.get("rid") or self.temp_rid()
        ver = record.get("ver") or 0
        record["rid"] = rid
        record["ver"] = ver + 1
        record["hash"] = hash or self.calc_hash(record)

        if ver == 0:
            try:  # insert only when record not exists.
                self.coll.insert_one(record)
            except errors.DuplicateKeyError:
                raise VerMismatchError(f"记录已存在: {rid}::{ver}")
        else:  # update only when record not changed.
            result = self.coll.update_one({"rid": rid, "ver": ver}, {"$set": record})
            if result.modified_count == 0:
                raise VerMismatchError(f"记录已过期: {rid}::{ver}")

        self.ver_coll.update_one(
            {"rid": rid, "ver": record["ver"]},
            {"$set": record},
            upsert=True,
        )

        return record

    def change_rid(self, old_rid: str, new_rid: str):
        self.coll.update_many({"rid": old_rid}, {"$set": {"rid": new_rid}})
        self.ver_coll.update_many({"rid": old_rid}, {"$set": {"rid": new_rid}})
