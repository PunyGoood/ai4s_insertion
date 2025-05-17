from typing import Dict

from .feishu_consts import FIELD_TYPE, META_FIELD_NAMES, META_FIELDS
from .feishu_table import FeishuBiTable
from .record_db import RecordDatabase, VerMismatchError


class FeishuBiTableSync:
    def __init__(self, bi_table: FeishuBiTable, db: RecordDatabase):
        self.bi_table = bi_table
        self.bi_fields = self._check_bi_fields()
        self.db = db
        self._update_buffer = []

    def _check_bi_fields(self):
        """检查多维表格列"""
        fields = self.bi_table.list_fields()
        field_names = set(f["field_name"] for f in fields)
        has_created = False
        for field in META_FIELDS:
            if field["field_name"] in field_names:
                continue
            self.bi_table.create_field(field)
            has_created = True
        if has_created:
            fields = self.bi_table.list_fields()
        return {f["field_name"]: f for f in fields}

    def _parse_record(self, record: dict) -> dict:
        fields = record.get("fields") or {}
        output = {}
        for name, value in fields.items():
            field = self.bi_fields.get(name)
            if field is None:
                raise RuntimeError(f"多维表格列不存在: {name}")
            typ = field["type"]
            if typ == FIELD_TYPE.FORMULA:
                assert isinstance(value, dict)
                typ = value["type"]
                value = value["value"]
            if typ == FIELD_TYPE.TEXT:
                # special case:
                # 英为财情（Investing.com）
                # [{'text': '英为财情（', 'type': 'text'}, {'link': 'https://investing.com/', 'text': 'Investing.com', 'type': 'url'}, {'text': '）', 'type': 'text'}]
                assert isinstance(value, list)
                output[name] = "".join([v["text"] for v in value])
            elif typ in (
                FIELD_TYPE.NUMBER,
                FIELD_TYPE.SINGLE_SELECT,
                FIELD_TYPE.MULTI_SELECT,
                FIELD_TYPE.URL,
                FIELD_TYPE.PHONE,
                FIELD_TYPE.TIME,
                FIELD_TYPE.BOOL,
                FIELD_TYPE.CTIME,
                FIELD_TYPE.MTIME,
            ):
                output[name] = value
            elif typ in (
                FIELD_TYPE.USER,
                FIELD_TYPE.CUSER,
                FIELD_TYPE.MUSER,
            ):
                assert isinstance(value, list)
                output[name] = [{"id": v["id"]} for v in value]
            # else:
            #     print("ignore field: ", typ, value)
        return output

    def _list_bi_records(self) -> Dict[str, dict]:
        # DEBUG
        import time

        begin = time.time()

        records = self.bi_table.list_records()
        _records = [self._parse_record(r) for r in records]
        try:
            return {r["rid"]: r for r in _records}
        except Exception as e:
            print(records)
            print(_records)
            raise e
        finally:
            print(f"list_bi_records: {time.time() - begin}")

    @staticmethod
    def _record_diff(new: dict, old: dict) -> dict:
        keys = set([*new.keys(), *old.keys()])
        keys = [k for k in keys if k not in META_FIELD_NAMES]
        diff = {}
        for key in keys:
            old_value = old.get(key)
            new_value = new.get(key)
            # fmt: off
            if old_value != new_value and not (
                isinstance(old_value, float) and
                isinstance(new_value, float) and
                abs(old_value - new_value) < 1e-6
            ):  # fmt: on
                diff[key] = new_value
        return diff

    def _update_bitable(self, rid: str, record: dict):
        """更新飞书多维表格"""
        print(f"更新飞书多维表格: {rid} {record}")
        # TODO: if record contains readony field, pop them.
        self._update_buffer.append((rid, record))
        if len(self._update_buffer) >= 100:
            self._flush_updates()

    def _flush_updates(self):
        if len(self._update_buffer) > 0:
            self.bi_table.batch_update_records(self._update_buffer)
            self._update_buffer.clear()

    def _try_update_db_record(self, record: dict, new_hash: str):
        rid = record["rid"]
        db_record = self.db.get_record(rid)
        if db_record is None:
            record["ver"] = 0  # reset version.
            updated = self.db.update_record(record, new_hash)
            record["ver"], record["hash"] = updated["ver"], updated["hash"]
            self._update_bitable(rid, {"ver": updated["ver"], "hash": updated["hash"]})
            return

        if new_hash == record["hash"]:
            return  # stop force resync if record is not changed.

        # correct version if needed.
        if record["ver"] < 0 or record["ver"] > db_record["ver"]:
            record["ver"] = db_record["ver"]

        # get updated record.
        history_record = self.db.get_ver_record(rid, record["ver"])
        record_diff = self._record_diff(record, history_record)
        updated = self.db.update_record({**db_record, **record_diff})  # ignore new_hash

        # update bitable record.
        u_diff = self._record_diff(updated, record)
        u_diff["ver"], u_diff["hash"] = updated["ver"], updated["hash"]
        record.update(u_diff)
        self._update_bitable(rid, u_diff)

    def _update_db_record(self, record: dict, new_hash: str):
        while True:
            try:
                self._try_update_db_record(record, new_hash)
                break
            except VerMismatchError:
                pass

    def sync(self, force_resync: bool = False):
        bi_records = self._list_bi_records()
        for rid, record in bi_records.items():
            record["ver"] = record.get("ver") or 0
            record["hash"] = record.get("hash")
            new_hash = self.db.calc_hash(record)
            if force_resync or new_hash != record["hash"]:
                self._update_db_record(record, new_hash)
        self._flush_updates()

        db_records = self.db.list_records()
        for db_record in db_records:
            rid = db_record.get("rid")
            if not rid:
                continue
            if self.db.is_temp_rid(rid):
                rid = self.bi_table.create_record({})
                self.db.change_rid(db_record["rid"], rid)
                u_diff = self._record_diff(db_record, {})
                u_diff["ver"], u_diff["hash"] = db_record["ver"], db_record["hash"]
                self._update_bitable(rid, u_diff)
                continue

            record = bi_records.get(rid)
            if record is None:
                continue  # record is deleted from bitable.

            if (record["ver"], record["hash"]) == (db_record["ver"], db_record["hash"]):
                continue  # record is not changed.

            # update bitable record.
            u_diff = self._record_diff(db_record, record)
            u_diff["ver"], u_diff["hash"] = db_record["ver"], db_record["hash"]
            record.update(u_diff)
            self._update_bitable(rid, u_diff)

        self._flush_updates()
    
    
