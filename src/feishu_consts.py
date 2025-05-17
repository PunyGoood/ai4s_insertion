from enum import IntEnum

# 17：附件
# 18：单向关联
# 21：双向关联
# 22：地理位置
# 23：群组
# 24：流程
# 3001：按钮


class FIELD_TYPE(IntEnum):
    TEXT = 1
    NUMBER = 2
    SINGLE_SELECT = 3
    MULTI_SELECT = 4
    TIME = 5
    BOOL = 7
    USER = 11
    PHONE = 13
    URL = 15
    LOOKUP = 19  # read-only
    FORMULA = 20  # read-only
    CTIME = 1001  # read-only
    MTIME = 1002  # read-only
    CUSER = 1003  # read-only
    MUSER = 1004  # read-only
    AUTO_SEQ = 1005  # read-only


def is_readonly_type(field_type: FIELD_TYPE) -> bool:
    return field_type in [
        FIELD_TYPE.LOOKUP,
        FIELD_TYPE.FORMULA,
        FIELD_TYPE.CTIME,
        FIELD_TYPE.MTIME,
        FIELD_TYPE.CUSER,
        FIELD_TYPE.MUSER,
        FIELD_TYPE.AUTO_SEQ,
    ]


META_FIELDS = [
    {
        "field_name": "rid",
        "property": {
            "formatter": "",
            "formula_expression": "RECORD_ID()",
        },
        "type": FIELD_TYPE.FORMULA,
        "ui_type": "Formula",
    },
    {
        "field_name": "ver",
        "property": {
            "formatter": "0",
        },
        "type": FIELD_TYPE.NUMBER,
        "ui_type": "Number",
    },
    {
        "field_name": "hash",
        "type": FIELD_TYPE.TEXT,
        "ui_type": "Text",
    },
    {
        "field_name": "ctime",
        "property": {
            "date_formatter": "yyyy/MM/dd",
        },
        "type": FIELD_TYPE.CTIME,
        "ui_type": "CreatedTime",
    },
    {
        "field_name": "mtime",
        "property": {
            "date_formatter": "yyyy/MM/dd",
        },
        "type": FIELD_TYPE.MTIME,
        "ui_type": "ModifiedTime",
    },
]

META_FIELD_NAMES = set(f["field_name"] for f in META_FIELDS)
