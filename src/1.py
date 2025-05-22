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
import asyncio
import aiohttp
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
# 假设这些函数也有异步版本，如果没有，需要创建
from .lark import(
    get_records_from_table_async,
    update_bitable_record_async
)

logging.basicConfig(level=logging.DEBUG)

from .linshi import get_records_from_table_1,get_records_from_table_2,update_tmp_record_status,update_all_tmp_records,extract_domain
    
#https://aicarrier.feishu.cn/wiki/SFrswlREwijtqDkHv6scbdWBnue?table=tblGz3Ls6CCGHlJP&view=vewTRw3wmL
LARK_TAB_ID_xsk="tblGz3Ls6CCGHlJP"
LARK_VIEW_ID_xsk="vewTRw3wmL"

#https://aicarrier.feishu.cn/wiki/ON7uwDVWaiC74LklNcbcoafqnMi?table=tbl5Ny0ghsOxDJcN&view=vewfUuYgQL
LARK_TAB_ID_tmp="tbl5Ny0ghsOxDJcN"
LARK_VIEW_ID_tmp="vewfUuYgQL"

app_token_1="Z9ErbJHAhajGrbsstwScuytonQg"
app_token_2="MIR0bNf1Eao09mswwMwcVrYJndd"

tmp_records=get_records_from_table_2(LARK_TAB_ID_tmp, LARK_VIEW_ID_tmp)