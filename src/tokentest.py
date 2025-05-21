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

    


    ais_records=get_records_from_table(LARK_TAB_ID_ais, LARK_VIEW_ID_ais)
    xsk_records = get_records_from_table(LARK_TAB_ID_xsk, LARK_VIEW_ID_xsk)


    
    save_records_to_json(ais_records, 'ais_records.json')
    save_records_to_json(xsk_records, 'xsk_records.json')

    ais_records = load_records_from_json('ais_records.json')
    xsk_records = load_records_from_json('xsk_records.json')

    links = {}
    processed_links = set()

