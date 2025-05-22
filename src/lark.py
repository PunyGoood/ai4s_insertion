import os
import time
import json
import math
import logging
import asyncio
import aiohttp
import requests
import pandas as pd
import numpy as np
from typing import List, Dict, Any
from datetime import datetime
from pytz import timezone
from pymongo import errors
import dotenv

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Feishu API Credentials
APP_ID = "cli_a880af1659b0d013"
APP_SECRET = "6LRmHThxkRlfIE2HEvNvpcI7SfkdPMZ3"

# Feishu Table Configuration
LARK_TAB_ID_AIS = "tblQV9fpL1IDwu5K"
LARK_VIEW_ID_AIS = "vewOC2wJP2"
LARK_TAB_ID_XSK = "tbllob6N3JsFsPw9"
LARK_VIEW_ID_XSK = "vewnVm2Nmj"

# App Token (Testing)
APP_TOKEN = "I2vVbMSTbavpxksuqnacSKdCnqg"
# APP_TOKEN = "Z9ErbJHAhajGrbsstwScuytonQg"  # Production Table

# Token Cache
_token_cache = {
    "token": None,
    "expire_at": 0
}
_token_expire = 7200  # Default token expiration time (seconds)

# --- Data Processing Functions ---

def ipupdate(source_df: pd.DataFrame, field_types: Dict[str, str]) -> None:
    """
    Update records in Feishu table based on DataFrame input.
    
    Args:
        source_df: DataFrame containing records to update
        field_types: Dictionary mapping column names to field types
    """
    update_records = []
    
    for _, row in source_df.iterrows():
        fields = {}
        for col, val in row.items():
            ftype = field_types.get(col)
            if pd.isna(val) or (isinstance(val, (list, np.ndarray)) and len(val) == 0):
                continue
                
            if ftype == "text":
                fields[col] = str(val).strip()
            elif ftype == "url":
                fields[col] = (
                    {"text": val.get("text", "").strip(), "link": val.get("link", "").strip()}
                    if isinstance(val, dict)
                    else {"text": "", "link": str(val).strip()}
                )
            elif ftype == "multi_select":
                fields[col] = (
                    [str(x).strip() for x in val]
                    if isinstance(val, list)
                    else [x.strip() for x in str(val).split(",") if x.strip()]
                )
            elif ftype == "datetime":
                fields[col] = int(float(val) * 1000)
                
        update_records.append({"record_id": row.get("record_id"), "fields": fields})
    
    # Save records to file
    with open('update_records_output.txt', 'w', encoding='utf-8') as file:
        for record in update_records:
            file.write(f"{record}\n")
    
    # Batch update records
    tenant_access_token = get_valid_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{LARK_TAB_ID_XSK}/records/batch_update"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    
    batch_size = 1000
    success, fail = 0, 0
    
    for i in range(0, len(update_records), batch_size):
        batch = update_records[i:i + batch_size]
        payload = {"records": batch}
        resp = requests.post(url, headers=headers, json=payload)
        
        if resp.status_code == 200 and resp.json().get("code") == 0:
            success += len(batch)
            logging.info(f"Batch {i // batch_size + 1} updated successfully: {len(batch)} records")
        else:
            fail += len(batch)
            logging.error(f"Batch {i // batch_size + 1} update failed: HTTP {resp.status_code}, Response: {resp.text}")
    
    logging.info(f"Batch update completed: {success} successful, {fail} failed")

def convert_to_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert Feishu records to a pandas DataFrame.
    
    Args:
        records: List of record dictionaries
    
    Returns:
        DataFrame containing the records
    """
    try:
        logging.info("Converting records to DataFrame...")
        data = []
        
        for record in records:
            row = {"record_id": record.get('record_id')}
            fields = record.get('fields', {})
            
            for key, value in fields.items():
                if key != 'record_id':
                    row[key] = value
            data.append(row)
            
        return pd.DataFrame(data)
    except Exception as e:
        logging.error(f"Error converting to DataFrame: {str(e)}")
        return pd.DataFrame()

# --- Token Management Functions ---

def get_valid_tenant_access_token() -> str:
    """
    Get a valid tenant access token, refreshing if expired.
    
    Returns:
        Valid tenant access token
    """
    now = time.time()
    if _token_cache["token"] is None or now > _token_cache["expire_at"] - 60:
        response_token = get_tenant_access_token()
        if response_token:
            _token_cache["token"] = response_token
            _token_cache["expire_at"] = now + _token_expire
    return _token_cache["token"]

def get_tenant_access_token() -> str:
    """
    Fetch a new tenant access token from Feishu API.
    
    Returns:
        Tenant access token or None if failed
    """
    global _token_expire
    token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    token_payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(token_url, headers=headers, json=token_payload)
        token_data = response.json()
        tenant_access_token = token_data.get("tenant_access_token")
        _token_expire = token_data.get("expire", 7200)
        logging.info(f"Successfully obtained access token {tenant_access_token}, expires in {_token_expire} seconds")
        return tenant_access_token
    except Exception as e:
        logging.error(f"Error obtaining token: {str(e)}")
        return None

# --- Feishu API Interaction Functions ---

def get_bitable_datas(
    tenant_access_token: str,
    app_token: str,
    table_id: str,
    view_id: str,
    page_token: str = '',
    page_size: int = 20
) -> Dict[str, Any]:
    """
    Fetch data from Feishu bitable.
    
    Args:
        tenant_access_token: Valid access token
        app_token: Application token
        table_id: Table ID
        view_id: View ID
        page_token: Pagination token
        page_size: Number of records per page
    
    Returns:
        JSON response from Feishu API
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size={page_size}&page_token={page_token}&user_id_type=user_id"
    payload_dict = {"view_id": view_id} if view_id else {}
    payload = json.dumps(payload_dict)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {tenant_access_token}'
    }
    
    response = requests.post(url, headers=headers, data=payload)
    return response.json()

def get_records_from_table(table_id: str, view_id: str, page_size: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch all records from a Feishu table.
    
    Args:
        table_id: Table ID
        view_id: View ID
        page_size: Number of records per page
    
    Returns:
        List of records
    """
    tenant_access_token = get_valid_tenant_access_token()
    page_token = ''
    page_size = 500
    has_more = True
    feishu_datas = []
    
    while has_more:
        response = get_bitable_datas(tenant_access_token, APP_TOKEN, table_id, view_id, page_token, page_size)
        if response['code'] == 0:
            page_token = response['data'].get('page_token')
            has_more = response['data'].get('has_more')
            feishu_datas.extend(response['data'].get('items'))
        else:
            logging.error(f"Error fetching records: {response['msg']}")
            raise Exception(response['msg'])
            
    return feishu_datas

def update_bitable_record(table_id: str, record_id: str, fields: Dict[str, Any]) -> bool:
    """
    Update a single record in Feishu bitable.
    
    Args:
        table_id: Table ID
        record_id: Record ID
        fields: Fields to update
    
    Returns:
        True if update successful, False otherwise
    """
    tenant_access_token = get_valid_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records/{record_id}"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {"fields": fields}
    response = requests.put(url, headers=headers, json=payload)
    
    if response.status_code == 200 and response.json().get("code") == 0:
        logging.info(f"Successfully updated record {record_id}")
        return True
    else:
        logging.error(f"Failed to update record: {response.text}")
        return False

def create_bitable_record(table_id: str, fields: Dict[str, Any]) -> bool:
    """
    Create a new record in Feishu bitable.
    
    Args:
        table_id: Table ID
        fields: Fields for the new record
    
    Returns:
        True if creation successful, False otherwise
    """
    tenant_access_token = get_valid_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {"fields": fields}
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200 and response.json().get("code") == 0:
        logging.info("Successfully created record")
        return True
    else:
        logging.error(f"Failed to create record: {response.text}")
        return False

# --- Utility Functions ---

def sanitize_for_json(obj: Any) -> Any:
    """
    Recursively clean NaN and infinity values for JSON compatibility.
    
    Args:
        obj: Object to sanitize
    
    Returns:
        JSON-compatible object
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        logging.warning(f"Found JSON non-compliant float value: {obj}, replacing with None")
        return None
    return obj

def save_records_to_json(records: List[Dict[str, Any]], filename: str) -> None:
    """
    Save records to a JSON file.
    
    Args:
        records: List of records to save
        filename: Output file name
    """
    try:
        cleaned_records = [sanitize_for_json(record) for record in records]
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(cleaned_records, f, ensure_ascii=False, indent=2)
        logging.info(f"Records saved to {filename}")
    except Exception as e:
        logging.error(f"Failed to save JSON file: {str(e)}")

def load_records_from_json(filename: str) -> List[Dict[str, Any]]:
    """
    Load records from a JSON file.
    
    Args:
        filename: Input file name
    
    Returns:
        List of records
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            records = json.load(f)
        logging.info(f"Successfully loaded {len(records)} records from {filename}")
        return records
    except Exception as e:
        logging.error(f"Failed to load JSON file: {str(e)}")
        return []

def normalize_url(raw_link: str) -> str:
    """
    Normalize URL to standard format (https://www.*).
    
    Args:
        raw_link: Raw URL string
    
    Returns:
        Normalized URL
    """
    if raw_link.startswith(("http://", "https://", "//")):
        raw_link = raw_link.split("://")[-1] if "://" in raw_link else raw_link[2:]
    
    if raw_link.startswith("www."):
        raw_link = raw_link[4:]
    
    raw_link = raw_link.rstrip("/")
    return f"https://www.{raw_link}"

# --- Async Functions ---

async def get_valid_tenant_access_token_async() -> str:
    """
    Get a valid tenant access token asynchronously, refreshing if expired.
    
    Returns:
        Valid tenant access token
    """
    now = time.time()
    if _token_cache["token"] is None or now > _token_cache["expire_at"] - 60:
        _token_cache["token"] = await get_tenant_access_token_async()
        _token_cache["expire_at"] = now + 6000
    return _token_cache["token"]

async def get_tenant_access_token_async() -> str:
    """
    Fetch a new tenant access token asynchronously.
    
    Returns:
        Tenant access token or None if failed
    """
    token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    token_payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    headers = {"Content-Type": "application/json"}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(token_url, headers=headers, json=token_payload) as response:
                token_data = await response.json()
                tenant_access_token = token_data.get("tenant_access_token")
                logging.info(f"Successfully obtained access token {tenant_access_token}")
                return tenant_access_token
    except Exception as e:
        logging.error(f"Error obtaining token: {str(e)}")
        return None

async def get_bitable_datas_async(
    tenant_access_token: str,
    app_token: str,
    table_id: str,
    view_id: str,
    page_token: str = '',
    page_size: int = 20
) -> Dict[str, Any]:
    """
    Fetch data from Feishu bitable asynchronously.
    
    Args:
        tenant_access_token: Valid access token
        app_token: Application token
        table_id: Table ID
        view_id: View ID
        page_token: Pagination token
        page_size: Number of records per page
    
    Returns:
        JSON response from Feishu API
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size={page_size}&page_token={page_token}&user_id_type=user_id"
    payload_dict = {"view_id": view_id} if view_id else {}
    payload = json.dumps(payload_dict)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {tenant_access_token}'
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=payload) as response:
            return await response.json()

async def get_records_from_table_async(table_id: str, view_id: str, page_size: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch all records from a Feishu table asynchronously.
    
    Args:
        table_id: Table ID
        view_id: View ID
        page_size: Number of records per page
    
    Returns:
        List of records
    """
    tenant_access_token = await get_valid_tenant_access_token_async()
    page_token = ''
    page_size = 500
    has_more = True
    feishu_datas = []
    
    while has_more:
        response = await get_bitable_datas_async(tenant_access_token, APP_TOKEN, table_id, view_id, page_token, page_size)
        if response['code'] == 0:
            page_token = response['data'].get('page_token')
            has_more = response['data'].get('has_more')
            feishu_datas.extend(response['data'].get('items'))
        else:
            logging.error(f"Error fetching records: {response['msg']}")
            raise Exception(response['msg'])
            
    return feishu_datas

async def update_bitable_record_async(table_id: str, record_id: str, fields: Dict[str, Any]) -> bool:
    """
    Update a single record in Feishu bitable asynchronously.
    
    Args:
        table_id: Table ID
        record_id: Record ID
        fields: Fields to update
    
    Returns:
        True if update successful, False otherwise
    """
    tenant_access_token = await get_valid_tenant_access_token_async()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records/{record_id}"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {"fields": fields}
    
    async with aiohttp.ClientSession() as session:
        async with session.put(url, headers=headers, json=payload) as response:
            response_json = await response.json()
            
            if response.status == 200 and response_json.get("code") == 0:
                logging.info(f"Successfully updated record {record_id}")
                return True
            else:
                logging.error(f"Failed to update record: {await response.text()}")
                return False

async def create_bitable_record_async(table_id: str, fields: Dict[str, Any]) -> bool:
    """
    Create a new record in Feishu bitable asynchronously.
    
    Args:
        table_id: Table ID
        fields: Fields for the new record
    
    Returns:
        True if creation successful, False otherwise
    """
    tenant_access_token = await get_valid_tenant_access_token_async()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {"fields": fields}
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as response:
            response_json = await response.json()
            
            if response.status == 200 and response_json.get("code") == 0:
                logging.info("Successfully created record")
                return True
            else:
                logging.error(f"Failed to create record: {await response.text()}")
                return False

async def batch_update_records_async(table_id: str, update_records: List[Dict[str, Any]], batch_size: int = 1000) -> tuple[int, int]:
    """
    Batch update records in Feishu bitable asynchronously.
    
    Args:
        table_id: Table ID
        update_records: List of records to update
        batch_size: Number of records per batch
    
    Returns:
        Tuple of (successful updates, failed updates)
    """
    tenant_access_token = await get_valid_tenant_access_token_async()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records/batch_update"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    
    success, fail = 0, 0
    
    for i in range(0, len(update_records), batch_size):
        batch = update_records[i:i + batch_size]
        payload = {"records": batch}
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                response_json = await response.json()
                
                if response.status == 200 and response_json.get("code") == 0:
                    success += len(batch)
                    logging.info(f"Batch {i // batch_size + 1} updated successfully: {len(batch)} records")
                else:
                    fail += len(batch)
                    logging.error(f"Batch {i // batch_size + 1} update failed: HTTP {response.status}, Response: {await response.text()}")
    
    logging.info(f"Batch update completed: {success} successful, {fail} failed")
    return success, fail