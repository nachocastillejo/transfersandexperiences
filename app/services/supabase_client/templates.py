import logging
from typing import Any, Dict, Optional, List
import requests
from flask import current_app

from app.services.supabase_client import (
    _get_supabase_headers,
    _rest_url,
)


def _get_phone_number_id() -> Optional[str]:
    """Get the current PHONE_NUMBER_ID from config."""
    try:
        return current_app.config.get('PHONE_NUMBER_ID')
    except Exception:
        return None


def insert_template_submission(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        headers = _get_supabase_headers(True)
        
        # Add phone_number_id if not already present
        if 'phone_number_id' not in record:
            phone_id = _get_phone_number_id()
            if phone_id:
                record['phone_number_id'] = phone_id
        
        # record needs to be a list for bulk insert, but here we insert one row
        resp = requests.post(_rest_url('template_submissions'), headers=headers, json=[record], timeout=10)
        
        if resp.status_code >= 300:
            logging.error(f"Supabase insert_template_submission failed: {resp.status_code} {resp.text}")
            return None
            
        data = resp.json()
        return data[0] if isinstance(data, list) and data else None
    except Exception as exc:
        logging.error(f"Supabase insert_template_submission exception: {exc}", exc_info=True)
        return None


def update_template_submission(submission_id: str, updates: Dict[str, Any]) -> bool:
    try:
        headers = _get_supabase_headers(True)
        params = {'id': f"eq.{submission_id}"}
        resp = requests.patch(_rest_url('template_submissions'), headers=headers, params=params, json=updates, timeout=10)
        
        if resp.status_code >= 300:
            logging.error(f"Supabase update_template_submission failed: {resp.status_code} {resp.text}")
            return False
            
        return True
    except Exception as exc:
        logging.error(f"Supabase update_template_submission exception: {exc}", exc_info=True)
        return False


def fetch_template_submissions(limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetch recent template submissions from Supabase, filtered by phone_number_id."""
    try:
        headers = _get_supabase_headers(True)
        params = {
            'select': 'id,created_at,template_name,language,category,total_recipients,successful_count,failed_count,sent_by,raw_data,phone_number_id,duration_seconds,estimated_cost',
            'order': 'created_at.desc',
            'limit': str(limit),
            'offset': str(offset),
        }
        
        # Filter by phone_number_id if available
        phone_id = _get_phone_number_id()
        if phone_id:
            params['phone_number_id'] = f"eq.{phone_id}"
        
        resp = requests.get(_rest_url('template_submissions'), headers=headers, params=params, timeout=10)
        
        if resp.status_code >= 300:
            logging.error(f"Supabase fetch_template_submissions failed: {resp.status_code} {resp.text}")
            return []
            
        return resp.json() or []
    except Exception as exc:
        logging.error(f"Supabase fetch_template_submissions exception: {exc}", exc_info=True)
        return []
