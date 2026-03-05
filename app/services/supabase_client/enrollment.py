import logging
from typing import Any, Dict, Optional, List
import requests

from app.services.supabase_client import (
    _get_supabase_headers,
    _rest_url,
    _get_phone_number_id,
)


def upsert_enrollment_context(wa_id: str, project_name: Optional[str], context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        headers = _get_supabase_headers(True)
        prefer_value = headers.get('Prefer', '')
        if 'resolution=merge-duplicates' not in prefer_value:
            prefer_value = f"resolution=merge-duplicates,{prefer_value}" if prefer_value else 'resolution=merge-duplicates'
        if 'return=representation' not in prefer_value:
            prefer_value = f"{prefer_value},return=representation"
        headers['Prefer'] = prefer_value

        params = {'on_conflict': 'wa_id,project_name,phone_number_id'}
        payload = [{
            'wa_id': wa_id,
            'project_name': project_name,
            'context': context or {},
            'phone_number_id': _get_phone_number_id(),
        }]
        resp = requests.post(_rest_url('enrollment_contexts'), params=params, headers=headers, json=payload, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase upsert_enrollment_context failed: {resp.status_code} {resp.text}")
            return None
        data = resp.json()
        return data[0] if isinstance(data, list) and data else None
    except Exception as exc:
        logging.error(f"Supabase upsert_enrollment_context exception: {exc}", exc_info=True)
        return None


def fetch_enrollment_context(wa_id: str, project_name: Optional[str] = None) -> Dict[str, Any]:
    try:
        headers = _get_supabase_headers(True)
        params: Dict[str, str] = {'select': 'context', 'wa_id': f"eq.{wa_id}"}
        if project_name is not None:
            params['project_name'] = f"eq.{project_name}"
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.get(_rest_url('enrollment_contexts'), headers=headers, params=params, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase fetch_enrollment_context failed: {resp.status_code} {resp.text}")
            return {}
        rows = resp.json() or []
        if not rows:
            return {}
        row = rows[0]
        ctx = row.get('context') or {}
        return dict(ctx)
    except Exception as exc:
        logging.error(f"Supabase fetch_enrollment_context exception: {exc}", exc_info=True)
        return {}


def merge_enrollment_context(wa_id: str, project_name: Optional[str], partial: Dict[str, Any]) -> bool:
    try:
        current_ctx = fetch_enrollment_context(wa_id, project_name) or {}
        for k, v in (partial or {}).items():
            if v is None:
                if k in current_ctx:
                    del current_ctx[k]
            else:
                current_ctx[k] = v
        return upsert_enrollment_context(wa_id, project_name, current_ctx) is not None
    except Exception as exc:
        logging.error(f"Supabase merge_enrollment_context exception: {exc}", exc_info=True)
        return False


def clear_enrollment_context_row(wa_id: str, project_name: Optional[str]) -> bool:
    try:
        headers = _get_supabase_headers(True)
        params: Dict[str, str] = {'wa_id': f"eq.{wa_id}"}
        if project_name is not None:
            params['project_name'] = f"eq.{project_name}"
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"

        logging.info(f"🗑️ Supabase DELETE request for wa_id={wa_id}, project={project_name}, pni={pni}")
        logging.info(f"🗑️ DELETE params: {params}")

        resp = requests.delete(_rest_url('enrollment_contexts'), headers=headers, params=params, timeout=10)

        logging.info(f"🗑️ Supabase DELETE response: {resp.status_code}")
        if resp.text:
            logging.info(f"🗑️ Supabase DELETE response body: {resp.text}")

        if resp.status_code >= 300:
            logging.error(f"Supabase clear_enrollment_context_row failed: {resp.status_code} {resp.text}")
            return False

        logging.info(f"✅ Supabase DELETE successful for {wa_id}")
        return True
    except Exception as exc:
        logging.error(f"Supabase clear_enrollment_context_row exception: {exc}", exc_info=True)
        return False


__all__ = [
    'upsert_enrollment_context',
    'fetch_enrollment_context',
    'merge_enrollment_context',
    'clear_enrollment_context_row',
]


