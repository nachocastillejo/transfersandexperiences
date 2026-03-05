import logging
from typing import List, Tuple, Optional
import requests

from app.services.supabase_client import (
    _get_supabase_headers,
    _rest_url,
)


def fetch_status_definitions() -> List[str]:
    try:
        headers = _get_supabase_headers(True)
        params = {'select': 'name', 'order': 'name.asc'}
        resp = requests.get(_rest_url('status_definitions'), headers=headers, params=params, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase fetch_status_definitions failed: {resp.status_code} {resp.text}")
            return []
        rows = resp.json() or []
        return [r.get('name') for r in rows if r.get('name')]
    except Exception as exc:
        logging.error(f"Supabase fetch_status_definitions exception: {exc}", exc_info=True)
        return []


def create_status_definition(name: str) -> Tuple[bool, Optional[str]]:
    try:
        headers = _get_supabase_headers(True)
        payload = {'name': name}
        resp = requests.post(_rest_url('status_definitions'), headers=headers, json=payload, timeout=15)
        if resp.status_code >= 300:
            if resp.status_code == 409:
                try:
                    body = resp.json() or {}
                    code = str(body.get('code') or '')
                    message = str(body.get('message') or '').lower()
                    if code == '23505' or 'duplicate key' in message or 'already exists' in message:
                        return True, name
                except Exception:
                    return True, name
            logging.error(f"Supabase create_status_definition failed: {resp.status_code} {resp.text}")
            return False, None
        data = resp.json()
        row = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})
        return True, (row.get('name') if isinstance(row, dict) else name)
    except Exception as exc:
        logging.error(f"Supabase create_status_definition exception: {exc}", exc_info=True)
        return False, None


def delete_status_definition(name: str) -> bool:
    try:
        headers = _get_supabase_headers(True)
        params = {'name': f"eq.{name}"}
        resp = requests.delete(_rest_url('status_definitions'), headers=headers, params=params, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase delete_status_definition failed: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as exc:
        logging.error(f"Supabase delete_status_definition exception: {exc}", exc_info=True)
        return False


__all__ = [
    'fetch_status_definitions',
    'create_status_definition',
    'delete_status_definition',
]


