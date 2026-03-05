import logging
import os
from typing import Optional, Dict
import requests

try:
    from flask import current_app as _flask_current_app  # type: ignore
except Exception:  # pragma: no cover
    _flask_current_app = None

from app.services.supabase_client import (
    is_supabase_enabled,
    _get_supabase_headers,
    _rest_url,
    _get_phone_number_id,
)


def update_previous_response_id(wa_id: str, response_id: str):
    """Update the previous_response_id for a given conversation."""
    if not is_supabase_enabled():
        return
    try:
        headers = _get_supabase_headers()
        wa_id_alt = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"
        params: Dict[str, str] = {'wa_id': f'in.({wa_id},{wa_id_alt})'}
        pni = _get_phone_number_id()
        if not pni:
            logging.warning("update_previous_response_id skipped: PHONE_NUMBER_ID not configured; avoiding cross-environment update")
            return
        params['phone_number_id'] = f'eq.{pni}'
        try:
            project_name = None
            if _flask_current_app:
                project_name = _flask_current_app.config.get('ENV_NAME')
            if not project_name:
                project_name = os.getenv('ENV_NAME')
            if project_name:
                params['project_name'] = f'eq.{project_name}'
        except Exception:
            pass
        payload = {'previous_response_id': response_id}
        resp = requests.patch(_rest_url('conversations'), headers=headers, params=params, json=payload, timeout=10)
        if resp.status_code in (200, 204):
            logging.info(f"Successfully updated previous_response_id for wa_id: {wa_id}")
        else:
            logging.error(f"Failed to update previous_response_id for wa_id: {wa_id}. Status: {resp.status_code}, Response: {resp.text}")
    except Exception as exc:
        logging.error(f"Exception while updating previous_response_id for wa_id: {wa_id}: {exc}", exc_info=True)


def get_previous_response_id(wa_id: str) -> Optional[str]:
    """Fetch the previous_response_id for a given conversation from Supabase."""
    if not is_supabase_enabled():
        return None
    try:
        headers = _get_supabase_headers()
        wa_id_alt = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"
        params: Dict[str, str] = {'wa_id': f'in.({wa_id},{wa_id_alt})', 'select': 'previous_response_id', 'limit': '1'}
        pni = _get_phone_number_id()
        if not pni:
            logging.warning("get_previous_response_id skipped: PHONE_NUMBER_ID not configured; avoiding cross-environment fetch")
            return None
        params['phone_number_id'] = f'eq.{pni}'
        try:
            project_name = None
            if _flask_current_app:
                project_name = _flask_current_app.config.get('ENV_NAME')
            if not project_name:
                project_name = os.getenv('ENV_NAME')
            if project_name:
                params['project_name'] = f'eq.{project_name}'
        except Exception:
            pass
        resp = requests.get(_rest_url('conversations'), headers=headers, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json() or []
            if data and isinstance(data[0], dict):
                return data[0].get('previous_response_id')
        else:
            logging.error(f"Failed to fetch previous_response_id for {wa_id}. Status: {resp.status_code}, Response: {resp.text}")
    except Exception as exc:
        logging.error(f"Exception fetching previous_response_id for {wa_id}: {exc}", exc_info=True)
    return None


def clear_previous_response_id(wa_id: str) -> bool:
    """Set conversations.previous_response_id to NULL for the given wa_id scoped to this environment."""
    if not is_supabase_enabled():
        return False
    try:
        headers = _get_supabase_headers()
        wa_id_alt = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"
        params: Dict[str, str] = {'wa_id': f'in.({wa_id},{wa_id_alt})'}
        pni = _get_phone_number_id()
        if not pni:
            logging.warning("clear_previous_response_id skipped: PHONE_NUMBER_ID not configured; avoiding cross-environment update")
            return False
        params['phone_number_id'] = f'eq.{pni}'
        try:
            project_name = None
            if _flask_current_app:
                project_name = _flask_current_app.config.get('ENV_NAME')
            if not project_name:
                project_name = os.getenv('ENV_NAME')
            if project_name:
                params['project_name'] = f'eq.{project_name}'
        except Exception:
            pass
        payload = {'previous_response_id': None}
        resp = requests.patch(_rest_url('conversations'), headers=headers, params=params, json=payload, timeout=10)
        if resp.status_code in (200, 204):
            logging.info(f"Cleared previous_response_id in Supabase for wa_id: {wa_id}")
            return True
        logging.error(f"Failed to clear previous_response_id for wa_id {wa_id}: {resp.status_code} {resp.text}")
        return False
    except Exception as exc:
        logging.error(f"Exception clearing previous_response_id for wa_id {wa_id}: {exc}", exc_info=True)
        return False


__all__ = [
    'update_previous_response_id',
    'get_previous_response_id',
    'clear_previous_response_id',
]


