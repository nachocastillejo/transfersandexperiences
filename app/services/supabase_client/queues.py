from typing import Any, Dict, List, Optional
import logging
import requests

from .core import _get_supabase_headers, _rest_url, _get_phone_number_id


def list_queues() -> List[Dict[str, Any]]:
    try:
        headers = _get_supabase_headers(True)
        params: Dict[str, str] = {'select': '*', 'order': 'created_at.desc'}
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.get(_rest_url('queues'), headers=headers, params=params, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase list_queues failed: {resp.status_code} {resp.text}")
            return []
        return resp.json() or []
    except Exception as exc:
        logging.error(f"Supabase list_queues exception: {exc}", exc_info=True)
        return []


def list_queues_for_email(email: str) -> List[Dict[str, Any]]:
    """Return queues where the email is a member (joins in two requests)."""
    try:
        email_norm = (email or '').lower().strip()
        if not email_norm:
            return []
        headers = _get_supabase_headers(True)
        # First fetch queue_ids for this email
        params_members = {'select': 'queue_id', 'email': f"eq.{email_norm}"}
        resp_m = requests.get(_rest_url('queue_members'), headers=headers, params=params_members, timeout=15)
        if resp_m.status_code >= 300:
            logging.error(f"Supabase list_queues_for_email members failed: {resp_m.status_code} {resp_m.text}")
            return []
        rows = resp_m.json() or []
        queue_ids = [r.get('queue_id') for r in rows if r.get('queue_id')]
        if not queue_ids:
            return []
        # Fetch queues by IDs
        params_q: Dict[str, str] = {'select': '*', 'id': 'in.(' + ','.join(queue_ids) + ')'}
        resp_q = requests.get(_rest_url('queues'), headers=headers, params=params_q, timeout=15)
        if resp_q.status_code >= 300:
            logging.error(f"Supabase list_queues_for_email queues failed: {resp_q.status_code} {resp_q.text}")
            return []
        return resp_q.json() or []
    except Exception as exc:
        logging.error(f"Supabase list_queues_for_email exception: {exc}", exc_info=True)
        return []


def create_queue(name: str, mode: Optional[str], statuses: List[str], created_by: Optional[str], attention: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Create a new queue with a single mode (bot or agent)."""
    try:
        headers = _get_supabase_headers(True)
        # Validate mode
        valid_mode = mode if mode in ('bot', 'agent') else 'bot'
        payload = {
            'name': name,
            'mode': valid_mode,
            'statuses': statuses or [],
            'phone_number_id': _get_phone_number_id(),
            'created_by': created_by,
        }
        if attention in ('needs', 'attended'):
            payload['attention'] = attention
        resp = requests.post(_rest_url('queues'), headers=headers, json=[payload], timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase create_queue failed: {resp.status_code} {resp.text}")
            return None
        data = resp.json()
        return data[0] if isinstance(data, list) and data else None
    except Exception as exc:
        logging.error(f"Supabase create_queue exception: {exc}", exc_info=True)
        return None


def update_queue(queue_id: str, name: Optional[str] = None, mode: Optional[str] = None, statuses: Optional[List[str]] = None, attention: Optional[str] = None) -> bool:
    """Update a queue. Mode must be 'bot' or 'agent' if provided."""
    try:
        headers = _get_supabase_headers(True)
        payload: Dict[str, Any] = {}
        if name is not None:
            payload['name'] = name
        if mode is not None and mode in ('bot', 'agent'):
            payload['mode'] = mode
        if statuses is not None:
            payload['statuses'] = statuses
        if attention in ('needs', 'attended'):
            payload['attention'] = attention
        if not payload:
            return True
        params = {'id': f"eq.{queue_id}"}
        resp = requests.patch(_rest_url('queues'), headers=headers, params=params, json=payload, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase update_queue failed: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as exc:
        logging.error(f"Supabase update_queue exception: {exc}", exc_info=True)
        return False


def get_queue(queue_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a single queue by ID."""
    try:
        headers = _get_supabase_headers(True)
        params = {'select': '*', 'id': f"eq.{queue_id}"}
        resp = requests.get(_rest_url('queues'), headers=headers, params=params, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase get_queue failed: {resp.status_code} {resp.text}")
            return None
        data = resp.json() or []
        return data[0] if data else None
    except Exception as exc:
        logging.error(f"Supabase get_queue exception: {exc}", exc_info=True)
        return None


def delete_queue(queue_id: str) -> bool:
    try:
        headers = _get_supabase_headers(True)
        params = {'id': f"eq.{queue_id}"}
        resp = requests.delete(_rest_url('queues'), headers=headers, params=params, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase delete_queue failed: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as exc:
        logging.error(f"Supabase delete_queue exception: {exc}", exc_info=True)
        return False


def list_queue_members(queue_id: str) -> List[str]:
    try:
        headers = _get_supabase_headers(True)
        params = {'select': 'email', 'queue_id': f"eq.{queue_id}"}
        resp = requests.get(_rest_url('queue_members'), headers=headers, params=params, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase list_queue_members failed: {resp.status_code} {resp.text}")
            return []
        rows = resp.json() or []
        return [r.get('email') for r in rows if r.get('email')]
    except Exception as exc:
        logging.error(f"Supabase list_queue_members exception: {exc}", exc_info=True)
        return []


def add_member_to_queue(queue_id: str, email: str) -> bool:
    try:
        headers = _get_supabase_headers(True)
        payload = [{'queue_id': queue_id, 'email': (email or '').lower().strip()}]
        resp = requests.post(_rest_url('queue_members'), headers=headers, json=payload, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase add_member_to_queue failed: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as exc:
        logging.error(f"Supabase add_member_to_queue exception: {exc}", exc_info=True)
        return False


def remove_member_from_queue(queue_id: str, email: str) -> bool:
    try:
        headers = _get_supabase_headers(True)
        params = {
            'queue_id': f"eq.{queue_id}",
            'email': f"eq.{(email or '').lower().strip()}"
        }
        resp = requests.delete(_rest_url('queue_members'), headers=headers, params=params, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase remove_member_from_queue failed: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as exc:
        logging.error(f"Supabase remove_member_from_queue exception: {exc}", exc_info=True)
        return False



