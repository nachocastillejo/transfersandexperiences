from typing import Any, Dict, Optional
import requests
from .core import _get_supabase_headers, _rest_url


def fetch_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    if not email:
        return None
    url = _rest_url('users')
    headers = _get_supabase_headers(use_service_role=True)
    params = {
        'select': 'id,email,role,is_active,timezone,locale,system_inbound_enabled,system_enrollment_enabled,email_inbound_enabled,email_enrollment_enabled,sound_enabled,created_at,updated_at',
        'email': f'eq.{email.lower()}',
        'limit': '1'
    }
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    if not resp.ok:
        return None
    arr = resp.json() if resp.content else []
    if isinstance(arr, list) and arr:
        return arr[0]
    return None


def upsert_user_prefs_by_email(email: str, prefs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not email:
        return None

    def _coerce_bool(val: Any) -> bool:
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return val != 0
        if isinstance(val, str):
            v = val.strip().lower()
            return v in ('1','true','t','yes','y','on')
        return bool(val)

    safe: Dict[str, Any] = {}
    for k in (
        'system_inbound_enabled',
        'system_enrollment_enabled',
        'email_inbound_enabled',
        'email_enrollment_enabled',
        'sound_enabled',
        'timezone',
        'locale',
    ):
        if k in prefs:
            safe[k] = _coerce_bool(prefs[k]) if k.endswith('_enabled') else prefs[k]

    # Prefer PATCH by email (no need to provide id)
    base = _rest_url('users')
    headers = _get_supabase_headers(use_service_role=True)
    headers['Prefer'] = 'return=representation'
    params = { 'email': f'eq.{email.lower()}' }
    resp = requests.patch(base, headers=headers, params=params, json=safe, timeout=10)
    if resp.ok:
        arr = resp.json() if resp.content else []
        if isinstance(arr, list) and arr:
            return arr[0]
        return {}

    # Fallback to UPSERT on conflict(email)
    safe_with_email = dict(safe)
    safe_with_email['email'] = email.lower()
    headers = _get_supabase_headers(use_service_role=True)
    headers['Prefer'] = 'resolution=merge-duplicates,return=representation'
    resp2 = requests.post(base, headers=headers, params={'on_conflict': 'email'}, json=[safe_with_email], timeout=10)
    if not resp2.ok:
        return None
    arr2 = resp2.json() if resp2.content else []
    if isinstance(arr2, list) and arr2:
        return arr2[0]
    return {}


def list_emails_with_flag(flag_column: str) -> list[str]:
    """Return lowercased emails for active users where given boolean flag is true.
    flag_column must be one of: system_inbound_enabled, system_enrollment_enabled, email_inbound_enabled, email_enrollment_enabled.
    """
    allowed = {
        'system_inbound_enabled', 'system_enrollment_enabled',
        'email_inbound_enabled', 'email_enrollment_enabled'
    }
    if flag_column not in allowed:
        return []
    url = _rest_url('users')
    headers = _get_supabase_headers(use_service_role=True)
    params = {
        'select': 'email',
        'is_active': 'eq.true',
        flag_column: 'eq.true'
    }
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    if not resp.ok:
        return []
    items = resp.json() if resp.content else []
    emails: list[str] = []
    if isinstance(items, list):
        for it in items:
            em = (it or {}).get('email')
            if em:
                emails.append(str(em).strip().lower())
    return list(sorted(set(emails)))


