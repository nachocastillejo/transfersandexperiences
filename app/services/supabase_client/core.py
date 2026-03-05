import os
import logging
import re
from typing import Dict, Optional
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

try:
    from flask import current_app as _flask_current_app  # type: ignore
except Exception:  # pragma: no cover
    _flask_current_app = None


def is_supabase_enabled() -> bool:
    value = os.getenv('SUPABASE_ON', 'False')
    is_on = str(value).lower() in ('1', 'true', 'yes')
    if not is_on:
        return False
    # Extra safety: ensure basic keys are present if supposedly enabled
    return bool(os.getenv('SUPABASE_URL') and (os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY')))


def _get_supabase_headers(use_service_role: bool = True) -> Dict[str, str]:
    api_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') if use_service_role else os.getenv('SUPABASE_ANON_KEY')
    if not api_key:
        raise RuntimeError('Missing Supabase API key environment variable')
    return {
        'apikey': api_key,
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'Prefer': 'return=representation',
    }


def _rest_url(table: str) -> str:
    url = os.getenv('SUPABASE_URL')
    if not url:
        raise RuntimeError('Missing SUPABASE_URL environment variable')
    return f"{url}/rest/v1/{table}"


def _rpc_url(function_name: str) -> str:
    url = os.getenv('SUPABASE_URL')
    if not url:
        raise RuntimeError('Missing SUPABASE_URL environment variable')
    return f"{url}/rest/v1/rpc/{function_name}"


def _build_prefix_tsquery(q: str) -> str:
    """Build Spanish-friendly tsquery with prefix operators for partial matching."""
    try:
        tokens = re.findall(r"[\wáéíóúüñç]+", q or "")
        tokens = [t.strip() for t in tokens if len(t.strip()) >= 2]
        if not tokens:
            return ""
        return " & ".join(f"{t}:*" for t in tokens)
    except Exception:
        return ""


def _get_phone_number_id() -> Optional[str]:
    """Return configured phone number id from Flask config or environment."""
    try:
        if _flask_current_app:
            v = _flask_current_app.config.get('PHONE_NUMBER_ID')
            if v:
                return str(v)
    except Exception:
        pass
    v = os.getenv('PHONE_NUMBER_ID')
    return str(v) if v else None


def _to_local_datetime(iso_ts: Optional[str]) -> Optional[datetime]:
    if not isinstance(iso_ts, str):
        return None
    try:
        # Normalize fractional seconds to 6 digits for Python's fromisoformat
        match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.?([\d]*)?([Z\+\-].*)', iso_ts)
        if not match:
            logging.error(f"SUPABASE_PARSE_ERROR: Could not parse timestamp with regex: {iso_ts}")
            return None

        main_part, fractional_part, tz_part = match.groups()
        fractional_part = (fractional_part or '').ljust(6, '0')[:6]
        fixed_iso_ts = f"{main_part}.{fractional_part}{tz_part}".replace('Z', '+00:00')

        aware_dt = datetime.fromisoformat(fixed_iso_ts)
        if aware_dt.tzinfo is None:
            aware_dt = aware_dt.replace(tzinfo=timezone.utc)

        madrid_tz = ZoneInfo('Europe/Madrid')
        local_dt = aware_dt.astimezone(madrid_tz)
        return local_dt
    except Exception as e:
        logging.error(f"SUPABASE_PARSE_ERROR: Failed to parse timestamp '{iso_ts}'. Error: {e}", exc_info=True)
        return None


__all__ = [
    'is_supabase_enabled',
    '_get_supabase_headers',
    '_rest_url',
    '_rpc_url',
    '_build_prefix_tsquery',
    '_get_phone_number_id',
    '_to_local_datetime',
]


