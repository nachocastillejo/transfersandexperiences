import logging
from typing import Any, Dict, List
import requests

from app.services.supabase_client import (
    _get_supabase_headers,
    _rest_url,
    _build_prefix_tsquery,
    _get_phone_number_id,
    _to_local_datetime,
)


def search_messages(query: str, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
    """Search messages via Supabase REST by text or sender_name (case-insensitive)."""
    try:
        headers = _get_supabase_headers(True)
        pni = _get_phone_number_id()

        like_query = f"%{query}%"
        params = {
            'select': '*',
            'or': f'(message_text.ilike.{like_query},sender_name.ilike.{like_query})',
            'order': 'created_at.desc',
            'limit': str(limit),
            'offset': str(max(0, int(offset))),
        }
        if pni:
            params['phone_number_id'] = f"eq.{pni}"

        resp = requests.get(_rest_url('messages'), headers=headers, params=params, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase search_messages failed: {resp.status_code} {resp.text}")
            return []

        rows = resp.json() or []
        messages: List[Dict[str, Any]] = []
        for r in rows:
            created_at = r.get('created_at')
            ts_dt = _to_local_datetime(created_at)
            ts_str = ts_dt.strftime('%Y-%m-%d %H:%M:%S') if ts_dt else None
            r['timestamp'] = ts_str
            messages.append(r)
        return messages
    except Exception as exc:
        logging.error(f"Supabase search_messages exception: {exc}", exc_info=True)
        return []


def search_messages_text_only(query: str, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
    """Search messages only by message_text using FTS fallbacks and ILIKE."""
    try:
        headers = _get_supabase_headers(True)
        pni = _get_phone_number_id()

        def _map_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            for r in rows or []:
                created_at = r.get('created_at')
                ts_dt = _to_local_datetime(created_at)
                ts_str = ts_dt.strftime('%Y-%m-%d %H:%M:%S') if ts_dt else None
                r['timestamp'] = ts_str
                out.append(r)
            return out

        # Prefer FTS with prefix operators
        ts_prefix = _build_prefix_tsquery(query)
        params_fts = {
            'select': '*',
            'order': 'created_at.desc',
            'limit': str(limit),
            'offset': str(max(0, int(offset))),
            'message_text': f"fts(spanish).{ts_prefix}" if ts_prefix else f"wfts(spanish).{query}",
        }
        if pni:
            params_fts['phone_number_id'] = f"eq.{pni}"
        resp = requests.get(_rest_url('messages'), headers=headers, params=params_fts, timeout=15)
        if resp.status_code == 200:
            rows = resp.json() or []
            if rows:
                return _map_rows(rows)

        # Fallback to other FTS variants
        for op in ("plfts", "phfts", "wfts"):
            params_alt = dict(params_fts)
            params_alt['message_text'] = f"{op}(spanish).{query}"
            resp_alt = requests.get(_rest_url('messages'), headers=headers, params=params_alt, timeout=15)
            if resp_alt.status_code == 200:
                rows_alt = resp_alt.json() or []
                if rows_alt:
                    return _map_rows(rows_alt)

        # Fallback to ILIKE
        like_query = f"%{query}%"
        params = {
            'select': '*',
            'order': 'created_at.desc',
            'limit': str(limit),
            'offset': str(max(0, int(offset))),
            'message_text': f'ilike.{like_query}',
        }
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp3 = requests.get(_rest_url('messages'), headers=headers, params=params, timeout=15)
        if resp3.status_code >= 300:
            logging.error(f"Supabase search_messages_text_only failed: {resp3.status_code} {resp3.text}")
            return []
        return _map_rows(resp3.json() or [])
    except Exception as exc:
        logging.error(f"Supabase search_messages_text_only exception: {exc}", exc_info=True)
        return []


def search_users(query: str, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    """Search distinct wa_id by sender_name or wa_id and return grouped results."""
    try:
        headers = _get_supabase_headers(True)
        pni = _get_phone_number_id()

        like_query = f"%{query}%"
        page_size = 500
        fetch_offset = 0
        wa_id_to_entry: Dict[str, Dict[str, Any]] = {}

        def _maybe_update_entry(row: Dict[str, Any]):
            wa = row.get('wa_id')
            if not wa:
                return
            created_at = row.get('created_at')
            ts_dt = _to_local_datetime(created_at)
            ts_str = ts_dt.strftime('%Y-%m-%d %H:%M:%S') if ts_dt else None
            existing = wa_id_to_entry.get(wa)
            if existing is None:
                wa_id_to_entry[wa] = {
                    'wa_id': wa,
                    'sender_name': wa,
                    'last_ts': ts_str
                }
            else:
                try:
                    if ts_str and (existing.get('last_ts') or '') < ts_str:
                        existing['last_ts'] = ts_str
                except Exception:
                    pass
                try:
                    direction = str(row.get('direction') or '')
                    name_val = row.get('sender_name')
                    if name_val and direction == 'inbound':
                        existing['sender_name'] = name_val
                except Exception:
                    pass

        while True:
            params: Dict[str, str] = {
                'select': ','.join(['id', 'created_at', 'sender_name', 'wa_id', 'direction']),
                'order': 'created_at.desc',
                'limit': str(page_size),
                'offset': str(fetch_offset),
            }
            if pni:
                params['phone_number_id'] = f"eq.{pni}"

            ts_prefix = _build_prefix_tsquery(query)
            or_fts = f"sender_name.fts(spanish).{ts_prefix}" if ts_prefix else f"sender_name.wfts(spanish).{query}"
            params['or'] = f"({or_fts},wa_id.ilike.{like_query})"
            resp = requests.get(_rest_url('messages'), headers=headers, params=params, timeout=15)
            if resp.status_code >= 300:
                params_fallback = dict(params)
                params_fallback['or'] = f"(sender_name.ilike.{like_query},wa_id.ilike.{like_query})"
                resp = requests.get(_rest_url('messages'), headers=headers, params=params_fallback, timeout=15)
                if resp.status_code >= 300:
                    logging.error(f"Supabase search_users failed: {resp.status_code} {resp.text}")
                    break

            rows = resp.json() or []
            if not rows:
                break
            for r in rows:
                _maybe_update_entry(r)
            fetch_offset += len(rows)
            if len(wa_id_to_entry) >= (offset + limit):
                break
            if len(rows) < page_size:
                break

        entries = list(wa_id_to_entry.values())
        try:
            entries.sort(key=lambda e: (e.get('last_ts') is None, e.get('last_ts')), reverse=True)
        except Exception:
            pass
        return entries[offset:offset+limit]
    except Exception as exc:
        logging.error(f"Supabase search_users exception: {exc}", exc_info=True)
        return []


def fetch_sender_name_map_for_wa_ids(wa_ids: List[str]) -> Dict[str, str]:
    """Return { wa_id: latest inbound sender_name } using Supabase messages."""
    try:
        headers = _get_supabase_headers(True)
        pni = _get_phone_number_id()
        out: Dict[str, str] = {}
        wlist = [w for w in (wa_ids or []) if w]
        if not wlist:
            return out

        def _in_list(vals):
            quoted = ','.join(f'"{v}"' for v in vals)
            return f'({quoted})'

        params_in: Dict[str, str] = {
            'select': 'wa_id,sender_name,created_at',
            'wa_id': f'in.{_in_list(wlist)}',
            'direction': 'eq.inbound',
            'sender_name': 'not.is.null',
            'order': 'created_at.desc'
        }
        if pni:
            params_in['phone_number_id'] = f"eq.{pni}"
        resp_in = requests.get(_rest_url('messages'), headers=headers, params=params_in, timeout=20)
        if resp_in.status_code == 200:
            for r in resp_in.json() or []:
                wa = r.get('wa_id')
                nm = r.get('sender_name')
                if wa and nm and wa not in out:
                    out[wa] = nm
        return out
    except Exception as exc:
        logging.error(f"Supabase fetch_sender_name_map_for_wa_ids exception: {exc}", exc_info=True)
        return {}


__all__ = [
    'search_messages',
    'search_messages_text_only',
    'search_users',
    'fetch_sender_name_map_for_wa_ids',
]


