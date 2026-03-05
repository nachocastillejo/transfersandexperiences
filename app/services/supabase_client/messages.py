import logging
from typing import Any, Dict, Optional, List
import requests
from zoneinfo import ZoneInfo

from app.services.supabase_client import (
    _get_supabase_headers,
    _rest_url,
    _get_phone_number_id,
    _to_local_datetime,
)


def insert_message(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        headers = _get_supabase_headers(True)
        pni = _get_phone_number_id()
        if pni and 'phone_number_id' not in record:
            record = {**record, 'phone_number_id': pni}
        # Media fields are now supported in the messages table
        resp = requests.post(_rest_url('messages'), headers=headers, json=[record], timeout=10)
        if resp.status_code >= 300:
            try:
                text = resp.text.lower()
            except Exception:
                text = ''
            if ('column' in text and 'model' in text) or ('unknown column' in text and 'model' in text):
                record_compat = {k: v for k, v in record.items() if k != 'model'}
                logging.warning("Retrying insert_message without 'model' field (server column missing)")
                resp2 = requests.post(_rest_url('messages'), headers=headers, json=[record_compat], timeout=10)
                if resp2.status_code >= 300:
                    logging.error(f"Supabase insert_message retry failed: {resp2.status_code} {resp2.text}")
                    return None
                data2 = resp2.json()
                return data2[0] if isinstance(data2, list) and data2 else None
            if ('column' in text and 'response_id' in text) or ('unknown column' in text and 'response_id' in text):
                record_compat = {k: v for k, v in record.items() if k != 'response_id'}
                logging.warning("Retrying insert_message without 'response_id' field (server column missing)")
                resp2 = requests.post(_rest_url('messages'), headers=headers, json=[record_compat], timeout=10)
                if resp2.status_code >= 300:
                    logging.error(f"Supabase insert_message retry (no response_id) failed: {resp2.status_code} {resp2.text}")
                    return None
                data2 = resp2.json()
                return data2[0] if isinstance(data2, list) and data2 else None
            logging.error(f"Supabase insert_message failed: {resp.status_code} {resp.text}")
            return None
        data = resp.json()
        return data[0] if isinstance(data, list) and data else None
    except Exception as exc:
        logging.error(f"Supabase insert_message exception: {exc}", exc_info=True)
        return None


def fetch_messages_for_conversation(wa_id: str, limit: int = 50, before_timestamp: str = None, after_timestamp: str = None, target_message_id: str = None) -> List[Dict[str, Any]]:
    def _map_row(r: Dict[str, Any]) -> Dict[str, Any]:
        created_at = r.get('created_at')
        ts_dt = _to_local_datetime(created_at)
        ts_str = ts_dt.strftime('%Y-%m-%d %H:%M:%S') if ts_dt else None
        return {
            'db_id': r.get('id'),
            'timestamp': ts_str,
            'wa_id': r.get('wa_id'),
            'sender_name': r.get('sender_name'),
            'message_text': r.get('message_text'),
            'direction': r.get('direction'),
            'proyecto': r.get('project_name'),
            'model': r.get('model'),
            'status': r.get('status'),
            'response_time_seconds': r.get('response_time_seconds'),
            'attempt_count': r.get('attempt_count'),
            'required_action': r.get('required_action'),
            'error_message': r.get('error_message'),
            'message_id': r.get('whatsapp_message_id') or (str(r.get('id')) if r.get('id') is not None else None),
            'media_type': r.get('media_type'),
            'media_url': r.get('media_url'),
            'media_filename': r.get('media_filename'),
            'media_mime_type': r.get('media_mime_type'),
            'media_size_bytes': r.get('media_size_bytes'),
        }

    try:
        headers = _get_supabase_headers(True)
        pni = _get_phone_number_id()
        wa_id_alt = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"

        if target_message_id:
            target_params: Dict[str, str] = {
                'select': ','.join([
                    'id', 'created_at', 'project_name', 'sender_name', 'wa_id', 'direction',
                    'message_text', 'model', 'whatsapp_message_id', 'status', 'response_time_seconds',
                    'attempt_count', 'required_action', 'error_message',
                    'media_type', 'media_url', 'media_filename', 'media_mime_type', 'media_size_bytes'
                ]),
                'wa_id': f"in.({wa_id},{wa_id_alt})",
                'limit': '1'
            }
            or_parts = [f"whatsapp_message_id.eq.{target_message_id}"]
            if str(target_message_id).isdigit():
                or_parts.append(f"id.eq.{target_message_id}")
            target_params['or'] = f"({','.join(or_parts)})"

            if pni:
                target_params['phone_number_id'] = f"eq.{pni}"
            target_resp = requests.get(_rest_url('messages'), headers=headers, params=target_params, timeout=15)
            if target_resp.status_code >= 300:
                logging.error(f"Supabase fetch target message failed: {target_resp.status_code} {target_resp.text}")
                return []
            target_rows = target_resp.json() or []
            if not target_rows:
                target_message_id = None
            else:
                target_row = target_rows[0]
                target_created_at = target_row.get('created_at')

                half = max(1, int(limit) // 2)
                # Filter to exclude pending/failed template messages
                status_filter = '(status.is.null,status.in.(sent,delivered,read,ignored_paused))'
                before_params: Dict[str, str] = {
                    'select': target_params['select'],
                    'wa_id': target_params['wa_id'],
                    'created_at': f"lt.{target_created_at}",
                    'order': 'created_at.desc',
                    'limit': str(half),
                    'or': status_filter,
                }
                if pni:
                    before_params['phone_number_id'] = f"eq.{pni}"
                before_resp = requests.get(_rest_url('messages'), headers=headers, params=before_params, timeout=15)
                if before_resp.status_code >= 300:
                    logging.error(f"Supabase fetch before window failed: {before_resp.status_code} {before_resp.text}")
                    before_rows: List[Dict[str, Any]] = []
                else:
                    before_rows = before_resp.json() or []

                after_params: Dict[str, str] = {
                    'select': target_params['select'],
                    'wa_id': target_params['wa_id'],
                    'created_at': f"gte.{target_created_at}",
                    'order': 'created_at.asc',
                    'limit': str(half),
                    'or': status_filter,
                }
                if pni:
                    after_params['phone_number_id'] = f"eq.{pni}"
                after_resp = requests.get(_rest_url('messages'), headers=headers, params=after_params, timeout=15)
                if after_resp.status_code >= 300:
                    logging.error(f"Supabase fetch after window failed: {after_resp.status_code} {after_resp.text}")
                    after_rows: List[Dict[str, Any]] = []
                else:
                    after_rows = after_resp.json() or []

                combined: List[Dict[str, Any]] = []
                seen_ids = set()
                for r in reversed(before_rows):
                    rid = r.get('id')
                    if rid in seen_ids:
                        continue
                    seen_ids.add(rid)
                    combined.append(_map_row(r))
                for r in after_rows:
                    rid = r.get('id')
                    if rid in seen_ids:
                        continue
                    seen_ids.add(rid)
                    combined.append(_map_row(r))
                return combined

        params = {
            'select': ','.join([
                'id', 'created_at', 'project_name', 'sender_name', 'wa_id', 'direction',
                'message_text', 'model', 'whatsapp_message_id', 'status', 'response_time_seconds',
                'attempt_count', 'required_action', 'error_message',
                'media_type', 'media_url', 'media_filename', 'media_mime_type', 'media_size_bytes'
            ]),
            'wa_id': f"in.({wa_id},{wa_id_alt})",
            'limit': str(limit),
            # Exclude pending/failed template messages - allow null (inbound) or confirmed statuses
            'or': '(status.is.null,status.in.(sent,delivered,read,ignored_paused))',
        }

        if before_timestamp:
            try:
                from datetime import datetime
                local_dt = datetime.strptime(before_timestamp, '%Y-%m-%d %H:%M:%S')
                madrid_tz = ZoneInfo('Europe/Madrid')
                utc_dt = local_dt.replace(tzinfo=madrid_tz).astimezone(ZoneInfo('UTC'))
                utc_str = utc_dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                params['created_at'] = f"lt.{utc_str}"
                params['order'] = 'created_at.desc'
            except Exception:
                params['order'] = 'created_at.desc'
        elif after_timestamp:
            try:
                from datetime import datetime
                local_dt = datetime.strptime(after_timestamp, '%Y-%m-%d %H:%M:%S')
                madrid_tz = ZoneInfo('Europe/Madrid')
                utc_dt = local_dt.replace(tzinfo=madrid_tz).astimezone(ZoneInfo('UTC'))
                utc_str = utc_dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                params['created_at'] = f"gt.{utc_str}"
                params['order'] = 'created_at.asc'
            except Exception:
                params['order'] = 'created_at.desc'
        else:
            params['order'] = 'created_at.desc'

        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.get(_rest_url('messages'), headers=headers, params=params, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase fetch_messages_for_conversation failed: {resp.status_code} {resp.text}")
            return []

        rows = resp.json() or []
        if params.get('order', '').endswith('desc'):
            return [_map_row(r) for r in reversed(rows)]
        return [_map_row(r) for r in rows]
    except Exception as exc:
        logging.error(f"Supabase fetch_messages_for_conversation exception: {exc}", exc_info=True)
        return []


def fetch_messages(limit: int = 1000) -> List[Dict[str, Any]]:
    try:
        headers = _get_supabase_headers(True)
        all_messages: List[Dict[str, Any]] = []
        page_size = 1000
        offset = 0
        while len(all_messages) < limit:
            current_limit = min(page_size, limit - len(all_messages))
            params = {
                'select': ','.join([
                    'id', 'created_at', 'project_name', 'sender_name', 'wa_id', 'direction',
                    'message_text', 'model', 'whatsapp_message_id', 'status', 'response_time_seconds',
                    'attempt_count', 'required_action', 'error_message'
                ]),
                'order': 'created_at.desc',
                'limit': str(current_limit),
                'offset': str(offset),
            }
            pni = _get_phone_number_id()
            if pni:
                params['phone_number_id'] = f"eq.{pni}"
            resp = requests.get(_rest_url('messages'), headers=headers, params=params, timeout=15)
            if resp.status_code >= 300:
                logging.error(f"Supabase fetch_messages failed: {resp.status_code} {resp.text}")
                break
            rows = resp.json() or []
            if not rows:
                break
            for r in reversed(rows):
                created_at = r.get('created_at')
                ts_dt = _to_local_datetime(created_at)
                ts_str = ts_dt.strftime('%Y-%m-%d %H:%M:%S') if ts_dt else None
                all_messages.append({
                    'db_id': r.get('id'),
                    'timestamp': ts_str,
                    'wa_id': r.get('wa_id'),
                    'sender_name': r.get('sender_name'),
                    'message_text': r.get('message_text'),
                    'direction': r.get('direction'),
                    'proyecto': r.get('project_name'),
                    'model': r.get('model'),
                    'status': r.get('status'),
                    'response_time_seconds': r.get('response_time_seconds'),
                    'attempt_count': r.get('attempt_count'),
                    'required_action': r.get('required_action'),
                    'error_message': r.get('error_message'),
                    'message_id': r.get('whatsapp_message_id') if str(r.get('direction') or '').startswith('outbound') else None,
                })
            offset += len(rows)
            if len(rows) < page_size:
                break
        return all_messages
    except Exception as exc:
        logging.error(f"Supabase fetch_messages exception: {exc}", exc_info=True)
        return []


def update_message_status_by_wamid(whatsapp_message_id: str, new_status: str, error_message: str = None) -> bool:
    """Update message status by WhatsApp message ID. Returns True if a row was updated."""
    try:
        headers = _get_supabase_headers(True)
        # Use Prefer: return=representation to get the updated row back
        headers['Prefer'] = 'return=representation'
        params = {'whatsapp_message_id': f"eq.{whatsapp_message_id}"}
        payload = {'status': new_status}
        if error_message:
            payload['error_message'] = error_message
        resp = requests.patch(_rest_url('messages'), params=params, headers=headers, json=payload, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase update_message_status failed: {resp.status_code} {resp.text}")
            return False
        # Check if any row was actually updated
        data = resp.json()
        return isinstance(data, list) and len(data) > 0
    except Exception as exc:
        logging.error(f"Supabase update_message_status exception: {exc}", exc_info=True)
        return False


def fetch_message_statuses_by_wamids(whatsapp_message_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch status info for multiple messages by their WhatsApp message IDs.
    Returns dict: { wamid: {'status': str, 'error_message': str|None} }
    """
    result: Dict[str, Dict[str, Any]] = {}
    if not whatsapp_message_ids:
        return result
    try:
        headers = _get_supabase_headers(True)
        # Build IN clause for the message IDs
        ids_str = ','.join(whatsapp_message_ids)
        params = {
            'select': 'whatsapp_message_id,status,error_message',
            'whatsapp_message_id': f"in.({ids_str})",
        }
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.get(_rest_url('messages'), headers=headers, params=params, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase fetch_message_statuses_by_wamids failed: {resp.status_code} {resp.text}")
            return result
        rows = resp.json() or []
        for r in rows:
            wamid = r.get('whatsapp_message_id')
            if wamid:
                result[wamid] = {
                    'status': r.get('status'),
                    'error_message': r.get('error_message'),
                }
        return result
    except Exception as exc:
        logging.error(f"Supabase fetch_message_statuses_by_wamids exception: {exc}", exc_info=True)
        return result


def fetch_last_inbound_timestamp(wa_id: str):
    try:
        headers = _get_supabase_headers(True)
        wa_id_alt = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"
        params = {
            'select': 'created_at',
            'wa_id': f"in.({wa_id},{wa_id_alt})",
            'direction': 'eq.inbound',
            'order': 'created_at.desc',
            'limit': '1',
        }
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.get(_rest_url('messages'), headers=headers, params=params, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"SUPABASE_FETCH_24H: Supabase fetch_last_inbound_timestamp failed: {resp.status_code} {resp.text}")
            return None
        rows = resp.json() or []
        if not rows:
            logging.warning(f"SUPABASE_FETCH_24H: No inbound message found for wa_id in ('{wa_id}', '{wa_id_alt}')")
            return None
        created_at = rows[0].get('created_at')
        local_dt = _to_local_datetime(created_at)
        return local_dt
    except Exception as exc:
        logging.error(f"SUPABASE_FETCH_24H: Supabase fetch_last_inbound_timestamp exception: {exc}", exc_info=True)
        return None


def fetch_message_by_wamid(whatsapp_message_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch a single message by its WhatsApp message ID (wamid).
    Used to retrieve the original message content when a user replies to it.
    Returns the message record or None if not found.
    """
    if not whatsapp_message_id:
        return None
    try:
        headers = _get_supabase_headers(True)
        params = {
            'select': ','.join([
                'id', 'created_at', 'project_name', 'sender_name', 'wa_id', 'direction',
                'message_text', 'model', 'whatsapp_message_id', 'status', 'response_time_seconds',
                'attempt_count', 'required_action', 'error_message',
                'media_type', 'media_url', 'media_filename', 'media_mime_type', 'media_size_bytes'
            ]),
            'whatsapp_message_id': f"eq.{whatsapp_message_id}",
            'limit': '1',
        }
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.get(_rest_url('messages'), headers=headers, params=params, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase fetch_message_by_wamid failed: {resp.status_code} {resp.text}")
            return None
        rows = resp.json() or []
        if not rows:
            return None
        r = rows[0]
        created_at = r.get('created_at')
        ts_dt = _to_local_datetime(created_at)
        ts_str = ts_dt.strftime('%Y-%m-%d %H:%M:%S') if ts_dt else None
        return {
            'db_id': r.get('id'),
            'timestamp': ts_str,
            'wa_id': r.get('wa_id'),
            'sender_name': r.get('sender_name'),
            'message_text': r.get('message_text'),
            'direction': r.get('direction'),
            'proyecto': r.get('project_name'),
            'model': r.get('model'),
            'status': r.get('status'),
            'whatsapp_message_id': r.get('whatsapp_message_id'),
            'media_type': r.get('media_type'),
            'media_url': r.get('media_url'),
        }
    except Exception as exc:
        logging.error(f"Supabase fetch_message_by_wamid exception: {exc}", exc_info=True)
        return None


__all__ = [
    'insert_message',
    'fetch_messages_for_conversation',
    'fetch_messages',
    'update_message_status_by_wamid',
    'fetch_last_inbound_timestamp',
    'fetch_message_statuses_by_wamids',
    'fetch_message_by_wamid',
]


def fetch_messages_for_wa(wa_id: str, limit: int = 100, before_local_ts: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch recent messages for a specific wa_id from Supabase REST (DESC order)."""
    try:
        headers = _get_supabase_headers(True)
        wa_id_alt = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"

        params: Dict[str, str] = {
            'select': ','.join([
                'id', 'created_at', 'project_name', 'sender_name', 'wa_id', 'direction',
                'message_text', 'model', 'whatsapp_message_id', 'status', 'response_time_seconds',
                'attempt_count', 'required_action', 'error_message'
            ]),
            'wa_id': f"in.({wa_id},{wa_id_alt})",
            'order': 'created_at.desc',
            'limit': str(limit),
            # Exclude pending/failed template messages
            'or': '(status.is.null,status.in.(sent,delivered,read,ignored_paused))',
        }

        if before_local_ts:
            try:
                from datetime import datetime, timezone
                madrid_tz = ZoneInfo('Europe/Madrid')
                dt_local = datetime.fromisoformat(before_local_ts.replace(' ', 'T'))
                if dt_local.tzinfo is None:
                    dt_local = dt_local.replace(tzinfo=madrid_tz)
                dt_utc = dt_local.astimezone(timezone.utc)
                before_iso = dt_utc.isoformat().replace('+00:00', 'Z')
                params['created_at'] = f"lt.{before_iso}"
            except Exception:
                pass

        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.get(_rest_url('messages'), headers=headers, params=params, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase fetch_messages_for_wa failed: {resp.status_code} {resp.text}")
            return []
        rows = resp.json() or []

        messages: List[Dict[str, Any]] = []
        for r in rows:
            created_at = r.get('created_at')
            ts_dt = _to_local_datetime(created_at)
            ts_str = ts_dt.strftime('%Y-%m-%d %H:%M:%S') if ts_dt else None
            direction = r.get('direction') or ''
            messages.append({
                'db_id': r.get('id'),
                'timestamp': ts_str,
                'wa_id': r.get('wa_id'),
                'sender_name': r.get('sender_name'),
                'message_text': r.get('message_text'),
                'direction': direction,
                'proyecto': r.get('project_name'),
                'model': r.get('model'),
                'status': r.get('status'),
                'response_time_seconds': r.get('response_time_seconds'),
                'attempt_count': r.get('attempt_count'),
                'required_action': r.get('required_action'),
                'error_message': r.get('error_message'),
                'message_id': r.get('whatsapp_message_id') if str(direction).startswith('outbound') else None,
            })
        return messages
    except Exception as exc:
        logging.error(f"Supabase fetch_messages_for_wa exception: {exc}", exc_info=True)
        return []



