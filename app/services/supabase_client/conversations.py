import logging
from typing import Any, Dict, List, Optional
import requests

from app.services.supabase_client import (
    _get_supabase_headers,
    _rest_url,
    _rpc_url,
    _get_phone_number_id,
    _to_local_datetime,
)

BATCH_SIZE = 50


def upsert_conversation(wa_id: str, project_name: Optional[str], last_message_text: Optional[str], last_direction: Optional[str]) -> Optional[Dict[str, Any]]:
    try:
        # Note: Do NOT include last_message_at here - it's managed by a Supabase trigger
        # that fires after message inserts (trg_sync_conversation_last_message)
        payload = {
            'wa_id': wa_id,
            'project_name': project_name,
            'last_message_text': last_message_text,
            'last_direction': last_direction,
            'phone_number_id': _get_phone_number_id(),
        }
        headers = _get_supabase_headers(True)
        prefer_value = headers.get('Prefer', '')
        if 'resolution=merge-duplicates' not in prefer_value:
            prefer_value = f"resolution=merge-duplicates,{prefer_value}" if prefer_value else 'resolution=merge-duplicates'
        if 'return=representation' not in prefer_value:
            prefer_value = f"{prefer_value},return=representation"
        headers['Prefer'] = prefer_value
        params = {'on_conflict': 'wa_id,project_name,phone_number_id'}
        resp = requests.post(_rest_url('conversations'), params=params, headers=headers, json=[payload], timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase upsert_conversation failed: {resp.status_code} {resp.text}")
            return None
        data = resp.json()
        return data[0] if isinstance(data, list) and data else None
    except Exception as exc:
        logging.error(f"Supabase upsert_conversation exception: {exc}", exc_info=True)
        return None


def fetch_all_conversations() -> List[Dict[str, Any]]:
    try:
        headers = _get_supabase_headers(True)
        all_conversations: List[Dict[str, Any]] = []
        seen_wa_ids = set()
        page_size = 1000
        offset = 0
        while True:
            params = {'select': 'wa_id', 'order': 'created_at.desc', 'limit': str(page_size), 'offset': str(offset)}
            pni = _get_phone_number_id()
            if pni:
                params['phone_number_id'] = f"eq.{pni}"
            else:
                logging.warning("No phone_number_id configured, returning empty conversations list")
                return []
            resp = requests.get(_rest_url('messages'), headers=headers, params=params, timeout=15)
            if resp.status_code >= 300:
                logging.error(f"Supabase fetch_all_conversations failed: {resp.status_code} {resp.text}")
                break
            rows = resp.json() or []
            if not rows:
                break
            batch_wa_ids = set()
            for r in rows:
                wa_id = r.get('wa_id')
                if wa_id and wa_id not in seen_wa_ids and wa_id not in batch_wa_ids:
                    batch_wa_ids.add(wa_id)
                    seen_wa_ids.add(wa_id)
                    all_conversations.append({'wa_id': wa_id})
            offset += len(rows)
            if len(rows) < page_size:
                break
        return all_conversations
    except Exception as exc:
        logging.error(f"Supabase fetch_all_conversations exception: {exc}", exc_info=True)
        return []


def fetch_all_conversation_summaries_fast(limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetch conversation summaries using optimized RPC v2 with native pagination.
    
    Uses get_conversation_summaries_v2 which queries the conversations table directly
    (with synced last_message_at) instead of aggregating all messages.
    
    If limit is None, fetches ALL conversations (for filtering use cases).
    """
    try:
        headers = _get_supabase_headers(True)
        pni = _get_phone_number_id()
        if not pni:
            logging.warning("No phone_number_id configured, returning empty summaries list")
            return []

        # Use RPC v2 with native pagination parameters
        # When limit is None, use a very high number to get all conversations
        payload = {
            'p_phone_number_id': pni,
            'p_limit': limit if isinstance(limit, int) and limit > 0 else 100000,
            'p_offset': max(0, int(offset)) if offset else 0
        }

        resp = requests.post(_rpc_url('get_conversation_summaries_v2'), headers=headers, json=payload, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase fetch_all_conversation_summaries_fast failed: {resp.status_code} {resp.text}")
            return []

        rows = resp.json() or []
        summaries: List[Dict[str, Any]] = []
        for r in rows:
            ts_dt = _to_local_datetime(r.get('last_message_time'))
            ts_str = ts_dt.strftime('%Y-%m-%d %H:%M:%S') if ts_dt else None

            summaries.append({
                'wa_id': r.get('wa_id'),
                'last_message_time': ts_str,
                'sender_name': r.get('sender_name') or 'Unknown',
                'last_message_text': r.get('last_message_text', ''),
                'last_message_direction': r.get('last_message_direction'),
                'last_message_status': r.get('last_message_status'),
                'proyecto': r.get('proyecto'),
                'last_message_model': r.get('last_message_model'),
                'needs_attention': bool(r.get('needs_attention')) if r.get('needs_attention') is not None else False,
                # v2 includes these fields - no extra enrichment needed
                'mode': r.get('mode') or 'bot',
                'estado_conversacion': r.get('estado_conversacion'),
                'assigned_queue_ids': r.get('assigned_queue_ids') or []
            })
        return summaries
    except Exception as exc:
        logging.error(f"Supabase fetch_all_conversation_summaries_fast exception: {exc}", exc_info=True)
        return []


def fetch_conversation_summary(wa_id: str) -> Dict[str, Any]:
    try:
        headers = _get_supabase_headers(True)
        pni = _get_phone_number_id()
        params = {
            'select': ','.join([
                'id', 'created_at', 'project_name', 'wa_id', 'direction',
                'message_text', 'model', 'whatsapp_message_id', 'status', 'response_time_seconds',
                'attempt_count', 'required_action', 'error_message'
            ]),
            'wa_id': f"eq.{wa_id}",
            'order': 'created_at.desc',
            'limit': '1',
        }
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.get(_rest_url('messages'), headers=headers, params=params, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"Supabase fetch_conversation_summary failed: {resp.status_code} {resp.text}")
            return {}
        rows = resp.json() or []
        if not rows:
            return {}
        r = rows[0]
        created_at = r.get('created_at')
        ts_dt = _to_local_datetime(created_at)
        ts_str = ts_dt.strftime('%Y-%m-%d %H:%M:%S') if ts_dt else None
        sender_name = "Unknown"
        inbound_params = {
            'select': 'sender_name',
            'wa_id': f"eq.{wa_id}",
            'direction': 'eq.inbound',
            'order': 'created_at.desc',
            'limit': '1',
        }
        if pni:
            inbound_params['phone_number_id'] = f"eq.{pni}"
        inbound_resp = requests.get(_rest_url('messages'), headers=headers, params=inbound_params, timeout=15)
        if inbound_resp.status_code == 200:
            inbound_rows = inbound_resp.json() or []
            if inbound_rows:
                sender_name = inbound_rows[0].get('sender_name', 'Unknown')
        return {
            'wa_id': wa_id,
            'last_message_time': ts_str,
            'sender_name': sender_name,
            'last_message_text': r.get('message_text', ''),
            'last_message_direction': r.get('direction'),
            'last_message_status': r.get('status'),
            'proyecto': r.get('project_name'),
            'last_message_model': r.get('model')
        }
    except Exception as exc:
        logging.error(f"Supabase fetch_conversation_summary exception: {exc}", exc_info=True)
        return {}


def update_conversation_estado_for_wa(wa_id: str, new_status: str) -> bool:
    try:
        headers = _get_supabase_headers(True)
        wa_id_alt = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"
        params: Dict[str, str] = {'wa_id': f"in.({wa_id},{wa_id_alt})"}
        payload = {'estado_conversacion': new_status}
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.patch(_rest_url('conversations'), headers=headers, params=params, json=payload, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase update_conversation_estado_for_wa failed: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as exc:
        logging.error(f"Supabase update_conversation_estado_for_wa exception: {exc}", exc_info=True)
        return False


def update_conversation_mode_for_wa(wa_id: str, mode: str) -> bool:
    try:
        if mode not in ('bot', 'agent'):
            raise ValueError("Invalid mode; must be 'bot' or 'agent')")
        headers = _get_supabase_headers(True)
        wa_id_alt = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"
        params: Dict[str, str] = {'wa_id': f"in.({wa_id},{wa_id_alt})"}
        payload = {'mode': mode}
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.patch(_rest_url('conversations'), headers=headers, params=params, json=payload, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase update_conversation_mode_for_wa failed: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as exc:
        logging.error(f"Supabase update_conversation_mode_for_wa exception: {exc}", exc_info=True)
        return False


def _get_conversation_ids_for_wa(wa_id: str) -> List[str]:
    headers = _get_supabase_headers(True)
    a = wa_id
    b = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"
    params: Dict[str, str] = {
        'select': 'id,wa_id',
        'wa_id': f'in.({a},{b})'
    }
    pni = _get_phone_number_id()
    if pni:
        params['phone_number_id'] = f"eq.{pni}"
    resp = requests.get(_rest_url('conversations'), headers=headers, params=params, timeout=10)
    if resp.status_code >= 300:
        logging.error(f"Supabase _get_conversation_ids_for_wa failed: {resp.status_code} {resp.text}")
        return []
    rows = resp.json() or []
    ids: List[str] = []
    for r in rows:
        cid = r.get('id')
        if cid:
            ids.append(cid)
    return ids


def fetch_conversation_assigned_queue_ids_for_wa(wa_id: str) -> List[str]:
    try:
        headers = _get_supabase_headers(True)
        a = wa_id
        b = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"
        params: Dict[str, str] = {
            'select': 'wa_id,assigned_queue_ids',
            'wa_id': f'in.({a},{b})'
        }
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.get(_rest_url('conversations'), headers=headers, params=params, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase fetch_conversation_assigned_queue_ids_for_wa failed: {resp.status_code} {resp.text}")
            return []
        rows = resp.json() or []
        if not rows:
            return []
        # Prefer exact wa_id match
        target = None
        for r in rows:
            if r.get('wa_id') == wa_id:
                target = r
                break
        if not target:
            target = rows[0]
        arr = target.get('assigned_queue_ids') or []
        # Normalize elements to strings
        return [str(x) for x in arr if x]
    except Exception as exc:
        logging.error(f"Supabase fetch_conversation_assigned_queue_ids_for_wa exception: {exc}", exc_info=True)
        return []


def update_conversation_assigned_queues_for_wa(wa_id: str, queue_ids: List[str]) -> bool:
    try:
        headers = _get_supabase_headers(True)
        wa_id_alt = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"
        params: Dict[str, str] = {'wa_id': f"in.({wa_id},{wa_id_alt})"}
        clean_ids = [str(q).strip() for q in (queue_ids or []) if str(q).strip()]
        payload: Dict[str, Any] = {'assigned_queue_ids': clean_ids}
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.patch(_rest_url('conversations'), headers=headers, params=params, json=payload, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase update_conversation_assigned_queues_for_wa failed: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as exc:
        logging.error(f"Supabase update_conversation_assigned_queues_for_wa exception: {exc}", exc_info=True)
        return False


def fetch_conversation_assigned_queue_for_wa(wa_id: str) -> Optional[str]:
    # Backward-compat shim: return first of list
    ids = fetch_conversation_assigned_queue_ids_for_wa(wa_id)
    return ids[0] if ids else None


def fetch_conversation_assigned_queue_map(wa_ids: List[str]) -> Dict[str, Optional[str]]:
    result: Dict[str, Optional[str]] = {}
    try:
        if not wa_ids:
            return result
        
        unique_ids = list(set(wa_ids))
        for i in range(0, len(unique_ids), BATCH_SIZE):
            chunk = unique_ids[i:i + BATCH_SIZE]
            
            headers = _get_supabase_headers(True)
            all_ids: List[str] = []
            seen = set()
            for w in chunk:
                if not w:
                    continue
                a = w
                b = w[1:] if w.startswith('+') else f"+{w}"
                for v in (a, b):
                    if v not in seen:
                        all_ids.append(v)
                        seen.add(v)
            
            if not all_ids:
                continue

            params: Dict[str, str] = {'select': 'wa_id,assigned_queue_id', 'wa_id': 'in.(' + ','.join(all_ids) + ')'}
            pni = _get_phone_number_id()
            if pni:
                params['phone_number_id'] = f"eq.{pni}"
            
            resp = requests.get(_rest_url('conversations'), headers=headers, params=params, timeout=15)
            if resp.status_code >= 300:
                logging.error(f"Supabase fetch_conversation_assigned_queue_map failed: {resp.status_code} {resp.text}")
                continue
            
            rows = resp.json() or []
            for r in rows:
                wa = r.get('wa_id')
                qid = r.get('assigned_queue_id')
                if not wa:
                    continue
                result[wa] = qid
                alt = wa[1:] if wa.startswith('+') else f"+{wa}"
                result[alt] = qid
                
        return result
    except Exception as exc:
        logging.error(f"Supabase fetch_conversation_assigned_queue_map exception: {exc}", exc_info=True)
        return result


def fetch_conversation_assigned_queue_ids_map(wa_ids: List[str]) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    try:
        if not wa_ids:
            return result
            
        unique_ids = list(set(wa_ids))
        for i in range(0, len(unique_ids), BATCH_SIZE):
            chunk = unique_ids[i:i + BATCH_SIZE]
            
            headers = _get_supabase_headers(True)
            all_ids: List[str] = []
            seen = set()
            for w in chunk:
                if not w:
                    continue
                a = w
                b = w[1:] if w.startswith('+') else f"+{w}"
                for v in (a, b):
                    if v not in seen:
                        all_ids.append(v)
                        seen.add(v)
            
            if not all_ids:
                continue

            params: Dict[str, str] = {'select': 'wa_id,assigned_queue_ids', 'wa_id': 'in.(' + ','.join(all_ids) + ')'}
            pni = _get_phone_number_id()
            if pni:
                params['phone_number_id'] = f"eq.{pni}"
            
            resp = requests.get(_rest_url('conversations'), headers=headers, params=params, timeout=15)
            if resp.status_code >= 300:
                logging.error(f"Supabase fetch_conversation_assigned_queue_ids_map failed: {resp.status_code} {resp.text} Params: {params}")
                continue
            
            rows = resp.json() or []
            for r in rows:
                wa = r.get('wa_id')
                arr = r.get('assigned_queue_ids') or []
                if not wa:
                    continue
                ids = [str(x) for x in arr if x]
                result[wa] = ids
                alt = wa[1:] if wa.startswith('+') else f"+{wa}"
                result[alt] = ids
                
        return result
    except Exception as exc:
        logging.error(f"Supabase fetch_conversation_assigned_queue_ids_map exception: {exc}", exc_info=True)
        return result


def fetch_conversation_fields_map(wa_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch multiple conversation fields in a single request.
    Returns { wa_id: { 'estado_conversacion': str|None, 'mode': str|None, 'needs_attention': bool, 'assigned_queue_ids': [str] } }
    """
    out: Dict[str, Dict[str, Any]] = {}
    try:
        if not wa_ids:
            return out
            
        unique_ids = list(set(wa_ids))
        for i in range(0, len(unique_ids), BATCH_SIZE):
            chunk = unique_ids[i:i + BATCH_SIZE]
            
            headers = _get_supabase_headers(True)
            # Build list with alt +wa variants to maximize matches
            all_ids: List[str] = []
            seen = set()
            for w in chunk:
                if not w:
                    continue
                a = w
                b = w[1:] if w.startswith('+') else f"+{w}"
                for v in (a, b):
                    if v not in seen:
                        all_ids.append(v)
                        seen.add(v)
            
            if not all_ids:
                continue

            params: Dict[str, str] = {
                'select': 'wa_id,estado_conversacion,mode,needs_attention,assigned_queue_ids',
                'wa_id': 'in.(' + ','.join(all_ids) + ')'
            }
            pni = _get_phone_number_id()
            if pni:
                params['phone_number_id'] = f"eq.{pni}"
            
            resp = requests.get(_rest_url('conversations'), headers=headers, params=params, timeout=15)
            if resp.status_code >= 300:
                logging.error(f"Supabase fetch_conversation_fields_map failed: {resp.status_code} {resp.text} Params: {params}")
                continue
            
            rows = resp.json() or []
            for r in rows:
                wa = r.get('wa_id')
                if not wa:
                    continue
                assigned = [str(x) for x in (r.get('assigned_queue_ids') or []) if x]
                out[wa] = {
                    'estado_conversacion': r.get('estado_conversacion'),
                    'mode': r.get('mode'),
                    'needs_attention': bool(r.get('needs_attention')) if r.get('needs_attention') is not None else False,
                    'assigned_queue_ids': assigned,
                }
                alt = wa[1:] if wa.startswith('+') else f"+{wa}"
                out[alt] = out[wa]
                
        return out
    except Exception as exc:
        logging.error(f"Supabase fetch_conversation_fields_map exception: {exc}", exc_info=True)
        return out

def fetch_conversation_status_map(wa_ids: List[str]) -> Dict[str, Optional[str]]:
    result: Dict[str, Optional[str]] = {}
    try:
        if not wa_ids:
            return result
            
        unique_ids = list(set(wa_ids))
        for i in range(0, len(unique_ids), BATCH_SIZE):
            chunk = unique_ids[i:i + BATCH_SIZE]
            
            headers = _get_supabase_headers(True)
            all_ids: List[str] = []
            seen = set()
            for w in chunk:
                if not w:
                    continue
                a = w
                b = w[1:] if w.startswith('+') else f"+{w}"
                for v in (a, b):
                    if v not in seen:
                        all_ids.append(v)
                        seen.add(v)
            
            if not all_ids:
                continue

            params: Dict[str, str] = {'select': 'wa_id,estado_conversacion', 'wa_id': 'in.(' + ','.join(all_ids) + ')'}
            pni = _get_phone_number_id()
            if pni:
                params['phone_number_id'] = f"eq.{pni}"
            
            resp = requests.get(_rest_url('conversations'), headers=headers, params=params, timeout=15)
            if resp.status_code >= 300:
                logging.error(f"Supabase fetch_conversation_status_map failed: {resp.status_code} {resp.text} Params: {params}")
                continue
            
            rows = resp.json() or []
            for r in rows:
                wa = r.get('wa_id')
                estado = r.get('estado_conversacion')
                if not wa:
                    continue
                result[wa] = estado
                alt = wa[1:] if wa.startswith('+') else f"+{wa}"
                result[alt] = estado
                
        return result
    except Exception as exc:
        logging.error(f"Supabase fetch_conversation_status_map exception: {exc}", exc_info=True)
        return result


def fetch_conversation_mode_map(wa_ids: List[str]) -> Dict[str, Optional[str]]:
    result: Dict[str, Optional[str]] = {}
    try:
        if not wa_ids:
            return result
            
        unique_ids = list(set(wa_ids))
        for i in range(0, len(unique_ids), BATCH_SIZE):
            chunk = unique_ids[i:i + BATCH_SIZE]
            
            headers = _get_supabase_headers(True)
            all_ids: List[str] = []
            seen = set()
            for w in chunk:
                if not w:
                    continue
                a = w
                b = w[1:] if w.startswith('+') else f"+{w}"
                for v in (a, b):
                    if v not in seen:
                        all_ids.append(v)
                        seen.add(v)
            
            if not all_ids:
                continue

            params: Dict[str, str] = {'select': 'wa_id,mode', 'wa_id': 'in.(' + ','.join(all_ids) + ')'}
            pni = _get_phone_number_id()
            if pni:
                params['phone_number_id'] = f"eq.{pni}"
            
            resp = requests.get(_rest_url('conversations'), headers=headers, params=params, timeout=10)
            if resp.status_code >= 300:
                logging.error(f"Supabase fetch_conversation_mode_map failed: {resp.status_code} {resp.text} Params: {params}")
                continue
            
            rows = resp.json() or []
            for r in rows:
                wa = r.get('wa_id')
                mode = r.get('mode')
                if not wa:
                    continue
                result[wa] = mode
                alt = wa[1:] if wa.startswith('+') else f"+{wa}"
                result[alt] = mode
                
        return result
    except Exception as exc:
        logging.error(f"Supabase fetch_conversation_mode_map exception: {exc}", exc_info=True)
        return result


def update_conversation_attention_for_wa(wa_id: str, needs_attention: bool) -> bool:
    try:
        headers = _get_supabase_headers(True)
        wa_id_alt = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"
        params: Dict[str, str] = {'wa_id': f"in.({wa_id},{wa_id_alt})"}
        payload = {'needs_attention': bool(needs_attention)}
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.patch(_rest_url('conversations'), headers=headers, params=params, json=payload, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase update_conversation_attention_for_wa failed: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as exc:
        logging.error(f"Supabase update_conversation_attention_for_wa exception: {exc}", exc_info=True)
        return False


def delete_conversation(wa_id: str) -> bool:
    """
    Elimina una conversación (y, de forma best-effort, sus mensajes asociados)
    para un wa_id dado. Usa variantes con y sin '+' para maximizar coincidencias.
    """
    try:
        headers = _get_supabase_headers(True)
        pni = _get_phone_number_id()

        # Normalizar wa_id y construir variante con y sin '+'
        wa_id_clean = (wa_id or "").lstrip("+")
        if not wa_id_clean:
            return False

        wa_ids_str = f"{wa_id_clean},+{wa_id_clean}"

        # 1) Intentar borrar mensajes primero (para evitar problemas de FK)
        try:
            params_msgs: Dict[str, str] = {"wa_id": f"in.({wa_ids_str})"}
            if pni:
                params_msgs["phone_number_id"] = f"eq.{pni}"
            headers_msgs = dict(headers)
            headers_msgs["Prefer"] = "return=minimal"
            resp_msgs = requests.delete(
                _rest_url("messages"),
                headers=headers_msgs,
                params=params_msgs,
                timeout=15,
            )
            if resp_msgs.status_code >= 300:
                logging.warning(
                    f"Supabase delete_conversation: error deleting messages for {wa_id}: "
                    f"{resp_msgs.status_code} {resp_msgs.text}"
                )
            else:
                logging.info(
                    f"Supabase delete_conversation: messages deleted (if any) for {wa_id}."
                )
        except Exception as msg_exc:
            logging.warning(
                f"Supabase delete_conversation: exception deleting messages for {wa_id}: {msg_exc}"
            )

        # 2) Borrar conversación(es)
        params_conv: Dict[str, str] = {"wa_id": f"in.({wa_ids_str})"}
        if pni:
            params_conv["phone_number_id"] = f"eq.{pni}"

        resp_conv = requests.delete(
            _rest_url("conversations"),
            headers=headers,
            params=params_conv,
            timeout=10,
        )
        if resp_conv.status_code >= 300:
            logging.error(
                f"Supabase delete_conversation failed: {resp_conv.status_code} {resp_conv.text}"
            )
            return False

        logging.info(f"Supabase delete_conversation: conversations deleted for {wa_id}.")
        return True
    except Exception as exc:
        logging.error(f"Supabase delete_conversation exception: {exc}", exc_info=True)
        return False


def fetch_conversation_mode_and_attention(wa_id: str) -> Dict[str, Any]:
    """Return {'mode': 'agent'|'bot'|None, 'needs_attention': bool} for a given wa_id (handles +alt)."""
    try:
        headers = _get_supabase_headers(True)
        a = wa_id
        b = wa_id[1:] if wa_id.startswith('+') else f"+{wa_id}"
        params: Dict[str, str] = {'select': 'wa_id,mode,needs_attention', 'wa_id': f'in.({a},{b})'}
        pni = _get_phone_number_id()
        if pni:
            params['phone_number_id'] = f"eq.{pni}"
        resp = requests.get(_rest_url('conversations'), headers=headers, params=params, timeout=10)
        if resp.status_code >= 300:
            logging.error(f"Supabase fetch_conversation_mode_and_attention failed: {resp.status_code} {resp.text}")
            return {'mode': None, 'needs_attention': False}
        rows = resp.json() or []
        if not rows:
            return {'mode': None, 'needs_attention': False}
        # Prefer exact wa_id match
        target = None
        for r in rows:
            if r.get('wa_id') == wa_id:
                target = r
                break
        if not target:
            target = rows[0]
        return {
            'mode': target.get('mode'),
            'needs_attention': bool(target.get('needs_attention')) if target.get('needs_attention') is not None else False
        }
    except Exception as exc:
        logging.error(f"Supabase fetch_conversation_mode_and_attention exception: {exc}", exc_info=True)
        return {'mode': None, 'needs_attention': False}


__all__ = [
    'upsert_conversation',
    'fetch_all_conversations',
    'fetch_all_conversation_summaries_fast',
    'fetch_conversation_summary',
    'update_conversation_estado_for_wa',
    'update_conversation_mode_for_wa',
    'fetch_conversation_status_map',
    'fetch_conversation_mode_map',
    'update_conversation_attention_for_wa',
    'update_conversation_assigned_queues_for_wa',
    'fetch_conversation_assigned_queue_for_wa',
    'fetch_conversation_assigned_queue_ids_for_wa',
    'fetch_conversation_assigned_queue_map',
    'delete_conversation',
]


