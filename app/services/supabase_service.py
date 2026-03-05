from typing import Any, Dict, Optional, List, Tuple
from datetime import datetime
from app.services.supabase_client import (
    is_supabase_enabled as _is_supabase_enabled_impl,
    _get_supabase_headers as _get_supabase_headers_impl,
    _rest_url as _rest_url_impl,
    _rpc_url as _rpc_url_impl,
    _get_phone_number_id as _get_phone_number_id_impl,
)
from app.services.supabase_client.search import (
    search_messages as _search_messages_impl,
    search_messages_text_only as _search_messages_text_only_impl,
    search_users as _search_users_impl,
    fetch_sender_name_map_for_wa_ids as _fetch_sender_name_map_for_wa_ids_impl,
)
from app.services.supabase_client.status_definitions import (
    fetch_status_definitions as _fetch_status_definitions_impl,
    create_status_definition as _create_status_definition_impl,
    delete_status_definition as _delete_status_definition_impl,
)
from app.services.supabase_client.prev_response import (
    update_previous_response_id as _update_previous_response_id_impl,
    get_previous_response_id as _get_previous_response_id_impl,
    clear_previous_response_id as _clear_previous_response_id_impl,
)
from app.services.supabase_client.enrollment import (
    upsert_enrollment_context as _upsert_enrollment_context_impl,
    fetch_enrollment_context as _fetch_enrollment_context_impl,
    merge_enrollment_context as _merge_enrollment_context_impl,
    clear_enrollment_context_row as _clear_enrollment_context_row_impl,
)
from app.services.supabase_client.messages import (
    insert_message as _insert_message_impl,
    fetch_messages_for_conversation as _fetch_messages_for_conversation_impl,
    fetch_messages as _fetch_messages_impl,
    update_message_status_by_wamid as _update_message_status_by_wamid_impl,
    fetch_last_inbound_timestamp as _fetch_last_inbound_timestamp_impl,
    fetch_messages_for_wa as _fetch_messages_for_wa_impl,
    fetch_message_statuses_by_wamids as _fetch_message_statuses_by_wamids_impl,
    fetch_message_by_wamid as _fetch_message_by_wamid_impl,
)
from app.services.supabase_client.conversations import (
    upsert_conversation as _upsert_conversation_impl,
    fetch_all_conversations as _fetch_all_conversations_impl,
    fetch_all_conversation_summaries_fast as _fetch_all_conversation_summaries_fast_impl,
    fetch_conversation_summary as _fetch_conversation_summary_impl,
    update_conversation_estado_for_wa as _update_conversation_estado_for_wa_impl,
    update_conversation_mode_for_wa as _update_conversation_mode_for_wa_impl,
    fetch_conversation_status_map as _fetch_conversation_status_map_impl,
    fetch_conversation_mode_map as _fetch_conversation_mode_map_impl,
    update_conversation_attention_for_wa as _update_conversation_attention_for_wa_impl,
    update_conversation_assigned_queues_for_wa as _update_conversation_assigned_queues_for_wa_impl,
    fetch_conversation_assigned_queue_for_wa as _fetch_conversation_assigned_queue_for_wa_impl,
    fetch_conversation_assigned_queue_map as _fetch_conversation_assigned_queue_map_impl,
    fetch_conversation_assigned_queue_ids_for_wa as _fetch_conversation_assigned_queue_ids_for_wa_impl,
    fetch_conversation_assigned_queue_ids_map as _fetch_conversation_assigned_queue_ids_map_impl,
    fetch_conversation_mode_and_attention as _fetch_conversation_mode_and_attention_impl,
    fetch_conversation_fields_map as _fetch_conversation_fields_map_impl,
    delete_conversation as _delete_conversation_impl,
)
from app.services.supabase_client.queues import (
    list_queues as _list_queues_impl,
    list_queues_for_email as _list_queues_for_email_impl,
    create_queue as _create_queue_impl,
    update_queue as _update_queue_impl,
    delete_queue as _delete_queue_impl,
    get_queue as _get_queue_impl,
    list_queue_members as _list_queue_members_impl,
    add_member_to_queue as _add_member_to_queue_impl,
    remove_member_from_queue as _remove_member_from_queue_impl,
)
from app.services.supabase_client.users import (
    fetch_user_by_email as _fetch_user_by_email_impl,
    upsert_user_prefs_by_email as _upsert_user_prefs_by_email_impl,
    list_emails_with_flag as _list_emails_with_flag_impl,
)
from app.services.supabase_client.processing_lock import (
    try_acquire_processing_lock as _try_acquire_processing_lock_impl,
    get_and_clear_pending_messages_atomic as _get_and_clear_pending_messages_atomic_impl,
    release_processing_lock_atomic as _release_processing_lock_atomic_impl,
)
try:
    from flask import current_app as _flask_current_app  # type: ignore
except Exception:  # pragma: no cover
    _flask_current_app = None



def upsert_conversation(wa_id: str, project_name: Optional[str], last_message_text: Optional[str], last_direction: Optional[str]) -> Optional[Dict[str, Any]]:
    return _upsert_conversation_impl(wa_id, project_name, last_message_text, last_direction)


def insert_message(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return _insert_message_impl(record)


def upsert_enrollment_context(wa_id: str, project_name: Optional[str], context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return _upsert_enrollment_context_impl(wa_id, project_name, context)


def fetch_enrollment_context(wa_id: str, project_name: Optional[str] = None) -> Dict[str, Any]:
    return _fetch_enrollment_context_impl(wa_id, project_name)


def merge_enrollment_context(wa_id: str, project_name: Optional[str], partial: Dict[str, Any]) -> bool:
    return _merge_enrollment_context_impl(wa_id, project_name, partial)


def clear_enrollment_context_row(wa_id: str, project_name: Optional[str]) -> bool:
    return _clear_enrollment_context_row_impl(wa_id, project_name)


def update_message_status_by_wamid(whatsapp_message_id: str, new_status: str, error_message: str = None) -> bool:
    return _update_message_status_by_wamid_impl(whatsapp_message_id, new_status, error_message)


def fetch_message_statuses_by_wamids(whatsapp_message_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    return _fetch_message_statuses_by_wamids_impl(whatsapp_message_ids)


def fetch_message_by_wamid(whatsapp_message_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a single message by its WhatsApp message ID (for reply context)."""
    return _fetch_message_by_wamid_impl(whatsapp_message_id)


def delete_conversation(wa_id: str) -> bool:
    return _delete_conversation_impl(wa_id)


def fetch_all_conversations() -> List[Dict[str, Any]]:
    return _fetch_all_conversations_impl()


def fetch_all_conversation_summaries_fast(limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
    return _fetch_all_conversation_summaries_fast_impl(limit, offset)


def fetch_conversation_summary(wa_id: str) -> Dict[str, Any]:
    return _fetch_conversation_summary_impl(wa_id)


def fetch_messages_for_conversation(wa_id: str, limit: int = 50, before_timestamp: str = None, after_timestamp: str = None, target_message_id: str = None) -> List[Dict[str, Any]]:
    return _fetch_messages_for_conversation_impl(wa_id, limit, before_timestamp, after_timestamp, target_message_id)


def fetch_messages(limit: int = 1000) -> List[Dict[str, Any]]:
    return _fetch_messages_impl(limit)

def fetch_last_inbound_timestamp(wa_id: str) -> Optional[datetime]:
    return _fetch_last_inbound_timestamp_impl(wa_id)


def fetch_messages_for_wa(wa_id: str, limit: int = 100, before_local_ts: Optional[str] = None) -> List[Dict[str, Any]]:
    return _fetch_messages_for_wa_impl(wa_id, limit, before_local_ts)


def update_conversation_estado_for_wa(wa_id: str, new_status: str) -> bool:
    return _update_conversation_estado_for_wa_impl(wa_id, new_status)


def fetch_conversation_status_map(wa_ids: List[str]) -> Dict[str, Optional[str]]:
    return _fetch_conversation_status_map_impl(wa_ids)


def update_conversation_mode_for_wa(wa_id: str, mode: str) -> bool:
    return _update_conversation_mode_for_wa_impl(wa_id, mode)


def fetch_conversation_mode_map(wa_ids: List[str]) -> Dict[str, Optional[str]]:
    return _fetch_conversation_mode_map_impl(wa_ids)


def update_conversation_attention_for_wa(wa_id: str, needs_attention: bool) -> bool:
    return _update_conversation_attention_for_wa_impl(wa_id, needs_attention)


def fetch_conversation_mode_and_attention(wa_id: str) -> Dict[str, Any]:
    return _fetch_conversation_mode_and_attention_impl(wa_id)


def update_conversation_assigned_queues_for_wa(wa_id: str, queue_ids: List[str]) -> bool:
    return _update_conversation_assigned_queues_for_wa_impl(wa_id, queue_ids)


def fetch_conversation_assigned_queue_for_wa(wa_id: str) -> Optional[str]:
    return _fetch_conversation_assigned_queue_for_wa_impl(wa_id)


def fetch_conversation_assigned_queue_map(wa_ids: List[str]) -> Dict[str, Optional[str]]:
    return _fetch_conversation_assigned_queue_map_impl(wa_ids)


def fetch_conversation_assigned_queue_ids_for_wa(wa_id: str) -> List[str]:
    return _fetch_conversation_assigned_queue_ids_for_wa_impl(wa_id)


def fetch_conversation_assigned_queue_ids_map(wa_ids: List[str]) -> Dict[str, List[str]]:
    return _fetch_conversation_assigned_queue_ids_map_impl(wa_ids)


def fetch_conversation_fields_map(wa_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    return _fetch_conversation_fields_map_impl(wa_ids)


def search_messages(query: str, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
    return _search_messages_impl(query, limit, offset)


# Text-only search in messages (exclude sender_name matches)
def search_messages_text_only(query: str, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
    return _search_messages_text_only_impl(query, limit, offset)

# --- Users/Conversations search helpers ---
def search_users(query: str, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    return _search_users_impl(query, limit, offset)

# --- Utilities to resolve sender_name for wa_ids ---
def fetch_sender_name_map_for_wa_ids(wa_ids: List[str]) -> Dict[str, str]:
    return _fetch_sender_name_map_for_wa_ids_impl(wa_ids)

# --- Status definitions helpers ---
def fetch_status_definitions() -> List[str]:
    return _fetch_status_definitions_impl()

def create_status_definition(name: str) -> Tuple[bool, Optional[str]]:
    return _create_status_definition_impl(name)

def delete_status_definition(name: str) -> bool:
    return _delete_status_definition_impl(name)


def update_previous_response_id(wa_id: str, response_id: str):
    return _update_previous_response_id_impl(wa_id, response_id)


def get_previous_response_id(wa_id: str) -> Optional[str]:
    return _get_previous_response_id_impl(wa_id)


def clear_previous_response_id(wa_id: str) -> bool:
    return _clear_previous_response_id_impl(wa_id)


def is_supabase_enabled() -> bool:
    return _is_supabase_enabled_impl()


def _get_supabase_headers(use_service_role: bool = True) -> Dict[str, str]:
    return _get_supabase_headers_impl(use_service_role)


def _rest_url(table: str) -> str:
    return _rest_url_impl(table)


def _rpc_url(function_name: str) -> str:
    return _rpc_url_impl(function_name)


def _get_phone_number_id() -> Optional[str]:
    return _get_phone_number_id_impl()


# --- Queues API wrappers ---
def list_queues() -> List[Dict[str, Any]]:
    """List queues and ensure a special 'Sin cola' queue exists for current phone_number_id.

    'Sin cola' represents unassigned conversations (default state for new chats).
    """
    queues = _list_queues_impl() or []
    try:
        # If 'Sin cola' not present, create it with no filters
        has_unassigned = any((q.get('name') or '').strip().lower() == 'sin cola' for q in queues)
        if not has_unassigned:
            created = _create_queue_impl('Sin cola', [], [], None, None)
            if created:
                # Prepend to make it visible first
                queues = [created] + queues
    except Exception:
        # If creation fails, just return existing queues
        pass
    
    # Sort queues: "Sin cola" and "Documentación" first, rest by creation date (desc)
    sin_cola_queue = [q for q in queues if (q.get('name') or '').strip().lower() == 'sin cola']
    documentacion_queue = [q for q in queues if (q.get('name') or '').strip().lower() == 'documentación']
    other_queues = [q for q in queues if (q.get('name') or '').strip().lower() not in ('sin cola', 'documentación')]
    # Other queues are already sorted by created_at.desc from the query
    
    return sin_cola_queue + documentacion_queue + other_queues


def list_queues_for_email(email: str) -> List[Dict[str, Any]]:
    queues = _list_queues_for_email_impl(email)
    # Apply same sorting: "Sin cola" and "Documentación" first, rest by creation date (desc)
    sin_cola_queue = [q for q in queues if (q.get('name') or '').strip().lower() == 'sin cola']
    documentacion_queue = [q for q in queues if (q.get('name') or '').strip().lower() == 'documentación']
    other_queues = [q for q in queues if (q.get('name') or '').strip().lower() not in ('sin cola', 'documentación')]
    # Other queues maintain their order from the query
    return sin_cola_queue + documentacion_queue + other_queues


def create_queue(name: str, mode: Optional[str], statuses: List[str], created_by: Optional[str], attention: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Create a queue with a single mode (bot or agent)."""
    return _create_queue_impl(name, mode, statuses, created_by, attention)


def update_queue(queue_id: str, name: Optional[str] = None, mode: Optional[str] = None, statuses: Optional[List[str]] = None, attention: Optional[str] = None) -> bool:
    """Update a queue. Mode must be 'bot' or 'agent' if provided."""
    return _update_queue_impl(queue_id, name, mode, statuses, attention)


def get_queue(queue_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a single queue by ID."""
    return _get_queue_impl(queue_id)


def delete_queue(queue_id: str) -> bool:
    return _delete_queue_impl(queue_id)


def list_queue_members(queue_id: str) -> List[str]:
    return _list_queue_members_impl(queue_id)


def add_member_to_queue(queue_id: str, email: str) -> bool:
    return _add_member_to_queue_impl(queue_id, email)


def remove_member_from_queue(queue_id: str, email: str) -> bool:
    return _remove_member_from_queue_impl(queue_id, email)

# --- Users (preferences) ---
def fetch_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    return _fetch_user_by_email_impl(email)


def upsert_user_prefs_by_email(email: str, prefs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return _upsert_user_prefs_by_email_impl(email, prefs)


def list_emails_with_flag(flag_column: str) -> List[str]:
    return _list_emails_with_flag_impl(flag_column)


# --- Processing Lock (atomic operations for message concatenation) ---
def try_acquire_processing_lock(wa_id: str, worker_id: str, project_name: Optional[str] = None, 
                                 lock_duration_seconds: int = 60, 
                                 message_to_buffer: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _try_acquire_processing_lock_impl(wa_id, worker_id, project_name, lock_duration_seconds, message_to_buffer)


def get_and_clear_pending_messages_atomic(wa_id: str, project_name: Optional[str] = None) -> List[Dict[str, Any]]:
    return _get_and_clear_pending_messages_atomic_impl(wa_id, project_name)


def release_processing_lock_atomic(wa_id: str, worker_id: Optional[str] = None, project_name: Optional[str] = None) -> bool:
    return _release_processing_lock_atomic_impl(wa_id, worker_id, project_name)
