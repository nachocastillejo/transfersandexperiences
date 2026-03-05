"""
Atomic processing lock operations for message concatenation.
Uses Supabase RPC functions to ensure atomicity in multi-process environments.
"""
import logging
import os
import time
from typing import Dict, Any, Optional, List
import requests

from . import _get_supabase_headers, _rpc_url, is_supabase_enabled, _get_phone_number_id


def try_acquire_processing_lock(
    wa_id: str,
    worker_id: str,
    project_name: Optional[str] = None,
    lock_duration_seconds: int = 60,
    message_to_buffer: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Atomically try to acquire a processing lock for a wa_id.
    
    If a lock is already active, optionally buffers the message.
    
    Args:
        wa_id: WhatsApp ID
        worker_id: Unique identifier for this worker
        project_name: Project name (defaults to ENV_NAME)
        lock_duration_seconds: How long the lock should last
        message_to_buffer: Message to buffer if lock is already held
            Format: {text: str, message_id: str, message_type: str, timestamp: float}
    
    Returns:
        {
            acquired: bool,      # True if lock was acquired
            buffered: bool,      # True if message was buffered
            existing_worker_id: str | None,  # Worker ID that holds the lock
            buffer_size: int     # Number of messages in buffer (if buffered)
        }
    """
    if not is_supabase_enabled():
        # Fallback: no atomicity, just return acquired=True
        logging.warning("Supabase not enabled, lock atomicity not guaranteed")
        return {"acquired": True, "buffered": False, "existing_worker_id": None}
    
    try:
        effective_project = project_name or os.getenv("ENV_NAME")
        phone_number_id = _get_phone_number_id()
        
        payload = {
            "p_wa_id": wa_id,
            "p_project_name": effective_project,
            "p_phone_number_id": phone_number_id,
            "p_worker_id": worker_id,
            "p_lock_duration_seconds": lock_duration_seconds,
            "p_message_to_buffer": message_to_buffer
        }
        
        response = requests.post(
            _rpc_url("try_acquire_processing_lock"),
            headers=_get_supabase_headers(),
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get("acquired"):
                logging.info(f"🔒 Atomic lock acquired for {wa_id} (worker: {worker_id})")
            elif result.get("buffered"):
                logging.info(f"📥 Message buffered atomically for {wa_id} (buffer size: {result.get('buffer_size', '?')})")
            return result
        else:
            logging.error(f"Failed to acquire lock via RPC: {response.status_code} - {response.text}")
            # Fallback: return acquired=True to avoid blocking
            return {"acquired": True, "buffered": False, "existing_worker_id": None, "error": response.text}
            
    except Exception as e:
        logging.error(f"Error in try_acquire_processing_lock: {e}")
        # Fallback: return acquired=True to avoid blocking
        return {"acquired": True, "buffered": False, "existing_worker_id": None, "error": str(e)}


def get_and_clear_pending_messages_atomic(
    wa_id: str,
    project_name: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Atomically get and clear pending messages for a wa_id.
    
    Returns:
        List of pending messages: [{text, message_id, message_type, timestamp}, ...]
    """
    if not is_supabase_enabled():
        return []
    
    try:
        effective_project = project_name or os.getenv("ENV_NAME")
        phone_number_id = _get_phone_number_id()
        
        payload = {
            "p_wa_id": wa_id,
            "p_project_name": effective_project,
            "p_phone_number_id": phone_number_id
        }
        
        response = requests.post(
            _rpc_url("get_and_clear_pending_messages"),
            headers=_get_supabase_headers(),
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            if isinstance(result, list) and len(result) > 0:
                logging.info(f"📤 Retrieved {len(result)} pending messages atomically for {wa_id}")
            return result if isinstance(result, list) else []
        else:
            logging.error(f"Failed to get pending messages via RPC: {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        logging.error(f"Error in get_and_clear_pending_messages_atomic: {e}")
        return []


def release_processing_lock_atomic(
    wa_id: str,
    worker_id: Optional[str] = None,
    project_name: Optional[str] = None
) -> bool:
    """
    Atomically release the processing lock for a wa_id.
    
    Args:
        wa_id: WhatsApp ID
        worker_id: Optional - only release if this worker owns the lock
        project_name: Project name (defaults to ENV_NAME)
    
    Returns:
        True if lock was released, False otherwise
    """
    if not is_supabase_enabled():
        return True
    
    try:
        effective_project = project_name or os.getenv("ENV_NAME")
        phone_number_id = _get_phone_number_id()
        
        payload = {
            "p_wa_id": wa_id,
            "p_project_name": effective_project,
            "p_phone_number_id": phone_number_id,
            "p_worker_id": worker_id
        }
        
        response = requests.post(
            _rpc_url("release_processing_lock"),
            headers=_get_supabase_headers(),
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            if result:
                logging.info(f"🔓 Atomic lock released for {wa_id}")
            return bool(result)
        else:
            logging.error(f"Failed to release lock via RPC: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logging.error(f"Error in release_processing_lock_atomic: {e}")
        return False
