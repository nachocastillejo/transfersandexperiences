import sqlite3
import os
import logging
from datetime import datetime
# Corrected import path for database_utils
from app.utils.database_utils import get_db, get_db_path 
from app.services.supabase_service import is_supabase_enabled, insert_message, upsert_conversation, update_message_status_by_wamid
from app.services.supabase_service import fetch_conversation_mode_map, list_emails_with_flag, fetch_conversation_mode_and_attention
from app.services.email_service import send_notification_email
from flask import current_app
from app.utils.meta_template_status_cache import get_pending_message_info, mark_message_created

# No longer needed: tempfile, time, shutil, Lock, CSV_HEADER, LOG_FILE_PATH, csv_lock, STATUS_ORDER (for this function)

def log_message_to_db(
    wa_id: str,
    sender_name: str,       # Actual sender: user's name for inbound, bot/agent name for outbound
    message_text: str,      # The content of this specific message
    direction: str,         # 'inbound', 'outbound_bot', or 'outbound_agent'
    project_name: str = 'Bot',
    timestamp: str | None = None, # Optional: YYYY-MM-DD HH:MM:SS, defaults to now
    whatsapp_message_id: str | None = None, # For outbound messages from WhatsApp API
    status: str | None = None,          # For outbound messages: 'sent', 'delivered', 'read'
    response_time_seconds: float | None = None, # For outbound_bot messages
    attempt_count: int | None = None,           # For outbound_bot messages
    required_action: str | None = None,       # If applicable
    error_message: str | None = None,         # If applicable
    model: str | None = None,                 # Model used to generate the message (for outbound_bot)
    response_id: str | None = None,           # OpenAI response/thread identifier to correlate assistant replies
    media_type: str | None = None,            # Type of media: 'image', 'document', 'video', 'audio'
    media_url: str | None = None,             # Storage URL or WhatsApp media URL
    media_filename: str | None = None,        # Original filename
    media_mime_type: str | None = None,       # MIME type of the file
    media_size_bytes: int | None = None       # Size in bytes
):
    """Logs a single message (inbound or outbound) to the SQLite database."""
    
    sql = """
        INSERT INTO messages (
            timestamp, project_name, sender_name, wa_id, direction, 
            message_text, model, whatsapp_message_id, status, response_time_seconds, 
            attempt_count, required_action, error_message, phone_number_id,
            media_type, media_url, media_filename, media_mime_type, media_size_bytes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    current_timestamp = timestamp if timestamp else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Ensure None for fields not applicable to inbound messages
    if direction == 'inbound':
        whatsapp_message_id = None
        status = None
        response_time_seconds = None
        attempt_count = None

    phone_number_id = None
    try:
        phone_number_id = current_app.config.get('PHONE_NUMBER_ID') if current_app else os.getenv('PHONE_NUMBER_ID')
    except Exception:
        phone_number_id = os.getenv('PHONE_NUMBER_ID')

    params = (
        current_timestamp,
        project_name,
        sender_name,
        wa_id,
        direction,
        message_text,
        model,
        whatsapp_message_id,
        status,
        response_time_seconds,
        attempt_count,
        required_action,
        error_message,
        phone_number_id,
        media_type,
        media_url,
        media_filename,
        media_mime_type,
        media_size_bytes
    )

    try:
        if is_supabase_enabled():
            try:
                supa_record = {
                    'project_name': project_name,
                    'sender_name': sender_name,
                    'wa_id': wa_id,
                    'direction': direction,
                    'message_text': message_text,
                    'model': model,
                    'whatsapp_message_id': whatsapp_message_id,
                    'status': status,
                    'response_time_seconds': response_time_seconds,
                    'attempt_count': attempt_count,
                    'required_action': required_action,
                    'error_message': error_message,
                    'phone_number_id': phone_number_id,
                    'response_id': response_id,
                    'media_type': media_type,
                    'media_url': media_url,
                    'media_filename': media_filename,
                    'media_mime_type': media_mime_type,
                    'media_size_bytes': media_size_bytes,
                }
                upsert_conversation(wa_id=wa_id, project_name=project_name, last_message_text=message_text, last_direction=direction)
                insert_message(supa_record)
                return 
            except Exception as supa_err:
                logging.warning(f"Supabase logging failed: {supa_err}")
                # Fallthrough to SQLite

        # Otherwise, write to SQLite (legacy/local mode)
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
    except Exception as e:
        logging.error(f"Error logging message to local DB for {wa_id}: {e}")

# STATUS_ORDER for tracking progression of WhatsApp delivery statuses
STATUS_ORDER = {'sent': 1, 'delivered': 2, 'read': 3, 'failed': 4}

def update_message_status_in_db(message_id_to_update: str, new_status: str, recipient_wa_id: str | None = None) -> bool:
    if not message_id_to_update or not new_status or new_status not in STATUS_ORDER:
        return False

    try:
        if is_supabase_enabled():
            try:
                if update_message_status_by_wamid(message_id_to_update, new_status):
                    return True
            except Exception as supa_err:
                logging.warning(f"Supabase status update failed: {supa_err}")

        # Local SQLite update
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM messages WHERE whatsapp_message_id = ?", (message_id_to_update,))
        row = cursor.fetchone()
        
        if row:
            current_status = row['status']
            if STATUS_ORDER.get(new_status, 0) > STATUS_ORDER.get(current_status, 0):
                cursor.execute(
                    "UPDATE messages SET status = ? WHERE whatsapp_message_id = ?",
                    (new_status, message_id_to_update)
                )
                conn.commit()
                return True
        return False
    except Exception as e:
        logging.error(f"Error updating status in local DB: {e}")
        return False