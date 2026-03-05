import threading
import time
import logging
import os
import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional
import sys
import multiprocessing

# Local imports are kept lightweight to avoid circular dependencies
from app.utils.enrollment_state import (
    get_enrollment_context,
    update_enrollment_context,
    set_enrollment_context,
    clear_enrollment_context,
)
from app.utils.extra_utils import delete_response_id


# Use multiprocessing on Linux/macOS, threading on Windows
if sys.platform.startswith("win"):
    _Process = threading.Thread
    _Event = threading.Event
    _Lock = threading.Lock
else:
    _Process = multiprocessing.Process
    _Event = multiprocessing.Event
    _Lock = multiprocessing.Lock

_inactivity_seconds: int = 300  # default 5 minutes

# Persistent store for last activity timestamps using SQLite for process-safety
_tracker_db_lock = _Lock()
_TRACKER_DB_PATH = os.path.join("db", "inactivity_tracker.db")

def _get_db_conn():
    """Establishes a connection to the SQLite tracker DB."""
    conn = sqlite3.connect(_TRACKER_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row  # Access columns by name
    return conn

def _init_tracker_db():
    """Initializes the tracker DB and creates the table if it doesn't exist."""
    try:
        with _tracker_db_lock:
            with _get_db_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS activity (
                        wa_id TEXT PRIMARY KEY,
                        last_activity_epoch REAL NOT NULL,
                        crm_fired INTEGER NOT NULL DEFAULT 0,
                        openai_fired INTEGER NOT NULL DEFAULT 0,
                        enrollment_fired INTEGER NOT NULL DEFAULT 0
                    )
                """)
                conn.commit()
    except Exception as e:
        logging.error(f"Failed to initialize inactivity tracker database: {e}")


# Background checker thread
_checker_thread: Optional[_Process] = None
_stop_checker = _Event()
_CHECK_INTERVAL_SECONDS = 300  # 5 minutes


# Independent timers for OpenAI conversation reset and Enrollment reset
_openai_reset_seconds: int = 0
_enrollment_reset_seconds: int = 0

# Local DB directory for responses_db used by delete_response_id
DB_DIRECTORY = "db"
if not os.path.exists(DB_DIRECTORY):
    try:
        os.makedirs(DB_DIRECTORY)
    except Exception:
        pass


def init_scheduler(app) -> None:
    """Initialize inactivity scheduler using app config.

    Reads CRM_AUTO_UPLOAD_INACTIVITY_MINUTES from Flask config, defaults to 5.
    """
    try:
        # Initialize the SQLite database first
        _init_tracker_db()
        
        minutes_val = app.config.get("CRM_AUTO_UPLOAD_INACTIVITY_MINUTES")
        seconds: int = 300
        if isinstance(minutes_val, (int, float)):
            seconds = int(float(minutes_val) * 60)
        elif isinstance(minutes_val, str) and minutes_val.strip():
            # Allow values like "5", "5m", "0.5h"
            txt = minutes_val.strip().lower()
            if txt.endswith("h"):
                seconds = int(float(txt[:-1]) * 3600)
            elif txt.endswith("m"):
                seconds = int(float(txt[:-1]) * 60)
            else:
                seconds = int(float(txt) * 60)
        global _inactivity_seconds
        _inactivity_seconds = max(60, seconds)  # minimum 60s safeguard
        app.logger.info(f"CRM auto-upload inactivity set to {_inactivity_seconds}s")

        # Parse optional auto-reset windows for OpenAI and enrollment
        def _parse_minutes_to_seconds(value: Optional[str | int | float]) -> int:
            if value is None:
                return 0
            if isinstance(value, (int, float)):
                try:
                    return max(60, int(float(value) * 60))
                except Exception:
                    return 0
            if isinstance(value, str):
                txtv = value.strip().lower()
                if not txtv:
                    return 0
                try:
                    if txtv.endswith("h"):
                        return max(60, int(float(txtv[:-1]) * 3600))
                    if txtv.endswith("m"):
                        return max(60, int(float(txtv[:-1]) * 60))
                    return max(60, int(float(txtv) * 60))
                except Exception:
                    return 0
            return 0

        global _openai_reset_seconds, _enrollment_reset_seconds
        unified = app.config.get("AUTO_RESET_INACTIVITY_MINUTES")
        if unified is not None and str(unified).strip():
            seconds_unified = _parse_minutes_to_seconds(unified)
            _openai_reset_seconds = seconds_unified
            _enrollment_reset_seconds = seconds_unified
        else:
            # Backward compatibility fallbacks
            _openai_reset_seconds = _parse_minutes_to_seconds(
                app.config.get("OPENAI_AUTO_RESET_INACTIVITY_MINUTES")
            )
            _enrollment_reset_seconds = _parse_minutes_to_seconds(
                app.config.get("ENROLLMENT_AUTO_RESET_INACTIVITY_MINUTES")
            )
        if _openai_reset_seconds:
            app.logger.info(f"OpenAI conversation auto-reset inactivity set to {_openai_reset_seconds}s")
        else:
            app.logger.info("OpenAI conversation auto-reset disabled")
        if _enrollment_reset_seconds:
            app.logger.info(f"Enrollment auto-reset inactivity set to {_enrollment_reset_seconds}s")
        else:
            app.logger.info("Enrollment auto-reset disabled")
        
        # Start the persistent periodic checker for all inactivity tasks
        start_periodic_inactivity_checker()
    except Exception as e:
        app.logger.error(f"Failed to initialize inactivity scheduler: {e}")


def mark_crm_data_changed(wa_id: str) -> None:
    """Mark that CRM-relevant data has changed for a user, enabling auto-upload."""
    if not wa_id:
        logging.warning("mark_crm_data_changed called with empty wa_id")
        return
    try:
        ctx = get_enrollment_context(wa_id) or {}
        logging.info(f"📝 Marking CRM data as changed for {wa_id}, current context keys: {list(ctx.keys())}")
        ctx["crm_data_changed"] = True
        set_enrollment_context(wa_id, ctx)
        logging.info(f"📝 CRM data marked as changed for {wa_id} - context updated")
    except Exception as e:
        logging.error(f"Failed to mark CRM data as changed for {wa_id}: {e}")


def mark_activity(wa_id: str) -> None:
    """Record activity for a wa_id, resetting all inactivity timers."""
    if not wa_id:
        return
    now_epoch = time.time()

    # Persist last activity time and reset all trigger flags for the periodic checker
    try:
        with _tracker_db_lock:
            with _get_db_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO activity (wa_id, last_activity_epoch, crm_fired, openai_fired, enrollment_fired)
                    VALUES (?, ?, 0, 0, 0)
                """, (wa_id, now_epoch))
                conn.commit()
        logging.info(f"📝 Marked activity for {wa_id} at epoch {now_epoch}, resetting all inactivity triggers.")
    except Exception as e:
        logging.error(f"Failed to write to inactivity_tracker DB for {wa_id}: {e}")


def _on_inactivity_fire_persistent(wa_id: str) -> None:
    """
    Called by the periodic checker for a conversation that has become inactive.
    Attempts to upload conversation data to the CRM.
    """
    try:
        logging.info(f"🚀 Processing CRM auto-upload for {wa_id} due to inactivity.")
        
        ctx = get_enrollment_context(wa_id) or {}
        
        # Build payload similar to whatsapp form handling
        payload = _build_crm_payload_from_context(wa_id, ctx)
        
        # Auto-upload simplification: always omit 'codigo_inscripcion' in inactivity uploads
        if "codigo_inscripcion" in payload:
            payload.pop("codigo_inscripcion", None)
        if "Codigo_inscripcion" in payload:
            payload.pop("Codigo_inscripcion", None)
        logging.info(f"⏭️ Auto-upload: omitting codigo_inscripcion for {wa_id} by design during inactivity uploads.")
        # For auto-upload, explicitly skip DNI requirement
        payload["_allow_missing_dni"] = True

        # Conditions:
        # 1) Required fields: Nombre and Telefono present (Email optional)
        nombre_ok = bool((payload.get("nombre") or payload.get("Nombre") or "").strip())
        telefono_ok = bool((payload.get("telefono") or payload.get("Telefono") or "").strip())
        
        if not (nombre_ok and telefono_ok):
            missing_fields = []
            if not nombre_ok:
                missing_fields.append("Nombre")
            if not telefono_ok:
                missing_fields.append("Teléfono")
            logging.info(f"❌ CRM auto-upload skipped for {wa_id}: missing required fields ({', '.join(missing_fields)})")
            return

        # 2) Only if data changed since last upload
        has_crm_changes = ctx.get("crm_data_changed", False)
        
        if not has_crm_changes:
            logging.info(f"⏭️ CRM auto-upload skipped for {wa_id}: no changes detected in enrollment data")
            return

        # 3) Submit to CRM
        try:
            from app.services.crm_service import inscribir_lead as _inscribir_lead
            result = _inscribir_lead(payload)
        except Exception as e:
            logging.error(f"CRM auto-upload unexpected error for {wa_id}: {e}")
            return

        if isinstance(result, dict) and result.get("codigo") == 200:
            # Update enrollment context markers
            try:
                current_ctx = get_enrollment_context(wa_id) or {}
                # Cleanup deprecated keys
                for k in ("last_crm_payload", "last_crm_payload_hash", "uploaded_to_crm", "last_crm_upload_at"):
                    if k in current_ctx:
                        current_ctx.pop(k, None)
                # Mark as uploaded and clear change flag
                current_ctx["ultima_subida_crm"] = _now_local_str()
                current_ctx["crm_data_changed"] = False
                set_enrollment_context(wa_id, current_ctx)
            except Exception as upd_err:
                logging.error(f"Failed to update enrollment markers after CRM upload for {wa_id}: {upd_err}")
            logging.info(f"✅ CRM auto-upload SUCCESS for {wa_id}")
        else:
            # Do not update success markers on failure; leave state so we can retry on next change
            try:
                desc = result.get("descripcion") if isinstance(result, dict) else str(result)
                logging.warning(f"CRM auto-upload FAILED for {wa_id}: {desc}")
            except Exception:
                logging.warning(f"CRM auto-upload FAILED for {wa_id}")
    except Exception as e:
        logging.error(f"Unexpected error in persistent inactivity handler for {wa_id}: {e}")


def _on_openai_inactivity_fire(wa_id: str) -> None:
    """Clear OpenAI previous_response_id after inactivity."""
    try:
        logging.info(f"🗑️ Clearing OpenAI conversation for {wa_id} due to inactivity")
        delete_response_id(DB_DIRECTORY, wa_id)
        logging.info(f"✅ OpenAI conversation reset SUCCESS for {wa_id}")
    except Exception as e:
        logging.error(f"❌ Unexpected error in OpenAI inactivity reset for {wa_id}: {e}")


def _on_enrollment_inactivity_fire(wa_id: str) -> None:
    """Clear enrollment context after inactivity."""
    try:
        logging.info(f"🗑️ Clearing enrollment context for {wa_id} due to inactivity")
        try:
            ok = clear_enrollment_context(wa_id)
            if ok:
                logging.info(f"✅ Enrollment context reset SUCCESS for {wa_id}")
            else:
                logging.warning(f"⚠️ Enrollment context reset returned False for {wa_id}")
        except Exception as cee:
            logging.error(f"❌ Error clearing enrollment context for {wa_id}: {cee}")
    except Exception as e:
        logging.error(f"❌ Unexpected error in Enrollment inactivity reset for {wa_id}: {e}")


def _build_crm_payload_from_context(wa_id: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
    telefono = _normalize_phone(wa_id)
    
    payload = {
        "nombre": ctx.get("nombre") or ctx.get("Nombre"),
        "apellidos": ctx.get("apellidos") or ctx.get("Apellidos"),
        "telefono": telefono,
        "email": ctx.get("email") or ctx.get("Email"),
        "dni": ctx.get("dni") or ctx.get("DNI") or ctx.get("nif"),
        "codigo_inscripcion": ctx.get("codigo_curso") or ctx.get("Codigo_inscripcion"),
        "situacion_laboral": ctx.get("situacion_laboral") or ctx.get("Situacion_laboral"),
        "direccion": ctx.get("direccion") or ctx.get("Dirección") or ctx.get("Direccion"),
        "provincia": ctx.get("provincia") or ctx.get("Provincia"),
        "titulacion": ctx.get("titulacion") or ctx.get("Titulacion") or ctx.get("nivel_formacion"),
        "sector": ctx.get("sector"),
    }
    return payload


def _normalize_phone(phone_like: Optional[str]) -> Optional[str]:
    if not phone_like:
        return None
    digits = ''.join(ch for ch in str(phone_like) if ch.isdigit())
    if digits.startswith("0034"):
        digits = digits[4:]
    elif digits.startswith("34"):
        digits = digits[2:]
    if len(digits) > 9:
        digits = digits[-9:]
    return digits




def _now_local_str() -> str:
    try:
        madrid_tz = ZoneInfo('Europe/Madrid')
    except Exception:
        madrid_tz = timezone.utc
    dt = datetime.now(madrid_tz)
    return dt.strftime('%Y-%m-%d - %H:%M:%S')


def _periodic_inactivity_checker():
    """Periodically checks for all conversations that are inactive and need actions."""
    while not _stop_checker.is_set():
        try:
            logging.info("🕵️ Running periodic inactivity check for all tasks...")
            
            records_to_process = []
            with _tracker_db_lock:
                try:
                    with _get_db_conn() as conn:
                        cursor = conn.cursor()
                        # Ensure table exists before trying to select
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='activity'")
                        if cursor.fetchone():
                            cursor.execute("SELECT * FROM activity")
                            records_to_process = cursor.fetchall()
                        else:
                            logging.warning("Inactivity 'activity' table not found. It might be initializing.")
                except Exception as db_err:
                    logging.error(f"Error reading from inactivity tracker DB: {db_err}")
                    records_to_process = []


            if not records_to_process:
                logging.info("🕵️ No pending conversations to check for inactivity.")
            else:
                logging.info(f"🕵️ Checking {len(records_to_process)} conversations for inactivity...")

            now_epoch = time.time()
            wa_ids_to_remove = []

            for record in records_to_process:
                wa_id = record['wa_id']
                try:
                    updates = {}
                    
                    elapsed_time = now_epoch - record['last_activity_epoch']

                    # --- 1. Check for CRM Auto-upload ---
                    if _inactivity_seconds > 0 and elapsed_time > _inactivity_seconds and not record['crm_fired']:
                        logging.info(f"🚀 CRM inactivity threshold reached for {wa_id}.")
                        _on_inactivity_fire_persistent(wa_id)
                        updates['crm_fired'] = 1

                    # --- 2. Check for OpenAI Conversation Reset ---
                    if _openai_reset_seconds > 0 and elapsed_time > _openai_reset_seconds and not record['openai_fired']:
                        logging.info(f"🚀 OpenAI reset inactivity threshold reached for {wa_id}.")
                        _on_openai_inactivity_fire(wa_id)
                        updates['openai_fired'] = 1
                    
                    # --- 3. Check for Enrollment Context Reset ---
                    if _enrollment_reset_seconds > 0 and elapsed_time > _enrollment_reset_seconds and not record['enrollment_fired']:
                        logging.info(f"🚀 Enrollment reset inactivity threshold reached for {wa_id}.")
                        _on_enrollment_inactivity_fire(wa_id)
                        updates['enrollment_fired'] = 1
                    
                    if updates:
                        with _tracker_db_lock:
                            with _get_db_conn() as conn:
                                set_clause = ", ".join([f"{key} = ?" for key in updates.keys()])
                                values = list(updates.values()) + [wa_id]
                                conn.execute(f"UPDATE activity SET {set_clause} WHERE wa_id = ?", tuple(values))
                                conn.commit()

                    # --- 4. Check for Cleanup ---
                    # A record is done if all enabled timers have fired
                    crm_done = (record['crm_fired'] or updates.get('crm_fired')) or _inactivity_seconds <= 0
                    openai_done = (record['openai_fired'] or updates.get('openai_fired')) or _openai_reset_seconds <= 0
                    enrollment_done = (record['enrollment_fired'] or updates.get('enrollment_fired')) or _enrollment_reset_seconds <= 0

                    if crm_done and openai_done and enrollment_done:
                        wa_ids_to_remove.append(wa_id)

                except Exception as e:
                    logging.error(f"Error checking inactivity for {wa_id}: {e}")

            if wa_ids_to_remove:
                with _tracker_db_lock:
                    with _get_db_conn() as conn:
                        conn.executemany("DELETE FROM activity WHERE wa_id = ?", [(wid,) for wid in wa_ids_to_remove])
                        conn.commit()
                logging.info(f"🧹 Cleaned up {len(wa_ids_to_remove)} fully processed conversations from inactivity tracker: {wa_ids_to_remove}")

            logging.info("✅ Periodic inactivity check finished.")

        except Exception as e:
            logging.error(f"Unexpected error in periodic inactivity checker: {e}")
        
        _stop_checker.wait(_CHECK_INTERVAL_SECONDS)


def start_periodic_inactivity_checker():
    """Starts the background thread for periodic checks."""
    global _checker_thread
    if _checker_thread is None or not _checker_thread.is_alive():
        _stop_checker.clear()
        _checker_thread = _Process(target=_periodic_inactivity_checker, daemon=True)
        _checker_thread.start()
        logging.info("🚀 Started periodic inactivity checker thread.")

def stop_periodic_inactivity_checker():
    """Stops the background checker thread."""
    global _checker_thread
    if _checker_thread and _checker_thread.is_alive():
        _stop_checker.set()
        _checker_thread.join(timeout=5)
        _checker_thread = None
        logging.info("🛑 Stopped periodic inactivity checker thread.")


