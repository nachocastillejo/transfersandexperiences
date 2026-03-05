import os
import shelve
import logging
from typing import Any, Dict
try:
    from flask import current_app
except Exception:  # pragma: no cover
    current_app = None

from app.services.supabase_service import is_supabase_enabled, fetch_enrollment_context as supa_fetch_enrollment_context, merge_enrollment_context as supa_merge_enrollment_context, upsert_enrollment_context as supa_upsert_enrollment_context, clear_enrollment_context_row as supa_clear_enrollment_context_row

DEFAULT_PROJECT_NAME = os.getenv('ENV_NAME')

DB_DIRECTORY = "db"
STATE_FILENAME = "enrollment_state"

def _get_state_path() -> str:
    if not os.path.exists(DB_DIRECTORY):
        os.makedirs(DB_DIRECTORY)
    return os.path.join(DB_DIRECTORY, STATE_FILENAME)

def get_enrollment_context(wa_id: str, project_name: str | None = None) -> Dict[str, Any]:
    try:
        if is_supabase_enabled():
            effective_project = project_name if project_name is not None else DEFAULT_PROJECT_NAME
            return supa_fetch_enrollment_context(wa_id, effective_project)
        # Local store: scope by phone number id as part of the key
        try:
            phone_number_id = current_app.config.get('PHONE_NUMBER_ID') if current_app else os.getenv('PHONE_NUMBER_ID')
        except Exception:
            phone_number_id = os.getenv('PHONE_NUMBER_ID')
        key = f"{phone_number_id or ''}::{wa_id}"
        with shelve.open(_get_state_path()) as db:
            return dict(db.get(key, {}))
    except Exception as e:
        logging.error(f"Error reading enrollment context for {wa_id}: {e}")
        return {}

def _publish_enrollment_update(wa_id: str, context: Dict[str, Any]) -> None:
    """Publish a realtime enrollment update event for this conversation."""
    try:
        # Supabase realtime will automatically trigger when enrollment_contexts table changes
        # No need for custom realtime_bus - Supabase handles this via postgres_changes
        logging.info(f"📡 Enrollment context updated for {wa_id} with {len(context)} fields - Supabase realtime will handle dashboard updates")
    except Exception as e:
        # Fail quietly; realtime is best-effort
        logging.error(f"Failed to log enrollment update for {wa_id}: {e}")

def set_enrollment_context(wa_id: str, data: Dict[str, Any], project_name: str | None = None) -> bool:
    try:
        if is_supabase_enabled():
            effective_project = project_name if project_name is not None else DEFAULT_PROJECT_NAME
            ok = supa_upsert_enrollment_context(wa_id, effective_project, dict(data or {})) is not None
            if ok:
                _publish_enrollment_update(wa_id, data or {})
            return ok
        try:
            phone_number_id = current_app.config.get('PHONE_NUMBER_ID') if current_app else os.getenv('PHONE_NUMBER_ID')
        except Exception:
            phone_number_id = os.getenv('PHONE_NUMBER_ID')
        key = f"{phone_number_id or ''}::{wa_id}"
        with shelve.open(_get_state_path(), writeback=True) as db:
            db[key] = dict(data or {})
        _publish_enrollment_update(wa_id, data or {})
        return True
    except Exception as e:
        logging.error(f"Error setting enrollment context for {wa_id}: {e}")
        return False

def update_enrollment_context(wa_id: str, partial: Dict[str, Any], project_name: str | None = None) -> bool:
    try:
        if is_supabase_enabled():
            effective_project = project_name if project_name is not None else DEFAULT_PROJECT_NAME
            ok = supa_merge_enrollment_context(wa_id, effective_project, partial or {})
            if ok:
                try:
                    ctx_now = supa_fetch_enrollment_context(wa_id, effective_project)
                except Exception:
                    ctx_now = None
                _publish_enrollment_update(wa_id, ctx_now or {})
            return ok
        try:
            phone_number_id = current_app.config.get('PHONE_NUMBER_ID') if current_app else os.getenv('PHONE_NUMBER_ID')
        except Exception:
            phone_number_id = os.getenv('PHONE_NUMBER_ID')
        key = f"{phone_number_id or ''}::{wa_id}"
        with shelve.open(_get_state_path(), writeback=True) as db:
            current = dict(db.get(key, {}))
            # Remove keys explicitly set to None; update others
            for k, v in (partial or {}).items():
                if v is None:
                    if k in current:
                        del current[k]
                else:
                    current[k] = v
            db[key] = current
        _publish_enrollment_update(wa_id, current)
        return True
    except Exception as e:
        logging.error(f"Error updating enrollment context for {wa_id}: {e}")
        return False

def clear_enrollment_context(wa_id: str, project_name: str | None = None) -> bool:
    try:
        if is_supabase_enabled():
            effective_project = project_name if project_name is not None else DEFAULT_PROJECT_NAME
            logging.info(f"🗑️ Attempting to clear enrollment context for {wa_id} in project {effective_project}")
            
            # Preserve ultima_subida_crm and inscripciones when clearing
            current_ctx = get_enrollment_context(wa_id) or {}
            preserved_data = {}
            preserved_fields = []
            if "ultima_subida_crm" in current_ctx:
                preserved_data["ultima_subida_crm"] = current_ctx["ultima_subida_crm"]
                preserved_fields.append("ultima_subida_crm")
            if "inscripciones" in current_ctx:
                preserved_data["inscripciones"] = current_ctx["inscripciones"]
                preserved_fields.append("inscripciones")
            if preserved_fields:
                logging.info(f"🔄 Preserving fields: {', '.join(preserved_fields)}")
            
            # Instead of DELETE, use UPSERT with preserved data to trigger realtime
            ok = supa_upsert_enrollment_context(wa_id, effective_project, preserved_data)
            if ok:
                logging.info(f"✅ Successfully cleared enrollment context for {wa_id} (preserving {', '.join(preserved_fields) if preserved_fields else 'nothing'}) - Supabase realtime should trigger via UPSERT")
                # Emit realtime update so dashboard refreshes captured data without reload
                _publish_enrollment_update(wa_id, preserved_data)
            else:
                logging.error(f"❌ Failed to clear enrollment context for {wa_id}")
            return ok
        try:
            phone_number_id = current_app.config.get('PHONE_NUMBER_ID') if current_app else os.getenv('PHONE_NUMBER_ID')
        except Exception:
            phone_number_id = os.getenv('PHONE_NUMBER_ID')
        key = f"{phone_number_id or ''}::{wa_id}"
        with shelve.open(_get_state_path(), writeback=True) as db:
            if key in db:
                # Preserve ultima_subida_crm and inscripciones when clearing
                current_data = dict(db[key])
                preserved_data = {}
                preserved_fields = []
                if "ultima_subida_crm" in current_data:
                    preserved_data["ultima_subida_crm"] = current_data["ultima_subida_crm"]
                    preserved_fields.append("ultima_subida_crm")
                if "inscripciones" in current_data:
                    preserved_data["inscripciones"] = current_data["inscripciones"]
                    preserved_fields.append("inscripciones")
                if preserved_fields:
                    logging.info(f"🔄 Preserving fields in local DB: {', '.join(preserved_fields)}")
                    db[key] = preserved_data
                else:
                    del db[key]
        # Emit realtime update for local store as well
        _publish_enrollment_update(wa_id, preserved_data if 'preserved_data' in locals() else {})
        return True
    except Exception as e:
        logging.error(f"Error clearing enrollment context for {wa_id}: {e}")
        return False


