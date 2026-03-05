import shelve
import os
import logging
import time
import threading
from datetime import datetime, timedelta

DB_DIRECTORY = "db"
AUTOMATION_DB_FILE = os.path.join(DB_DIRECTORY, "automation_status_db.dat")

# Ensure the db directory exists
if not os.path.exists(DB_DIRECTORY):
    try:
        os.makedirs(DB_DIRECTORY)
    except OSError as e:
        logging.error(f"Error creating directory {DB_DIRECTORY}: {e}")

def pause_automation(wa_id: str, reason: str = "Paused via system", duration_minutes: int | None = None) -> bool:
    """
    Pauses automation for a given wa_id.
    Stores status, reason, timestamp, and optional expiry_timestamp.
    """
    try:
        with shelve.open(AUTOMATION_DB_FILE, writeback=True) as db:
            pause_details_entry = {
                "status": "paused",
                "reason": reason,
                "timestamp": time.time(),
                "duration_minutes": duration_minutes,
                "expiry_timestamp": (time.time() + duration_minutes * 60) if duration_minutes else None
            }
            db[wa_id] = pause_details_entry
        expiry_info = f"until {datetime.fromtimestamp(pause_details_entry['expiry_timestamp']).strftime('%Y-%m-%d %H:%M:%S')}" if pause_details_entry['expiry_timestamp'] else "indefinitely"
        logging.info(f"Automation paused for {wa_id}. Reason: {reason}. Duration: {duration_minutes or 'indefinite'} minutes ({expiry_info}).")
        return True
    except Exception as e:
        logging.error(f"Error pausing automation for {wa_id} in {AUTOMATION_DB_FILE}: {e}")
        return False

def resume_automation(wa_id: str) -> bool:
    """
    Resumes automation for a given wa_id by deleting their entry.
    """
    try:
        with shelve.open(AUTOMATION_DB_FILE, writeback=True) as db:
            if wa_id in db:
                del db[wa_id]
                logging.info(f"Automation manually resumed for {wa_id}.")
                return True
            else:
                logging.info(f"Attempted to manually resume automation for {wa_id}, but it was not paused or had already expired.")
                return False
    except Exception as e:
        logging.error(f"Error resuming automation for {wa_id} in {AUTOMATION_DB_FILE}: {e}")
        return False

def is_automation_paused(wa_id: str) -> bool:
    """
    Checks if automation is currently paused for a given wa_id.
    Considers timed pauses and automatically resumes if expired.
    
    When Supabase is enabled, the conversation 'mode' field is the source of truth:
    - mode == 'agent' → bot is paused
    - mode == 'bot' → bot is active (NOT paused)
    
    The local shelve store is only used as fallback when Supabase is not enabled
    or when the Supabase check fails.
    """
    try:
        # If Supabase is enabled, use conversation "mode" as the source of truth.
        # In Supabase, mode == 'agent' implies the bot must be paused for this wa_id.
        # mode == 'bot' means the bot is active and should NOT be paused.
        try:
            from app.services.supabase_service import is_supabase_enabled, fetch_conversation_mode_map  # local import to avoid circular deps
            if is_supabase_enabled():
                try:
                    mode_map = fetch_conversation_mode_map([wa_id]) or {}
                    mode_value = mode_map.get(wa_id)
                    if mode_value == 'agent':
                        logging.debug(f"Automation check (Supabase): {wa_id} is in 'agent' mode → paused.")
                        return True
                    elif mode_value == 'bot':
                        # Explicitly in 'bot' mode - bot is active, NOT paused
                        logging.debug(f"Automation check (Supabase): {wa_id} is in 'bot' mode → active.")
                        return False
                    # If mode is None or unknown, fall through to local store check
                except Exception as e:
                    logging.debug(f"Automation Supabase mode check failed for {wa_id}: {e}")
        except Exception:
            # If Supabase helpers are unavailable, fall back to local store
            pass

        with shelve.open(AUTOMATION_DB_FILE, writeback=True) as db: # writeback=True for auto-cleanup
            user_pause_info = db.get(wa_id)
            if user_pause_info and user_pause_info.get("status") == "paused":
                expiry_ts = user_pause_info.get("expiry_timestamp")
                if expiry_ts and time.time() > expiry_ts:
                    # Pause has expired
                    del db[wa_id]
                    logging.info(f"Timed pause for {wa_id} expired. Automation automatically resumed.")
                    return False # No longer paused
                # Still paused (either indefinite or timed pause not yet expired)
                logging.debug(f"Automation check: {wa_id} is currently paused. Reason: {user_pause_info.get('reason')}")
                return True
            return False # Not paused or no entry
    except Exception as e:
        logging.error(f"Error checking/cleaning automation status for {wa_id} in {AUTOMATION_DB_FILE}: {e}")
        return False # Fail safe

def get_pause_details(wa_id: str) -> dict | None:
    """
    Retrieves the pause details for a given wa_id, if currently paused.
    This will reflect auto-resumption if a timed pause has expired due to is_automation_paused being called.
    """
    if not is_automation_paused(wa_id): # This call also handles expiry cleanup
        return None
    # If still paused after the check, retrieve the details again
    try:
        with shelve.open(AUTOMATION_DB_FILE) as db:
            return db.get(wa_id) # Return the potentially updated state
    except Exception as e:
        logging.error(f"Error retrieving post-check pause details for {wa_id}: {e}")
        return None 


# --- Timed revert to Supabase 'bot' mode management ---
_revert_timers_lock = threading.Lock()
_revert_timers: dict[str, threading.Timer] = {}


def cancel_mode_revert_timer(wa_id: str) -> None:
    """Cancel any scheduled mode revert timer for this wa_id (no error if none)."""
    try:
        with _revert_timers_lock:
            t = _revert_timers.pop(wa_id, None)
        if t is not None:
            try:
                t.cancel()
            except Exception:
                pass
    except Exception as e:
        logging.debug(f"cancel_mode_revert_timer error for {wa_id}: {e}")


def _revert_mode_to_bot(wa_id: str) -> None:
    """Background task: attempt to revert conversation mode to 'bot' in Supabase and clear local pause."""
    try:
        # Best-effort Supabase update
        try:
            from app.services.supabase_service import is_supabase_enabled, update_conversation_mode_for_wa
            if is_supabase_enabled():
                ok = update_conversation_mode_for_wa(wa_id, 'bot')
                if not ok:
                    logging.error(f"Timed revert failed to update Supabase mode to 'bot' for {wa_id}")
            else:
                logging.debug("Supabase not enabled; timed revert only clears local pause state.")
        except Exception as sb_err:
            logging.error(f"Timed revert Supabase error for {wa_id}: {sb_err}")

        # Clear local pause entry regardless, since timed pause has ended
        try:
            resume_automation(wa_id)
        except Exception:
            pass
    finally:
        # Ensure timer reference is cleaned up
        try:
            with _revert_timers_lock:
                _revert_timers.pop(wa_id, None)
        except Exception:
            pass


def schedule_mode_revert(wa_id: str, duration_minutes: int) -> None:
    """Schedule a revert to 'bot' mode after duration_minutes. Replaces any existing timer for this wa_id."""
    if not wa_id:
        return
    try:
        seconds = max(1, int(duration_minutes * 60))
    except Exception:
        seconds = 60
    # Replace prior timer if any
    cancel_mode_revert_timer(wa_id)
    try:
        timer = threading.Timer(seconds, _revert_mode_to_bot, args=(wa_id,))
        timer.daemon = True
        with _revert_timers_lock:
            _revert_timers[wa_id] = timer
        timer.start()
        logging.info(f"Scheduled timed revert to 'bot' mode for {wa_id} in {seconds}s")
    except Exception as e:
        logging.error(f"Failed to schedule mode revert for {wa_id}: {e}")