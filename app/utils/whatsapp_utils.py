import logging
import json
import time
import shelve
import re
import os
import requests
import threading
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import current_app, jsonify
import unicodedata
try:
    # Importar mapa de provincias del servicio CRM para reutilizar nombres oficiales
    from app.services.crm_service import PROVINCIAS_MAP
except Exception:
    PROVINCIAS_MAP = {}
# Updated import for the database logger
from app.utils.message_logger import log_message_to_db 
# Importamos funciones de otros servicios
from app.services.openai_service import generate_response
from app.services.extra_service import add_current_date_to_question, add_dates_to_question
# Importar funciones de mensajería desde el nuevo módulo
from app.utils.messaging_utils import (
    get_text_message_input,
    get_button_message_input,
    get_list_message_input,
    get_template_message_input,
    get_flow_message_input,
    send_message,
    send_typing_indicator,
)
from app.utils.extra_utils import delete_response_id, log_sector_activity_choice, read_fast_message_rules, read_sector_definitions
from app.utils.enrollment_state import (
    get_enrollment_context,
    update_enrollment_context,
    clear_enrollment_context,
    set_enrollment_context,
)
from app.utils.inactivity_scheduler import mark_activity, _now_local_str
from app.services.supabase_service import (
    is_supabase_enabled as _sb_on,
    update_conversation_mode_for_wa as _sb_update_mode,
    update_conversation_attention_for_wa as _sb_update_attention,
    update_conversation_estado_for_wa as _sb_update_estado,
    update_conversation_assigned_queues_for_wa as _sb_update_queues,
    fetch_last_inbound_timestamp as _sb_fetch_last_inbound,
    try_acquire_processing_lock as _sb_try_acquire_lock,
    get_and_clear_pending_messages_atomic as _sb_get_pending_messages,
    release_processing_lock_atomic as _sb_release_lock,
    fetch_message_by_wamid as _sb_fetch_message_by_wamid,
)
from app.services.supabase_storage import upload_file_to_storage
from app.utils import whatsapp_interactive_utils

# Directorio para almacenar la base de datos (usada para guardar el estado de threads)
DB_DIRECTORY = "db"
if not os.path.exists(DB_DIRECTORY):
    os.makedirs(DB_DIRECTORY)
# Directorio temporal para descargar audios
AUDIO_TEMP_DIR = os.path.join(DB_DIRECTORY, "audio_temp")
if not os.path.exists(AUDIO_TEMP_DIR):
    os.makedirs(AUDIO_TEMP_DIR)

# Cargar las definiciones de sectores una vez al iniciar
SECTOR_DEFINITIONS_TEXT = read_sector_definitions()

# --- Funciones para el buffer de mensajes pendientes (concatenación) ---
# Note: add_pending_message is now done atomically via _sb_try_acquire_lock RPC

def get_and_clear_pending_messages(wa_id: str) -> list:
    """
    Obtiene y limpia el buffer de mensajes pendientes para un wa_id.
    Uses atomic RPC to prevent race conditions.
    Devuelve una lista de mensajes pendientes.
    """
    try:
        # Use atomic RPC to get and clear pending messages
        pending = _sb_get_pending_messages(wa_id)
        
        if pending and len(pending) > 0:
            logging.info(f"📤 Retrieved and cleared {len(pending)} pending messages atomically for {wa_id}")
        
        return pending if isinstance(pending, list) else []
    except Exception as e:
        logging.error(f"Error getting pending messages for {wa_id}: {e}")
        return []


def get_reply_context_text(message_info: dict) -> str | None:
    """
    Si el usuario está respondiendo a un mensaje específico (por ejemplo, una plantilla),
    busca el mensaje original en la BD y devuelve su texto para añadirlo al contexto de OpenAI.
    
    WhatsApp incluye un campo 'context' con 'id' cuando el usuario responde a un mensaje.
    """
    try:
        context = message_info.get("context")
        if not context:
            return None
        
        # El campo 'id' contiene el whatsapp_message_id del mensaje al que responde
        replied_to_wamid = context.get("id")
        if not replied_to_wamid:
            return None
        
        logging.info(f"📎 User is replying to message: {replied_to_wamid}")
        
        # Buscar el mensaje original en la base de datos
        original_message = _sb_fetch_message_by_wamid(replied_to_wamid)
        if not original_message:
            logging.warning(f"Could not find original message {replied_to_wamid} in database")
            return None
        
        original_text = original_message.get("message_text", "").strip()
        if not original_text:
            return None
        
        # Determinar si fue un mensaje del bot o del usuario
        direction = original_message.get("direction", "")
        if direction.startswith("outbound"):
            prefix = "[El usuario está respondiendo a este mensaje que le enviaste anteriormente]"
        else:
            prefix = "[El usuario está respondiendo a este mensaje anterior]"
        
        logging.info(f"📎 Found reply context: {original_text[:50]}...")
        return f"{prefix}:\n\"{original_text}\""
    except Exception as e:
        logging.error(f"Error getting reply context: {e}")
        return None


def consolidate_messages(base_message: str, pending_messages: list) -> str:
    """
    Consolida el mensaje base con los mensajes pendientes del buffer.
    Los mensajes se concatenan con saltos de línea.
    """
    if not pending_messages:
        return base_message
    
    all_messages = [base_message] if base_message else []
    
    # Ordenar mensajes pendientes por timestamp
    sorted_pending = sorted(pending_messages, key=lambda x: x.get("timestamp", 0))
    
    for msg in sorted_pending:
        text = msg.get("text", "").strip()
        if text:
            all_messages.append(text)
    
    consolidated = "\n".join(all_messages)
    logging.info(f"📝 Consolidated {len(pending_messages)} pending messages into one")
    return consolidated


# --- Funciones auxiliares para la extracción de datos y clasificación ---

def _maybe_reset_conversation_after_24h(wa_id: str) -> None:
    """Si el último inbound fue hace más de 24h, resetea estado/mode/attention/colas.

    - estado_conversacion → 'Abierta'
    - mode → 'bot'
    - needs_attention → False (atendida)
    - assigned_queue_ids → [] (Sin cola)
    """
    try:
        if not _sb_on():
            return
        last_inbound = _sb_fetch_last_inbound(wa_id)
        # Si nunca ha habido inbound, no hacemos ningún reset (primer mensaje de la conversación)
        if last_inbound is None:
            return

        now_local = datetime.now(ZoneInfo('Europe/Madrid'))
        # Coerce to tz-aware Europe/Madrid if naive
        if getattr(last_inbound, 'tzinfo', None) is None:
            last_inbound = last_inbound.replace(tzinfo=ZoneInfo('Europe/Madrid'))
        delta = now_local - last_inbound
        # Solo reseteamos si han pasado más de 24h desde el último inbound
        if delta <= timedelta(hours=24):
            return

        # >24h desde el último inbound → reset de estado/mode/colas
        try:
            ok_status = _sb_update_estado(wa_id, 'Abierta')
            if ok_status:
                # Log a system message so the automatic reopen appears inline in the chat
                try:
                    try:
                        project_name_from_config = current_app.config.get("ENV_NAME", "Bot")
                    except Exception:
                        project_name_from_config = "Bot"
                    log_message_to_db(
                        wa_id=wa_id,
                        sender_name="System",
                        message_text='Estado actualizado a "Abierta" (reinicio automático por inactividad)',
                        direction='outbound_system',
                        project_name=project_name_from_config,
                    )
                except Exception as _log_reset_err:
                    logging.error(
                        f"Error logging auto-reset status change for {wa_id}: {_log_reset_err}"
                    )
        except Exception as e:
            logging.error(f"Failed to reset estado_conversacion for {wa_id}: {e}")
        try:
            _sb_update_mode(wa_id, 'bot')
        except Exception as e:
            logging.error(f"Failed to reset mode for {wa_id}: {e}")
        # Ensure local pause (agent mode) is cleared so the bot can answer immediately
        try:
            from app.utils.automation_manager import resume_automation as _resume_automation
            _resume_automation(wa_id)
        except Exception as e:
            logging.error(f"Failed to clear local automation pause for {wa_id}: {e}")
        try:
            _sb_update_attention(wa_id, False)
        except Exception as e:
            logging.error(f"Failed to reset needs_attention for {wa_id}: {e}")
        try:
            _sb_update_queues(wa_id, [])
        except Exception as e:
            logging.error(f"Failed to clear assigned queues for {wa_id}: {e}")
    except Exception as err:
        logging.error(f"Error evaluating 24h reset for {wa_id}: {err}")

def _extract_user_data(raw_question: str, wa_id: str):
    """
    Extrae email, nombre y provincia del texto de un mensaje y actualiza el contexto de inscripción.
    """
    if not raw_question or not isinstance(raw_question, str):
        return

    # Capturar email
    try:
        email_match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", raw_question)
        if email_match:
            user_email = email_match.group(0)
            update_enrollment_context(wa_id, {"email": user_email})
            logging.info(f"Captured email from user message for {wa_id}: {user_email}")
            # Mark CRM data as changed
            from app.utils.inactivity_scheduler import mark_crm_data_changed
            mark_crm_data_changed(wa_id)
    except Exception as cap_err:
        logging.error(f"Error capturing email from text for {wa_id}: {cap_err}")

    # Capturar nombre
    try:
        env_name_rule = current_app.config.get("ENV_NAME", "transfersandexperiences")
        rules = read_fast_message_rules(env_name_rule) or {}
        patterns = rules.get("patterns", {}) or {}
        names_list = [n.strip() for n in (patterns.get("names") or []) if isinstance(n, str) and n.strip()]
        if names_list:
            escaped_names = [re.escape(n) for n in names_list]
            names_regex = r"(?i)(?<!\w)(?:" + "|".join(escaped_names) + r")(?!\w)"
            m = re.search(names_regex, raw_question)
            if m:
                detected_name = m.group(0).strip()
                detected_lower = detected_name.lower()

                # Heuristic to avoid false positive for verb "leo" (e.g., "leo el periódico")
                skip_name = False
                if detected_lower == "leo":
                    after_text = raw_question[m.end():]
                    next_word_match = re.match(r"\s+([a-záéíóúñ]+)", after_text, flags=re.IGNORECASE)
                    if next_word_match:
                        next_word = next_word_match.group(1).lower()
                        if next_word in ("el", "la", "los", "las", "un", "una", "unos", "unas", "en", "de", "del", "al"):
                            logging.info(f"Skipping 'leo' as a name for {wa_id} due to context after match: '{next_word}'")
                            skip_name = True

                if not skip_name:
                    detected_to_store = detected_name if re.search(r"[A-ZÁÉÍÓÚÑ]", detected_name) else detected_name.title()
                    update_enrollment_context(wa_id, {"nombre": detected_to_store})
                    logging.info(f"Captured first name from user message for {wa_id}: {detected_to_store}")
                    # Mark CRM data as changed
                    from app.utils.inactivity_scheduler import mark_crm_data_changed
                    mark_crm_data_changed(wa_id)
    except Exception as name_err:
        logging.error(f"Error capturing name from text for {wa_id}: {name_err}")

    # Capturar provincia
    try:
        def _norm(s: str) -> str:
            s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
            return re.sub(r"[^a-z0-9]", "", s.lower())

        user_norm = _norm(raw_question)
        matched_prov = None
        for prov_name in PROVINCIAS_MAP.keys():
            if not isinstance(prov_name, str):
                continue
            if _norm(prov_name) in user_norm:
                matched_prov = prov_name
                break
        if matched_prov:
            update_enrollment_context(wa_id, {"provincia": matched_prov})
            logging.info(f"Captured province from user message for {wa_id}: {matched_prov}")
            # Mark CRM data as changed
            from app.utils.inactivity_scheduler import mark_crm_data_changed
            mark_crm_data_changed(wa_id)
    except Exception as prov_err:
        logging.error(f"Error capturing province from text for {wa_id}: {prov_err}")


def _handle_sector_classification(actividad_text: str, wa_id: str):
    """
    Clasifica el sector basado en el texto de la actividad, actualiza el contexto
    y devuelve el mensaje de notificación para el usuario.
    """
    sector_message_text_to_send = None
    try:
        ctx_pre = get_enrollment_context(wa_id)
        awaiting_activity = bool(ctx_pre.get("awaiting_activity"))
        sector_already_notified = bool(ctx_pre.get("sector_notified"))
        
        if awaiting_activity and not sector_already_notified:
            actividad_text = (actividad_text or "").strip()
            sector_list = [
                "Act. Físico Deportivas", "Administración y gestión", "Agrario", "Comercio", "Construcción",
                "Economía e Industria Digital (Teleco)", "Educación", "Energía", "Finanzas", "Gran distribución (Almacenes)",
                "Industria Alimentaria", "Información y Comunicación y Artes Gráficas", "Marítima y actividades porturias",
                "Metal", "Pesca", "Química / Laboratorio", "Sanidad", "Servicios (Otros)", "Servicios a las empresas",
                "Servicios Medioambientales", "Textil y Confección y Piel", "Transporte y Logística", "Turismo"
            ]
            try:
                system_instructions = (
                    "Eres un clasificador de sector. "
                    "Devuelve exactamente uno de estos sectores (texto exacto, sin explicaciones adicionales): "
                    + "; ".join(sector_list)
                    + ". Si no encaja claramente, devuelve 'Servicios (Otros)'.\n\n"
                    + "Definiciones para ayudarte a decidir con precisión:\n"
                    + (SECTOR_DEFINITIONS_TEXT.strip() if SECTOR_DEFINITIONS_TEXT else "")
                )
                resp = openai_client.responses.create(
                    model="gpt-5-mini",
                    instructions=system_instructions,
                    input=[{"role": "user", "content": f"Actividad de la empresa: {actividad_text}"}],
                )
                chosen = (getattr(resp, "output_text", "") or "").strip()
            except Exception as cls_err:
                logging.error(f"Error classifying sector via OpenAI for {wa_id}: {cls_err}")
                chosen = ""

            if chosen not in sector_list:
                chosen = "Servicios (Otros)"

            try:
                update_enrollment_context(wa_id, {"sector": chosen, "sector_notified": True, "awaiting_activity": False, "actividad_empresa": actividad_text})
                # Mark CRM data as changed
                from app.utils.inactivity_scheduler import mark_crm_data_changed
                mark_crm_data_changed(wa_id)
            except Exception as save_err:
                logging.error(f"Error saving classified sector for {wa_id}: {save_err}")
            
            try:
                log_sector_activity_choice(wa_id, actividad_text, chosen)
            except Exception as _csv_err:
                logging.error(f"Error logging sector activity CSV for {wa_id}: {_csv_err}")

            sector_message_text_to_send = f"Sector elegido: {chosen}"
    except Exception as act_err:
        logging.error(f"Error handling sector classification for {wa_id}: {act_err}")
    
    return sector_message_text_to_send

# --- Funciones auxiliares para manejo de Audio ---

def get_media_url(media_id):
    """Obtiene la URL de descarga de un archivo multimedia de WhatsApp."""
    access_token = current_app.config.get('ACCESS_TOKEN')
    version = current_app.config.get('VERSION')
    if not all([access_token, version]):
        logging.error("Error getting media URL: Missing WhatsApp API configuration.")
        return None
    
    url = f"https://graph.facebook.com/{version}/{media_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        media_url = data.get("url")
        if not media_url:
            logging.error(f"Failed to get media URL for ID {media_id}. Response: {data}")
            return None
        logging.info(f"Obtained media URL for ID {media_id}")
        return media_url
    except requests.Timeout:
        logging.error(f"Timeout occurred while getting media URL for ID {media_id}")
        return None
    except requests.RequestException as e:
        error_details = f"Status: {e.response.status_code}, Body: {e.response.text}" if e.response else str(e)
        logging.error(f"Request failed getting media URL for ID {media_id}: {error_details}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error getting media URL for ID {media_id}: {e}")
        return None

def download_media(media_url, save_path):
    """Descarga un archivo multimedia desde una URL."""
    access_token = current_app.config.get('ACCESS_TOKEN')
    if not access_token:
        logging.error("Error downloading media: Missing WhatsApp API Access Token.")
        return False

    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(media_url, headers=headers, stream=True, timeout=30) # Increased timeout for download
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"Media downloaded successfully to {save_path}")
        return True
    except requests.Timeout:
        logging.error(f"Timeout occurred while downloading media from URL: {media_url}")
        return False
    except requests.RequestException as e:
        error_details = f"Status: {e.response.status_code}" if e.response else str(e)
        logging.error(f"Request failed downloading media from URL {media_url}: {error_details}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error downloading media from {media_url}: {e}")
        return False

def transcribe_audio(file_path):
    """Transcribe un archivo de audio usando OpenAI Whisper."""
    try:
        with open(file_path, "rb") as audio_file:
            transcript = openai_client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file
            )
        logging.info(f"Audio transcribed successfully: {file_path}")
        return transcript.text
    except Exception as e:
        logging.error(f"Error during audio transcription for {file_path}: {e}")
        return None
    finally:
        # Clean up the downloaded file
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info(f"Cleaned up temporary audio file: {file_path}")
        except Exception as e:
            logging.error(f"Error cleaning up temporary audio file {file_path}: {e}")

# --- Fin Funciones auxiliares ---

def process_text_for_whatsapp(text):
    """
    Procesa el texto para adaptarlo al formato de WhatsApp:
      - Elimina patrones entre 【 y 】.
      - Convierte el formato de negritas de **texto** a *texto*.
    
    Args:
        text (str): Texto original.
        
    Returns:
        str: Texto formateado para WhatsApp.
    """
    # Eliminar patrones del tipo 【...】
    text = re.sub(r"\【.*?\】", "", text).strip()
    # Convertir **texto** en *texto*
    whatsapp_style_text = re.sub(r"\*\*(.*?)\*\*", r"*\1*", text)
    return whatsapp_style_text


def process_whatsapp_message(body):
    """
    Procesa un mensaje entrante de WhatsApp (texto o audio).
    Realiza los siguientes pasos:
      1. Extrae la información del remitente (wa_id, nombre) y el mensaje.
      2. Preprocesa el mensaje (por ejemplo, añadiendo fechas).
      3. Verifica si la automatización está pausada y, de ser así, notifica y termina.
      4. Llama a OpenAI para obtener la respuesta, incluyendo número de intentos y acción requerida.
      5. Adapta la respuesta al formato de WhatsApp y la envía.
      6. Envía la respuesta a Slack.
      7. Registra la conversación en la base de datos (inbound y outbound por separado).
    
    Args:
        body (dict): Cuerpo JSON recibido desde el webhook de WhatsApp.
    """
    from app.utils.slack_utils import send_message_slack
    from app.utils.automation_manager import is_automation_paused

    start_time = time.time()
    raw_question = None
    inbound_display_text = None  # What we will show in dashboard for inbound (user-visible selection)
    error_occurred_early = False # Flag to track if error happened before OpenAI call
    wa_message_id_sent_by_bot = None # Variable to store the sent message ID by bot
    name = "Desconocido"
    wa_id = "Desconocido"
    message_type = "Unknown"
    # Use ENV_NAME from config as the project/bot identifier
    project_name_from_config = current_app.config.get("ENV_NAME", "Bot") 
    message_timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Default to now
    # Defer sector announcement until after main reply is sent (ensures UI order)
    sector_message_text_to_send = None

    try:
        value = body["entry"][0]["changes"][0]["value"]
        contact_info = value["contacts"][0]
        message_info = value["messages"][0]
        
        wa_id = contact_info["wa_id"]
        name = contact_info["profile"]["name"]
        message_id_incoming = message_info.get("id")
        message_type = message_info.get("type")
        
        # >>> EXTRACT REPLY CONTEXT IF USER IS REPLYING TO A SPECIFIC MESSAGE <<<
        # This is used to provide context to OpenAI when user replies to a template/message
        reply_context_text = get_reply_context_text(message_info)

        # >>> ATOMIC LOCK ACQUISITION AND MESSAGE CONCATENATION <<<
        # Uses Supabase RPC for atomic lock acquisition to prevent race conditions
        # in multi-process environments (Linux uses multiprocessing)
        import os as _os_lock
        processing_lock_acquired = False
        worker_id = f"worker_{_os_lock.getpid()}_{int(time.time() * 1000)}"
        
        # Helper to release the lock (defined early so it can be used in any return path)
        def release_processing_lock():
            nonlocal processing_lock_acquired
            if processing_lock_acquired:
                try:
                    _sb_release_lock(wa_id, worker_id)
                except Exception as lock_rel_err:
                    logging.error(f"Error releasing processing lock for {wa_id}: {lock_rel_err}")
        
        try:
            # For text messages, try to acquire lock atomically with message buffering
            if message_type == "text":
                text_body = message_info.get("text", {}).get("body", "")
                if text_body and text_body.strip().lower() != "ping":
                    # Prepare message for buffering (in case lock is already held)
                    incoming_ts = message_info.get('timestamp')
                    message_to_buffer = {
                        "text": text_body,
                        "message_id": message_id_incoming,
                        "message_type": "text",
                        "timestamp": float(incoming_ts) if incoming_ts else time.time()
                    }
                    
                    # Try to acquire lock atomically - if already locked, message is buffered in one operation
                    # Lock duration: 30 seconds (same as stale check for quick recovery if worker dies)
                    lock_result = _sb_try_acquire_lock(
                        wa_id=wa_id,
                        worker_id=worker_id,
                        lock_duration_seconds=30,
                        message_to_buffer=message_to_buffer
                    )
                    
                    if lock_result.get("acquired"):
                        # We got the lock, proceed with processing
                        processing_lock_acquired = True
                    elif lock_result.get("buffered"):
                        # Message was buffered atomically, now log inbound and return
                        existing_worker_id = lock_result.get("existing_worker_id", "unknown")
                        
                        # Send typing indicator
                        if message_id_incoming:
                            try:
                                app_instance = current_app._get_current_object()
                                indicator_thread = threading.Thread(target=send_typing_indicator, args=(app_instance, message_id_incoming))
                                indicator_thread.daemon = True
                                indicator_thread.start()
                            except Exception as ti_err:
                                logging.error(f"Failed to send typing indicator for buffered message {wa_id}: {ti_err}")
                        
                        # Get timestamp for logging
                        buffered_timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        if incoming_ts:
                            try:
                                buffered_timestamp_str = datetime.fromtimestamp(int(incoming_ts)).strftime('%Y-%m-%d %H:%M:%S')
                            except ValueError:
                                pass
                        
                        # Log the inbound message to the database
                        try:
                            log_message_to_db(
                                wa_id=wa_id,
                                sender_name=name,
                                message_text=text_body,
                                direction='inbound',
                                project_name=project_name_from_config,
                                timestamp=buffered_timestamp_str,
                                whatsapp_message_id=message_id_incoming
                            )
                        except Exception as log_err:
                            logging.error(f"Error logging buffered inbound message for {wa_id}: {log_err}")
                        
                        # Mark activity and handle 24h reset
                        try:
                            mark_activity(wa_id)
                        except Exception:
                            pass
                        try:
                            _maybe_reset_conversation_after_24h(wa_id)
                        except Exception:
                            pass
                        
                        # Extract user data from message
                        try:
                            _extract_user_data(text_body, wa_id)
                        except Exception:
                            pass
                        
                        logging.info(f"✅ Message buffered atomically for {wa_id}. Worker {existing_worker_id} will process it.")
                        return  # Exit - the active worker will handle this message
                    else:
                        # Lock acquisition returned neither acquired nor buffered (shouldn't happen)
                        # Fallback: acquire lock normally
                        processing_lock_acquired = True
                        logging.warning(f"⚠️ Unexpected lock result for {wa_id}, proceeding anyway: {lock_result}")
                else:
                    # Ping message or empty - acquire lock without buffering
                    lock_result = _sb_try_acquire_lock(wa_id=wa_id, worker_id=worker_id)
                    processing_lock_acquired = lock_result.get("acquired", True)
            else:
                # Non-text messages: acquire lock, wait if necessary
                lock_result = _sb_try_acquire_lock(wa_id=wa_id, worker_id=worker_id)
                
                if lock_result.get("acquired"):
                    processing_lock_acquired = True
                else:
                    # Lock is held by another worker, wait for it to release
                    existing_worker_id = lock_result.get("existing_worker_id", "unknown")
                    logging.info(f"Message from {wa_id} received while processing lock is active (worker: {existing_worker_id}). Waiting for non-text message type...")
                    
                    retries = 0
                    while retries < 30:
                        time.sleep(1)
                        lock_result = _sb_try_acquire_lock(wa_id=wa_id, worker_id=worker_id)
                        if lock_result.get("acquired"):
                            processing_lock_acquired = True
                            logging.info(f"Lock for {wa_id} acquired after waiting. Proceeding with {message_type} message.")
                            break
                        retries += 1
                    
                    if not processing_lock_acquired:
                        logging.warning(f"Lock for {wa_id} did not release in time. Proceeding anyway with {message_type} message.")
                        processing_lock_acquired = True  # Proceed anyway
            
        except Exception as lock_err:
            logging.error(f"Error during atomic lock acquisition for {wa_id}: {lock_err}")
            # Fallback: proceed without lock guarantee
            processing_lock_acquired = True

        # Get incoming message timestamp if available
        incoming_ts = message_info.get('timestamp') 
        if incoming_ts:
            try:
                message_timestamp_str = datetime.fromtimestamp(int(incoming_ts)).strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                logging.warning(f"Could not parse incoming message timestamp '{incoming_ts}'. Defaulting to now.")

        if message_type == "text":
            # Mark inbound activity
            try:
                mark_activity(wa_id)
            except Exception:
                pass
            # Reset conversation fields if 24h window expired before this inbound
            try:
                _maybe_reset_conversation_after_24h(wa_id)
            except Exception as _reset_err:
                logging.error(f"24h reset check failed for text inbound {wa_id}: {_reset_err}")
            raw_question = message_info.get("text", {}).get("body")
            if not raw_question:
                 logging.warning(f"Received text message from {wa_id} with no body.")
                 error_occurred_early = True
            elif raw_question.strip().lower() == "ping":
                 logging.info(f"Ignoring 'ping' message from {wa_id} to avoid log spam.")
                 release_processing_lock()
                 return
            else:
                 inbound_display_text = raw_question
                 # Extraer datos del usuario (email, nombre, provincia)
                 _extract_user_data(raw_question, wa_id)

                 # Auto ON attention when inbound arrives and automation is paused (agent mode)
                 try:
                     from app.utils.automation_manager import is_automation_paused as _paused
                     if _paused(wa_id) and _sb_on():
                         try:
                             _sb_update_attention(wa_id, True)
                         except Exception as _e_att:
                             logging.error(f"Failed to auto-set needs_attention ON for {wa_id}: {_e_att}")
                 except Exception as _e_auto_on:
                     logging.error(f"Auto attention ON check failed for {wa_id}: {_e_auto_on}")

                 # Si se esperaba la actividad de la empresa, clasificar el sector
                 sector_message_text_to_send = _handle_sector_classification(raw_question, wa_id)

        elif message_type == "audio":
            try:
                mark_activity(wa_id)
            except Exception:
                pass
            # Reset conversation fields if 24h window expired before this inbound
            try:
                _maybe_reset_conversation_after_24h(wa_id)
            except Exception as _reset_err:
                logging.error(f"24h reset check failed for audio inbound {wa_id}: {_reset_err}")
            audio_id = message_info.get("audio", {}).get("id")
            if audio_id:
                logging.info(f"Processing audio message ID: {audio_id} from {wa_id}")
                media_url = get_media_url(audio_id)
                if media_url:
                    temp_filename = f"{audio_id}.ogg"
                    save_path = os.path.join(AUDIO_TEMP_DIR, temp_filename)
                    if download_media(media_url, save_path):
                        raw_question = transcribe_audio(save_path)
                        if raw_question is None:
                            logging.error(f"Transcription failed for audio {audio_id} from {wa_id}.")
                            raw_question = "[Error en transcripción de audio]"
                            error_occurred_early = True
                        else:
                            logging.info(f"Transcription result for {wa_id}: '{raw_question[:50]}...'")
                            inbound_display_text = raw_question
                            # Extraer datos del usuario (email, nombre, provincia) de la transcripción
                            _extract_user_data(raw_question, wa_id)
                            # Auto ON attention for inbound audio if paused
                            try:
                                from app.utils.automation_manager import is_automation_paused as _paused
                                if _paused(wa_id) and _sb_on():
                                    try:
                                        _sb_update_attention(wa_id, True)
                                    except Exception as _e_att:
                                        logging.error(f"Failed to auto-set needs_attention ON for {wa_id} (audio): {_e_att}")
                            except Exception as _e_auto_on:
                                logging.error(f"Auto attention ON (audio) check failed for {wa_id}: {_e_auto_on}")
                            # Si se esperaba la actividad de la empresa, clasificar el sector
                            sector_message_text_to_send = _handle_sector_classification(raw_question, wa_id)
                    else:
                        logging.error(f"Download failed for audio {audio_id} from {wa_id}.")
                        raw_question = "[Error en descarga de audio]"
                        error_occurred_early = True
                else:
                    logging.error(f"Could not get media URL for audio {audio_id} from {wa_id}.")
                    raw_question = "[Error obteniendo URL de audio]"
                    error_occurred_early = True
            else:
                logging.warning(f"Received audio message from {wa_id} with no audio ID.")
                raw_question = "[Mensaje de audio sin ID]"
                error_occurred_early = True
        elif message_type in ["image", "video", "document", "sticker"]:
            # Handle media messages (image, video, document, sticker)
            try:
                mark_activity(wa_id)
            except Exception:
                pass
            
            # Reset conversation fields if 24h window expired before this inbound
            try:
                _maybe_reset_conversation_after_24h(wa_id)
            except Exception as _reset_err:
                logging.error(f"24h reset check failed for {message_type} inbound {wa_id}: {_reset_err}")
            
            # Extract media information based on type
            media_info = message_info.get(message_type, {})
            media_id = media_info.get("id")
            media_mime_type = media_info.get("mime_type")
            media_filename = media_info.get("filename")  # May not exist for images/stickers
            caption = media_info.get("caption", "")  # Caption if provided
            
            if not media_id:
                logging.warning(f"Received {message_type} message from {wa_id} with no media ID.")
                raw_question = f"[{message_type.capitalize()} sin ID]"
                error_occurred_early = True
            else:
                logging.info(f"Processing {message_type} message ID: {media_id} from {wa_id}")
                
                # Get media URL from WhatsApp
                media_url = get_media_url(media_id)
                
                if media_url:
                    # Prepare display text for dashboard
                    type_emoji = {
                        "image": "📷",
                        "video": "🎥",
                        "document": "📄",
                        "sticker": "🎨"
                    }.get(message_type, "📎")
                    
                    if caption:
                        inbound_display_text = f"{type_emoji} {message_type.capitalize()}: {caption}"
                    else:
                        inbound_display_text = f"{type_emoji} {message_type.capitalize()}"
                    
                    if media_filename:
                        inbound_display_text += f" ({media_filename})"
                    
                    # If the user added a caption, treat it like a normal text message (bot should respond).
                    # Otherwise, keep legacy behavior: just log the media and don't respond.
                    _caption_text = (caption or "").strip()
                    if _caption_text:
                        raw_question = _caption_text
                        # Extraer datos del usuario (email, nombre, provincia) del caption
                        _extract_user_data(raw_question, wa_id)

                        # Auto ON attention when inbound arrives and automation is paused (agent mode)
                        try:
                            from app.utils.automation_manager import is_automation_paused as _paused
                            if _paused(wa_id) and _sb_on():
                                try:
                                    _sb_update_attention(wa_id, True)
                                except Exception as _e_att:
                                    logging.error(f"Failed to auto-set needs_attention ON for {wa_id} ({message_type} caption): {_e_att}")
                        except Exception as _e_auto_on:
                            logging.error(f"Auto attention ON check failed for {wa_id} ({message_type} caption): {_e_auto_on}")

                        # Si se esperaba la actividad de la empresa, clasificar el sector
                        sector_message_text_to_send = _handle_sector_classification(raw_question, wa_id)
                    else:
                        # For compatibility with rest of the flow, set raw_question to not be None
                        # but don't send it to OpenAI (we'll return early after logging)
                        raw_question = f"[Media: {message_type}]"
                    
                    # Download media and upload to Supabase Storage for permanent access
                    storage_url = None
                    media_size_bytes = None
                    
                    if _sb_on():
                        try:
                            # Generate filename if not provided
                            if not media_filename:
                                ext_map = {
                                    'image/jpeg': 'jpg',
                                    'image/png': 'png',
                                    'image/gif': 'gif',
                                    'image/webp': 'webp',
                                    'video/mp4': 'mp4',
                                    'video/3gpp': '3gp',
                                    'audio/ogg': 'ogg',
                                    'audio/mpeg': 'mp3',
                                    'audio/mp4': 'm4a',
                                }
                                ext = ext_map.get(media_mime_type, 'bin')
                                media_filename = f"{message_type}_{media_id}.{ext}"
                            
                            # Download to temp file
                            temp_dir = tempfile.gettempdir()
                            temp_path = os.path.join(temp_dir, f"wa_media_{media_id}_{int(time.time())}")
                            
                            if download_media(media_url, temp_path):
                                # Get file size
                                try:
                                    media_size_bytes = os.path.getsize(temp_path)
                                except Exception:
                                    pass
                                
                                # Upload to Supabase Storage
                                date_path = datetime.now().strftime('%Y/%m')
                                storage_path = f"inbound/{date_path}/{wa_id}_{int(time.time())}_{media_filename}"
                                storage_url = upload_file_to_storage(temp_path, storage_path, media_mime_type or 'application/octet-stream')
                                
                                if storage_url:
                                    logging.info(f"Media uploaded to Supabase Storage: {storage_url}")
                                else:
                                    logging.warning(f"Failed to upload media to Supabase Storage, using WhatsApp URL")
                                    storage_url = media_url  # Fallback to WhatsApp URL
                                
                                # Clean up temp file
                                try:
                                    if os.path.exists(temp_path):
                                        os.remove(temp_path)
                                except Exception as cleanup_err:
                                    logging.error(f"Error cleaning up temp media file: {cleanup_err}")
                            else:
                                logging.error(f"Failed to download media {media_id} from {wa_id}")
                                storage_url = media_url  # Fallback to WhatsApp URL
                        except Exception as upload_err:
                            logging.error(f"Error uploading media to Storage for {wa_id}: {upload_err}")
                            storage_url = media_url  # Fallback to WhatsApp URL
                    else:
                        # If Supabase is not enabled, use WhatsApp URL directly
                        storage_url = media_url
                    
                    # Log the media message to database with Storage URL
                    try:
                        log_message_to_db(
                            wa_id=wa_id,
                            sender_name=name,
                            message_text=inbound_display_text,
                            direction='inbound',
                            project_name=project_name_from_config,
                            timestamp=message_timestamp_str,
                            media_type=message_type,
                            media_url=storage_url,  # Use Storage URL instead of WhatsApp URL
                            media_filename=media_filename,
                            media_mime_type=media_mime_type,
                            media_size_bytes=media_size_bytes
                        )
                        logging.info(f"Media message logged for {wa_id}: {message_type}")
                    except Exception as log_err:
                        logging.error(f"Error logging media message for {wa_id}: {log_err}")
                    
                    # Notify Slack about the media message
                    try:
                        slack_msg = inbound_display_text
                        if caption:
                            slack_msg += f"\n{caption}"
                        send_message_slack(wa_id, name, slack_msg, "", bot_active=False)
                    except Exception as slack_err:
                        logging.error(f"Error notifying Slack for media message {wa_id}: {slack_err}")
                    
                    # Auto-set needs_attention if automation is paused
                    try:
                        from app.utils.automation_manager import is_automation_paused as _paused
                        if _paused(wa_id) and _sb_on():
                            try:
                                _sb_update_attention(wa_id, True)
                            except Exception as _e_att:
                                logging.error(f"Failed to auto-set needs_attention ON for {wa_id} ({message_type}): {_e_att}")
                    except Exception as _e_auto_on:
                        logging.error(f"Auto attention ON ({message_type}) check failed for {wa_id}: {_e_auto_on}")
                    
                    # If there's no caption, don't send media to OpenAI - just log and return.
                    # If there's a caption, continue through the normal flow so the bot replies to it.
                    if not _caption_text:
                        # If bot is active (not paused), send a message explaining we can't process media without text
                        # Only for image, video, and sticker - documents might be intentionally sent without text
                        if message_type in ["image", "video", "sticker"]:
                            try:
                                if not is_automation_paused(wa_id):
                                    no_media_msg = "¡Ups! Solo puedo entender mensajes de texto. Si quieres que te ayude, por favor añade unas palabras o una pequeña descripción 😊."
                                    no_media_data = get_text_message_input(wa_id, no_media_msg)
                                    no_media_wamid = send_message(no_media_data)
                                    log_message_to_db(
                                        wa_id=wa_id,
                                        sender_name="Bot",
                                        message_text=no_media_msg,
                                        direction='outbound_bot',
                                        project_name=project_name_from_config,
                                        whatsapp_message_id=no_media_wamid,
                                        status='sent'
                                    )
                                    logging.info(f"{message_type.capitalize()} without caption from {wa_id} - bot replied with media guidance.")
                            except Exception as _media_reply_err:
                                logging.error(f"Error sending no-caption media reply to {wa_id}: {_media_reply_err}")
                        
                        logging.info(f"{message_type.capitalize()} message from {wa_id} logged successfully. No bot response.")
                        release_processing_lock()
                        return
                    logging.info(f"{message_type.capitalize()} message from {wa_id} includes caption; continuing to bot processing.")
                else:
                    logging.error(f"Could not get media URL for {message_type} {media_id} from {wa_id}.")
                    raw_question = f"[Error obteniendo URL de {message_type}]"
                    error_occurred_early = True
        
        elif message_type == "interactive":
            # Reset conversation fields if 24h window expired before this inbound
            try:
                _maybe_reset_conversation_after_24h(wa_id)
            except Exception as _reset_err:
                logging.error(f"24h reset check failed for interactive inbound {wa_id}: {_reset_err}")

            # If automation is paused (Agent Mode), ignore interactive replies as well
            try:
                if is_automation_paused(wa_id):
                    inter = message_info.get("interactive", {})
                    inter_type = inter.get("type", "interactive")
                    desc = "[Respuesta interactiva]"
                    if inter_type == "button_reply":
                        desc = inter.get("button_reply", {}).get("title") or desc
                    elif inter_type == "list_reply":
                        desc = inter.get("list_reply", {}).get("title") or desc
                    elif inter_type == "nfm_reply":
                        desc = "[Formulario enviado]"

                    # Log inbound so it appears in dashboard
                    try:
                        log_message_to_db(
                            wa_id=wa_id,
                            sender_name=name,
                            message_text=desc,
                            direction='inbound',
                            project_name=project_name_from_config,
                            timestamp=message_timestamp_str
                        )
                    except Exception as _log_inter_paused_err:
                        logging.error(f"Error logging paused interactive inbound for {wa_id}: {_log_inter_paused_err}")

                    # Notify Slack without bot response
                    try:
                        send_message_slack(wa_id, name, desc.replace('\n',' ').replace('\r',' '), "", False)
                    except Exception as _slack_inter_paused_err:
                        logging.error(f"Error notifying Slack for paused interactive inbound {wa_id}: {_slack_inter_paused_err}")

                    logging.info(f"Automatización pausada para {wa_id}. Respuesta interactiva ignorada.")
                    release_processing_lock()
                    return
            except Exception as _paused_inter_err:
                logging.error(f"Pause check failed for interactive inbound {wa_id}: {_paused_inter_err}")
            interactive_result = whatsapp_interactive_utils.handle_interactive_message(
                message_info, wa_id, name, project_name_from_config, message_timestamp_str, message_id_incoming
            )
            
            raw_question = interactive_result.get('raw_question')
            inbound_display_text = interactive_result.get('inbound_display_text')
            error_occurred_early = interactive_result.get('error', False)
            
            # Log Inbound Message for interactive messages (even if status is 'stop')
            if raw_question and not error_occurred_early:
                log_message_to_db(
                    wa_id=wa_id,
                    sender_name=name,
                    message_text=(inbound_display_text if inbound_display_text else raw_question),
                    direction='inbound',
                    project_name=project_name_from_config,
                    timestamp=message_timestamp_str
                )
            
            if interactive_result.get('status') == 'stop':
                release_processing_lock()
                return
        
        elif message_type == "button":
            # Handle button responses from WhatsApp templates (quick reply buttons)
            # This is different from interactive.button_reply which is for interactive message buttons
            try:
                mark_activity(wa_id)
            except Exception:
                pass
            
            # Reset conversation fields if 24h window expired before this inbound
            try:
                _maybe_reset_conversation_after_24h(wa_id)
            except Exception as _reset_err:
                logging.error(f"24h reset check failed for template button inbound {wa_id}: {_reset_err}")
            
            button_data = message_info.get("button", {})
            button_text = button_data.get("text", "")
            button_payload = button_data.get("payload", "")
            
            # The button text is what the user saw and clicked
            raw_question = button_text if button_text else button_payload if button_payload else "[Respuesta de botón de plantilla]"
            inbound_display_text = f"🔘 {raw_question}"  # Show with button emoji
            
            logging.info(f"Template button response from {wa_id}: text='{button_text}', payload='{button_payload}'")
            
            # Log the template button response to database
            try:
                log_message_to_db(
                    wa_id=wa_id,
                    sender_name=name,
                    message_text=inbound_display_text,
                    direction='inbound',
                    project_name=project_name_from_config,
                    timestamp=message_timestamp_str
                )
            except Exception as log_err:
                logging.error(f"Error logging template button response for {wa_id}: {log_err}")
            
            # Notify Slack about the template button response
            try:
                send_message_slack(wa_id, name, inbound_display_text.replace('\n', ' ').replace('\r', ' '), "", bot_active=False)
            except Exception as slack_err:
                logging.error(f"Error notifying Slack for template button response {wa_id}: {slack_err}")
            
            # Check if automation is paused (agent mode)
            automation_paused = False
            try:
                automation_paused = is_automation_paused(wa_id)
            except Exception:
                pass
            
            # Auto-set needs_attention if automation is paused
            if automation_paused:
                try:
                    if _sb_on():
                        _sb_update_attention(wa_id, True)
                except Exception as _e_att:
                    logging.error(f"Failed to auto-set needs_attention ON for {wa_id} (template button): {_e_att}")
                
                # In agent mode, just log and return without sending to OpenAI
                logging.info(f"Template button response from {wa_id} logged (agent mode). No bot response.")
                release_processing_lock()
                return
            
            # >>> BOT MODE: Process template button response with OpenAI <<<
            # When in bot mode, we send the button response to OpenAI with the template context
            # so the bot can respond intelligently based on what the user clicked.
            logging.info(f"Template button response from {wa_id} in bot mode - will process with OpenAI")
        
        else:
            logging.warning(f"Received unhandled message type '{message_type}' from {wa_id}. Ignoring.")
            release_processing_lock()
            return

    except (KeyError, IndexError, TypeError) as e:
        logging.error(f"Error extracting initial message data from body: {e}. Body: {body}")
        # Log attempt if wa_id was extracted
        log_message_to_db(
            wa_id=wa_id, # Might be "Desconocido" if extraction failed early
            sender_name=name, # Might be "Desconocido"
            message_text="[Error en extracción inicial de datos]",
            direction='inbound',
            project_name=project_name_from_config,
            timestamp=message_timestamp_str,
            error_message=f"Error extracting initial data: {str(e)}",
            status='extraction_failed'
        )
        release_processing_lock()
        return

    if raw_question is None: # Should be caught by error_occurred_early or unhandled type
        logging.warning(f"No processable content after extraction for {wa_id}. Type: {message_type}. Exiting.")
        log_message_to_db(
            wa_id=wa_id,
            sender_name=name,
            message_text="[Contenido no procesable o mensaje vacío]",
            direction='inbound',
            project_name=project_name_from_config,
            timestamp=message_timestamp_str,
            error_message=f"No processable content, type: {message_type}",
            status='extraction_failed'
        )
        release_processing_lock()
        return

    # Log Inbound Message (if not an early extraction error and content exists)
    # Skip for interactive messages and button messages as they are logged above
    # Media types (image/video/document/sticker) are logged inside their own handler above.
    if not error_occurred_early and raw_question and message_type not in ("interactive", "button", "image", "video", "document", "sticker"):
        log_message_to_db(
            wa_id=wa_id,
            sender_name=name,
            message_text=(inbound_display_text if inbound_display_text else raw_question),
            direction='inbound',
            project_name=project_name_from_config,
            timestamp=message_timestamp_str
        )

    # Handle reset command ('borrar') to restart the conversation/thread
    try:
        if isinstance(raw_question, str) and raw_question.strip().lower() == "borrar":
            # Clear the stored previous_response_id using centralized logic (Supabase or local)
            delete_response_id(DB_DIRECTORY, wa_id)
            # Clear enrollment context so the enrollment flow restarts as well
            try:
                logging.info(f"🔄 Clearing enrollment context for {wa_id} due to 'borrar' command")
                clear_enrollment_context(wa_id)
                logging.info(f"✅ Enrollment context cleared for {wa_id} - realtime update should be published")
            except Exception as _clear_err:
                logging.error(f"Error clearing enrollment context for {wa_id}: {_clear_err}")

            # Inform the user and log the outbound message
            confirmation_text = "La conversación se ha reiniciado. Puedes empezar de nuevo cuando quieras. 🙂"
            recipient_reset = f"+{wa_id}"
            data_reset = get_text_message_input(recipient_reset, confirmation_text)
            wa_message_id_reset = send_message(data_reset)

            # Notify Slack as well
            try:
                send_message_slack(wa_id, name, raw_question.replace('\n', ' ').replace('\r', ' '), confirmation_text)
            except Exception as slack_err:
                logging.error(f"Error notifying Slack about reset for {wa_id}: {slack_err}")

            # Log the outbound confirmation
            log_message_to_db(
                wa_id=wa_id,
                sender_name=project_name_from_config,
                message_text=confirmation_text,
                direction='outbound_bot',
                project_name=project_name_from_config,
                whatsapp_message_id=wa_message_id_reset,
                status='sent' if wa_message_id_reset else 'failed',
                response_time_seconds=None,
                attempt_count=0,
                required_action='Reset',
                error_message=None
            )
            release_processing_lock()
            return
    except Exception as reset_err:
        logging.error(f"Error handling 'borrar' reset for {wa_id}: {reset_err}")

    if is_automation_paused(wa_id):
        send_message_slack(wa_id, name, raw_question.replace('\n', ' ').replace('\r', ' '), "", False)
        logging.info(f"Automatización pausada para {wa_id}. Mensaje ignorado (ya logueado como inbound).")
        # Inbound message already logged above. We can update its status or add a note if needed,
        # or rely on the Slack notification and dashboard to show it's paused.
        # For DB consistency, we could update the existing inbound log or log a specific 'paused_notice' type if desired.
        # For now, simply returning after Slack notification as the inbound is logged.
        release_processing_lock()
        return

    if message_id_incoming:
        app_instance = current_app._get_current_object()
        indicator_thread = threading.Thread(target=send_typing_indicator, args=(app_instance, message_id_incoming))
        indicator_thread.start()
    else:
        logging.warning(f"No incoming message_id found for message from {wa_id}. Cannot send typing indicator.")

    # >>> CONSOLIDATE PENDING MESSAGES BEFORE CALLING OPENAI <<<
    # Check if there are any pending messages from the buffer and consolidate them
    try:
        initial_pending = get_and_clear_pending_messages(wa_id)
        if initial_pending:
            raw_question = consolidate_messages(raw_question, initial_pending)
            logging.info(f"📝 Initial consolidation for {wa_id}: merged {len(initial_pending)} pending messages")
    except Exception as consolidate_err:
        logging.error(f"Error consolidating initial pending messages for {wa_id}: {consolidate_err}")

    # Apply optional date decoration controlled via env mode DATES_IN_INPUT: 'long' | 'short' | 'false'
    try:
        dates_mode = (current_app.config.get("DATES_IN_INPUT") or "long").strip().lower()
    except Exception:
        dates_mode = "long"
    
    def prepare_question_for_openai(question_text: str) -> str:
        """Prepara la pregunta con decoración de fechas y contexto de respuesta según configuración."""
        if error_occurred_early:
            return question_text
        
        # Añadir contexto de respuesta si el usuario está respondiendo a un mensaje específico
        # Esto es útil cuando responde a una plantilla de Meta
        result = question_text
        if reply_context_text:
            result = f"{reply_context_text}\n\n[Mensaje del usuario]: {question_text}"
        
        # Aplicar decoración de fechas
        if dates_mode == "false":
            return result
        elif dates_mode == "short":
            return add_current_date_to_question(result)
        else:
            return add_dates_to_question(result)

    # >>> LOOP TO CONSOLIDATE MESSAGES UNTIL NO NEW ONES ARRIVE <<<
    # This ensures messages are concatenated until the exact moment before sending
    app_instance = current_app._get_current_object()
    max_reconsolidation_attempts = 5
    total_intentos = 0
    
    for consolidation_attempt in range(max_reconsolidation_attempts):
        question_for_openai = prepare_question_for_openai(raw_question)
        
        response_data = generate_response(app_instance, question_for_openai, wa_id, name, message_id_incoming)
        respuesta = response_data.get("respuesta", "")
        intentos = response_data.get("intentos", 0)
        total_intentos += intentos
        accion_requerida = response_data.get("accion_requerida", "Ninguna")
        openai_error = response_data.get("error") 
        openai_response_id = response_data.get("response_id")
        
        # >>> CHECK FOR NEW MESSAGES BEFORE SENDING <<<
        # This is the key: we check right before sending if new messages have arrived
        try:
            new_pending = get_and_clear_pending_messages(wa_id)
            if new_pending:
                # New messages arrived while processing, consolidate and re-process
                raw_question = consolidate_messages(raw_question, new_pending)
                logging.info(f"🔄 Reconsolidation {consolidation_attempt + 1} for {wa_id}: {len(new_pending)} new messages arrived, re-calling OpenAI")
                
                # Send typing indicator to show we're still processing
                if message_id_incoming:
                    try:
                        indicator_thread = threading.Thread(target=send_typing_indicator, args=(app_instance, message_id_incoming))
                        indicator_thread.daemon = True
                        indicator_thread.start()
                    except Exception:
                        pass
                
                continue  # Go back to beginning of loop to re-call OpenAI with consolidated message
        except Exception as new_pending_err:
            logging.error(f"Error checking for new pending messages for {wa_id}: {new_pending_err}")
        
        # No new messages, exit the loop and proceed to send
        if consolidation_attempt > 0:
            logging.info(f"✅ Final consolidation complete for {wa_id} after {consolidation_attempt + 1} attempts")
        break
    else:
        # Max attempts reached (should be rare)
        logging.warning(f"⚠️ Max reconsolidation attempts ({max_reconsolidation_attempts}) reached for {wa_id}")

    # Update intentos with total across all consolidation attempts
    intentos = total_intentos

    processed_response = process_text_for_whatsapp(respuesta)
    recipient = f"+{wa_id}"

    should_send = bool((respuesta or "").strip())
    data_to_send = None
    if should_send:
        data_to_send, new_accion_requerida = whatsapp_interactive_utils.build_interactive_response(recipient, processed_response)
        if new_accion_requerida:
            accion_requerida = new_accion_requerida

    if should_send:
        wa_message_id_sent_by_bot = send_message(data_to_send)
    else:
        wa_message_id_sent_by_bot = None

    # Map statuses to Supabase-allowed values
    # Allowed: 'sent','delivered','read','failed','ignored_paused'
    if wa_message_id_sent_by_bot:
        final_status = 'sent'
    else:
        final_status = 'ignored_paused' if not should_send else 'failed'
    final_error_message = openai_error

    if should_send and wa_message_id_sent_by_bot is None:
        # Enviar notificación de error al usuario si el envío falló
        error_message_to_user = "¡Ups! Algo no salió bien al intentar enviarte la respuesta. Por favor, intenta iniciar la conversación de nuevo. 😊"
        error_data = get_text_message_input(recipient, error_message_to_user)
        send_message(error_data)
        final_error_message = openai_error if openai_error else "Error enviando mensaje a WhatsApp"

    if should_send:
        send_message_slack(wa_id, name, raw_question.replace('\n', ' ').replace('\r', ' '), processed_response)

    total_response_time = time.time() - start_time

    # Log Outbound Bot Message only when there is content to show to the user
    if should_send:
        log_message_to_db(
            wa_id=wa_id,
            sender_name=project_name_from_config, # Bot's name from ENV_NAME
            message_text=processed_response,
            direction='outbound_bot',
            project_name=project_name_from_config, # Log ENV_NAME as project_name
            whatsapp_message_id=wa_message_id_sent_by_bot,
            status=final_status,
            response_time_seconds=float(f"{total_response_time:.2f}"),
            attempt_count=intentos,
            required_action=accion_requerida,
            error_message=final_error_message,
            response_id=openai_response_id
        )

    # If sector message is pending for this inbound, send it now (after main bot reply)
    try:
        if sector_message_text_to_send:
            recipient_sector = f"+{wa_id}"
            data_sector = get_text_message_input(recipient_sector, sector_message_text_to_send)
            sent_sector_id = send_message(data_sector)
            try:
                log_message_to_db(
                    wa_id=wa_id,
                    sender_name=project_name_from_config,
                    message_text=sector_message_text_to_send,
                    direction='outbound_bot',
                    project_name=project_name_from_config,
                    whatsapp_message_id=sent_sector_id,
                    status='sent' if sent_sector_id else 'failed',
                    response_id=openai_response_id
                )
            except Exception as _log_err:
                logging.error(f"Error logging deferred sector message for {wa_id}: {_log_err}")
    except Exception as send_err:
        logging.error(f"Error sending deferred sector message for {wa_id}: {send_err}")
    
    # >>> FINAL CHECK FOR ORPHANED PENDING MESSAGES <<<
    # This handles the race condition where a message arrives between the last
    # get_and_clear_pending_messages() call and the lock release.
    # We must process any remaining pending messages before releasing the lock.
    try:
        max_final_checks = 3  # Prevent infinite loops
        for final_check in range(max_final_checks):
            final_pending = get_and_clear_pending_messages(wa_id)
            if not final_pending:
                break  # No orphaned messages, safe to release lock
            
            logging.info(f"🔔 Found {len(final_pending)} orphaned pending messages for {wa_id} after sending response. Processing...")
            
            # Consolidate orphaned messages
            orphaned_question = consolidate_messages("", final_pending)
            if not orphaned_question.strip():
                break
            
            # Log the orphaned messages as inbound (they were already logged when buffered, but mark context)
            logging.info(f"📝 Processing orphaned message for {wa_id}: {orphaned_question[:100]}...")
            
            # Send typing indicator
            if message_id_incoming:
                try:
                    indicator_thread = threading.Thread(target=send_typing_indicator, args=(app_instance, message_id_incoming))
                    indicator_thread.daemon = True
                    indicator_thread.start()
                except Exception:
                    pass
            
            # Generate response for orphaned messages
            try:
                orphan_question = prepare_question_for_openai(orphaned_question)
                orphan_response_data = generate_response(app_instance, orphan_question, wa_id, name, message_id_incoming)
                orphan_respuesta = orphan_response_data.get("respuesta", "")
                
                if orphan_respuesta and orphan_respuesta.strip():
                    orphan_processed = process_text_for_whatsapp(orphan_respuesta)
                    orphan_recipient = f"+{wa_id}"
                    orphan_data, orphan_accion = whatsapp_interactive_utils.build_interactive_response(orphan_recipient, orphan_processed)
                    orphan_msg_id = send_message(orphan_data)
                    
                    # Log the orphan response
                    log_message_to_db(
                        wa_id=wa_id,
                        sender_name=project_name_from_config,
                        message_text=orphan_processed,
                        direction='outbound_bot',
                        project_name=project_name_from_config,
                        whatsapp_message_id=orphan_msg_id,
                        status='sent' if orphan_msg_id else 'failed',
                        response_id=orphan_response_data.get("response_id")
                    )
                    
                    # Notify Slack
                    send_message_slack(wa_id, name, orphaned_question.replace('\n', ' ').replace('\r', ' '), orphan_processed)
                    
                    logging.info(f"✅ Orphaned message for {wa_id} processed and responded")
            except Exception as orphan_err:
                logging.error(f"Error processing orphaned message for {wa_id}: {orphan_err}")
                break
    except Exception as final_check_err:
        logging.error(f"Error in final pending messages check for {wa_id}: {final_check_err}")
    
    # >>> RELEASE PROCESSING LOCK <<<
    release_processing_lock()
