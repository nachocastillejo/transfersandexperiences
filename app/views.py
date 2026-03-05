import logging
import os
import threading
import multiprocessing
import time
import sys
import json
from datetime import datetime

from flask import Blueprint, request, jsonify, current_app, Response, redirect, url_for, send_from_directory, render_template
from .decorators.security import signature_required
from .utils.whatsapp_utils import process_whatsapp_message
from app.utils.message_logger import update_message_status_in_db
from app.utils.database_utils import get_db

from app.services.supabase_service import delete_conversation, fetch_messages_for_wa, update_message_status_by_wamid
from .utils.performance_monitor import performance_monitor

# Se crea el blueprint para agrupar las rutas de webhook
webhook_blueprint = Blueprint("webhook", __name__)

@webhook_blueprint.route('/')
def home():
    """
    Página principal (visor visual HTML).
    """
    return render_template('demo_chat.html')

@webhook_blueprint.route('/api/demo_logs', methods=['GET'])
def get_demo_logs():
    """
    Devuelve las conversaciones en JSON para el visor web (desde SQLite).
    """
    try:
        db = get_db()
        # Fetch last 100 messages for the demo view
        rows = db.execute(
            "SELECT timestamp, wa_id as phone, direction, sender_name as sender, response_time_seconds as responseTime, message_text as message "
            "FROM messages ORDER BY id DESC LIMIT 100"
        ).fetchall()
        
        # Convert rows to a list of dicts
        messages = [dict(row) for row in rows]
        # Return reversed so they are in chronological order for the frontend to process/group
        messages.reverse()
        
        return jsonify(messages)
    except Exception as e:
        logging.error(f"Error fetching demo logs from SQLite: {e}")
        return jsonify([])

@webhook_blueprint.route('/ping', methods=['GET'])
def ping():
    """
    Endpoint simple para hacer ping y recibir un pong.
    """
    return 'pong', 200

@webhook_blueprint.route('/favicon.ico')
def favicon():
    # Serve favicon directly to avoid issues with external static mappings in production
    try:
        icons_dir = os.path.join(current_app.root_path, 'dashboard', 'static_dashboard', 'images')
        return send_from_directory(icons_dir, 'favicon.ico')
    except Exception:
        return Response(status=204)

def verify(webhook_type="Message"):
    """Verifica el token de suscripción de WhatsApp (modo subscribe)."""
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")
    verify_token_config = current_app.config.get("VERIFY_TOKEN")

    if mode == "subscribe" and token == verify_token_config:
        logging.info(f"✔️ 0. Webhook ({webhook_type}) verified.")
        return challenge, 200
    else:
        logging.error(f"❌ 0. Webhook ({webhook_type}) verification failed. Mode: {mode}, Token: {token}, Expected: {verify_token_config}")
        return (
            jsonify({"status": "error", "message": "Verification failed or missing parameters"}),
            403,
        )

@performance_monitor
def handle_message(app_instance, body):
    """
    Procesa de forma síncrona un mensaje entrante de WhatsApp.
    
    Args:
        app_instance (Flask): Instancia de la aplicación Flask para crear el contexto.
        body (dict): Cuerpo JSON con la información del mensaje.
    """
    start_time = time.time()

    # Procesar con el contexto de la aplicación
    with app_instance.app_context():
        access_token = current_app.config.get('ACCESS_TOKEN')
        version = current_app.config.get('VERSION')
        phone_number_id = current_app.config.get('PHONE_NUMBER_ID')

        # Verificar configuraciones requeridas
        if not all([access_token, version, phone_number_id]):
            logging.error("❌ Missing required configurations (ACCESS_TOKEN, VERSION, PHONE_NUMBER_ID).")
            return

        try:
            process_whatsapp_message(body)
            elapsed_time = time.time() - start_time
            logging.info(f"✅ Message handled in {elapsed_time:.2f}s.\n" + "-"*60)
        except Exception as e:
            logging.error(f"❌ Error during asynchronous message processing: {e}. Body: {body}\n")

def handle_message_async(app_instance, body):
    """
    Dispara el procesamiento del mensaje en un thread o proceso, según el sistema operativo.
    
    Args:
        app_instance (Flask): Instancia de la aplicación para disponer del contexto.
        body (dict): Cuerpo JSON con el mensaje entrante de WhatsApp.
    """
    if sys.platform.startswith("win"):
        # En Windows se recomiendan threads para evitar problemas con Process y multiprocessing
        thread = threading.Thread(target=handle_message, args=(app_instance, body))
        thread.start()
    else:
        # En otros sistemas se puede usar multiprocessing
        process = multiprocessing.Process(target=handle_message, args=(app_instance, body))
        process.start()

# --- Unified Webhook for Incoming Messages and Status Updates --- 
@webhook_blueprint.route("/webhook", methods=["GET"])
def webhook_get():
    """Endpoint GET para la verificación del webhook (unificada)."""
    return verify(webhook_type="Main") 

@webhook_blueprint.route("/webhook", methods=["POST"])
@signature_required
def webhook_post():
    """Endpoint POST que recibe mensajes y actualizaciones de estado de WhatsApp."""
    current_time_str = datetime.now().strftime("%H:%M:%S")
    body = request.get_json()

    if body is None:
        logging.error("❌ Cuerpo vacío en la petición POST /webhook.")
        return jsonify({'status': 'error', 'message': 'Empty body'}), 400

    try:
        entry = body.get('entry', [{}])[0]
        changes = entry.get('changes', [{}])[0]
        value = changes.get('value')

        if not value: # Ensure 'value' exists before proceeding
            logging.warning("⚠️ Evento desconocido recibido en /webhook (falta 'value').")
            logging.debug(f"Webhook body without 'value': {json.dumps(body)}")
            return jsonify({'status': 'received_no_value'}), 200

        # Filter by phone_number_id to avoid cross-environment responses
        incoming_phone_number_id = (value.get('metadata') or {}).get('phone_number_id')
        configured_phone_number_id = current_app.config.get('PHONE_NUMBER_ID')
        if incoming_phone_number_id and configured_phone_number_id and incoming_phone_number_id != configured_phone_number_id:
            logging.info(
                f"Ignoring event for phone_number_id={incoming_phone_number_id} (configured={configured_phone_number_id})."
            )
            return jsonify({'status': 'ignored_wrong_phone_number_id'}), 200

        # Track what was processed in this webhook event
        # WhatsApp can bundle statuses AND messages in a single webhook delivery,
        # so we must process BOTH (not use if/elif which would drop messages).
        processed_parts = []

        # --- Process message status updates ---
        if 'statuses' in value and isinstance(value['statuses'], list):
            for status_info in value['statuses']:
                if isinstance(status_info, dict):
                    message_id = status_info.get('id') # This is the wamid
                    status = status_info.get('status')
                    recipient_wa_id = status_info.get('recipient_id') # The user's phone number
                    timestamp = status_info.get('timestamp')
                    status_time_str = datetime.fromtimestamp(int(timestamp)).strftime("%H:%M:%S") if timestamp else "N/A"

                    if message_id and status in ['delivered', 'read', 'failed']:
                        # Cleaner log message
                        logging.info(f"📲 Status update received for {recipient_wa_id}: '{status.capitalize()}' [{status_time_str}]")
                        
                        error_text = None
                        if status == 'failed':
                            errors = status_info.get('errors', [])
                            if errors:
                                logging.error(
                                    f"❌ Delivery failure details for {recipient_wa_id} (ID {message_id}): {json.dumps(errors)}"
                                )
                                # Construir un mensaje legible a partir del payload de errores
                                try:
                                    details_parts = []
                                    for err in errors:
                                        if not isinstance(err, dict):
                                            details_parts.append(str(err))
                                            continue
                                        msg = err.get("message") or err.get("title") or ""
                                        extra = ""
                                        error_data = err.get("error_data") or {}
                                        if isinstance(error_data, dict):
                                            extra = error_data.get("details") or ""
                                        text = msg
                                        if extra and extra not in text:
                                            text = f"{text} ({extra})" if text else extra
                                        if text:
                                            details_parts.append(text)
                                    if details_parts:
                                        error_text = "; ".join(details_parts)
                                    else:
                                        error_text = json.dumps(errors, ensure_ascii=False)
                                except Exception:
                                    # Como fallback, almacenamos el JSON bruto
                                    error_text = json.dumps(errors, ensure_ascii=False)

                        # Actualizar el status del mensaje directamente en Supabase
                        # El mensaje ya fue creado con status='pending' al enviar la plantilla
                        try:
                            updated = update_message_status_by_wamid(message_id, status, error_text)
                            if updated:
                                if status == 'failed':
                                    logging.info(f"❌ Template delivery failed for {recipient_wa_id}")
                                elif status in ('sent', 'delivered'):
                                    logging.debug(f"✅ Template status updated to '{status}' for {recipient_wa_id}")
                            else:
                                # Fallback: usar update_message_status_in_db para mensajes no-template
                                update_message_status_in_db(message_id, status, recipient_wa_id)
                        except Exception as update_err:
                            logging.error(f"Error updating message status: {update_err}")
                            update_message_status_in_db(message_id, status, recipient_wa_id)
                    else:
                        logging.debug(f"Ignoring status update (ID={message_id}, Status='{status}')")
                else:
                    logging.warning(f"Found non-dict item in statuses list via /webhook: {status_info}")
            processed_parts.append('status_processed')

        # --- Process new messages (independent of statuses above) ---
        if 'messages' in value and isinstance(value['messages'], list):
            if processed_parts:
                logging.info(f"📦 Webhook contains both statuses and messages - processing both")

            message_data = value['messages'][0]
            message_type = message_data.get("type", "unknown")

            if message_type == "text":
                message_text = message_data.get("text", {}).get("body", "No text found")
                log_message_text = message_text.replace('\n', ' ').replace('\r', ' ').replace('\\', ' ').strip()
                while '  ' in log_message_text:
                    log_message_text = log_message_text.replace('  ', ' ')
                logging.info(f"📩 1. TEXT Message received [{current_time_str}]: {log_message_text}\n")
            elif message_type == "audio":
                audio_id = message_data.get("audio", {}).get("id")
                logging.info(f"🎙️ 1. AUDIO Message received [{current_time_str}]: ID {audio_id}\n")
            elif message_type == "image":
                image_id = message_data.get("image", {}).get("id")
                caption = message_data.get("image", {}).get("caption", "")
                caption_preview = f" - Caption: {caption[:50]}..." if caption else ""
                logging.info(f"📷 1. IMAGE Message received [{current_time_str}]: ID {image_id}{caption_preview}\n")
            elif message_type == "video":
                video_id = message_data.get("video", {}).get("id")
                caption = message_data.get("video", {}).get("caption", "")
                caption_preview = f" - Caption: {caption[:50]}..." if caption else ""
                logging.info(f"🎥 1. VIDEO Message received [{current_time_str}]: ID {video_id}{caption_preview}\n")
            elif message_type == "document":
                doc_id = message_data.get("document", {}).get("id")
                filename = message_data.get("document", {}).get("filename", "Unknown")
                logging.info(f"📄 1. DOCUMENT Message received [{current_time_str}]: {filename} (ID: {doc_id})\n")
            elif message_type == "sticker":
                sticker_id = message_data.get("sticker", {}).get("id")
                logging.info(f"🎨 1. STICKER Message received [{current_time_str}]: ID {sticker_id}\n")
            elif message_type == "interactive":
                interactive_data = message_data.get("interactive", {})
                interactive_type = interactive_data.get("type", "unknown")
                if interactive_type == "nfm_reply":
                    logging.info(f"📝 1. INTERACTIVE Message received [{current_time_str}]: Form submission (nfm_reply)\n")
                elif interactive_type == "button_reply":
                    button_title = interactive_data.get("button_reply", {}).get("title", "Unknown button")
                    logging.info(f"🔘 1. INTERACTIVE Message received [{current_time_str}]: Button '{button_title}'\n")
                else:
                    logging.info(f"🔄 1. INTERACTIVE Message received [{current_time_str}]: Type '{interactive_type}'\n")
            elif message_type == "button":
                # Template quick reply button response
                button_data = message_data.get("button", {})
                button_text = button_data.get("text", "Unknown")
                logging.info(f"🔘 1. TEMPLATE BUTTON Message received [{current_time_str}]: '{button_text}'\n")
            else:
                logging.warning(f"⚠️ Received message of unhandled type '{message_type}' [{current_time_str}].\n")
            
            app_instance = current_app._get_current_object()
            handle_message_async(app_instance, body)
            processed_parts.append('message_processing_started')

        # --- If nothing was processed, log unknown event ---
        if not processed_parts:
            logging.warning("⚠️ Evento desconocido recibido en /webhook (no es 'messages' ni 'statuses').")
            logging.debug(f"Unknown event body in /webhook: {json.dumps(body)}")
            return jsonify({'status': 'received_unknown'}), 200

        return jsonify({'status': '_and_'.join(processed_parts)}), 200
            
    except (KeyError, IndexError, TypeError) as e:
        logging.error(f"Error parsing incoming webhook data on /webhook: {e}. Body: {json.dumps(body)}")
        return jsonify({'status': 'error', 'message': 'Error parsing request body'}), 400
    except Exception as e:
        logging.error(f"Unexpected error processing POST /webhook: {e}. Body: {json.dumps(body)}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'Internal server error'}), 500

# The /webhook/status routes are no longer needed as their logic is merged above.
# @webhook_blueprint.route("/webhook/status", methods=["GET"])
# def webhook_status_get():
#     challenge, status_code = verify(webhook_type="Status") 
#     if status_code == 200:
#         return Response(challenge, status=200)
#     else:
#         return challenge
        
# @webhook_blueprint.route("/webhook/status", methods=["POST"])
# def webhook_status_post():
#     # Logic for processing status updates has been moved to the main /webhook POST handler
#     return jsonify({"status": "ok_deprecated_use_main_webhook"}), 200
