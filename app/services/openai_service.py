import os
from flask import current_app
import json
import logging
import time
import random # Import random module
import threading # Add this import
import sys
import multiprocessing
from openai import OpenAI, APIError, APITimeoutError
# from app.utils.openai_functions import * # Removed to break circular dependency
from app.utils.extra_utils import (
    get_previous_response_id,
    store_current_response_id,
    delete_response_id,
    read_instructions,
    read_functions,
    is_missing_email
)
# Import fast routing helpers
from app.utils.extra_utils import is_fast_message, get_fast_model_name
# Import the entire module to inspect its functions
import app.utils.openai_functions as processors_module 
import inspect # Import inspect module
# Import messaging utilities
from app.utils.messaging_utils import get_text_message_input, send_message, send_typing_indicator, get_button_message_input
from app.utils.messaging_utils import get_template_message_input, get_flow_message_input
from app.utils.message_logger import log_message_to_db
from app.utils.enrollment_state import get_enrollment_context, update_enrollment_context
from app.utils.extra_utils import log_sector_activity_choice
from app.services.supabase_service import is_supabase_enabled, insert_message as save_message_to_supabase, update_previous_response_id as update_id_in_supabase

# Use multiprocessing on Linux/macOS, threading on Windows for short-lived tasks
if sys.platform.startswith("win"):
    _Process = threading.Thread
else:
    _Process = multiprocessing.Process

# Configuración de OpenAI
OPENAI_ASSISTANT_ID = os.getenv("OPENAI_ASSISTANT_ID")
OPENAI_MODEL_NAME_DEFAULT = "gpt-4o-mini"

def get_openai_client():
    """
    Inicializa el cliente de OpenAI usando el API key de la config de Flask o del entorno.
    """
    try:
        api_key = current_app.config.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
    except Exception:
        api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        logging.error("❌ OPENAI_API_KEY no encontrada en la configuración.")
        return None

    return OpenAI(api_key=api_key, timeout=20.0, max_retries=3)

# Directorio para la base de datos (almacena el estado de threads)
DB_DIRECTORY = "db"
if not os.path.exists(DB_DIRECTORY):
    os.makedirs(DB_DIRECTORY)

# Obtener el nombre del entorno; se usa "default" si no se especifica
ENV_NAME = os.getenv("ENV_NAME", "default")

# Mensajes preliminares para funciones específicas
FUNCTION_PRELIMINARY_MESSAGES = {
    "check_availability": "Un momento, estoy comprobando la disponibilidad... 📅",
    "detect_appointment": "¡Genial! Estoy agendando tu cita ahora mismo... ✍️",
    "cancel_appointment": "Entendido, dame un segundo para cancelar tu cita... 🗑️",
    "recomendar_cursos": [
        "¡Entendido! Buscando los cursos perfectos para ti... 🕵️‍♂️",
        "De acuerdo, estoy buscando las mejores opciones de formación... 📚✨",
        "¡Manos a la obra! Revisando nuestro catálogo de cursos... 💻",
        "Perfecto, consultando los cursos disponibles para tu perfil... 🤔"
    ],
    "get_real_time_data": "Consultando información actualizada, ¡un instante por favor!... 🌐",
    "inscribir_lead_crm": "¡Perfecto! Estoy guardando tus datos para inscribirte. Un momento... 📝",
    # Añade aquí más funciones si es necesario
}

def get_response_api(app, message, client, wa_id, message_id=None):
    """
    Solicita la respuesta del modelo a través de la API de OpenAI y procesa llamadas a funciones si las hay.

    Args:
        app (Flask): The Flask application instance.
        message (str): Mensaje de entrada.
        client (OpenAI): Instancia del cliente OpenAI.
        wa_id (str): Identificador de WhatsApp para el seguimiento del hilo.
        message_id (str, optional): ID del mensaje original de WhatsApp. Defaults to None.

    Returns:
        str: Texto de la respuesta generada por el modelo.
    """
    if client is None:
        client = get_openai_client()
    
    if not client:
        return "Lo siento, hay un problema de configuración con el servicio de IA. Por favor, inténtalo más tarde.", None

    OPENAI_MODEL_NAME = current_app.config.get("OPENAI_MODEL_NAME") or OPENAI_MODEL_NAME_DEFAULT
    
    response_sent_manually = False # Flag for manual responses
    lock_was_set = False
    try:
        previous_response_id = get_previous_response_id(DB_DIRECTORY, wa_id)
        try:
            tools = read_functions(ENV_NAME)
            instructions = read_instructions(ENV_NAME)
        except Exception as e:
            logging.error(f"Error al leer funciones o instrucciones para ENV '{ENV_NAME}': {e}")
            return "Error interno al cargar la configuración del asistente.", None

        input_messages = [{"role": "user", "content": message}]
        
        # Determine model for this request (fast for greetings/name/email)
        try:
            fast_model_name = get_fast_model_name(ENV_NAME, fallback="gpt-4o-mini")
            chosen_model = fast_model_name if is_fast_message(message or "", ENV_NAME) else OPENAI_MODEL_NAME
        except Exception as _fast_err:
            logging.error(f"Error determining fast model: {_fast_err}")
            chosen_model = OPENAI_MODEL_NAME

        try:
            is_fast = (chosen_model == fast_model_name)
        except Exception:
            is_fast = False
        logging.info(f"🔧 OpenAI model selected for wa_id {wa_id}: {chosen_model} (fast={is_fast})")

        # Add reasoning parameter for gpt-5 models
        reasoning_param = {}
        if chosen_model and 'gpt-5' in chosen_model:
            reasoning_param = {"reasoning": {"effort": "low"}}

        try:
            # Solicitar la respuesta del modelo con el historial y las instrucciones proporcionadas
            response = client.responses.create(
                model=chosen_model,
                instructions=instructions,
                input=input_messages,
                previous_response_id=previous_response_id,
                tools=tools,
                **reasoning_param
            )
            current_response_id = response.id
            store_current_response_id(DB_DIRECTORY, current_response_id, wa_id)

        except APITimeoutError as e:
            # Handle timeout errors separately
            logging.error(f"OpenAI API Timeout for wa_id {wa_id}: {e}")
            return "La solicitud tardó demasiado. Por favor, inténtalo de nuevo.", None
        except APIError as e:
            # Check for specific errors: 400 Bad Request with "No tool output found" or "previous_response_not_found"
            error_message_lower = str(e.body.get("message", "") if e.body else "").lower()
            should_retry = False
            if hasattr(e, 'status_code') and e.status_code == 400:
                if "no tool output found" in error_message_lower:
                    logging.warning(f"OpenAI API Error 400 (No tool output found) for wa_id {wa_id}. Deleting response ID and retrying. Error: {e}")
                    should_retry = True
                elif "previous response with id" in error_message_lower and "not found" in error_message_lower:
                    logging.warning(f"OpenAI API Error 400 (Previous response not found) for wa_id {wa_id}. Deleting response ID and retrying. Error: {e}")
                    should_retry = True

            if should_retry:
                # Delete the potentially problematic response ID entry
                delete_response_id(DB_DIRECTORY, wa_id)
                
                # Retry the call without previous_response_id
                try:
                    logging.info(f"Retrying OpenAI call for wa_id {wa_id} without previous_response_id.")
                    response = client.responses.create(
                        model=chosen_model,
                        instructions=instructions,
                        input=input_messages,
                        previous_response_id=None, # Retry without previous ID
                        tools=tools,
                        **reasoning_param
                    )
                    current_response_id = response.id
                    store_current_response_id(DB_DIRECTORY, current_response_id, wa_id)
                    # Continue processing the response as normal if retry succeeds
                except APITimeoutError as retry_e:
                    logging.error(f"Timeout en el reintento de OpenAI para wa_id {wa_id}: {retry_e}")
                    return "La solicitud tardó demasiado en el reintento. Por favor, inténtalo de nuevo.", None
                except APIError as retry_e:
                    logging.error(f"Error de API de OpenAI en el reintento tras error 400: {retry_e}")
                    return "Hubo un problema al comunicarme con el asistente después de un reintento. Inténtalo de nuevo más tarde.", None
                except Exception as retry_e:
                    logging.error(f"Error inesperado en el reintento tras error 400 de OpenAI: {retry_e}")
                    return "Ocurrió un error inesperado durante el reintento. Por favor, informa al administrador.", None
            else:
                # Handle other API errors as before
                logging.error(f"Error de API de OpenAI al obtener respuesta inicial: {e}")
                return "Hubo un problema al comunicarme con el asistente. Inténtalo de nuevo más tarde.", None
        except Exception as e:
            logging.error(f"Error inesperado al obtener respuesta inicial de OpenAI: {e}")
            return "Ocurrió un error inesperado. Por favor, informa al administrador.", None


        # Procesar llamadas a funciones definidas en la respuesta
        # gpt-5 puede anteponer elementos de tipo "reasoning" u otros antes de los tool calls.
        # Por ello, detectamos si hay algún elemento de tipo tool call en cualquier posición.
        def _has_pending_tool_call(resp) -> bool:
            try:
                return any(
                    getattr(item, "type", None) in ("function_call", "tool_call")
                    for item in (getattr(resp, "output", []) or [])
                )
            except Exception:
                return False

        sent_course_cards = False
        # Evitar mensajes preliminares duplicados para la misma función en este ciclo
        prelim_recomendar_sent = False
        # Señal para cortar el bucle cuando no hay cursos (evitar llamadas repetidas)
        stop_after_recommendation = False
        # Último mensaje de resultado devuelto por una tool (para pass-through cuando no hay resultados)
        last_tool_result_message = None
        # Cuando enviamos tarjetas por WhatsApp, haremos un finalize mínimo para mantener el hilo
        minimal_finalize_needed = False

        # Helper to re-trigger typing indicator with a small delay to improve reliability
        def _delayed_typing_indicator(app_instance, msg_id, delay_seconds: float = 0.5):
            try:
                if delay_seconds and delay_seconds > 0:
                    time.sleep(delay_seconds)
                send_typing_indicator(app_instance, msg_id)
            except Exception as _typing_err:
                logging.error(f"Error sending delayed typing indicator for {msg_id}: {_typing_err}")
        while _has_pending_tool_call(response):
            tool_call_outputs = []

            # Mapeo dinámico de nombres de funciones a sus procesadores
            function_processors = {}
            for name, func in inspect.getmembers(processors_module, inspect.isfunction):
                if name.startswith("process_"):
                    # Extraer el nombre base (ej: 'check_availability' de 'process_check_availability')
                    base_name = name[len("process_"):] 
                    function_processors[base_name] = func
            
            logging.debug(f"Discovered function processors: {list(function_processors.keys())}")

            for tool_call in response.output:
                if getattr(tool_call, "type", None) not in ("function_call", "tool_call"):
                    continue

                # --- Mensaje automático de "Sector elegido: X" justo cuando el LLM fija el sector ---
                try:
                    if getattr(tool_call, "name", None) == "recomendar_cursos":
                        try:
                            args_preview = json.loads(getattr(tool_call, "arguments", "{}") or "{}")
                        except Exception:
                            args_preview = {}
                        sector_arg = (args_preview.get("sector") or "").strip()
                        situacion_arg = (args_preview.get("situacion_laboral") or "").strip().lower()
                        # Notify sector as soon as it is fixed by the model, regardless of situacion_laboral
                        if sector_arg and sector_arg != "N/A":
                            ctx = get_enrollment_context(wa_id)
                            sector_notified = bool(ctx.get("sector_notified"))
                            if not sector_notified:
                                try:
                                    # Persistir sector y marcar notificado para no duplicar
                                    update_enrollment_context(wa_id, {"sector": sector_arg, "sector_notified": True})
                                except Exception as _ctx_err:
                                    logging.error(f"No se pudo actualizar el contexto de sector para {wa_id}: {_ctx_err}")
                                # Intentar registrar CSV temporal usando actividad si está disponible en contexto
                                try:
                                    actividad_ctx = (ctx.get("actividad_empresa") or "").strip()
                                    if actividad_ctx:
                                        log_sector_activity_choice(wa_id, actividad_ctx, sector_arg)
                                except Exception as _csv_err:
                                    logging.error(f"Error logging sector activity CSV (tool_call) for {wa_id}: {_csv_err}")
                                try:
                                    # Enviar mensaje independiente al usuario y registrarlo para dashboard
                                    recipient_id = f"+{wa_id}"
                                    sector_msg = f"Sector elegido: {sector_arg}"
                                    msg_data = get_text_message_input(recipient_id, sector_msg)
                                    sent_sector_id = send_message(msg_data)
                                    try:
                                        env_name = current_app.config.get("ENV_NAME", "Bot")
                                        log_message_to_db(
                                            wa_id=wa_id,
                                            sender_name=env_name,
                                            message_text=sector_msg,
                                            direction='outbound_bot',
                                            project_name=env_name,
                                            whatsapp_message_id=sent_sector_id,
                                            status='sent' if sent_sector_id else 'failed',
                                            model=chosen_model,
                                            response_id=current_response_id
                                        )
                                    except Exception as _log_err:
                                        logging.error(f"Error logging sector message for {wa_id}: {_log_err}")
                                    logging.info(f"Mensaje de sector enviado a {recipient_id}: '{sector_msg}'")
                                except Exception as _send_err:
                                    logging.error(f"Error enviando mensaje de sector para {wa_id}: {_send_err}")
                except Exception as sector_hook_err:
                    logging.error(f"Error en hook de sector elegido: {sector_hook_err}")

                # --- Enviar mensaje preliminar --- 
                message_or_list = FUNCTION_PRELIMINARY_MESSAGES.get(tool_call.name)
                preliminary_message = None
                if isinstance(message_or_list, list):
                    preliminary_message = random.choice(message_or_list)
                elif isinstance(message_or_list, str):
                    preliminary_message = message_or_list
                    
                if preliminary_message and not (getattr(tool_call, "name", None) == "recomendar_cursos" and prelim_recomendar_sent):
                    try:
                        recipient_id = f"+{wa_id}" # Asegurarse de que el wa_id tiene el prefijo +
                        message_data = get_text_message_input(recipient_id, preliminary_message)
                        sent_pre_id = send_message(message_data)
                        if getattr(tool_call, "name", None) == "recomendar_cursos":
                            prelim_recomendar_sent = True
                        try:
                            env_name = current_app.config.get("ENV_NAME", "Bot")
                            log_message_to_db(
                                wa_id=wa_id,
                                sender_name=env_name,
                                message_text=preliminary_message,
                                direction='outbound_bot',
                                project_name=env_name,
                                whatsapp_message_id=sent_pre_id,
                                status='sent' if sent_pre_id else 'failed',
                                model=chosen_model,
                                response_id=current_response_id
                            )
                        except Exception as log_err:
                            logging.error(f"Error logging preliminary message for {wa_id}: {log_err}")
                        logging.info(f"Sent preliminary message for {tool_call.name} to {recipient_id}: '{preliminary_message}'")
                    except Exception as msg_err:
                        # Loggear el error pero continuar, no es crítico
                        logging.error(f"Error sending preliminary message for {tool_call.name} to {recipient_id}: {msg_err}")
                # --- Fin envío mensaje preliminar ---

                # >>> Evitar reinscripción del mismo curso leyendo el código de los argumentos <<<
                try:
                    if getattr(tool_call, "name", None) == "inscribir_lead_crm":
                        # Parsear argumentos de la tool para obtener codigo curso
                        args_preview = {}
                        try:
                            args_preview = json.loads(getattr(tool_call, "arguments", "{}") or "{}")
                        except Exception:
                            args_preview = {}
                        codigo_tool = (
                            args_preview.get("codigo_curso")
                            or args_preview.get("Codigo_inscripcion")
                            or args_preview.get("referencia_curso")
                            or args_preview.get("codigo_inscripcion")
                        )
                        if codigo_tool:
                            ctx_check = get_enrollment_context(wa_id)
                            history = list(ctx_check.get("inscripciones") or [])
                            if any(isinstance(r, dict) and r.get("codigo_curso") == codigo_tool for r in history):
                                recipient_id = f"+{wa_id}"
                                already_text = (
                                    "Ya estabas inscrito en ese curso ✅. Si quieres otro, elige un curso diferente."
                                )
                                msg_data = get_text_message_input(recipient_id, already_text)
                                sent_already_id = send_message(msg_data)
                                try:
                                    env_name = current_app.config.get("ENV_NAME", "Bot")
                                    log_message_to_db(
                                        wa_id=wa_id,
                                        sender_name=env_name,
                                        message_text=already_text,
                                        direction='outbound_bot',
                                        project_name=env_name,
                                        whatsapp_message_id=sent_already_id,
                                        status='sent' if sent_already_id else 'failed',
                                        response_id=current_response_id
                                    )
                                except Exception as _log_already_err:
                                    logging.error(f"Error logging already-enrolled message for {wa_id}: {_log_already_err}")
                                # Saltar el procesamiento de esta tool call
                                continue
                except Exception as _insc_err:
                    logging.error(f"Error comprobando inscripcion_ok antes de tool_call inscribir_lead_crm: {_insc_err}")

                # >>> Enviar indicador de escritura OTRA VEZ después del mensaje preliminar <<<
                if message_id:
                    try:
                        # Ensure we pass a real Flask app instance to the thread
                        try:
                            app_instance = app or current_app._get_current_object()
                        except Exception:
                            app_instance = app
                        # Trigger typing indicator after a brief delay to avoid duplicate-read suppression
                        indicator_thread = _Process(target=_delayed_typing_indicator, args=(app_instance, message_id, 0.6))
                        indicator_thread.daemon = True
                        indicator_thread.start()
                    except Exception as _ti_err:
                        logging.error(f"Failed to start typing indicator thread after preliminary message: {_ti_err}")
                else:
                    # Log warning if message_id wasn't passed down correctly
                    logging.warning(f"Cannot send typing indicator after preliminary message for {tool_call.name} because message_id is missing.")

                try:
                    # Obtener el procesador de función correspondiente
                    processor = function_processors.get(tool_call.name)
                    
                    if processor:
                        # Para collect_contact: además de procesar, persistimos en enrollment nombre/email
                        if tool_call.name == "collect_contact":
                            result_data = processor(tool_call)
                            try:
                                output_json_tmp = json.loads(result_data.get("output", "{}"))
                                status_tmp = output_json_tmp.get("status")
                                message_tmp = output_json_tmp.get("message")
                                contact_payload = {}
                                if status_tmp == "success":
                                    try:
                                        contact_payload = json.loads(message_tmp) if isinstance(message_tmp, str) else (message_tmp or {})
                                    except Exception:
                                        contact_payload = {}
                                if isinstance(contact_payload, dict):
                                    name_val = (contact_payload.get("name") or "").strip()
                                    email_val = (contact_payload.get("email") or "").strip()
                                    if name_val and email_val:
                                        try:
                                            update_enrollment_context(wa_id, {"nombre": name_val, "email": email_val})
                                        except Exception as _ctx_err:
                                            logging.error(f"No se pudo actualizar enrollment con nombre/email para {wa_id}: {_ctx_err}")
                            except Exception as _persist_err:
                                logging.error(f"Error persistiendo contacto tras collect_contact: {_persist_err}")
                        elif tool_call.name == "recomendar_cursos":
                            result_data = processor(tool_call, wa_id=wa_id)
                        elif tool_call.name == "inscribir_lead_crm":
                            result_data = processor(tool_call, wa_id=wa_id)
                        else:
                            result_data = processor(tool_call)
                    else:
                        logging.warning(f"Tool call desconocido: {tool_call.name}")
                        result_data = {
                            "output": json.dumps({
                                "status": "error", 
                                "message": f"Función desconocida: {tool_call.name}"
                            })
                        }

                    # Extraer el mensaje del resultado de forma segura
                    try:
                        output_json = json.loads(result_data.get("output", "{}"))
                        result_message = output_json.get("message", "Error al procesar la función.")
                        status_value = (output_json.get("status") or "").strip().lower()
                    except (json.JSONDecodeError, TypeError) as json_e:
                        logging.error(f"Error al decodificar JSON del resultado de '{tool_call.name}': {json_e}")
                        result_message = "Hubo un error interno al procesar la solicitud."
                        status_value = "error"

                    # El mensaje que devolveremos al modelo será el propio resultado
                    # Guardar el último mensaje de tool para posibles rutas de pass-through
                    try:
                        last_tool_result_message = str(result_message)
                    except Exception:
                        last_tool_result_message = result_message

                    # Si la herramienta devolvió un error para inscribir_lead_crm, comunicárselo al usuario
                    try:
                        if tool_call.name == "inscribir_lead_crm" and isinstance(output_json, dict) and output_json.get("status") == "error":
                            recipient_id = f"+{wa_id}"
                            msg_data = get_text_message_input(recipient_id, str(result_message))
                            sent_error_id = send_message(msg_data)
                            try:
                                env_name = current_app.config.get("ENV_NAME", "Bot")
                                log_message_to_db(
                                    wa_id=wa_id,
                                    sender_name=env_name,
                                    message_text=str(result_message),
                                    direction='outbound_bot',
                                    project_name=env_name,
                                    whatsapp_message_id=sent_error_id,
                                    status='sent' if sent_error_id else 'failed',
                                    response_id=current_response_id
                                )
                            except Exception as _log_error_err:
                                logging.error(f"Error logging DNI error message for {wa_id}: {_log_error_err}")
                            logging.info(f"Notificado al usuario error de DNI/NIE para {recipient_id}: '{result_message}'")
                            # Tras el error de DNI, reenviar formulario de inscripción (Flow o plantilla)
                            try:
                                template_name = current_app.config.get("WHATSAPP_TEMPLATE_ENROLL", "cuestionario_inscripcion")
                                language_code = current_app.config.get("WHATSAPP_TEMPLATE_LANG", "es_ES")
                                # Choose Flow ID based on whether email is stored
                                ctx_email = get_enrollment_context(wa_id)
                                email_present = not is_missing_email(ctx_email.get("email") or ctx_email.get("Email"))
                                chosen_flow_id = (
                                    current_app.config.get("WHATSAPP_FLOW_ID_INSCRIPCION")
                                    if email_present else
                                    current_app.config.get("WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL")
                                )
                                flow_present = bool(chosen_flow_id and current_app.config.get("WHATSAPP_FLOW_TOKEN"))
                                sent_form_id = None
                                if flow_present:
                                    header_text = "Inscripción"
                                    body_text = "Abre de nuevo el formulario y completa el DNI correctamente."
                                    footer_text = "Transfers & Experiences"
                                    flow_payload = get_flow_message_input(
                                        recipient_id,
                                        header_text,
                                        body_text,
                                        footer_text,
                                        chosen_flow_id,
                                        current_app.config.get("WHATSAPP_FLOW_TOKEN"),
                                        current_app.config.get("WHATSAPP_FLOW_ACTION", "navigate"),
                                        current_app.config.get("WHATSAPP_FLOW_CTA"),
                                        "3",
                                        current_app.config.get("WHATSAPP_FLOW_ACTION_SCREEN"),
                                        json.loads(current_app.config.get("WHATSAPP_FLOW_ACTION_PAYLOAD_JSON")) if current_app.config.get("WHATSAPP_FLOW_ACTION_PAYLOAD_JSON") else None,
                                    )
                                    logging.info("Reenviando Flow de inscripción tras DNI inválido.")
                                    sent_form_id = send_message(flow_payload)
                                else:
                                    payload = get_template_message_input(recipient_id, template_name, language_code, None)
                                    logging.info("Reenviando plantilla de inscripción tras DNI inválido.")
                                    sent_form_id = send_message(payload)
                                try:
                                    env_name = current_app.config.get("ENV_NAME", "Bot")
                                    log_message_to_db(
                                        wa_id=wa_id,
                                        sender_name=env_name,
                                        message_text="Reenvío de formulario de inscripción",
                                        direction='outbound_bot',
                                        project_name=env_name,
                                        whatsapp_message_id=sent_form_id,
                                        status='sent' if sent_form_id else 'failed',
                                        model=chosen_model,
                                        response_id=current_response_id
                                    )
                                except Exception as _log_err2:
                                    logging.error(f"Error registrando reenvío de formulario para {wa_id}: {_log_err2}")
                            except Exception as _resend_err:
                                logging.error(f"Error reenviando formulario tras DNI inválido para {wa_id}: {_resend_err}")
                            # Completar el ciclo del modelo enviando el output de la tool antes de cortar
                            try:
                                call_identifier_tmp = getattr(tool_call, "call_id", None) or getattr(tool_call, "id", None)
                                finalize_messages = [{"role": "user", "content": message}] + [{
                                    "type": "function_call_output",
                                    "call_id": call_identifier_tmp,
                                    "output": str(result_message)
                                }]
                                response_finalize = client.responses.create(
                                    model=chosen_model,
                                    instructions=instructions,
                                    input=finalize_messages,
                                    previous_response_id=current_response_id,
                                    tools=tools,
                                    **reasoning_param
                                )
                                current_response_id = response_finalize.id
                                store_current_response_id(DB_DIRECTORY, current_response_id, wa_id)
                            except APITimeoutError as _timeout_fin_err:
                                logging.error(f"Timeout al finalizar tool_call inscribir_lead_crm (error): {_timeout_fin_err}")
                            except APIError as _api_fin_err:
                                logging.error(f"Error de API al finalizar tool_call inscribir_lead_crm (error): {_api_fin_err}")
                            except Exception as _fin_err:
                                logging.error(f"Error finalizando tool_call inscribir_lead_crm (error): {_fin_err}")
                            # Cortar el ciclo para evitar que el modelo invente confirmaciones de inscripción
                            class _Tmp:
                                id = current_response_id
                                output_text = ""
                            response = _Tmp()
                            # Romper procesamiento de tool calls
                            tool_call_outputs = []
                            break
                    except Exception as _notify_err:
                        logging.error(f"Error notificando mensaje de error al usuario: {_notify_err}")

                    # Si la función devuelve cursos, enviarlos como tarjetas con botón "Inscribirme"
                    if tool_call.name == "recomendar_cursos":
                        if (status_value == "success") and not sent_course_cards:
                            try:
                                # 'result_message' es un string JSON de la lista de cursos
                                courses = json.loads(result_message)
                                if isinstance(courses, list) and len(courses) > 0:
                                    recipient_id = f"+{wa_id}"
                                    sent_count = 0
                                    for course in courses:
                                        body = course.get("whatsapp_card_text") or course.get("curso") or "Curso"
                                        # Limitar longitud para cumplir con API
                                        if isinstance(body, str) and len(body) > 900:
                                            body = body[:900] + "…"
                                        button_id = course.get("whatsapp_button_id") or f"curso_{sent_count}"
                                        button_title = course.get("whatsapp_button_title") or "Inscribirme"
                                        # Preparar botón de Más Info (si hay enlace)
                                        enlace_url = (
                                            course.get("enlace_acortado")
                                            or course.get("enlace")
                                            or course.get("link")
                                        )
                                        # Extraer sufijo (código) para mapear botones de info
                                        try:
                                            id_suffix = (button_id.split("_", 1)[1]) if isinstance(button_id, str) and "_" in button_id else str(sent_count)
                                        except Exception:
                                            id_suffix = str(sent_count)
                                        # Construir botones: primero "Más Info" si hay enlace, luego "Inscribirme"
                                        try:
                                            buttons_payload = []
                                            if enlace_url:
                                                buttons_payload.append({"id": f"info_{id_suffix}", "title": "Más Info"})
                                            buttons_payload.append({"id": button_id, "title": button_title})
                                            # Guardar mapping de enlace para el botón "Más Info"
                                            try:
                                                ctx_curr = get_enrollment_context(wa_id)
                                                course_links = dict(ctx_curr.get("course_links") or {})
                                                course_links[str(id_suffix)] = str(enlace_url or "").strip()
                                                # Guardar también mapping de nombres por código/sufijo para mostrar en dashboard
                                                course_names = dict(ctx_curr.get("course_names") or {})
                                                try:
                                                    course_name_val = (course.get("curso") or "").strip()
                                                except Exception:
                                                    course_name_val = str(course.get("whatsapp_card_text") or "").strip()
                                                if course_name_val:
                                                    course_names[str(id_suffix)] = course_name_val
                                                update_enrollment_context(wa_id, {"course_links": course_links, "course_names": course_names})
                                            except Exception as _ctx_link_err:
                                                logging.error(f"No se pudo actualizar course_links para {wa_id}: {_ctx_link_err}")
                                            message_data = get_button_message_input(
                                                recipient_id,
                                                body,
                                                buttons_payload
                                            )
                                            sent_id_card = send_message(message_data)
                                            try:
                                                env_name = current_app.config.get("ENV_NAME", "Bot")
                                                log_message_to_db(
                                                    wa_id=wa_id,
                                                    sender_name=env_name,
                                                    message_text=body,
                                                    direction='outbound_bot',
                                                    project_name=env_name,
                                                    whatsapp_message_id=sent_id_card,
                                                    status='sent' if sent_id_card else 'failed',
                                                    model=chosen_model,
                                                    required_action=f"interactive_buttons:{json.dumps(buttons_payload, ensure_ascii=False)}",
                                                    response_id=current_response_id
                                                )
                                            except Exception as log_err:
                                                logging.error(f"Error logging course card button message for {wa_id}: {log_err}")
                                            sent_count += 1
                                        except Exception as send_err:
                                            logging.error(f"Error enviando tarjeta de curso: {send_err}")
                                    # Enviar botón para "No me interesa ningún curso" (sin más texto)
                                    try:
                                        explanation_text = "No me interesa ningún curso"
                                        button_title = "No me interesa"
                                        buttons_payload = [{"id": "courses_none", "title": button_title}]
                                        none_button = get_button_message_input(
                                            recipient_id,
                                            explanation_text,
                                            buttons_payload
                                        )
                                        sent_id_none = send_message(none_button)
                                        try:
                                            env_name = current_app.config.get("ENV_NAME", "Bot")
                                            log_message_to_db(
                                                wa_id=wa_id,
                                                sender_name=env_name,
                                                message_text=explanation_text,
                                                direction='outbound_bot',
                                                project_name=env_name,
                                                whatsapp_message_id=sent_id_none,
                                                status='sent' if sent_id_none else 'failed',
                                                model=chosen_model,
                                                required_action=f"interactive_buttons:{json.dumps(buttons_payload, ensure_ascii=False)}",
                                                response_id=current_response_id
                                            )
                                        except Exception as log_err:
                                            logging.error(f"Error logging 'no interest' button for {wa_id}: {log_err}")
                                    except Exception as send_err:
                                        logging.error(f"Error enviando botón de 'No me interesa': {send_err}")
                                    sent_course_cards = True
                                    # Reiniciar el contador de página a 1 al enviar la primera tanda
                                    try:
                                        update_enrollment_context(wa_id, {"pagina_actual": 1})
                                    except Exception as ctx_err:
                                        logging.error(f"No se pudo actualizar 'pagina_actual' a 1 para {wa_id}: {ctx_err}")
                                    # Indicar al modelo que ya se enviaron los cursos y que espere selección
                                    response_sent_manually = True
                                    # Enviar de vuelta al modelo el JSON completo de cursos como salida de la tool
                                    # para que quede en el contexto del hilo
                                    minimal_finalize_needed = True
                                    # >>> LOCK HANDLING <<<
                                    # Check if a lock is already active from whatsapp_utils.py
                                    # If so, don't set our own lock to avoid conflicts
                                    existing_ctx = get_enrollment_context(wa_id)
                                    existing_lock = existing_ctx.get("processing_lock_until")
                                    if not existing_lock or time.time() >= existing_lock:
                                        # No active lock, set our own
                                        lock_until_ts = time.time() + 30  # Lock for 30 seconds max
                                        worker_id = f"worker_{os.getpid()}_{int(time.time())}"
                                        update_enrollment_context(wa_id, {
                                            "processing_lock_until": lock_until_ts,
                                            "processing_worker_id": worker_id,
                                            "processing_start_time": time.time()
                                        })
                                        lock_was_set = True
                                        logging.info(f"🔒 OpenAI service lock acquired for {wa_id}")
                                    else:
                                        # Lock already active, don't override it
                                        logging.info(f"🔒 Using existing lock for {wa_id}, not setting new one")
                                else:
                                    # Lista vacía: cortar para evitar re-llamadas y duplicados
                                    stop_after_recommendation = True
                            except Exception as e:
                                logging.error(f"No se pudieron procesar/enviar tarjetas de cursos: {e}")
                                stop_after_recommendation = True
                        else:
                            # status != success (p.ej., not_found): no intentar parsear ni enviar tarjetas
                            stop_after_recommendation = True

                    # En Responses API, la devolución de resultado de herramienta requiere el identificador de la llamada.
                    # En gpt-5, el identificador se expone como 'id'; en versiones previas podía venir como 'call_id'.
                    call_identifier = getattr(tool_call, "call_id", None) or getattr(tool_call, "id", None)
                    tool_call_outputs.append({
                        "type": "function_call_output",
                        "call_id": call_identifier,
                        "output": str(result_message)
                    })

                except Exception as e:
                    logging.error(f"Error al procesar tool call '{tool_call.name}': {e}")
                    call_identifier = getattr(tool_call, "call_id", None) or getattr(tool_call, "id", None)
                    tool_call_outputs.append({
                        "type": "function_call_output",
                        "call_id": call_identifier,
                        "output": f"Error interno al ejecutar la función {tool_call.name}."
                    })

            # Construir la nueva entrada para la siguiente llamada
            next_input_messages = [{"role": "user", "content": message}] + tool_call_outputs

            try:
                # Si ya hemos enviado el catálogo, completar tool call mínimamente y cortar el bucle
                if response_sent_manually:
                    if minimal_finalize_needed:
                        try:
                            logging.debug("Finalizando tool_call 'recomendar_cursos' con payload mínimo para mantener el hilo.")
                            response_finalize = client.responses.create(
                                model=chosen_model,
                                instructions=instructions,
                                input=next_input_messages,
                                previous_response_id=current_response_id,
                                tools=tools,
                                #temperature=0,
                                max_output_tokens=16,
                                **reasoning_param
                            )
                            current_response_id = response_finalize.id
                            store_current_response_id(DB_DIRECTORY, current_response_id, wa_id)
                        except APITimeoutError as _timeout_fin_err:
                            logging.error(f"Timeout al finalizar mínimamente recomendar_cursos: {_timeout_fin_err}")
                        except APIError as _api_fin_err:
                            logging.error(f"Error de API al finalizar mínimamente recomendar_cursos: {_api_fin_err}")
                        except Exception as _fin_err:
                            logging.error(f"Error finalizando mínimamente recomendar_cursos: {_fin_err}")
                    # Evitar respuesta visible del modelo (hemos enviado WhatsApp)
                    class _Tmp:
                        id = current_response_id
                        output_text = ""
                    response = _Tmp()
                    # Importante: salir del bucle para no hacer la llamada completa posterior a OpenAI
                    break
                    
                # Si no hay cursos (not_found u otro estado), pedir al modelo que genere el mensaje y DEVOLVERLO
                elif stop_after_recommendation:
                    # Enviar el mensaje neutral directamente por WhatsApp y finalizar mínimamente el hilo
                    try:
                        recipient_id = f"+{wa_id}"
                        not_found_text = str(last_tool_result_message) if last_tool_result_message is not None else "Ahora mismo no hay cursos que encajen con esos criterios."
                        msg_data = get_text_message_input(recipient_id, not_found_text)
                        sent_id = send_message(msg_data)
                        try:
                            env_name = current_app.config.get("ENV_NAME", "Bot")
                            log_message_to_db(
                                wa_id=wa_id,
                                sender_name=env_name,
                                message_text=not_found_text,
                                direction='outbound_bot',
                                project_name=env_name,
                                whatsapp_message_id=sent_id,
                                status='sent' if sent_id else 'failed',
                                model=chosen_model,
                                response_id=current_response_id
                            )
                        except Exception as _log_err:
                            logging.error(f"Error logging not-found message for {wa_id}: {_log_err}")
                    except Exception as _send_err:
                        logging.error(f"Error enviando mensaje not-found por WhatsApp: {_send_err}")

                    # Finalizar mínimamente la tool_call para avanzar el previous_response_id y no perder el hilo
                    try:
                        logging.debug("Finalizando tool_call 'recomendar_cursos' (not_found) con payload mínimo para mantener el hilo.")
                        response_finalize = client.responses.create(
                            model=chosen_model,
                            instructions=instructions,
                            input=next_input_messages,
                            previous_response_id=current_response_id,
                            tools=tools,
                            #temperature=0,
                            max_output_tokens=16,
                            **reasoning_param
                        )
                        current_response_id = response_finalize.id
                        store_current_response_id(DB_DIRECTORY, current_response_id, wa_id)
                    except APITimeoutError as _timeout_fin_err:
                        logging.error(f"Timeout al finalizar mínimamente recomendar_cursos (not_found): {_timeout_fin_err}")
                    except APIError as _api_fin_err:
                        logging.error(f"Error de API al finalizar mínimamente recomendar_cursos (not_found): {_api_fin_err}")
                    except Exception as _fin_err:
                        logging.error(f"Error finalizando mínimamente recomendar_cursos (not_found): {_fin_err}")

                    # Evitar respuesta visible del modelo (hemos enviado WhatsApp) y salir
                    class _Tmp:
                        id = current_response_id
                        output_text = ""
                    response = _Tmp()
                    break

                response = client.responses.create(
                    model=chosen_model,
                    instructions=instructions,
                    input=next_input_messages,
                    previous_response_id=current_response_id,
                    tools=tools,
                    **reasoning_param
                )
                current_response_id = response.id
                store_current_response_id(DB_DIRECTORY, current_response_id, wa_id)
                logging.debug(f"Respuesta de OpenAI tras tool call: {response}")

            except APITimeoutError as e:
                logging.error(f"Timeout de OpenAI en llamada posterior a tool call para wa_id {wa_id}: {e}")
                return "La solicitud tardó demasiado. Por favor, inténtalo de nuevo.", None
            except APIError as e:
                logging.error(f"Error de API de OpenAI en llamada posterior a tool call: {e}")
                return "Hubo un problema al continuar la conversación con el asistente después de ejecutar una acción.", None
            except Exception as e:
                logging.error(f"Error inesperado en llamada posterior a tool call de OpenAI: {e}")
                return "Ocurrió un error inesperado al continuar la conversación.", None

        # Almacenar el identificador de la respuesta final (si no hubo error antes)
        if response:
          current_response_id = response.id
          store_current_response_id(DB_DIRECTORY, current_response_id, wa_id)
          
          # Guardar en Supabase si está habilitado
          if is_supabase_enabled():
              update_id_in_supabase(wa_id, current_response_id)
              
          if response_sent_manually:
              return "", current_response_id
          return response.output_text, current_response_id
        else:
          # Si response es None por un error previo, devolver un mensaje genérico
          return "No se pudo obtener una respuesta final del asistente.", current_response_id if 'current_response_id' in locals() else None
    finally:
        if lock_was_set:
            # >>> RELEASE LOCK HERE <<<
            # Only release if this was our lock (not an external one from whatsapp_utils.py)
            try:
                logging.info(f"🔓 Releasing OpenAI service processing lock for wa_id {wa_id}.")
                update_enrollment_context(wa_id, {
                    "processing_lock_until": None,
                    "processing_worker_id": None,
                    "processing_start_time": None
                })
            except Exception as lock_release_err:
                logging.error(f"❌ Error releasing lock for {wa_id}: {lock_release_err}")
            
# --- Minimal thread update helper for non-tool triggers (e.g., 'No me interesa') ---
def append_thread_with_payload(app, wa_id: str, user_note: str, assistant_payload_text: str) -> bool:
    """
    Añade una actualización mínima al hilo de OpenAI sin invocar tools:
    - user_note se guarda como mensaje del usuario para contexto (ej. "[trigger:courses_none] página 2 enviada").
    - assistant_payload_text se guarda como mensaje del asistente (ej. JSON de cursos),
      limitando la respuesta del modelo a 16 tokens para avanzar el previous_response_id sin texto visible.
    """
    try:
        start_time = time.time()
        # Respetar lock de procesamiento si está activo para evitar carreras con tool-calls
        try:
            ctx_lock = get_enrollment_context(wa_id)
            lock_until = ctx_lock.get("processing_lock_until") if isinstance(ctx_lock, dict) else None
            # Espera breve (máx ~2s) si el lock sigue activo
            if isinstance(lock_until, (int, float)):
                attempts = 0
                while (lock_until - time.time()) > 0 and attempts < 4:
                    time.sleep(min(lock_until - time.time(), 0.5))
                    attempts += 1
        except Exception as _lock_err:
            logging.debug(f"append_thread_with_payload: no se pudo evaluar lock para {wa_id}: {_lock_err}")
        prev_id = get_previous_response_id(DB_DIRECTORY, wa_id)
        instructions = read_instructions(ENV_NAME)
        input_messages = [
            {"role": "user", "content": str(user_note or "")},
            {"role": "assistant", "content": str(assistant_payload_text or "")},
        ]
        
        # Add reasoning parameter for gpt-5 models
        model_name = OPENAI_MODEL_NAME
        reasoning_param = {}
        if model_name and 'gpt-5' in model_name:
            reasoning_param = {"reasoning": {"effort": "low"}}
            
        response = client.responses.create(
            model=model_name,
            instructions=instructions,
            input=input_messages,
            previous_response_id=prev_id,
            #temperature=0,
            max_output_tokens=16,
            **reasoning_param
        )
        store_current_response_id(DB_DIRECTORY, response.id, wa_id)
        elapsed_time = time.time() - start_time
        try:
            raw_log_text = (assistant_payload_text or "").replace('\n', ' ').replace('\r', '')
            log_result = (raw_log_text[:20] + '...') if len(raw_log_text) > 20 else raw_log_text
        except Exception:
            log_result = "[payload logged]"
        logging.info(f"🧠 OpenAI thread append: '{log_result}' (generated in -> {elapsed_time:.2f} seconds)")
        return True
    except APITimeoutError as timeout_err:
        logging.error(f"append_thread_with_payload timeout for {wa_id}: {timeout_err}")
        return False
    except APIError as api_err:
        logging.error(f"append_thread_with_payload API error for {wa_id}: {api_err}")
        return False

# Removed async variant per user request; we will call append_thread_with_payload synchronously.

def generate_response(app, message_body, wa_id, name, message_id=None):
    """
    Genera la respuesta del asistente utilizando OpenAI a partir del mensaje del usuario.

    Args:
        app (Flask): The Flask application instance.
        message_body (str): Mensaje procesado del usuario que se agrega al hilo.
        wa_id (str): Identificador de WhatsApp.
        name (str): Nombre del usuario.
        message_id (str, optional): ID del mensaje original de WhatsApp. Defaults to None.

    Returns:
        dict: Diccionario con la respuesta generada, número de intentos, acción requerida y error (si ocurre).
    """
    start_time = time.time()
    result = None
    try:
        result_text, result_response_id = get_response_api(app, message_body, get_openai_client(), wa_id, message_id)
        elapsed_time = time.time() - start_time
        # Truncate the log_result for cleaner logging
        raw_log_text = result_text.replace('\\n', ' ').replace('\\r', '') if result_text else "[No response]"
        log_result = (raw_log_text[:20] + '...') if len(raw_log_text) > 20 else raw_log_text
        logging.info(f"🧠 2. OpenAI response: '{log_result}' (generated in -> {elapsed_time:.2f} seconds)\n")
        return {
            "respuesta": result_text,
            "intentos": 1,
            "accion_requerida": "Ninguna",
            "error": None,
            "response_id": result_response_id
        }
    except Exception as e:
        elapsed_time = time.time() - start_time
        # Truncate the log_result for cleaner logging in case of error
        raw_log_text_error = result_text.replace('\\n', ' ').replace('\\r', '') if 'result_text' in locals() and result_text else "[No response before error]"
        log_result_error = (raw_log_text_error[:20] + '...') if len(raw_log_text_error) > 20 else raw_log_text_error
        print()
        logging.exception(f"Excepción no controlada en generate_response (Response so far: '{log_result_error}') para wa_id {wa_id} tras {elapsed_time:.2f} segundos: {e}\\n")
        return {
            "respuesta": "Lo siento, ocurrió un error inesperado al procesar tu solicitud.",
            "intentos": 1,
            "accion_requerida": "Error",
            "error": str(e),
            "response_id": None
        }
