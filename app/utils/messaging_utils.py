import logging
import json
import time
import random
import requests
from flask import current_app, jsonify, has_app_context

def _truncate_for_whatsapp(value, max_length):
    """
    Trunca un texto para cumplir los límites de WhatsApp.

    Args:
        value (Any): Texto de entrada.
        max_length (int): Longitud máxima permitida.

    Returns:
        str: Texto truncado (con «…» si aplica) dentro del límite.
    """
    try:
        text = str(value) if value is not None else ""
    except Exception:
        text = ""
    if max_length is None or max_length <= 0:
        return text
    if len(text) <= max_length:
        return text
    if max_length == 1:
        return text[:1]
    return text[: max_length - 1] + "…"

def log_http_response(response):
    """
    Registra el código de estado HTTP de la respuesta.
    
    Args:
        response (requests.Response): Respuesta obtenida de la API.
    """
    logging.info(f"Status: {response.status_code}")
    # Si se necesita, se pueden activar estos logs para más detalles:
    # logging.info(f"Content-type: {response.headers.get('content-type')}")
    # logging.info(f"Body: {response.text}")

def get_text_message_input(recipient, text):
    """
    Prepara el payload JSON para enviar un mensaje a WhatsApp.
    
    Args:
        recipient (str): Número de teléfono del destinatario (debe incluir '+').
        text (str): Contenido del mensaje.
        
    Returns:
        str: Cadena JSON con la estructura requerida por la API.
    """
    return json.dumps({
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": text
        }
    })

def get_button_message_input(recipient, body_text, buttons):
    """
    Crea el payload JSON para un mensaje interactivo con botones de respuesta.

    Args:
        recipient (str): Número del destinatario con prefijo '+'.
        body_text (str): Texto del cuerpo del mensaje.
        buttons (list[dict]): Lista de botones con forma {"id": str, "title": str}.

    Returns:
        str: Cadena JSON con la estructura requerida por WhatsApp Cloud API.
    """
    formatted_buttons = []
    for btn in buttons:
        if not btn or not btn.get("id") or not btn.get("title"):
            continue
        title = _truncate_for_whatsapp(btn.get("title"), 20)  # Límite WhatsApp: 20 chars para títulos de botones
        formatted_buttons.append({
            "type": "reply",
            "reply": {
                "id": btn.get("id"),
                "title": title,
            },
        })

    return json.dumps({
        "messaging_product": "whatsapp",
        "to": recipient,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {
                "text": body_text
            },
            "action": {
                "buttons": formatted_buttons
            }
        }
    })

def get_list_message_input(recipient, body_text, rows, button_label="Seleccionar", section_title="Opciones"):
    """
    Crea el payload JSON para un mensaje interactivo de tipo lista.

    Args:
        recipient (str): Número del destinatario con prefijo '+'.
        body_text (str): Texto del cuerpo del mensaje.
        rows (list[dict]): Filas con forma {"id": str, "title": str, "description": Optional[str]}.
        button_label (str): Texto del botón que abre la lista.
        section_title (str): Título de la sección.

    Returns:
        str: Cadena JSON válida para WhatsApp Cloud API.
    """
    # Normalizar y truncar filas según límites de WhatsApp
    # - title: máx 24 caracteres
    # - description: máx 72 caracteres
    formatted_rows = []
    for r in rows:
        if not r or not r.get("id") or not r.get("title"):
            continue
        title = _truncate_for_whatsapp(r.get("title"), 24)
        row = {"id": r["id"], "title": title}
        if r.get("description"):
            row["description"] = _truncate_for_whatsapp(r.get("description"), 72)
        formatted_rows.append(row)

    # WhatsApp limits: max 10 rows per section. Chunk rows into multiple sections if needed.
    sections = []
    if len(formatted_rows) <= 10:
        sections.append({"title": _truncate_for_whatsapp(section_title, 24), "rows": formatted_rows})
    else:
        for idx in range(0, len(formatted_rows), 10):
            chunk = formatted_rows[idx:idx+10]
            title = _truncate_for_whatsapp(f"{section_title} {idx//10 + 1}", 24)
            sections.append({"title": title, "rows": chunk})

    payload = {
        "messaging_product": "whatsapp",
        "to": recipient,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": body_text},
            "action": {
                "button": _truncate_for_whatsapp(button_label, 20),
                "sections": sections
            }
        }
    }
    return json.dumps(payload)

def get_template_message_input(recipient: str, template_name: str, language_code: str = "es_ES", components=None):
    """
    Crea el payload JSON para enviar una plantilla de WhatsApp por nombre.

    Args:
        recipient (str): Número del destinatario con prefijo '+'.
        template_name (str): Nombre de la plantilla aprobada (p.ej. 'cuestionario_inscripcion').
        language_code (str): Código de idioma (p.ej. 'es_ES').
        components (list | None): Componentes opcionales (header/body/buttons) si la plantilla tiene variables.

    Returns:
        str: Cadena JSON con la estructura requerida por WhatsApp Cloud API.
    """
    payload = {
        "messaging_product": "whatsapp",
        "to": recipient,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language_code}
        }
    }
    if components:
        payload["template"]["components"] = components
    return json.dumps(payload)

def get_flow_message_input(
    recipient: str,
    header_text: str,
    body_text: str,
    footer_text: str | None,
    flow_id: str,
    flow_token: str,
    flow_action: str = "navigate",
    flow_cta: str | None = None,
    flow_message_version: str = "3",
    flow_action_screen: str | None = None,
    flow_action_payload: dict | None = None,
):
    """
    Crea el payload JSON para enviar un mensaje interactivo de tipo Flow.

    Referencia: Meta Docs (Interactive Flow Message)
    """
    # WhatsApp constraint: header text must be <= 60 characters
    try:
        from app.utils.messaging_utils import _truncate_for_whatsapp as _truncate
    except Exception:
        # Fallback local truncation if import path changes
        def _truncate(value, max_length):
            try:
                text = str(value) if value is not None else ""
            except Exception:
                text = ""
            if max_length is None or max_length <= 0:
                return text
            if len(text) <= max_length:
                return text
            if max_length == 1:
                return text[:1]
            return text[: max_length - 1] + "…"

    safe_header_text = _truncate(header_text, 60)

    interactive_obj = {
        "type": "flow",
        "header": {"type": "text", "text": safe_header_text},
        "body": {"text": body_text},
        "action": {
            "name": "flow",
            "parameters": {
                "flow_id": flow_id,
                "flow_message_version": flow_message_version,
                "flow_token": flow_token,
                "flow_action": flow_action,
            },
        },
    }

    if footer_text:
        interactive_obj["footer"] = {"text": footer_text}
    if flow_cta:
        interactive_obj["action"]["parameters"]["flow_cta"] = flow_cta
    if flow_action_screen:
        interactive_obj["action"]["parameters"]["flow_action_screen"] = flow_action_screen
    if isinstance(flow_action_payload, dict) and flow_action_payload:
        interactive_obj["action"]["parameters"]["flow_action_payload"] = flow_action_payload

    payload = {
        "messaging_product": "whatsapp",
        "to": recipient,
        "type": "interactive",
        "interactive": interactive_obj,
    }
    return json.dumps(payload)

def get_media_message_input(recipient, media_type, media_url_or_id, caption=None, filename=None):
    """
    Crea el payload JSON para enviar un mensaje multimedia (imagen, documento, video, audio).
    
    Args:
        recipient (str): Número del destinatario con prefijo '+'.
        media_type (str): Tipo de media: 'image', 'document', 'video', 'audio'.
        media_url_or_id (str): URL del archivo o ID del media en WhatsApp.
        caption (str, optional): Texto de caption (solo para image, video, document).
        filename (str, optional): Nombre del archivo (requerido para document).
        
    Returns:
        str: Cadena JSON con la estructura requerida por WhatsApp Cloud API.
    """
    if media_type not in ('image', 'document', 'video', 'audio'):
        raise ValueError(f"Invalid media_type: {media_type}")
    
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient,
        "type": media_type
    }
    
    media_obj = {}
    
    # Determinar si es URL o ID
    if media_url_or_id.startswith('http://') or media_url_or_id.startswith('https://'):
        media_obj["link"] = media_url_or_id
    else:
        media_obj["id"] = media_url_or_id
    
    # Agregar caption si aplica
    if caption and media_type in ('image', 'video', 'document'):
        media_obj["caption"] = caption
    
    # Agregar filename para documentos
    if media_type == 'document' and filename:
        media_obj["filename"] = filename
    
    payload[media_type] = media_obj
    
    return json.dumps(payload)

def upload_media_to_whatsapp(file_path, mime_type):
    """
    Sube un archivo multimedia a WhatsApp Cloud API y retorna el media ID.
    
    Args:
        file_path (str): Ruta local del archivo a subir.
        mime_type (str): Tipo MIME del archivo (ej: 'image/jpeg', 'application/pdf').
        
    Returns:
        str | dict: Media ID si fue exitoso, o dict con 'error' si falló.
    """
    access_token = current_app.config.get('ACCESS_TOKEN')
    version = current_app.config.get('VERSION')
    phone_number_id = current_app.config.get('PHONE_NUMBER_ID')
    
    if not all([access_token, version, phone_number_id]):
        logging.error("WhatsApp API credentials missing in config.")
        return {'error': 'Credenciales de WhatsApp no configuradas'}
    
    url = f"https://graph.facebook.com/{version}/{phone_number_id}/media"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    try:
        with open(file_path, 'rb') as f:
            files = {
                'file': (file_path, f, mime_type),
                'messaging_product': (None, 'whatsapp'),
                'type': (None, mime_type)
            }
            
            response = requests.post(url, headers=headers, files=files, timeout=60)
            
            if 200 <= response.status_code < 300:
                try:
                    response_data = response.json()
                    media_id = response_data.get('id')
                    if media_id:
                        logging.info(f"Media uploaded successfully. ID: {media_id}")
                        return media_id
                    else:
                        logging.error(f"Media upload response missing ID: {response_data}")
                        return {'error': 'WhatsApp no devolvió ID del archivo'}
                except ValueError:
                    logging.error(f"WhatsApp API returned non-JSON response: {response.text}")
                    return {'error': 'Respuesta inválida de WhatsApp'}
            else:
                logging.error(f"WhatsApp API media upload error {response.status_code}: {response.text}")
                # Parse error message from WhatsApp
                error_msg = 'Error al subir archivo a WhatsApp'
                try:
                    error_data = response.json()
                    wa_error = error_data.get('error', {})
                    error_code = wa_error.get('code', '')
                    error_message = wa_error.get('message', '')
                    
                    # Check for file size error
                    if 'file size' in error_message.lower() or error_code == 100:
                        error_msg = 'El archivo excede el tamaño máximo permitido por WhatsApp'
                    elif error_message:
                        error_msg = f'Error de WhatsApp: {error_message}'
                except:
                    pass
                return {'error': error_msg}
                
    except requests.exceptions.Timeout:
        logging.error(f"Timeout uploading media to WhatsApp")
        return {'error': 'Tiempo de espera agotado al subir el archivo'}
    except Exception as e:
        logging.error(f"Error uploading media to WhatsApp: {e}")
        return {'error': f'Error al subir archivo: {str(e)}'}

def send_message(data):
    """
    Sends a message using WhatsApp Cloud API and returns the message ID on success.
    
    Args:
        data (str): Cadena JSON con el mensaje a enviar.
        
    Returns:
        str: El ID del mensaje enviado si el envío fue exitoso, None en caso contrario.
    """
    access_token = current_app.config.get('ACCESS_TOKEN')
    version = current_app.config.get('VERSION')
    phone_number_id = current_app.config.get('PHONE_NUMBER_ID')

    if not all([access_token, version, phone_number_id]):
        logging.error("WhatsApp API credentials missing in config.")
        return None # Return None to indicate failure

    url = f"https://graph.facebook.com/{version}/{phone_number_id}/messages"
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    max_attempts = 4
    base_backoff = 0.5

    try:
        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.post(url, data=data, headers=headers, timeout=15)

                if 200 <= response.status_code < 300:
                    try:
                        response_data = response.json()
                    except ValueError:
                        logging.error(f"WhatsApp API returned non-JSON success response: {response.text}")
                        return None

                    message_id = None
                    if response_data.get('messages') and len(response_data['messages']) > 0:
                        message_id = response_data['messages'][0].get('id')
                    if message_id:
                        return message_id
                    logging.error(f"Message sent but no message ID in success response: {response_data}")
                    return None

                # Non-2xx → decide whether to retry
                content = response.text
                json_body = None
                try:
                    json_body = response.json()
                    content = json.dumps(json_body, ensure_ascii=False)
                except ValueError:
                    pass

                # Identify transient conditions
                status = response.status_code
                is_transient = False
                if status >= 500 or status == 429:
                    is_transient = True
                else:
                    try:
                        if isinstance(json_body, dict):
                            err = json_body.get("error") or {}
                            if isinstance(err, dict):
                                if bool(err.get("is_transient")):
                                    is_transient = True
                                # Meta often wraps transient outages as OAuthException code 2
                                if (err.get("type") == "OAuthException" and int(err.get("code", 0)) == 2):
                                    is_transient = True
                    except Exception:
                        pass

                if is_transient and attempt < max_attempts:
                    delay = min(4.0, base_backoff * (2 ** (attempt - 1))) + random.uniform(0, 0.25)
                    logging.warning(f"WhatsApp API transient error {status}: {content}. Retrying in {delay:.2f}s (attempt {attempt}/{max_attempts})...")
                    time.sleep(delay)
                    continue

                # Not transient or out of attempts → log and give up
                logging.error(f"WhatsApp API error {status} (url={url}): {content}")
                return None

            except requests.Timeout:
                if attempt < max_attempts:
                    delay = min(4.0, base_backoff * (2 ** (attempt - 1))) + random.uniform(0, 0.25)
                    logging.warning(f"Timeout sending WhatsApp message. Retrying in {delay:.2f}s (attempt {attempt}/{max_attempts})...")
                    time.sleep(delay)
                    continue
                logging.error(f"Timeout occurred sending message via API after {max_attempts} attempts. URL: {url}")
                return None
            except requests.RequestException as e:
                # Network error; often transient
                if attempt < max_attempts:
                    delay = min(4.0, base_backoff * (2 ** (attempt - 1))) + random.uniform(0, 0.25)
                    error_details = f"Status: {e.response.status_code}, Body: {e.response.text}" if e.response else str(e)
                    logging.warning(f"RequestException sending WhatsApp message: {error_details}. Retrying in {delay:.2f}s (attempt {attempt}/{max_attempts})...")
                    time.sleep(delay)
                    continue
                error_details = f"Status: {e.response.status_code}, Body: {e.response.text}" if e.response else str(e)
                logging.error(f"Request failed sending message via API after {max_attempts} attempts (url={url}): {error_details}")
                return None

        # If loop ends without return, treat as failure
        logging.error("send_message: Exited retry loop without success or explicit failure reason.")
        return None

    except Exception as e:
        logging.error(f"Unexpected error sending message via API: {e}")
        return None # Return None on other errors

def send_typing_indicator(app, message_id):
    """
    Marks a message as read (which often triggers a typing indicator). Runs in a thread.
    
    Args:
        app (Flask): The Flask application instance.
        message_id (str): The WAMID (WhatsApp Message ID) of the incoming message.
    
    Returns:
        bool: True if the request was likely successful (status 2xx), False otherwise.
    """
    # Short delay before marking as read
    # time.sleep(0.5) 
    
    # Check if we are in an application context
    context_available = has_app_context()
    if not context_available:
        # If not, push one using the passed app instance
        app_context = app.app_context()
        app_context.push()
        # logging.debug("Pushed app context for send_typing_indicator thread.") # Quieter
    else:
        app_context = None # Indicate we didn't need to push one

    try:
        access_token = current_app.config.get('ACCESS_TOKEN')
        version = current_app.config.get('VERSION')
        phone_number_id = current_app.config.get('PHONE_NUMBER_ID')

        if not all([access_token, version, phone_number_id, message_id]):
            logging.warning("Cannot send read receipt (typing indicator): Missing config or message_id.")
            return False

        url = f"https://graph.facebook.com/{version}/{phone_number_id}/messages"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        data = json.dumps({
            "messaging_product": "whatsapp",
            "status": "read",
            "message_id": message_id,
            "typing_indicator": {
                "type": "text"
            }
        })

        response = requests.post(url, headers=headers, data=data, timeout=10)
        if response.status_code == 200:
            logging.info(f"⌨️ Typing indicator sent for message {message_id}")
        else:
            logging.warning(f"Failed to send typing indicator for {message_id}. Status: {response.status_code}, Response: {response.text}")
            return False

    except requests.Timeout:
        logging.error(f"Timeout marking message {message_id} as read.")
        return False
    except requests.RequestException as e:
        logging.error(f"RequestException marking message {message_id} as read: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error in send_typing_indicator for {message_id}: {e}")
        return False
    finally:
        # If we pushed an app context, pop it
        if app_context:
            app_context.pop()
            # logging.debug("Popped app context for send_typing_indicator thread.") # Quieter
 