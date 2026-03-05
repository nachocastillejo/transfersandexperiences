import logging
import re
from typing import Any, Dict, List, Optional

import requests
from flask import current_app

from app.utils.messaging_utils import (
    get_template_message_input,
    send_message,
)


_PLACEHOLDER_RE = re.compile(r"\{\{(\d+)\}\}")
_CACHED_WABA_ID: Optional[str] = None


def _get_whatsapp_basic_config() -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Lee de la configuración básica necesaria para usar la Cloud API.
    """
    access_token = current_app.config.get("ACCESS_TOKEN")
    version = current_app.config.get("VERSION")
    phone_number_id = current_app.config.get("PHONE_NUMBER_ID")

    if not all([access_token, version, phone_number_id]):
        logging.error(
            "WhatsApp API configuration missing. "
            f"ACCESS_TOKEN set: {bool(access_token)}, "
            f"VERSION: {version!r}, PHONE_NUMBER_ID: {phone_number_id!r}"
        )
        return None, None, None

    return access_token, version, phone_number_id


def _get_waba_id_from_config() -> Optional[str]:
    """
    Obtiene el ID de la cuenta de WhatsApp Business (WABA) únicamente desde la configuración.
    
    Asumimos que el proyecto proporciona WHATSAPP_BUSINESS_ACCOUNT_ID en el .env.
    """
    global _CACHED_WABA_ID

    if _CACHED_WABA_ID:
        return _CACHED_WABA_ID

    try:
        cfg_waba_id = (current_app.config.get("WHATSAPP_BUSINESS_ACCOUNT_ID") or "").strip()
    except Exception:
        cfg_waba_id = ""

    if not cfg_waba_id:
        logging.error(
            "WHATSAPP_BUSINESS_ACCOUNT_ID is not configured. "
            "Set it in your env (e.g. envs/transfersandexperiences.env) to use Meta templates from dashboard."
        )
        return None

    _CACHED_WABA_ID = cfg_waba_id
    return _CACHED_WABA_ID


def _extract_body_text(components: List[Dict[str, Any]]) -> str:
    """
    Intenta extraer un texto representativo del cuerpo de la plantilla
    a partir de los componentes devueltos por la API de Meta.
    """
    if not components:
        return ""

    for comp in components:
        try:
            ctype = (comp.get("type") or "").upper()
        except Exception:
            ctype = ""
        if ctype != "BODY":
            continue

        text = comp.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip()

        example = comp.get("example") or {}
        body_examples = example.get("body_text") or []
        if isinstance(body_examples, list) and body_examples:
            first = body_examples[0]
            if isinstance(first, str):
                return first.strip()

    return ""


def _count_placeholders(text: str) -> int:
    """
    Cuenta cuántos placeholders {{n}} distintos aparecen en un texto.
    """
    if not text:
        return 0
    try:
        indices = {int(m.group(1)) for m in _PLACEHOLDER_RE.finditer(text)}
        return max(indices) if indices else 0
    except Exception:
        # Si algo raro ocurre, no bloquear el flujo: asumimos 0 variables.
        return 0


def _extract_buttons(components: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Extrae información de botones de los componentes de una plantilla de WhatsApp.
    
    Devuelve una lista de dicts con:
      - type: tipo de botón (QUICK_REPLY, URL, PHONE_NUMBER, etc.)
      - title: texto del botón
    """
    if not components:
        return []
    
    buttons = []
    for comp in components:
        try:
            ctype = (comp.get("type") or "").upper()
        except Exception:
            ctype = ""
        
        if ctype != "BUTTONS":
            continue
        
        btn_list = comp.get("buttons") or []
        for btn in btn_list:
            if not isinstance(btn, dict):
                continue
            btn_type = btn.get("type", "")
            btn_text = btn.get("text", "")
            if btn_text:
                buttons.append({
                    "type": btn_type,
                    "title": btn_text
                })
    
    return buttons


def render_template_body(body_text: str, body_variables: Optional[List[Any]]) -> str:
    """
    Renderiza el cuerpo de una plantilla sustituyendo {{n}} por los valores
    de `body_variables` (lista 0-based → {{1}}, {{2}}, ...).
    """
    if not body_text or not body_variables:
        return body_text or ""

    def _replace(match: re.Match) -> str:
        try:
            idx = int(match.group(1)) - 1
        except Exception:
            return match.group(0)
        if 0 <= idx < len(body_variables):
            value = body_variables[idx]
            return "" if value is None else str(value)
        return match.group(0)

    try:
        return _PLACEHOLDER_RE.sub(_replace, body_text)
    except Exception:
        # En caso de error, devolver el body original sin romper el flujo
        return body_text or ""


def list_whatsapp_templates(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Lista plantillas de WhatsApp usando la Cloud API, normalizadas para el dashboard.

    Devuelve una lista de dicts con:
      - name
      - language
      - category
      - status
      - body_text
      - body_variable_count
    """
    access_token, version, _phone_number_id = _get_whatsapp_basic_config()
    if not access_token:
        return []

    waba_id = _get_waba_id_from_config()
    if not waba_id:
        # Sin WABA_ID configurado no podemos listar plantillas.
        return []

    url = f"https://graph.facebook.com/{version}/{waba_id}/message_templates"
    params = {
        "limit": limit,
        "access_token": access_token,
    }

    try:
        response = requests.get(url, params=params, timeout=15)
    except requests.Timeout:
        logging.error("Timeout while listing WhatsApp templates from Meta API.")
        return []
    except requests.RequestException as e:
        logging.error(f"Error requesting WhatsApp templates: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error requesting WhatsApp templates: {e}")
        return []

    try:
        data = response.json()
    except ValueError:
        logging.error(f"Non-JSON response while listing templates: {response.text}")
        return []

    if not (200 <= response.status_code < 300):
        logging.error(
            "WhatsApp templates API error %s: %s",
            response.status_code,
            data,
        )
        return []

    raw_templates = data.get("data") or []
    normalized: List[Dict[str, Any]] = []

    for tpl in raw_templates:
        if not isinstance(tpl, dict):
            continue
        name = tpl.get("name")
        if not name:
            continue

        # language puede venir como string o como dict.
        language = tpl.get("language")
        if isinstance(language, dict):
            language = language.get("code") or language.get("value")

        category = tpl.get("category")
        status = tpl.get("status")
        components = tpl.get("components") or []
        body_text = _extract_body_text(components)
        body_variable_count = _count_placeholders(body_text)
        buttons = _extract_buttons(components)

        normalized.append(
            {
                "name": name,
                "language": language,
                "category": category,
                "status": status,
                "body_text": body_text,
                "body_variable_count": body_variable_count,
                "buttons": buttons,
            }
        )

    return normalized


def _normalize_recipient(phone: str) -> Optional[str]:
    """
    Normaliza un número de teléfono a un formato aceptable por la API.

    Regla simple:
      - Elimina espacios.
      - Si empieza por '+', se mantiene.
      - Si empieza por '00', se convierte a '+'.
      - En otro caso, se antepone '+'.
    """
    if not phone:
        return None
    p = str(phone).strip().replace(" ", "")
    if not p:
        return None
    if p.startswith("+"):
        return p
    if p.startswith("00"):
        return f"+{p[2:]}"
    return f"+{p}"


def _build_template_components(
    body_variables: Optional[List[Any]] = None,
    header_parameters: Optional[List[Dict[str, Any]]] = None,
    button_parameters: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    Construye la lista de componentes (header, body, buttons) para la plantilla.
    """
    components = []

    # 1. Header
    if header_parameters:
        # header_parameters debe ser una lista de objetos param: [{"type": "image", "image": {...}}, ...]
        components.append({
            "type": "header",
            "parameters": header_parameters
        })

    # 2. Body
    # Mantiene compatibilidad: transforma lista simple de valores en params tipo text
    if body_variables:
        body_params = []
        for value in body_variables:
            text_value = "" if value is None else str(value)
            body_params.append({"type": "text", "text": text_value})
        
        components.append({
            "type": "body",
            "parameters": body_params
        })

    # 3. Buttons
    # button_parameters se espera como lista de dicts:
    # [{"type": "button", "sub_type": "url", "index": 0, "parameters": [...]}, ...]
    # O simplificado: [{"sub_type": "url", "index": 0, "parameters": [...]}]
    if button_parameters:
        for btn in button_parameters:
            # Aseguramos estructura mínima
            if not isinstance(btn, dict):
                continue
            
            # Si el usuario ya manda el objeto completo con type="button", lo usamos.
            # Si no, lo construimos.
            if btn.get("type") == "button":
                components.append(btn)
            else:
                components.append({
                    "type": "button",
                    "sub_type": btn.get("sub_type", "quick_reply"),
                    "index": btn.get("index", 0),
                    "parameters": btn.get("parameters", [])
                })

    return components


def send_whatsapp_template_message_to_number(
    phone: str,
    template_name: str,
    language_code: str = "es_ES",
    body_variables: Optional[List[Any]] = None,
    header_parameters: Optional[List[Dict[str, Any]]] = None,
    button_parameters: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Envía una plantilla de WhatsApp a un único número.

    `body_variables` es una lista ordenada que corresponde a {{1}}, {{2}}, etc.
    `header_parameters` lista de parámetros del header (ej. media).
    `button_parameters` lista de definiciones de botones.
    """
    result: Dict[str, Any] = {
        "phone": phone,
        "success": False,
        "message_id": None,
        "error": None,
    }

    recipient = _normalize_recipient(phone)
    if not recipient:
        result["error"] = "Invalid phone number format"
        return result

    if not template_name:
        result["error"] = "Missing template name"
        return result

    try:
        components = _build_template_components(
            body_variables=body_variables,
            header_parameters=header_parameters,
            button_parameters=button_parameters
        )
        
        # Si components está vacío, pasamos None para que no se envíe la key "components"
        # (a menos que la plantilla requiera components vacíos, pero la API suele omitirlo si no hay)
        payload = get_template_message_input(
            recipient=recipient,
            template_name=template_name,
            language_code=language_code or "es_ES",
            components=components if components else None,
        )
    except Exception as e:
        logging.error(f"Error building WhatsApp template payload: {e}")
        result["error"] = "Error building template payload"
        return result

    try:
        message_id = send_message(payload)
    except Exception as e:
        logging.error(f"Error sending WhatsApp template message via API: {e}")
        result["error"] = "Error sending template message"
        return result

    if not message_id:
        result["error"] = "WhatsApp API did not return a message ID"
        return result

    result["success"] = True
    result["message_id"] = message_id
    return result


