"""
Utilidades para manejar mensajes interactivos de WhatsApp.
"""
import logging
import json
import re
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import unicodedata

from flask import current_app

from app.utils.enrollment_state import (get_enrollment_context,
                                          set_enrollment_context,
                                          update_enrollment_context)
from app.utils.extra_utils import is_missing_email
from app.utils.inactivity_scheduler import _now_local_str, mark_activity
from app.utils.message_logger import log_message_to_db
from app.utils.messaging_utils import (get_button_message_input,
                                       get_flow_message_input,
                                       get_list_message_input,
                                       get_template_message_input,
                                       get_text_message_input, send_message,
                                       send_typing_indicator)


def _get_madrid_timestamp() -> str:
    """Devuelve la fecha/hora actual en zona horaria de Madrid."""
    try:
        madrid_tz = ZoneInfo('Europe/Madrid')
        dt = datetime.now(madrid_tz)
    except Exception:
        dt = datetime.now()
    return dt.strftime('%Y-%m-%dT%H:%M:%S')


def handle_interactive_message(message_info, wa_id, name, project_name_from_config, message_timestamp_str, message_id_incoming):
    """
    Procesa un mensaje interactivo entrante de WhatsApp.
    Devuelve un diccionario con el estado y los datos necesarios para continuar el procesamiento.
    """
    raw_question = None
    inbound_display_text = None
    error_occurred_early = False

    try:
        mark_activity(wa_id)
    except Exception:
        pass

    interactive = message_info.get("interactive", {})
    interactive_type = interactive.get("type")

    if interactive_type == "button_reply":
        reply = interactive.get("button_reply", {})
        reply_id = reply.get("id")
        reply_title = reply.get("title")
        
        # Botón "Más Info": info_<sufijo>
        if isinstance(reply_id, str) and reply_id.startswith("info_"):
            try:
                id_suffix = reply_id.split("_", 1)[1]
            except Exception:
                id_suffix = None
            try:
                ctx_links = get_enrollment_context(wa_id)
                course_links = ctx_links.get("course_links") or {}
                link_to_open = None
                if id_suffix is not None:
                    link_to_open = course_links.get(str(id_suffix))
                # Si no tenemos el sufijo, intentar por código actual
                if not link_to_open:
                    code_curr = (ctx_links.get("codigo_curso") or "").strip()
                    link_to_open = course_links.get(str(code_curr)) if code_curr else None
                if link_to_open:
                    recipient_open = f"+{wa_id}"
                    try:
                        # Enviar el enlace como texto (WhatsApp no soporta botón URL en mensajes interactivos clásicos)
                        open_msg = f"Aquí tienes más información: {link_to_open}"
                        data_open = get_text_message_input(recipient_open, open_msg)
                        sent_open_id = send_message(data_open)
                        try:
                            log_message_to_db(
                                wa_id=wa_id,
                                sender_name=project_name_from_config,
                                message_text=open_msg,
                                direction='outbound_bot',
                                project_name=project_name_from_config,
                                whatsapp_message_id=sent_open_id,
                                status='sent' if sent_open_id else 'failed'
                            )
                        except Exception as _log_open_err:
                            logging.error(f"Error logging 'Más Info' link send for {wa_id}: {_log_open_err}")
                    except Exception as _send_open_err:
                        logging.error(f"Error sending 'Más Info' link for {wa_id}: {_send_open_err}")
                else:
                    logging.warning(f"No stored link for 'Más Info' button '{reply_id}' for {wa_id}.")
            except Exception as _info_err:
                logging.error(f"Error handling 'Más Info' button for {wa_id}: {_info_err}")
            return {'status': 'stop'}

        if reply_id and reply_id.startswith("curso_"):
            selected_code = reply_id[len("curso_"):]
            raw_question = selected_code

            try:
                # Intentar recuperar el nombre del curso desde el contexto para mostrarlo en el dashboard
                try:
                    ctx_names = get_enrollment_context(wa_id) or {}
                    names_map = dict(ctx_names.get("course_names") or {})
                    selected_name = names_map.get(str(selected_code)) or ""
                except Exception:
                    selected_name = ""
                display_text = f"Inscribirme ({selected_code}" + (f" - {selected_name}" if selected_name else "") + ")"

                log_message_to_db(
                    wa_id=wa_id,
                    sender_name=name,
                    message_text=display_text,
                    direction='inbound',
                    project_name=project_name_from_config,
                    timestamp=message_timestamp_str
                )
            except Exception as _log_in_err:
                logging.error(f"Error logging inbound 'Inscribirme' click for {wa_id}: {_log_in_err}")

            try:
                update_enrollment_context(wa_id, {"codigo_curso": selected_code})
            except Exception as ctx_err:
                logging.error(f"Error updating enrollment context for {wa_id} with course '{selected_code}': {ctx_err}")

            try:
                recipient_flow = f"+{wa_id}"
                template_name = current_app.config.get("WHATSAPP_TEMPLATE_ENROLL", "cuestionario_inscripcion")
                language_code = current_app.config.get("WHATSAPP_TEMPLATE_LANG", "es_ES")
                components = None
                ctx_for_components = get_enrollment_context(wa_id)
                nombre_val = (ctx_for_components.get("nombre") or name or "").strip()
                apellidos_val = (ctx_for_components.get("apellidos") or "").strip()
                dni_val = (ctx_for_components.get("dni") or ctx_for_components.get("nif") or "").strip()
                direccion_val = (ctx_for_components.get("direccion") or "").strip()

                try:
                    force_components = bool(current_app.config.get("WHATSAPP_TEMPLATE_FORCE_COMPONENTS"))
                    components_json = current_app.config.get("WHATSAPP_TEMPLATE_COMPONENTS")
                    if force_components and components_json:
                        env_components = json.loads(components_json)
                        placeholder_map = {
                            "{{Nombre}}": nombre_val,
                            "{{Apellidos}}": apellidos_val,
                            "{{DNI}}": dni_val,
                            "{{Direccion}}": direccion_val,
                        }
                        try:
                            for comp in env_components if isinstance(env_components, list) else []:
                                if isinstance(comp, dict) and comp.get("type") == "body":
                                    params = comp.get("parameters", [])
                                    for p in params:
                                        if isinstance(p, dict) and p.get("type") == "text":
                                            txt = p.get("text")
                                            if isinstance(txt, str) and txt in placeholder_map:
                                                p["text"] = placeholder_map[txt]
                        except Exception as sub_err:
                            logging.error(f"Error substituting placeholders in template components: {sub_err}")
                        components = env_components
                except Exception as comp_err:
                    logging.error(f"Invalid WHATSAPP_TEMPLATE_COMPONENTS JSON: {comp_err}")

                def _sanitize_components(comps):
                    try:
                        if not isinstance(comps, list):
                            return comps
                        for comp in comps:
                            if isinstance(comp, dict) and comp.get("type") == "body":
                                params = comp.get("parameters", [])
                                for p in params:
                                    if isinstance(p, dict) and p.get("type") == "text":
                                        txt = p.get("text")
                                        if not isinstance(txt, str) or not txt.strip():
                                            p["text"] = "-"
                        return comps
                    except Exception:
                        return comps

                components = _sanitize_components(components)

                try:
                    ctx_email_probe = get_enrollment_context(wa_id)
                    email_present = not is_missing_email(ctx_email_probe.get("email") or ctx_email_probe.get("Email"))
                    chosen_flow_id = (
                        current_app.config.get("WHATSAPP_FLOW_ID_INSCRIPCION")
                        if email_present else
                        current_app.config.get("WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL")
                    )
                    flow_id_present = bool(chosen_flow_id)
                    flow_token = current_app.config.get("WHATSAPP_FLOW_TOKEN")
                    if flow_token:
                        flow_action = current_app.config.get("WHATSAPP_FLOW_ACTION", "navigate")
                        flow_screen = current_app.config.get("WHATSAPP_FLOW_ACTION_SCREEN")
                        flow_payload_json = current_app.config.get("WHATSAPP_FLOW_ACTION_PAYLOAD_JSON")
                        flow_button = {
                            "type": "button",
                            "sub_type": "flow",
                            "index": "0",
                            "parameters": [
                                {
                                    "type": "action",
                                    "action": {
                                        "flow_token": flow_token,
                                        "flow_action": flow_action
                                    }
                                }
                            ]
                        }
                        if flow_screen:
                            flow_button["parameters"][0]["action"]["flow_action_screen"] = flow_screen
                        try:
                            if flow_payload_json:
                                flow_button["parameters"][0]["action"]["flow_action_payload"] = json.loads(flow_payload_json)
                        except Exception as opt_err:
                            logging.warning(f"Optional Flow action payload JSON invalid: {opt_err}")

                        if not components or not isinstance(components, list):
                            components = []
                        components.append(flow_button)
                        logging.info(f"Flow button attached to template (flow_token present, flow_id_configured={flow_id_present}).")
                except Exception as flow_err:
                    logging.error(f"Error building Flow button component: {flow_err}")

                try:
                    ctx_email_probe2 = get_enrollment_context(wa_id)
                    email_present2 = not is_missing_email(ctx_email_probe2.get("email") or ctx_email_probe2.get("Email"))
                except Exception:
                    email_present2 = False
                chosen_flow_id2 = (
                    current_app.config.get("WHATSAPP_FLOW_ID_INSCRIPCION")
                    if email_present2 else
                    current_app.config.get("WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL")
                )
                flow_present = bool(chosen_flow_id2 and current_app.config.get("WHATSAPP_FLOW_TOKEN"))
                if not flow_present and not bool(current_app.config.get("WHATSAPP_TEMPLATE_FORCE_COMPONENTS")):
                    components = None

                sent_id = None
                if flow_present:
                    try:
                        header_text = "Inscripción"
                        body_text = "Completa tus datos para inscribirte al curso seleccionado."
                        footer_text = "Transfers & Experiences"
                        flow_payload = get_flow_message_input(
                            recipient_flow,
                            header_text,
                            body_text,
                            footer_text,
                            chosen_flow_id2,
                            current_app.config.get("WHATSAPP_FLOW_TOKEN"),
                            current_app.config.get("WHATSAPP_FLOW_ACTION", "navigate"),
                            current_app.config.get("WHATSAPP_FLOW_CTA"),
                            "3",
                            current_app.config.get("WHATSAPP_FLOW_ACTION_SCREEN"),
                            json.loads(current_app.config.get("WHATSAPP_FLOW_ACTION_PAYLOAD_JSON")) if current_app.config.get("WHATSAPP_FLOW_ACTION_PAYLOAD_JSON") else None,
                        )
                        logging.info(f"Sending INTERACTIVE FLOW payload (not template) for course {selected_code}.")
                        sent_id = send_message(flow_payload)
                    except Exception as flow_send_err:
                        logging.error(f"Error building/sending interactive Flow message: {flow_send_err}")
                else:
                    def _try_send(lang: str, comps):
                        payload = get_template_message_input(recipient_flow, template_name, lang, comps)
                        try:
                            payload_obj = json.loads(payload)
                            to_value = payload_obj.get("to", "")
                            if isinstance(to_value, str) and len(to_value) > 4:
                                masked = to_value[:-4].replace("0", "*").replace("1", "*").replace("2", "*").replace("3", "*").replace("4", "*").replace("5", "*").replace("6", "*").replace("7", "*").replace("8", "*").replace("9", "*") + to_value[-4:]
                                payload_obj["to"] = masked
                            logging.info(f"Sending template payload: name='{template_name}', lang='{lang}', has_components={bool(comps)} -> {json.dumps(payload_obj, ensure_ascii=False)}")
                        except Exception:
                            pass
                        return send_message(payload)

                    sent_id = _try_send(language_code, components)
                    if not sent_id and components:
                        logging.warning("Retrying template send without components due to previous failure.")
                        sent_id = _try_send(language_code, None)
                    if not sent_id and bool(current_app.config.get("WHATSAPP_TEMPLATE_ALLOW_ALT_LANG")):
                        alt_lang = "es" if language_code.lower() == "es_es" else "es_ES"
                        logging.warning(f"Retrying template send with alternate language '{alt_lang}'.")
                        sent_id = _try_send(alt_lang, components)
                        if not sent_id and components:
                            sent_id = _try_send(alt_lang, None)
                
                if sent_id:
                    logging.info(f"Enrollment template '{template_name}' sent to {recipient_flow} for course {selected_code}")
                    try:
                        log_message_to_db(
                            wa_id=wa_id,
                            sender_name=project_name_from_config,
                            message_text="Plantilla de inscripción enviada",
                            direction='outbound_bot',
                            project_name=project_name_from_config,
                            whatsapp_message_id=sent_id,
                            status='sent'
                        )
                    except Exception as log_err:
                        logging.error(f"Error logging enrollment template message for {wa_id}: {log_err}")
                else:
                    logging.error("Failed to send enrollment template (no message id returned). Sending fallback text to user.")
                    fallback_text = "No he podido abrir el formulario ahora mismo. Por favor, responde 'inscripción' para reintentarlo."
                    fallback_payload = get_text_message_input(recipient_flow, fallback_text)
                    sent_fallback_id = send_message(fallback_payload)
                    try:
                        log_message_to_db(
                            wa_id=wa_id,
                            sender_name=project_name_from_config,
                            message_text=fallback_text,
                            direction='outbound_bot',
                            project_name=project_name_from_config,
                            whatsapp_message_id=sent_fallback_id,
                            status='sent' if sent_fallback_id else 'failed'
                        )
                    except Exception as _log_fallback_err:
                        logging.error(f"Error logging fallback message for {wa_id}: {_log_fallback_err}")
            except Exception as t_err:
                logging.error(f"Error sending enrollment template for {wa_id}: {t_err}")
                try:
                    recipient_flow = f"+{wa_id}"
                    fallback_text = "Ha ocurrido un problema al abrir el formulario. Inténtalo de nuevo en unos segundos."
                    fallback_payload = get_text_message_input(recipient_flow, fallback_text)
                    sent_fallback2_id = send_message(fallback_payload)
                    try:
                        log_message_to_db(
                            wa_id=wa_id,
                            sender_name=project_name_from_config,
                            message_text=fallback_text,
                            direction='outbound_bot',
                            project_name=project_name_from_config,
                            whatsapp_message_id=sent_fallback2_id,
                            status='sent' if sent_fallback2_id else 'failed'
                        )
                    except Exception as _log_fallback2_err:
                        logging.error(f"Error logging fallback2 message for {wa_id}: {_log_fallback2_err}")
                except Exception:
                    pass

            return {'status': 'stop'}
        
        elif reply_id == "courses_none":
            from app.services.drive_service import get_and_filter_courses as _fetch_courses
            from app.services.openai_service import append_thread_with_payload as _append_thread
            try:
                try:
                    log_message_to_db(
                        wa_id=wa_id,
                        sender_name=name,
                        message_text="No me interesa",
                        direction='inbound',
                        project_name=project_name_from_config,
                        timestamp=message_timestamp_str
                    )
                except Exception as _log_none_err:
                    logging.error(f"Error logging inbound 'No me interesa' click for {wa_id}: {_log_none_err}")

                try:
                    if message_id_incoming:
                        try:
                            app_instance = current_app._get_current_object()
                        except Exception:
                            app_instance = current_app
                        ti_thread = threading.Thread(target=send_typing_indicator, args=(app_instance, message_id_incoming))
                        ti_thread.daemon = True
                        ti_thread.start()
                    else:
                        logging.warning(f"No incoming message_id for 'courses_none' from {wa_id}. Cannot send typing indicator.")
                except Exception as _ti_err:
                    logging.error(f"Failed to start typing indicator thread for 'courses_none': {_ti_err}")

                ctx = get_enrollment_context(wa_id)
                curr = (ctx.get("current_search") or {}) if isinstance(ctx, dict) else {}
                origen = curr.get("provincia") or ctx.get("provincia") or ctx.get("origen") or "OFERTA ESTATAL 24"
                situacion = curr.get("situacion_laboral") or ctx.get("situacion_laboral") or "desempleado"
                nivel = curr.get("nivel_formacion") or ctx.get("nivel_formacion")
                sector_ctx = curr.get("sector") or ctx.get("sector") or "N/A"
                modalidad_ctx = curr.get("modalidad") or ctx.get("modalidad") or "N/A"
                tematica_ctx = curr.get("tematica") or "N/A"
                formacion_ctx = ctx.get("formacion") or "N/A"
                next_page = int((curr.get("pagina_actual") or 1)) + 1
                try:
                    new_curr = dict(curr)
                    new_curr["pagina_actual"] = next_page
                    update_enrollment_context(wa_id, {"current_search": new_curr})
                except Exception:
                    update_enrollment_context(wa_id, {"pagina_actual": next_page})
                
                if next_page > 3:
                    recipient = f"+{wa_id}"
                    if isinstance(tematica_ctx, str) and tematica_ctx.strip().lower() != 'n/a':
                        msg_no_more = (
                            f"¡Entendido! 😊 Por ahora no tengo más cursos de '{tematica_ctx}' que encajen contigo. "
                            "Si quieres, puedo buscar más cursos de otras temáticas o sin una temática en específico."
                        )
                    else:
                        msg_no_more = (
                            "¡Entendido! 😊 Por ahora no tengo más cursos que encajen contigo. "
                            "Si quieres, puedo seguir buscando añadiendo una temática en específico (por ejemplo, 'idiomas', 'marketing digital', etc.)."
                        )
                    data_to_send = get_text_message_input(recipient, msg_no_more)
                    wa_message_id_sent_by_bot = send_message(data_to_send)
                    
                    log_message_to_db(
                        wa_id=wa_id,
                        sender_name=project_name_from_config,
                        message_text=msg_no_more,
                        direction='outbound_bot',
                        project_name=project_name_from_config,
                        whatsapp_message_id=wa_message_id_sent_by_bot,
                        status='sent' if wa_message_id_sent_by_bot else 'failed',
                        response_time_seconds=0.0, # This will be recalculated outside
                        attempt_count=0,
                        required_action='NoMoreResults',
                        error_message=None
                    )
                    return {'status': 'stop'}

                try:
                    logging.info(
                        f"🔎 Buscando más cursos (siguiente tanda). Criterios: origen='{origen}', situacion='{situacion}', nivel='{nivel}', formacion='{formacion_ctx}', sector='{sector_ctx}', modalidad='{modalidad_ctx}', tematica='{tematica_ctx}'"
                    )
                    courses = _fetch_courses(
                        origen,
                        situacion,
                        nivel,
                        pagina=next_page,
                        sector=sector_ctx,
                        modalidad=modalidad_ctx,
                        tematica=tematica_ctx,
                        wa_id=wa_id,
                        formacion=formacion_ctx,
                    )
                except Exception as fetch_err:
                    logging.error(f"Error fetching courses for page {next_page}: {fetch_err}")
                    courses = []

                recipient = f"+{wa_id}"
                if courses:
                    try:
                        preface = "Aquí tienes más opciones que podrían encajar contigo:"
                        data_preface = get_text_message_input(recipient, preface)
                        sent_preface_id = send_message(data_preface)
                        try:
                            log_message_to_db(
                                wa_id=wa_id,
                                sender_name=project_name_from_config,
                                message_text=preface,
                                direction='outbound_bot',
                                project_name=project_name_from_config,
                                whatsapp_message_id=sent_preface_id,
                                status='sent' if sent_preface_id else 'failed'
                            )
                        except Exception as _log_pref_err:
                            logging.error(f"Error logging preface message for {wa_id}: {_log_pref_err}")
                    except Exception:
                        pass
                    
                    sent_count = 0
                    for course in courses:
                        body = course.get("whatsapp_card_text") or course.get("curso") or "Curso"
                        if isinstance(body, str) and len(body) > 900:
                            body = body[:900] + "…"
                        button_id = course.get("whatsapp_button_id") or f"curso_{sent_count}"
                        button_title = course.get("whatsapp_button_title") or "Inscribirme"
                        enlace_url = (
                            course.get("enlace_acortado")
                            or course.get("enlace")
                            or course.get("link")
                        )
                        try:
                            # Construir botones con "Más Info" primero si hay enlace
                            try:
                                id_suffix = (button_id.split("_", 1)[1]) if isinstance(button_id, str) and "_" in button_id else str(sent_count)
                            except Exception:
                                id_suffix = str(sent_count)
                            buttons_payload = []
                            if enlace_url:
                                buttons_payload.append({"id": f"info_{id_suffix}", "title": "Más Info"})
                            buttons_payload.append({"id": button_id, "title": button_title})
                            # Guardar mapping del enlace para el botón "Más Info"
                            try:
                                ctx_curr2 = get_enrollment_context(wa_id)
                                course_links2 = dict(ctx_curr2.get("course_links") or {})
                                course_links2[str(id_suffix)] = str(enlace_url or "").strip()
                                # Guardar también mapping de nombres por código/sufijo para mostrar en dashboard
                                course_names2 = dict(ctx_curr2.get("course_names") or {})
                                try:
                                    course_name_val = (course.get("curso") or "").strip()
                                except Exception:
                                    course_name_val = str(course.get("whatsapp_card_text") or "").strip()
                                if course_name_val:
                                    course_names2[str(id_suffix)] = course_name_val
                                update_enrollment_context(wa_id, {"course_links": course_links2, "course_names": course_names2})
                            except Exception as _ctx_link_err2:
                                logging.error(f"No se pudo actualizar course_links (paged) para {wa_id}: {_ctx_link_err2}")
                            message_data = get_button_message_input(recipient, body, buttons_payload)
                            sent_id_course = send_message(message_data)
                            try:
                                log_message_to_db(
                                    wa_id=wa_id,
                                    sender_name=project_name_from_config,
                                    message_text=body,
                                    direction='outbound_bot',
                                    project_name=project_name_from_config,
                                    whatsapp_message_id=sent_id_course,
                                    status='sent' if sent_id_course else 'failed',
                                    required_action=f"interactive_buttons:{json.dumps(buttons_payload, ensure_ascii=False)}"
                                )
                            except Exception as log_err:
                                logging.error(f"Error logging button message (course card) for {wa_id}: {log_err}")
                            sent_count += 1
                        except Exception as send_err:
                            logging.error(f"Error enviando tarjeta de curso (p{next_page}): {send_err}")
                    
                    try:
                        buttons_payload = [{"id": "courses_none", "title": "No me interesa"}]
                        none_button = get_button_message_input(recipient, "No me interesa ningún curso", buttons_payload)
                        sent_id_none = send_message(none_button)
                        try:
                            log_message_to_db(
                                wa_id=wa_id,
                                sender_name=project_name_from_config,
                                message_text="No me interesa ningún curso",
                                direction='outbound_bot',
                                project_name=project_name_from_config,
                                whatsapp_message_id=sent_id_none,
                                status='sent' if sent_id_none else 'failed',
                                required_action=f"interactive_buttons:{json.dumps(buttons_payload, ensure_ascii=False)}"
                            )
                        except Exception as log_err:
                            logging.error(f"Error logging 'No me interesa' button for {wa_id}: {log_err}")
                    except Exception as send_err:
                        logging.error(f"Error enviando botón de 'No me interesa' en p{next_page}: {send_err}")

                    # Registrar en OpenAI la nueva tanda de cursos (finalize mínimo, 16 tokens)
                    try:
                        from app.services.openai_service import append_thread_with_payload as _append
                        user_note = f"[trigger:courses_none] Enviada página {next_page}"
                        assistant_payload_text = json.dumps(courses, ensure_ascii=False)
                        _append(current_app, wa_id, user_note, assistant_payload_text)
                    except Exception as _append_err:
                        logging.error(f"No se pudo registrar en OpenAI la tanda p{next_page} para {wa_id}: {_append_err}")
                else:
                    if isinstance(tematica_ctx, str) and tematica_ctx.strip().lower() != 'n/a':
                        msg = (
                            f"¡Entendido! 😊 Por ahora no tengo más cursos de la temática '{tematica_ctx}' que encajen contigo. "
                            "Si quieres, puedo buscar más cursos de otras temáticas o sin una temática en específico."
                        )
                    else:
                        msg = (
                            "¡Entendido! 😊 Por ahora no tengo más cursos que encajen contigo. "
                            "Si quieres, puedo seguir buscando añadiendo una temática en específico (por ejemplo, 'idiomas', 'marketing digital', etc.)."
                        )
                    data_to_send = get_text_message_input(recipient, msg)
                    sent_id_final = send_message(data_to_send)
                    try:
                        log_message_to_db(
                            wa_id=wa_id,
                            sender_name=project_name_from_config,
                            message_text=msg,
                            direction='outbound_bot',
                            project_name=project_name_from_config,
                            whatsapp_message_id=sent_id_final,
                            status='sent' if sent_id_final else 'failed'
                        )
                    except Exception as _log_final_err:
                        logging.error(f"Error logging 'no more results' message for {wa_id}: {_log_final_err}")
            except Exception as e:
                logging.error(f"Error handling 'No me interesa' flow for {wa_id}: {e}")
            return {'status': 'stop'}
        else:
            # mapping se usa aquí: aseguremos que esté definido
            mapping = {
                "situacion_ocupado": "ocupado",
                "situacion_desempleado": "desempleado",
                "situacion_autonomo": "autónomo",
                "nivel_1": "Nivel 1",
                "nivel_2": "Nivel 2",
                "nivel_3": "Nivel 3",
                "nivel_sin": "Nivel 0",
            }
            raw_question = mapping.get(reply_id) or reply_title or reply_id or "[Respuesta de botón vacía]"
            inbound_display_text = reply_title or raw_question
            try:
                ctx_updates = {}
                if reply_id in ("situacion_ocupado", "situacion_desempleado", "situacion_autonomo"):
                    ctx_updates["situacion_laboral"] = mapping.get(reply_id)
                if reply_id in ("nivel_1", "nivel_2", "nivel_3", "nivel_sin"):
                    ctx_updates["nivel_formacion"] = mapping.get(reply_id)
                if ctx_updates:
                    update_enrollment_context(wa_id, ctx_updates)
                    # Mark CRM data as changed
                    from app.utils.inactivity_scheduler import mark_crm_data_changed
                    mark_crm_data_changed(wa_id)
            except Exception as upd_err:
                logging.error(f"Error updating enrollment context from button selection for {wa_id}: {upd_err}")

    elif interactive_type == "list_reply":
        reply = interactive.get("list_reply", {})
        reply_id = reply.get("id")
        reply_title = reply.get("title")
        mapping = {
            "situacion_ocupado": "ocupado",
            "situacion_desempleado": "desempleado",
            "situacion_autonomo": "autónomo",
            "nivel_1": "Nivel 1",
            "nivel_2": "Nivel 2",
            "nivel_3": "Nivel 3",
            "nivel_sin": "Nivel 0",
        }
        if reply_id and reply_id.startswith("curso_"):
            raw_question = reply_id[len("curso_"):]
            inbound_display_text = reply_title or raw_question
        else:
            raw_question = mapping.get(reply_id) or reply_title or reply_id or "[Respuesta de lista vacía]"
            inbound_display_text = reply_title or raw_question
            # Paginación de titulaciones: si el usuario pulsa "Ver más", enviamos la segunda página
            if reply_id == "tit_more":
                try:
                    recipient_more = f"+{wa_id}"
                    rows_all = [
                        {"id": "tit_1", "title": "SIN ESTUDIOS"},
                        {"id": "tit_2", "title": "EST. PRIMARIOS", "description": "ESTUDIOS PRIMARIOS"},
                        {"id": "tit_13", "title": "FP GR. MEDIO", "description": "FP GRADO MEDIO"},
                        {"id": "tit_14", "title": "ESO"},
                        {"id": "tit_4", "title": "BACHILLERATO"},
                        {"id": "tit_9", "title": "DOCTORADO"},
                        {"id": "tit_17", "title": "MÁSTER"},
                        {"id": "tit_21", "title": "FP GR. SUPERIOR", "description": "FP GRADO SUPERIOR"},
                        {"id": "tit_22", "title": "GRADO UNIV.", "description": "GRADO UNIVERSITARIO"},
                        {"id": "tit_27", "title": "CP NIVEL 1", "description": "CERTIFICADO DE PROFESIONALIDAD DE NIVEL 1"},
                        {"id": "tit_10", "title": "ACCESO UNI >25", "description": "ACCESO UNIVERSIDAD MAYORES 25"},
                        {"id": "tit_11", "title": "CP NIVEL 2", "description": "CERTIFICADO DE PROFESIONALIDAD DE NIVEL 2"},
                        {"id": "tit_12", "title": "CP NIVEL 3", "description": "CERTIFICADO DE PROFESIONALIDAD DE NIVEL 3"},
                        {"id": "tit_30", "title": "PROF. MÚSICA/DANZA", "description": "ENSEÑANZAS PROFESIONALES DE MÚSICA Y DANZA"},
                    ]
                    rows_page2 = rows_all[9:]
                    body_more = "🎓 Selecciona tu titulación académica homologada en España (2/2)"
                    data_more = get_list_message_input(recipient_more, body_more, rows_page2, button_label="Elegir", section_title="Titulación")
                    send_message(data_more)
                except Exception as _more_err:
                    logging.error(f"Error sending 'Ver más' titulaciones page for {wa_id}: {_more_err}")
                return {'status': 'stop'}
            # Persist selection if applicable
            try:
                ctx_updates = {}
                if reply_id in ("situacion_ocupado", "situacion_desempleado", "situacion_autonomo"):
                    ctx_updates["situacion_laboral"] = mapping.get(reply_id)

                # Nueva lógica: ids 'tit_<codigo>' → guardar código CRM y nivel derivado (0–3)
                if isinstance(reply_id, str) and reply_id.startswith("tit_"):
                    try:
                        code_str = reply_id.split("_", 1)[1]
                        crm_code = int(code_str)
                        crm_to_level = {
                            1: 0,   # SIN ESTUDIOS
                            2: 1,   # ESTUDIOS PRIMARIOS
                            13: 2,  # FP GRADO MEDIO
                            14: 2,  # ESO
                            4: 3,   # BACHILLERATO
                            9: 3,   # DOCTORADO
                            17: 3,  # MÁSTER
                            21: 3,  # FP GRADO SUPERIOR
                            22: 3,  # GRADO UNIVERSITARIO
                            27: 1,  # CERT. PROF. NIVEL 1
                            10: 3,  # ACCESO UNI >25
                            11: 2,  # CERT. PROF. NIVEL 2
                            12: 3,  # CERT. PROF. NIVEL 3
                            30: 0,  # ENSEÑ. PROF. MÚSICA Y DANZA
                        }
                        derived_level = crm_to_level.get(crm_code)
                        ctx_updates["titulacion"] = crm_code
                        # Guardar también el nombre de la titulación en 'formacion'
                        if reply_title:
                            ctx_updates["formacion"] = reply_title
                        if derived_level is not None:
                            # Ajustar raw_question para que el asistente entienda la respuesta automáticamente
                            if derived_level == 0:
                                ctx_updates["nivel_formacion"] = "Nivel 0"
                                raw_question = "Nivel 0"  # Internal processing only; dashboard shows inbound_display_text
                            else:
                                ctx_updates["nivel_formacion"] = f"Nivel {derived_level}"
                                raw_question = f"Nivel {derived_level}"  # Internal processing only; dashboard shows inbound_display_text
                    except Exception as _map_err:
                        logging.error(f"Error mapping titulacion code from list reply '{reply_id}' for {wa_id}: {_map_err}")

                # Compatibilidad previa con 'nivel_1/2/3/sin'
                if reply_id in ("nivel_1", "nivel_2", "nivel_3", "nivel_sin"):
                    ctx_updates["nivel_formacion"] = mapping.get(reply_id)

                if ctx_updates:
                    update_enrollment_context(wa_id, ctx_updates)
            except Exception as upd_err:
                logging.error(f"Error updating enrollment context from list selection for {wa_id}: {upd_err}")
    
    elif interactive_type == "nfm_reply":
        from app.services.crm_service import inscribir_lead
        nfm = interactive.get("nfm_reply", {})
        response_json_str = nfm.get("response_json")
        try:
            form_data = json.loads(response_json_str) if response_json_str else {}
        except Exception as parse_err:
            logging.error(f"Error parsing nfm_reply.response_json for {wa_id}: {parse_err}")
            form_data = {}
        
        try:
            logging.info(f"Flow form_data keys: {list((form_data or {}).keys())}")
        except Exception:
            pass

        def _normalize_key(k: str) -> str:
            try:
                k = unicodedata.normalize('NFKD', k).encode('ascii', 'ignore').decode('ascii')
            except Exception:
                pass
            return re.sub(r"[^a-z0-9]", "_", k.lower()).strip("_")

        normalized_form = {}
        try:
            for k, v in (form_data or {}).items():
                if isinstance(k, str):
                    normalized_form[_normalize_key(k)] = v
        except Exception:
            normalized_form = {}
        
        def _pick_by_terms(terms):
            try:
                for key, value in normalized_form.items():
                    k = key.lower()
                    if any(term in k for term in terms):
                        if isinstance(value, str):
                            val = value.strip()
                        else:
                            val = value
                        if val:
                            return val
            except Exception:
                pass
            return None

        mapped_form = {
            "nombre": _pick_by_terms(["nombre", "name"]),
            "apellidos": _pick_by_terms(["apellidos", "surname", "last_name", "lastname"]),
            "dni": _pick_by_terms(["dni", "nif", "documento", "identificacion", "identification"]),
            "direccion": _pick_by_terms(["direccion", "direccin", "direccion_postal", "address"]) 
        }

        ctx = get_enrollment_context(wa_id)
        merged = {**ctx, **{k: v for k, v in mapped_form.items() if v}}

        try:
            update_enrollment_context(wa_id, merged)
            # Mark CRM data as changed if any relevant fields were updated
            if any(merged.get(k) for k in ["nombre", "apellidos", "dni", "direccion"]):
                from app.utils.inactivity_scheduler import mark_crm_data_changed
                mark_crm_data_changed(wa_id)
        except Exception as m_err:
            logging.error(f"Error saving merged enrollment context for {wa_id}: {m_err}")

        try:
            from app.services.openai_service import append_thread_with_payload as _append
            user_note = "[event:enrollment_form_submitted]"
            payload_obj = {"event": "enrollment_form_submitted", "form_data": mapped_form}
            payload_text = json.dumps(payload_obj, ensure_ascii=False)
            _append(current_app, wa_id, user_note, payload_text)
        except Exception as _append_submit_err:
            logging.error(f"Could not log form submission to OpenAI for {wa_id}: {_append_submit_err}")
        
        # Log the "Formulario enviado" message to database
        try:
            log_message_to_db(
                wa_id=wa_id,
                sender_name=name,
                message_text="Formulario enviado",
                direction='inbound',
                project_name=project_name_from_config,
                timestamp=message_timestamp_str
            )
        except Exception as _log_form_err:
            logging.error(f"Error logging inbound 'Formulario enviado' message for {wa_id}: {_log_form_err}")
        
        # Note: Bot pause moved to after successful CRM enrollment


        try:
            telefono = wa_id
            nombre = merged.get("nombre") or merged.get("Nombre")
            apellidos = merged.get("apellidos") or merged.get("Apellidos")
            nif = merged.get("dni") or merged.get("DNI") or merged.get("nif")
            direccion = merged.get("direccion") or merged.get("Dirección") or merged.get("Direccion")
            email = merged.get("email") or merged.get("Email")
            provincia = merged.get("provincia") or merged.get("Provincia")
            situacion_laboral = merged.get("situacion_laboral") or merged.get("Situacion_laboral")
            titulacion = merged.get("titulacion") or merged.get("Titulacion") or merged.get("nivel_formacion")
            codigo_inscripcion = merged.get("codigo_curso") or merged.get("Codigo_inscripcion")

            # Validar que el contexto de inscripción no haya expirado (datos críticos presentes)
            # Estos campos vienen del contexto previo (no del formulario) y se pierden si expira
            missing_fields = []
            if not codigo_inscripcion:
                missing_fields.append("curso")
            if not email:
                missing_fields.append("email")
            if not situacion_laboral:
                missing_fields.append("situación laboral")
            if not titulacion:
                missing_fields.append("nivel de formación")
            
            if missing_fields:
                logging.warning(f"Enrollment context expired for {wa_id}: missing fields {missing_fields}. Asking user to restart.")
                recipient_expired = f"+{wa_id}"
                expired_msg = (
                    "⚠️ Tu sesión de inscripción ha expirado por inactividad. "
                    "Por favor, vuelve a iniciar la conversación para inscribirte en un curso."
                )
                data_expired = get_text_message_input(recipient_expired, expired_msg)
                sent_expired_id = send_message(data_expired)
                try:
                    log_message_to_db(
                        wa_id=wa_id, sender_name=project_name_from_config, message_text=expired_msg,
                        direction='outbound_bot', project_name=project_name_from_config,
                        whatsapp_message_id=sent_expired_id, status='sent' if sent_expired_id else 'failed'
                    )
                except Exception as _log_expired_err:
                    logging.error(f"Error logging expired-context notice for {wa_id}: {_log_expired_err}")
                return {'status': 'stop'}

            try:
                if codigo_inscripcion:
                    current_ctx = get_enrollment_context(wa_id)
                    history = list(current_ctx.get("inscripciones") or [])
                    if any(isinstance(r, dict) and r.get("codigo_curso") == codigo_inscripcion for r in history):
                        recipient_ok = f"+{wa_id}"
                        already_msg = "Ya tenemos registrada tu inscripción en ese curso ✅. Si deseas inscribirte en otro, elige un curso diferente."
                        data_ok = get_text_message_input(recipient_ok, already_msg)
                        sent_dupe_id = send_message(data_ok)
                        try:
                            log_message_to_db(
                                wa_id=wa_id, sender_name=project_name_from_config, message_text=already_msg,
                                direction='outbound_bot', project_name=project_name_from_config,
                                whatsapp_message_id=sent_dupe_id, status='sent' if sent_dupe_id else 'failed'
                            )
                        except Exception as _log_dup_err2:
                            logging.error(f"Error logging duplicate-enrollment notice for {wa_id}: {_log_dup_err2}")
                        return {'status': 'stop'}
            except Exception as _dup_err:
                logging.error(f"Error checking duplicate enrollment by course for {wa_id}: {_dup_err}")

            if telefono.startswith("+"):
                telefono = telefono[1:]

            crm_result = inscribir_lead({
                "nombre": nombre, "apellidos": apellidos, "telefono": telefono, "email": email, "nif": nif,
                "codigo_inscripcion": codigo_inscripcion, "situacion_laboral": situacion_laboral,
                "direccion": direccion, "provincia": provincia, "titulacion": titulacion, "sector": merged.get("sector"),
            })

            if crm_result.get("codigo") == 200:
                confirmation = "¡Inscripción completada! ✅ Nuestro equipo se pondrá en contacto contigo en breve para confirmarte los siguientes pasos. Gracias por confiar en Transfers & Experiences. 📚✨"
                recipient_ok = f"+{wa_id}"
                data_ok = get_text_message_input(recipient_ok, confirmation)
                try:
                    current_ctx = get_enrollment_context(wa_id)
                    history = list(current_ctx.get("inscripciones") or [])
                    timestamp_ahora = _get_madrid_timestamp()
                    record = {"codigo_curso": codigo_inscripcion, "fecha": timestamp_ahora}
                    history.append(record)
                    for k in ("last_crm_payload", "last_crm_payload_hash", "uploaded_to_crm", "last_crm_upload_at", "ultima_inscripcion"):
                        if k in current_ctx:
                            current_ctx.pop(k, None)
                    current_ctx.update({
                        "inscripciones": history, 
                        "ultima_subida_crm": timestamp_ahora,
                        "crm_data_changed": False
                    })
                    set_enrollment_context(wa_id, current_ctx)
                except Exception as mark_err:
                    logging.error(f"Error updating enrollment history for {wa_id}: {mark_err}")
                sent_ok_id = send_message(data_ok)
                try:
                    log_message_to_db(
                        wa_id=wa_id, sender_name=project_name_from_config, message_text=confirmation,
                        direction='outbound_bot', project_name=project_name_from_config,
                        whatsapp_message_id=sent_ok_id, status='sent' if sent_ok_id else 'failed'
                    )
                except Exception as _log_conf_err:
                    logging.error(f"Error logging enrollment confirmation for {wa_id}: {_log_conf_err}")
                # Persistir en OpenAI que la inscripción se ha completado (contexto del hilo)
                try:
                    from app.services.openai_service import append_thread_with_payload as _append
                    user_note = "[event:enrollment_completed]"
                    payload_obj = {"event": "enrollment_completed", "codigo": codigo_inscripcion}
                    payload_text = json.dumps(payload_obj, ensure_ascii=False)
                    _append(current_app, wa_id, user_note, payload_text)
                except Exception as _append_ok_err:
                    logging.error(f"No se pudo registrar en OpenAI la inscripción completada para {wa_id}: {_append_ok_err}")
                
                # Post-enrollment state update - PAUSE BOT AFTER SUCCESSFUL ENROLLMENT
                try:
                    def _update_status(w, s): return True
                    from app.services.supabase_service import is_supabase_enabled as _sb_on, update_conversation_mode_for_wa as _sb_set_mode, update_conversation_attention_for_wa as _sb_set_attention
                    from app.utils.automation_manager import pause_automation as _pause_automation

                    logging.info(f"Updating status and mode post-enrollment for {wa_id}...")

                    # 1. Update status to 'Documentación Pendiente'
                    status_ok = _update_status(wa_id, "Documentación Pendiente")
                    if not status_ok:
                        logging.warning(f"Failed to update status for {wa_id} after enrollment.")
                    else:
                        # Log a system message so the change appears inline in the chat (igual que cambio manual)
                        try:
                            log_message_to_db(
                                wa_id=wa_id,
                                sender_name="System",
                                message_text='Estado actualizado a "Documentación Pendiente"',
                                direction='outbound_system',
                                project_name=project_name_from_config,
                            )
                        except Exception as _log_status_err:
                            logging.error(
                                f"Error logging post-enrollment status change for {wa_id}: {_log_status_err}"
                            )

                        # Assign fixed 'Documentación' queue only from automation path (not manual UI)
                        try:
                            from app.services.supabase_service import (
                                is_supabase_enabled as _sb_on2,
                                list_queues as _sb_list_queues,
                                create_queue as _sb_create_queue,
                                fetch_conversation_assigned_queue_ids_for_wa as _sb_fetch_qids,
                                update_conversation_assigned_queues_for_wa as _sb_update_qids,
                            )
                            if _sb_on2():
                                queues = _sb_list_queues() or []
                                doc_q = next((q for q in queues if (q or {}).get('name') == 'Documentación'), None)
                                if not doc_q:
                                    try:
                                        # Create Documentación queue with 'agent' mode
                                        doc_q = _sb_create_queue('Documentación', 'agent', ['Documentación Pendiente'], None, None)
                                    except Exception:
                                        doc_q = None
                                doc_qid = (doc_q or {}).get('id')
                                if doc_qid:
                                    try:
                                        current_ids = _sb_fetch_qids(wa_id) or []
                                    except Exception:
                                        current_ids = []
                                    # Add Doc queue id if not present and persist
                                    if str(doc_qid) not in [str(x) for x in current_ids]:
                                        new_ids = list(current_ids) + [str(doc_qid)]
                                        ok_update = _sb_update_qids(wa_id, new_ids)
                                        if not ok_update:
                                            logging.warning(f"Failed to assign 'Documentación' queue for {wa_id}")
                        except Exception as _assign_q_err:
                            logging.error(f"Error assigning 'Documentación' queue for {wa_id}: {_assign_q_err}")

                    # 2. Update mode to 'agent' or pause automation
                    if _sb_on():
                        mode_ok = _sb_set_mode(wa_id, 'agent')
                        if not mode_ok:
                            logging.warning(f"Failed to set mode to 'agent' in Supabase for {wa_id}.")
                        # 3. Mark conversation as "no atendida" (needs_attention = True)
                        try:
                            attention_ok = _sb_set_attention(wa_id, True)
                            if not attention_ok:
                                logging.warning(f"Failed to set needs_attention=True for {wa_id} after enrollment.")
                            else:
                                logging.info(f"Conversation {wa_id} marked as 'no atendida' after enrollment.")
                        except Exception as _att_err:
                            logging.error(f"Error setting needs_attention for {wa_id} after enrollment: {_att_err}")
                    else:
                        _pause_automation(wa_id, reason="Inscripción completada - Documentación Pendiente")

                except Exception as post_enrollment_err:
                    logging.error(f"Error in post-enrollment status/mode update for {wa_id}: {post_enrollment_err}")
                
                return {'status': 'stop'}
            else:
                comm_error = crm_result.get("codigo") == 500
                error_msg = ("Ha habido un error de comunicación, intenta completar de nuevo el formulario" if comm_error 
                             else crm_result.get("descripcion", "Ocurrió un error al inscribirte."))
                recipient_err = f"+{wa_id}"

                try:
                    ctx_email3 = get_enrollment_context(wa_id)
                    email_present3 = not is_missing_email(ctx_email3.get("email") or ctx_email3.get("Email"))
                    chosen_flow_id3 = (
                        current_app.config.get("WHATSAPP_FLOW_ID_INSCRIPCION") if email_present3 else
                        current_app.config.get("WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL")
                    )
                    flow_present = bool(chosen_flow_id3 and current_app.config.get("WHATSAPP_FLOW_TOKEN"))
                    sent_form_id = None
                    if flow_present:
                        header_text = error_msg if comm_error else "Inscripción"
                        body_text = "Completa de nuevo el formulario" if comm_error else "Abre de nuevo el formulario y completa el DNI correctamente."
                        footer_text = "Transfers & Experiences"
                        flow_payload = get_flow_message_input(
                            recipient_err, header_text, body_text, footer_text, chosen_flow_id3,
                            current_app.config.get("WHATSAPP_FLOW_TOKEN"),
                            current_app.config.get("WHATSAPP_FLOW_ACTION", "navigate"),
                            current_app.config.get("WHATSAPP_FLOW_CTA"), "3",
                            current_app.config.get("WHATSAPP_FLOW_ACTION_SCREEN"),
                            json.loads(current_app.config.get("WHATSAPP_FLOW_ACTION_PAYLOAD_JSON")) if current_app.config.get("WHATSAPP_FLOW_ACTION_PAYLOAD_JSON") else None,
                        )
                        logging.info("Reenviando Flow de inscripción (nfm_reply) con título personalizado si aplica.")
                        sent_form_id = send_message(flow_payload)
                    else:
                        template_name = current_app.config.get("WHATSAPP_TEMPLATE_ENROLL", "cuestionario_inscripcion")
                        language_code = current_app.config.get("WHATSAPP_TEMPLATE_LANG", "es_ES")
                        payload = get_template_message_input(recipient_err, template_name, language_code, None)
                        logging.info("Reenviando plantilla de inscripción (nfm_reply) tras error de DNI o comunicación.")
                        sent_form_id = send_message(payload)

                    if not comm_error:
                        data_err = get_text_message_input(recipient_err, error_msg)
                        sent_err_id = send_message(data_err)
                        try:
                            log_message_to_db(
                                wa_id=wa_id,
                                sender_name=project_name_from_config,
                                message_text=error_msg,
                                direction='outbound_bot',
                                project_name=project_name_from_config,
                                whatsapp_message_id=sent_err_id,
                                status='sent' if sent_err_id else 'failed'
                            )
                        except Exception as _log_err_msg_err:
                            logging.error(f"Error logging error message for {wa_id}: {_log_err_msg_err}")
                    try:
                        log_message_to_db(
                            wa_id=wa_id, sender_name=project_name_from_config, message_text="Reenvío de formulario de inscripción",
                            direction='outbound_bot', project_name=project_name_from_config,
                            whatsapp_message_id=sent_form_id, status='sent' if sent_form_id else 'failed'
                        )
                    except Exception as _log_err2:
                        logging.error(f"Error registrando reenvío de formulario (nfm_reply) para {wa_id}: {_log_err2}")
                except Exception as _resend_err:
                    logging.error(f"Error reenviando formulario tras DNI inválido (nfm_reply) para {wa_id}: {_resend_err}")
                return {'status': 'stop'}
        except Exception as crm_err:
            logging.error(f"Error completing CRM enrollment for {wa_id}: {crm_err}")
            try:
                recipient_fail = f"+{wa_id}"
                fail_msg = "Lo siento, hubo un problema al finalizar la inscripción. Intenta de nuevo más tarde."
                data_fail = get_text_message_input(recipient_fail, fail_msg)
                sent_fail_id = send_message(data_fail)
                try:
                    log_message_to_db(
                        wa_id=wa_id,
                        sender_name=project_name_from_config,
                        message_text=fail_msg,
                        direction='outbound_bot',
                        project_name=project_name_from_config,
                        whatsapp_message_id=sent_fail_id,
                        status='sent' if sent_fail_id else 'failed'
                    )
                except Exception as _log_fail_err:
                    logging.error(f"Error logging fail message for {wa_id}: {_log_fail_err}")
            except Exception:
                pass
        raw_question = "[Formulario de inscripción completado]"
        inbound_display_text = "Rellenar - Respuesta enviada"

    else:
        logging.warning(f"Unhandled interactive type '{interactive_type}' from {wa_id}. Treating as unsupported.")
        raw_question = "[Mensaje interactivo no soportado]"
        error_occurred_early = True

    return {
        'status': 'continue',
        'raw_question': raw_question,
        'inbound_display_text': inbound_display_text,
        'error': error_occurred_early
    }

def build_interactive_response(recipient, processed_response):
    """
    Construye una respuesta interactiva (botones, listas) a partir de un texto de respuesta del bot.
    """
    data_to_send = None
    accion_requerida = None
    
    try:
        resp_text = processed_response or ""
        flags = re.IGNORECASE | re.MULTILINE
        pattern_situacion = r"¿[^?\n]{0,200}situaci[oó]n\s+laboral[^?\n]{0,200}\?"
        pattern_nivel = (
            r"(selecciona\s+tu\s+titulaci[oó]n\s+acad[eé]mica"
            r"|¿[^?\n]{0,220}(titulaci[oó]n|nivel\s+de\s+formaci[oó]n|estudios\s+acad[eé]micos)[^?\n]{0,220}\?"
            r"|necesito\s+saber\s+tu\s+nivel\s+de\s+formaci[oó]n"
            r"|cu[aá]les\s+son\s+tus\s+estudios\s+acad[eé]micos)"
        )
        pattern_actividad = r"a\s+qu[eé]\s+se\s+dedica"

        if re.search(pattern_situacion, resp_text, flags):
            buttons = [
                {"id": "situacion_ocupado", "title": "Ocupado"},
                {"id": "situacion_desempleado", "title": "Desempleado"},
                {"id": "situacion_autonomo", "title": "Autónomo"}
            ]
            body_text = processed_response
            if isinstance(body_text, str) and len(body_text) > 1000:
                body_text = body_text[:1000] + "…"
            data_to_send = get_button_message_input(recipient, body_text, buttons)
            accion_requerida = f"interactive_buttons:{json.dumps(buttons, ensure_ascii=False)}"
        
        elif re.search(pattern_nivel, resp_text, flags):
            rows_all = [
                {"id": "tit_1", "title": "SIN ESTUDIOS"},
                {"id": "tit_2", "title": "EST. PRIMARIOS", "description": "ESTUDIOS PRIMARIOS"},
                {"id": "tit_13", "title": "FP GR. MEDIO", "description": "FP GRADO MEDIO"},
                {"id": "tit_14", "title": "ESO"},
                {"id": "tit_4", "title": "BACHILLERATO"},
                {"id": "tit_9", "title": "DOCTORADO"},
                {"id": "tit_17", "title": "MÁSTER"},
                {"id": "tit_21", "title": "FP GR. SUPERIOR", "description": "FP GRADO SUPERIOR"},
                {"id": "tit_22", "title": "GRADO UNIV.", "description": "GRADO UNIVERSITARIO"},
                {"id": "tit_27", "title": "CP NIVEL 1", "description": "CERTIFICADO DE PROFESIONALIDAD DE NIVEL 1"},
                {"id": "tit_10", "title": "ACCESO UNI >25", "description": "ACCESO UNIVERSIDAD MAYORES 25"},
                {"id": "tit_11", "title": "CP NIVEL 2", "description": "CERTIFICADO DE PROFESIONALIDAD DE NIVEL 2"},
                {"id": "tit_12", "title": "CP NIVEL 3", "description": "CERTIFICADO DE PROFESIONALIDAD DE NIVEL 3"},
                {"id": "tit_30", "title": "PROF. MÚSICA/DANZA", "description": "ENSEÑANZAS PROFESIONALES DE MÚSICA Y DANZA"},
            ]
            rows_page1 = rows_all[:9] + [{"id": "tit_more", "title": "Ver más opciones"}]
            body_text = processed_response
            if isinstance(body_text, str) and len(body_text) > 1000:
                body_text = body_text[:1000] + "…"
            data_to_send = get_list_message_input(recipient, body_text, rows_page1, button_label="Elegir", section_title="Titulación")
            accion_requerida = f"interactive_list:{json.dumps(rows_page1, ensure_ascii=False)}"
        
        elif re.search(pattern_actividad, resp_text, flags):
            try:
                wa_id = recipient.strip('+')
                update_enrollment_context(wa_id, {"awaiting_activity": True, "sector_notified": False})
            except Exception as _ctx_err:
                logging.error(f"Error setting awaiting_activity for {recipient}: {_ctx_err}")
            data_to_send = get_text_message_input(recipient, processed_response)
            
    except Exception as e:
        logging.error(f"Error building interactive message payload: {e}. Falling back to text message.")
        data_to_send = get_text_message_input(recipient, processed_response)

    if data_to_send is None:
        data_to_send = get_text_message_input(recipient, processed_response)

    return data_to_send, accion_requerida
