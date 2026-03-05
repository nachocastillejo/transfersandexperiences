# openai_functions.py

import json
import logging
import time
import requests
from app.services.calendar_service import (
    add_event_to_calendar,
    delete_event_from_calendar,
    only_check_availability
)
from app.utils.extra_utils import parse_datetime
from app.utils.extra_utils import validate_and_normalize_spanish_tax_id
from app.services.perplexity_service import generate_response_perplexity
from app.utils.enrollment_state import update_enrollment_context, get_enrollment_context


def GPTRequest(client, prompt):
    """Realiza una petición al modelo GPT-4.1-mini de OpenAI y devuelve la respuesta."""
    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}],
        #temperature=1,
        max_tokens=3000,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    return response.choices[0].message.content


def process_check_availability(tool_call):
    """
    Procesa el tool_call para la función 'check_availability'.

    Extrae la hora de la cita desde los argumentos en JSON, verifica la disponibilidad,
    y retorna un diccionario con el ID del tool_call y el resultado formateado en JSON.

    Args:
        tool_call: Objeto con atributos 'id' y 'function'. 'function' debe tener un atributo
                   'arguments' en formato JSON que incluya la clave "appointment_time".

    Returns:
        dict: Diccionario con:
            - "tool_call_id": ID del tool_call.
            - "output": JSON string con las claves "status" y "message".
    """
    try:
        # Parsear los argumentos del tool_call desde JSON
        args = json.loads(tool_call.arguments)

        # Argumentos
        appointment_time = args.get("appointment_time", "Hora no proporcionada")
        
        # Registrar la solicitud de agendamiento
        logging.info(f"📅 Checking availability for: {appointment_time}")

        # Convertir la hora de la cita a un formato ISO usando la función parse_datetime
        appointment_time = parse_datetime(appointment_time)
        
        # Acción -> Verificar disponibilidad
        reply, status = only_check_availability(appointment_time)
        log_reply = reply.replace('\n', ' ')
        logging.info(f"📤 Function Response sent to OpenAI: {log_reply} - Status: {status}\n")
        
        # Retornar el resultado formateado
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({"status": status, "message": reply})
        }
    
    except json.JSONDecodeError as e:
        logging.error(f"❌ Error decodificando JSON en check_availability: {e}")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({
                "status": "error",
                "message": "Formato JSON inválido en los argumentos."
            })
        }
    except Exception as e:
        logging.error(f"❌ Error inesperado en check_availability: {e}")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({
                "status": "error",
                "message": f"Error inesperado: {str(e)}"
            })
        }


def process_detect_appointment(tool_call):
    """
    Procesa el tool_call para agendar una cita (detect_appointment).

    Extrae los parámetros (nombre, email, teléfono y hora de la cita) de un string JSON,
    intenta agendar el evento en Google Calendar y retorna un diccionario con el resultado.

    Args:
        tool_call: Objeto que debe incluir los atributos 'id' y 'function'. 
                   La propiedad 'function.arguments' es un string en formato JSON con las claves:
                   "name", "email", "phone" y "appointment_time".

    Returns:
        dict: Diccionario con:
              - "tool_call_id": ID del tool_call.
              - "output": String en formato JSON con las claves "status" y "message" que indican el resultado.
    """
    try:
        # Parsear los argumentos del tool_call desde JSON
        args = json.loads(tool_call.arguments)

        # Argumentos:
        name_arg = args.get("name", "Nombre no proporcionado")
        email = args.get("email", "Email no proporcionado")
        phone = args.get("phone", "Teléfono no proporcionado")
        appointment_time = args.get("appointment_time", "Hora no proporcionada")

        # Registrar la solicitud de agendamiento
        logging.info(f"📅 Scheduling an appointment for: {name_arg} ({email}) ({phone}) at {appointment_time}")

        # Convertir la hora de la cita a un formato ISO usando la función parse_datetime
        appointment_time = parse_datetime(appointment_time)

        # Acción -> Agendar evento en el calendario
        reply, status = add_event_to_calendar(name_arg, email, phone, appointment_time)
        log_reply = reply.replace('\n', ' ')
        logging.info(f"📤 Function Response sent to OpenAI: {log_reply} - Status: {status}\n")

        # Retornar el resultado formateado
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({"status": status, "message": reply})
        }
    
    except json.JSONDecodeError as e:
        logging.error(f"❌ Error decodificando JSON en detect_appointment: {e}")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({
                "status": "error",
                "message": "Formato JSON inválido en los argumentos."
            })
        }
    except Exception as e:
        logging.error(f"❌ Error inesperado en detect_appointment: {e}")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({
                "status": "error",
                "message": f"Error inesperado: {str(e)}"
            })
        }


def process_cancel_appointment(tool_call):
    """
    Procesa el tool_call para cancelar una cita (cancel_appointment).

    Extrae de los argumentos en JSON el nombre, email y hora de la cita, y luego
    intenta eliminar el evento correspondiente en el calendario. Retorna un diccionario
    con el ID del tool_call y un mensaje en formato JSON indicando el resultado.

    Args:
        tool_call: Objeto que debe tener los atributos 'id' y 'function'. La propiedad 
                   'function.arguments' es un string en formato JSON que incluye las claves:
                   "name", "email" y "appointment_time".

    Returns:
        dict: Diccionario con:
              - "tool_call_id": ID del tool_call.
              - "output": String JSON con las claves "status" y "message" que indican el resultado.
    """
    try:
        # Convertir los argumentos de JSON a un diccionario
        args = json.loads(tool_call.arguments)
        name_arg = args.get("name", "Nombre no proporcionado")
        email = args.get("email", "Email no proporcionado")
        appointment_time = args.get("appointment_time", "Hora no proporcionada")

        # Registrar la intención de cancelar la cita
        logging.info(f"🗑️ Eliminando cita para: {name_arg} ({email}) el {appointment_time}")

        # Convertir la hora de la cita a formato ISO usando la función parse_datetime
        appointment_time_str = parse_datetime(appointment_time)

        # Intentar eliminar el evento del calendario
        reply, status = delete_event_from_calendar(name_arg, email, appointment_time_str)
        log_reply = reply.replace('\n', ' ')
        logging.info(f"📤 Function Response sent to OpenAI: {log_reply} - Status: {status}\n")
        # Retornar el resultado formateado
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({"status": status, "message": reply})
        }

    except json.JSONDecodeError as e:
        logging.error(f"❌ Error decodificando JSON en cancel_appointment: {e}")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({
                "status": "error",
                "message": "Formato JSON inválido en los argumentos."
            })
        }
    except Exception as e:
        logging.error(f"❌ Error inesperado en cancel_appointment: {e}")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({
                "status": "error",
                "message": f"Error inesperado: {str(e)}"
            })
        }


def process_recomendar_cursos(tool_call, wa_id=None):
    """
    Procesa el tool_call para la función 'recomendar_cursos'.

    Extrae los criterios (provincia, situación laboral, nivel de formación)
    y opcionalmente el indicador 'buscar_alternativas' de los argumentos en JSON,
    busca cursos en Google Sheets que coincidan con esos criterios (prioritarios
    o alternativos según corresponda), y retorna un diccionario con el ID del tool_call
    y el resultado formateado en JSON.

    Args:
        tool_call: Objeto con atributos 'id' y 'function'. 'function' debe tener un atributo
                   'arguments' en formato JSON que incluya las claves "provincia",
                   "situacion_laboral", "nivel_formacion" y opcionalmente "buscar_alternativas".

    Returns:
        dict: Diccionario con:
            - "tool_call_id": ID del tool_call.
            - "output": JSON string con las claves "status" y "message".
    """
    try:
        # ADDED LOGGING:
        logging.info(f"Function Call Received: recommendar_cursos")
        # Parsear los argumentos del tool_call desde JSON
        args = json.loads(tool_call.arguments)

        # Argumentos (ahora usamos 'origen' en lugar de 'provincia')
        origen = args.get("origen", "Origen no proporcionado")
        situacion_laboral = args.get("situacion_laboral", "Situación laboral no proporcionada")
        nivel_formacion = args.get("nivel_formacion", "Nivel de formación no proporcionado")
        sector = args.get("sector", "N/A")
        modalidad = args.get("modalidad", "N/A")
        tematica = args.get("tematica", "N/A")
        codigo = args.get("codigo", "N/A")
        # Leer número de página (iteración)
        pagina = int(args.get("pagina", 1))
        # Prioridad determinada por PP + PC

        # Obtener nombre de formación del contexto si está disponible
        formacion_nombre = "N/A"
        if wa_id:
            try:
                ctx = get_enrollment_context(wa_id) or {}
                formacion_nombre = ctx.get("formacion") or "N/A"
            except Exception:
                pass

        # Registrar SIEMPRE la solicitud de recomendación con TODOS los parámetros
        logging.info(
            "📚 Buscando cursos para: "
            f"Origen='{origen}', "
            f"Situación='{situacion_laboral}', "
            f"Formación='{nivel_formacion}'" + (f" ({formacion_nombre})" if formacion_nombre != "N/A" else "") + ", "
            f"Sector='{sector}', "
            f"Modalidad='{modalidad}', "
            f"Temática='{tematica}', "
            f"Página={pagina}, "
            f"Código='{codigo}'"
        )

        # Persist active filters under a transient 'current_search' namespace for pagination only
        try:
            if wa_id:
                update_enrollment_context(wa_id, {
                    "current_search": {
                        "provincia": origen,
                        "situacion_laboral": situacion_laboral,
                        "nivel_formacion": nivel_formacion,
                        "sector": sector,
                        "modalidad": modalidad,
                        "tematica": tematica,
                        "pagina_actual": pagina,
                        "codigo": codigo
                    }
                })
                logging.info(f"📡 current_search updated for {wa_id} (pagination-scoped filters)")
        except Exception as _ctx_err:
            logging.error(f"Error updating current_search in context for {wa_id}: {_ctx_err}")

        # --- Lógica para leer y filtrar desde Google Sheets --- 
        filtered_courses = []
        try:
            # Importación local para evitar ciclos
            from app.services.drive_service import get_and_filter_courses
            # Llamar a la función en drive_service para obtener y filtrar los cursos
            # Filtrando cursos con timeout implícito
            logging.info(f"🔍 Iniciando búsqueda de cursos para {wa_id}...")
            filtered_courses = get_and_filter_courses(origen, situacion_laboral, nivel_formacion, pagina=pagina, sector=sector, modalidad=modalidad, tematica=tematica, codigo=codigo, wa_id=wa_id)
            logging.info(f"✅ Búsqueda de cursos completada para {wa_id}. Encontrados: {len(filtered_courses)} cursos")
            if filtered_courses:
                print("\n" + "="*40)
                print("📚 Cursos filtrados encontrados:")
                print("="*40)
                for idx, curso in enumerate(filtered_courses, 1):
                    nombre = curso.get("curso", "Sin nombre")
                    codigo = curso.get("codigo", "Sin código")
                    origen_curso = curso.get("origen", curso.get("provincia", "Sin origen"))
                    fecha_inicio = curso.get("f.inicio", "Sin fecha")
                    print(f"{idx}. {nombre}")
                    print("-"*40)
                print(f"Total: {len(filtered_courses)} cursos encontrados.\n")
            else:
                print("No se encontraron cursos que coincidan con los criterios.")
        except ImportError:
            logging.error("❌ Error crítico: No se pudo importar 'get_and_filter_courses' desde 'app.services.drive_service'. La funcionalidad de recomendación no está disponible.")
            # Retornar un error indicando que el servicio no está disponible
            return {
                "tool_call_id": tool_call.id,
                "output": json.dumps({
                    "status": "error",
                    "message": "Lo siento, el servicio de recomendación de cursos no está disponible en este momento."
                })
            }
        except MemoryError as mem_e:
            # Error específico de memoria - crítico
            logging.error(f"💥 ERROR DE MEMORIA en recomendar_cursos para {wa_id}: {mem_e}")
            return {
                "tool_call_id": tool_call.id,
                "output": json.dumps({
                    "status": "error",
                    "message": "Lo siento, el sistema está experimentando problemas de memoria. Por favor, intenta de nuevo en unos minutos."
                })
            }
        except Exception as drive_e:
            # Capturar errores específicos de la función de drive_service si es necesario
            logging.error(f"❌ Error al obtener cursos desde drive_service para {wa_id}: {drive_e}")
            return {
                "tool_call_id": tool_call.id,
                "output": json.dumps({
                    "status": "error",
                    "message": f"Error al buscar cursos: {str(drive_e)}"
                })
            }

        # Formatear la respuesta: si no hay cursos en esta página y es la 3, mensaje final
        if filtered_courses:
            reply_message = json.dumps(filtered_courses, ensure_ascii=False, indent=2)
            status = "success"
        else:
            # Mantener el comportamiento neutral estándar cuando no hay resultados
            if isinstance(tematica, str) and tematica.strip().lower() != 'n/a':
                reply_message = (
                    f"Ahora mismo no tengo cursos de '{tematica}' que encajen con esos criterios. "
                    "Si quieres, puedo buscar más cursos de otras temáticas o sin una temática en específico."
                )
            else:
                reply_message = (
                    "Ahora mismo no tengo cursos que encajen con esos criterios. "
                    "Si quieres, puedo seguir buscando añadiendo una temática en específico (por ejemplo, 'idiomas', 'marketing digital', etc.)."
                )
            status = "not_found"

        # Log a truncated version or summary if the full JSON is too long for logs
        log_reply = (reply_message[:10] + '...') if len(reply_message) > 10 else reply_message
        log_reply = log_reply.replace('\n', ' ') # Replace newlines for cleaner log output
        # Añadir qué tipo de cursos se buscaron al log
        logging.info(f"📤 Function Response sent to OpenAI: {log_reply} - Status: {status}\n") 

        # Retornar el resultado formateado
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({"status": status, "message": reply_message})
        }

    except json.JSONDecodeError as e:
        logging.error(f"❌ Error decodificando JSON en recomendar_cursos: {e}. Argumentos recibidos: '{tool_call.arguments}'")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({
                "status": "error",
                "message": "Formato JSON inválido en los argumentos."
            })
        }
    except Exception as e:
        logging.error(f"❌ Error inesperado en recomendar_cursos: {e}")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({
                "status": "error",
                "message": f"Error inesperado al procesar la recomendación: {str(e)}"
            })
        }


def process_inscribir_lead_crm(tool_call, wa_id=None):
    """
    Procesa el tool_call para la función 'inscribir_lead_crm'. (MOCKED PARA DEMO)
    """
    logging.info("MOCKED: Function Call Received: inscribir_lead_crm")
    return {
        "tool_call_id": tool_call.id,
        "output": json.dumps({
            "status": "success",
            "message": "Inscripción en CRM completada (MOCK)"
        })
    }



def process_insert_question(tool_call):
    """
    Procesa el tool_call para la función 'insert_question'.

    Args:
        tool_call: Objeto que contiene la información de la función a procesar.

    Returns:
        dict: Resultado del procesamiento en formato diccionario.
    """
    try:
        args = json.loads(tool_call.arguments)
        # Importación local para evitar ciclos
        from app.services.drive_service import insert_question  
        insert_question(args["question"])
        logging.info("Pregunta registrada en Google Sheets")
        logging.info("Run (insert_question) completado")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({
                "status": "success",
                "message": "Pregunta registrada en Google Sheets"
            })
        }
    except Exception as e:
        logging.error(f"Error en insert_question: {str(e)}")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({"error": str(e)})
        }


def process_get_real_time_data(client, tool_call, message_body):
    try:
        args = json.loads(tool_call.arguments)
        start_temp = time.time()
        message_rt = generate_response_perplexity(args["question"])
        logging.info(f"Respuesta de Perplexity en -> {time.time() - start_temp:.2f} segundos")
        
        # Importación local para evitar el import circular
        from app.services.openai_service import detect_language_name  
        language_of_question = detect_language_name(message_body)
        logging.info(f"Idioma detectado para Perplexity: {language_of_question}")
        
        prompt = (
            f"Improve the answer I am sending you and avoid inconsistencies. "
            f"Respond in {language_of_question} as a super friendly assistant. "
            "Do not mention that you are translating or enhancing the text, "
            "just respond naturally. Use emojis to make it more engaging and warm. "
            "Organize the citations for a good viz of the answer: "
            f"{message_rt}"
        )
        start_temp = time.time()
        improved_response = GPTRequest(client, prompt)
        logging.info(f"Respuesta mejorada por GPT-4.1-mini en -> {time.time() - start_temp:.2f} segundos")
        
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({"status": "success", "response": improved_response})
        }
    except Exception as e:
        logging.error(f"Error en get_real_time_data: {str(e)}")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({"error": str(e)})
        }


def process_collect_contact(tool_call):
    """
    Procesa el tool_call para la función 'collect_contact'.

    Guarda en el estado de inscripción (enrollment state) el nombre y el email
    del usuario cuando ambos se proporcionan.

    Args:
        tool_call: Objeto con atributos 'id' y 'arguments' (JSON string con 'name' y 'email').

    Returns:
        dict: { "tool_call_id": id, "output": "{\"status\": \"success\", \"message\": \"OK\"}" }
    """
    try:
        args = json.loads(tool_call.arguments)
        name_value = (args.get("name") or "").strip()
        email_value = (args.get("email") or "").strip()

        # Validación mínima
        if not name_value or not email_value:
            return {
                "tool_call_id": tool_call.id,
                "output": json.dumps({
                    "status": "error",
                    "message": "Faltan 'name' o 'email' para guardar el contacto."
                })
            }

        # Extraer el wa_id desde el contexto de OpenAI no es directo aquí; el servicio llama al processor.
        # Este processor solo devuelve el echo para que el servicio lo persistia.
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({
                "status": "success",
                "message": json.dumps({"name": name_value, "email": email_value}, ensure_ascii=False)
            })
        }
    except Exception as e:
        logging.error(f"Error en collect_contact: {str(e)}")
        return {
            "tool_call_id": tool_call.id,
            "output": json.dumps({"status": "error", "message": str(e)})
        }
