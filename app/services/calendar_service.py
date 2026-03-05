import os
import smtplib
from email.message import EmailMessage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import locale
import sys
import pytz
from app.utils.extra_utils import load_business_hours, load_holidays, round_up_time
import logging

# Cargar las configuraciones desde variables de entorno
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "")
CALENDAR_ID = os.getenv("CALENDAR_ID", "")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = os.getenv("SMTP_PORT", "587")
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
BUSINESS_HOURS = load_business_hours(os.getenv("BUSINESS_HOURS", "{}"))
HOLIDAYS = load_holidays(os.getenv("HOLIDAYS", "[]"))
ROUNDING_INTERVAL = int(os.getenv("ROUNDING_INTERVAL") or 30)
MAIN_LANGUAGE = os.getenv("MAIN_LANGUAGE", "spanish").lower() # Leer idioma, por defecto spanish

# Definir plantillas de correo electrónico
EMAIL_TEMPLATES = {
    "spanish": {
        "add_subject": "📅 Confirmación de tu cita: {summary}",
        "add_body": """¡Hola {first_name}!

✅ Tu cita ha sido confirmada exitosamente.
📅 Fecha: {formatted_date} de {start_hour} a {end_hour} horas

🙍‍♂️ Nombre: {name}
📞 Teléfono: {phone}
✉️ Email: {email}

Nuestro equipo médico, está comprometido con ofrecerte los más altos estándares de calidad y atención personalizada.

¡Estamos deseando conocerte y ayudarte a alcanzar tus objetivos estéticos!

Atentamente,

""",
        "delete_subject": "📅 Confirmación de eliminación de tu cita: {summary}",
        "delete_body": """¡Hola {first_name}!

❌ Tu cita ha sido eliminada exitosamente.
📅 Fecha: {formatted_date} de {start_hour} a {end_hour} horas

Si necesitas reprogramar tu cita o tienes alguna pregunta, no dudes en contactarnos.

Atentamente,

"""
    },
    "english": {
        "add_subject": "📅 Appointment Confirmation: {summary}",
        "add_body": """Hello {first_name}!

✅ Your appointment has been successfully confirmed.
📅 Date: {formatted_date} from {start_hour} to {end_hour}

🙍‍♂️ Name: {name}
📞 Phone: {phone}
✉️ Email: {email}

Our medical team is committed to offering you the highest standards of quality and personalized care.

We look forward to meeting you and helping you achieve your aesthetic goals!

Sincerely,

""",
        "delete_subject": "📅 Appointment Cancellation Confirmation: {summary}",
        "delete_body": """Hello {first_name}!

❌ Your appointment has been successfully cancelled.
📅 Date: {formatted_date} from {start_hour} to {end_hour}

If you need to reschedule your appointment or have any questions, please do not hesitate to contact us.

Sincerely,

"""
    }
}

def get_calendar_service():
    """
    Autentica y construye el servicio de Google Calendar.
    
    Retorna:
      googleapiclient.discovery.Resource: Servicio autenticado de Google Calendar.
    
    Lanza:
      Exception: En caso de error al autenticar o construir el servicio.
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/calendar"]
        )
        service = build("calendar", "v3", credentials=credentials)
        return service
    except Exception as e:
        raise Exception(f"Error authenticating with Google Calendar: {str(e)}") # English error message



def only_check_availability(start_time, max_search_days=7,
                            min_gap_between_alternatives=30, num_alternatives=4, 
                            duration_minutes=30, timezone="Europe/Madrid", rounding_interval=30):
    """
    Busca alternativas para agendar una cita en Google Calendar considerando el número de slots 
    definidos en BUSINESS_HOURS para cada intervalo. Ahora se utiliza events.list para obtener 
    los eventos individuales y contar correctamente el solapamiento.
    
    Se redondea la hora de inicio y las alternativas al siguiente múltiplo de 'rounding_interval' minutos.
    
    Parámetros:
      start_time (datetime): Fecha y hora de inicio como objeto datetime.
      max_search_days (int): Número máximo de días a buscar alternativas.
      min_gap_between_alternatives (int): Intervalo en minutos para separar alternativas.
      num_alternatives (int): Número de alternativas a retornar.
      duration_minutes (int): Duración de la cita en minutos (por defecto 30).
      timezone (str): Zona horaria (por defecto "Europe/Madrid").
      rounding_interval (int): Intervalo de redondeo en minutos (0, 15, 30, etc).
    
    Retorna:
      tuple: (mensaje, status) donde status es "appointment_time_available" si la franja solicitada
             está libre, "available" si se encontraron huecos alternativos, o "not_available" si no se hallaron huecos.
    """
    tz = pytz.timezone(timezone)
    now = datetime.now(tz)
    
    # Ajustar start_time si es pasada
    if start_time < now:
        start_time = now + timedelta(minutes=30)
    duration = timedelta(minutes=duration_minutes)
    requested_end = start_time + duration

    service = get_calendar_service()
    search_limit = start_time + timedelta(days=max_search_days)
    
    # Obtener eventos individuales con events.list
    events_result = service.events().list(
        calendarId=CALENDAR_ID,
        timeMin=start_time.isoformat(),
        timeMax=search_limit.isoformat(),
        singleEvents=True,
        orderBy='startTime'
    ).execute()
    events_list = events_result.get('items', [])

    def get_business_interval(candidate_time):
        intervals = BUSINESS_HOURS.get(candidate_time.weekday(), [])
        for interval in intervals:
            # Se utiliza tz.localize para asignar la zona horaria Europe/Madrid sin conversión
            block_start = tz.localize(datetime.combine(candidate_time.date(), interval["start"]))
            block_end = tz.localize(datetime.combine(candidate_time.date(), interval["end"]))
            if candidate_time >= block_start and (candidate_time + duration) <= block_end:
                return interval, block_start, block_end
        return None, None, None

    requested_interval, req_block_start, req_block_end = get_business_interval(start_time)
    if requested_interval:
        count = 0
        for event in events_list:
            event_start = datetime.fromisoformat(event['start'].get('dateTime', event['start'].get('date'))).astimezone(tz)
            event_end = datetime.fromisoformat(event['end'].get('dateTime', event['end'].get('date'))).astimezone(tz)
            if event_start < requested_end and event_end > start_time:
                count += 1
        requested_available = count < requested_interval["slots"]
    else:
        requested_available = False

    alternatives = []
    days_to_check = [start_time + timedelta(days=i) for i in range(max_search_days + 1)]
    last_alternative_start = None

    for day in days_to_check:
        if day.date() in HOLIDAYS or day.weekday() not in BUSINESS_HOURS:
            continue

        for interval in BUSINESS_HOURS[day.weekday()]:
            # Se utiliza tz.localize para asignar la zona horaria Europe/Madrid sin conversión
            block_start = tz.localize(datetime.combine(day.date(), interval["start"]))
            block_end = tz.localize(datetime.combine(day.date(), interval["end"]))
            allowed_slots = interval["slots"]

            if day.date() == start_time.date():
                search_start = max(start_time, block_start, now + timedelta(minutes=30))
            else:
                search_start = block_start

            current_time = round_up_time(search_start, rounding_interval)
            search_end = block_end - duration

            day_events = [
                event for event in events_list 
                if datetime.fromisoformat(event['start'].get('dateTime', event['start'].get('date'))).astimezone(tz).date() == day.date()
            ]
            sorted_day_events = sorted(day_events, key=lambda x: datetime.fromisoformat(x['start'].get('dateTime', x['start'].get('date'))))
            
            while current_time <= search_end and len(alternatives) < num_alternatives:
                if last_alternative_start:
                    min_allowed = round_up_time(last_alternative_start + timedelta(minutes=min_gap_between_alternatives), rounding_interval)
                    current_time = max(current_time, min_allowed)
                    if current_time > search_end:
                        break

                candidate_end = current_time + duration

                if not (current_time >= block_start and candidate_end <= block_end):
                    current_time += timedelta(minutes=min_gap_between_alternatives)
                    continue

                overlap_count = 0
                conflict_event = None
                for event in sorted_day_events:
                    event_start = datetime.fromisoformat(event['start'].get('dateTime', event['start'].get('date'))).astimezone(tz)
                    event_end = datetime.fromisoformat(event['end'].get('dateTime', event['end'].get('date'))).astimezone(tz)
                    if event_start < candidate_end and event_end > current_time:
                        overlap_count += 1
                        conflict_event = event
                        if overlap_count >= allowed_slots:
                            break
                
                if overlap_count < allowed_slots:
                    alternatives.append(current_time)
                    last_alternative_start = current_time
                    current_time = round_up_time(current_time + timedelta(minutes=min_gap_between_alternatives), rounding_interval)
                else:
                    if conflict_event:
                        new_time = datetime.fromisoformat(conflict_event['end'].get('dateTime', conflict_event['end'].get('date'))).astimezone(tz)
                        current_time = round_up_time(new_time, rounding_interval)
                    else:
                        current_time += timedelta(minutes=min_gap_between_alternatives)
            if len(alternatives) >= num_alternatives:
                break
        if len(alternatives) >= num_alternatives:
            break

    if alternatives:
        if MAIN_LANGUAGE == "english":
             message = "The first available slots found after the requested date and time are:"
             date_format = "%m/%d at %H:%M"
             end_format = "%H:%M"
        else: # Default to Spanish
             message = "Los primeros huecos libres encontrados tras la fecha y hora solicitada son:"
             date_format = "%d/%m a las %H:%M"
             end_format = "%H:%M"

        for i, alt in enumerate(alternatives[:num_alternatives], 1):
            alt_end = alt + duration
            message += f"\n{i}. {alt.strftime(date_format)} - {alt_end.strftime(end_format)}"
        status = "appointment_time_available" if requested_available else "available"
        return message, status
    else:
        message = "No available slots found." if MAIN_LANGUAGE == "english" else "No se han encontrado huecos libres."
        return message, "not_available"



def add_event_to_calendar(name, email, phone, time):
    """
    Creates an event in Google Calendar and sends a confirmation email.
    Language depends on MAIN_LANGUAGE env variable.
    """
    start_dt = time
    end_dt = start_dt + timedelta(minutes=30)
    # Use generic summary, language-specific part is in the email template
    summary = "Appointment with " + name if MAIN_LANGUAGE == "english" else "Cita con " + name
    description = "First consultation with the doctor" if MAIN_LANGUAGE == "english" else "Primera consulta con la doctora"
    emails_destinatarios = [EMAIL_USER, email]

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/calendar"]
    )
    service = build("calendar", "v3", credentials=credentials)

    # Verify availability before scheduling the event
    reply, status = only_check_availability(start_dt)
    if status != "appointment_time_available":
        return reply, status # Return the message from only_check_availability (already localized)

    event = {
        "summary": summary,
        "description": description,
        "start": {
            "dateTime": start_dt.isoformat(),
            "timeZone": "Europe/Madrid" # Keep timezone for Calendar event
        },
        "end": {
            "dateTime": end_dt.isoformat(),
            "timeZone": "Europe/Madrid" # Keep timezone for Calendar event
        },
    }
    try:
        event_result = service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
        event_link = event_result.get('htmlLink')
    except Exception as e:
        logging.error(f"Error creating Google Calendar event: {e}")
        error_message = "Error creating the event." if MAIN_LANGUAGE == "english" else "Error al crear el evento."
        return error_message, "error"

    # Set locale for date formatting based on language
    lang_code = "en_US.UTF-8" if MAIN_LANGUAGE == "english" else "es_ES.UTF-8"
    windows_locale = "English_United States.1252" if MAIN_LANGUAGE == "english" else "Spanish_Spain.1252"
    
    try:
        if sys.platform.startswith("win"):
            locale.setlocale(locale.LC_TIME, windows_locale)
        else:
            locale.setlocale(locale.LC_TIME, lang_code)
    except locale.Error as e:
         logging.warning(f"Could not set locale {lang_code}/{windows_locale}: {e}. Using default locale.")
         # Fallback formatting if locale setting fails
         formatted_start_dt = start_dt.strftime("%Y-%m-%d") if MAIN_LANGUAGE == 'english' else start_dt.strftime("%d-%m-%Y")

    # Format date and time using locale settings
    date_format_str = "%A, %B %d, %Y" if MAIN_LANGUAGE == "english" else "%A, %d de %B de %Y"
    try:
      # Capitalize Spanish date format manually if needed (locale might not do it)
      formatted_start_dt_raw = start_dt.strftime(date_format_str)
      formatted_start_dt = formatted_start_dt_raw.capitalize() if MAIN_LANGUAGE == "spanish" else formatted_start_dt_raw
    except Exception as format_e:
        logging.error(f"Error formatting date with locale: {format_e}. Falling back.")
        formatted_start_dt = start_dt.strftime("%Y-%m-%d") if MAIN_LANGUAGE == 'english' else start_dt.strftime("%d-%m-%Y")


    formatted_start_hr = start_dt.strftime("%H:%M")
    formatted_end_hr = end_dt.strftime("%H:%M")
    first_name = name.split()[0].capitalize()

    # Select email template based on language
    lang_key = "english" if MAIN_LANGUAGE == "english" else "spanish"
    template = EMAIL_TEMPLATES.get(lang_key, EMAIL_TEMPLATES["spanish"]) # Default to Spanish if key invalid

    # Send confirmation email
    msg = EmailMessage()
    msg["Subject"] = template["add_subject"].format(summary=summary)
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(emails_destinatarios)
    msg.set_content(template["add_body"].format(
        first_name=first_name,
        formatted_date=formatted_start_dt,
        start_hour=formatted_start_hr,
        end_hour=formatted_end_hr,
        name=name,
        phone=phone,
        email=email
    ))

    try:
        # Determine SMTP class based on port (SSL common for 465)
        smtp_class = smtplib.SMTP_SSL if str(SMTP_PORT) == '465' else smtplib.SMTP
        with smtp_class(SMTP_SERVER, SMTP_PORT) as server:
             if str(SMTP_PORT) != '465': # StartTLS for non-SSL ports like 587
                server.starttls()
             server.login(EMAIL_USER, EMAIL_PASSWORD)
             server.send_message(msg)
    except Exception as e:
        logging.error(f"⚠️ Error sending email: {e}") # Keep emoji but use English log message

    # Construct response message based on language
    if MAIN_LANGUAGE == "english":
        reply = (
            f"Event created successfully. Appointment scheduled for {formatted_start_dt} "
            f"from {formatted_start_hr} to {formatted_end_hr}."
        )
    else: # Default Spanish
        reply = (
            f"Evento creado exitosamente. Cita agendada para {formatted_start_dt} de "
            f"{formatted_start_hr} a {formatted_end_hr}."
        )
    status = "event_added"
    return reply, status


def delete_event_from_calendar(name, email, time):
    """
    Deletes an event from Google Calendar and sends a cancellation email.
    Language depends on MAIN_LANGUAGE env variable.
    """
    start_dt = time
    end_dt = start_dt + timedelta(minutes=30)

    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/calendar"]
        )
        service = build("calendar", "v3", credentials=credentials)
    except Exception as e:
         error_message = f"Error authenticating with Google Calendar: {str(e)}" if MAIN_LANGUAGE == "english" else f"Error al autenticar con Google Calendar: {str(e)}"
         return (error_message, "error")

    time_min = start_dt.isoformat()
    time_max = end_dt.isoformat()
    # Use generic search query part, language-specific part is in the email template
    search_query = "Appointment with " + name if MAIN_LANGUAGE == "english" else "Cita con " + name

    try:
        events_result = service.events().list(
            calendarId=CALENDAR_ID,
            timeMin=time_min,
            timeMax=time_max,
            q=search_query, # Use the constructed search query
            singleEvents=True,
            orderBy="startTime"
        ).execute()
    except Exception as e:
        error_message = f"Error searching for the event: {str(e)}" if MAIN_LANGUAGE == "english" else f"Error al buscar el evento: {str(e)}"
        return (error_message, "error")

    events = events_result.get('items', [])
    if not events:
        error_message = "No event found matching the provided criteria." if MAIN_LANGUAGE == "english" else "No se encontró ningún evento que coincida con los criterios proporcionados."
        return (error_message, "error")

    event = events[0]
    event_id = event['id']
    # Use summary from the event, might not match exactly the search query if edited
    summary = event.get('summary', 'No Title' if MAIN_LANGUAGE == "english" else 'Sin Título')
    event_start = event['start'].get('dateTime', event['start'].get('date'))
    event_end = event['end'].get('dateTime', event['end'].get('date'))
    timezone_val = event['start'].get('timeZone', 'Europe/Madrid') # Keep timezone for parsing

    try:
        tz = pytz.timezone(timezone_val) # Use timezone from event
        event_start_dt = datetime.fromisoformat(event_start).astimezone(tz)
        event_end_dt = datetime.fromisoformat(event_end).astimezone(tz)
    except Exception as e:
        error_message = f"Error processing event dates: {str(e)}" if MAIN_LANGUAGE == "english" else f"Error al procesar las fechas del evento: {str(e)}"
        return (error_message, "error")

    try:
        service.events().delete(calendarId=CALENDAR_ID, eventId=event_id).execute()
    except Exception as e:
        error_message = f"Error deleting the event: {str(e)}" if MAIN_LANGUAGE == "english" else f"Error al eliminar el evento: {str(e)}"
        return (error_message, "error")

    # Set locale for date formatting based on language
    lang_code = "en_US.UTF-8" if MAIN_LANGUAGE == "english" else "es_ES.UTF-8"
    windows_locale = "English_United States.1252" if MAIN_LANGUAGE == "english" else "Spanish_Spain.1252"
    
    try:
        if sys.platform.startswith("win"):
            locale.setlocale(locale.LC_TIME, windows_locale)
        else:
            locale.setlocale(locale.LC_TIME, lang_code)
    except locale.Error as e:
         logging.warning(f"Could not set locale {lang_code}/{windows_locale}: {e}. Using default locale.")
         # Fallback formatting if locale setting fails
         formatted_start_dt = event_start_dt.strftime("%Y-%m-%d") if MAIN_LANGUAGE == 'english' else event_start_dt.strftime("%d-%m-%Y")

    # Format date and time using locale settings
    date_format_str = "%A, %B %d, %Y" if MAIN_LANGUAGE == "english" else "%A, %d de %B de %Y"
    try:
      # Capitalize Spanish date format manually if needed (locale might not do it)
      formatted_start_dt_raw = event_start_dt.strftime(date_format_str)
      formatted_start_dt = formatted_start_dt_raw.capitalize() if MAIN_LANGUAGE == "spanish" else formatted_start_dt_raw
    except Exception as format_e:
        logging.error(f"Error formatting date with locale: {format_e}. Falling back.")
        formatted_start_dt = event_start_dt.strftime("%Y-%m-%d") if MAIN_LANGUAGE == 'english' else event_start_dt.strftime("%d-%m-%Y")

    formatted_start_hr = event_start_dt.strftime("%H:%M")
    formatted_end_hr = event_end_dt.strftime("%H:%M")
    first_name = name.split()[0].capitalize()

    # Select email template based on language
    lang_key = "english" if MAIN_LANGUAGE == "english" else "spanish"
    template = EMAIL_TEMPLATES.get(lang_key, EMAIL_TEMPLATES["spanish"]) # Default to Spanish

    emails_destinatarios = [EMAIL_USER, email]

    msg = EmailMessage()
    msg["Subject"] = template["delete_subject"].format(summary=summary)
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(emails_destinatarios)
    msg.set_content(template["delete_body"].format(
        first_name=first_name,
        formatted_date=formatted_start_dt,
        start_hour=formatted_start_hr,
        end_hour=formatted_end_hr
    ))

    try:
         # Determine SMTP class based on port (SSL common for 465)
         smtp_class = smtplib.SMTP_SSL if str(SMTP_PORT) == '465' else smtplib.SMTP
         with smtp_class(SMTP_SERVER, SMTP_PORT) as server:
             if str(SMTP_PORT) != '465': # StartTLS for non-SSL ports like 587
                 server.starttls()
             server.login(EMAIL_USER, EMAIL_PASSWORD)
             server.send_message(msg)
    except Exception as e:
        logging.error(f"⚠️ Error sending email: {e}") # Keep emoji but use English log message

    # Construct response message based on language
    success_message = "Event deleted successfully." if MAIN_LANGUAGE == "english" else "Evento eliminado exitosamente."
    return (success_message, "success")
