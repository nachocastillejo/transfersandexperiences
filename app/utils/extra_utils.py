from datetime import datetime
from zoneinfo import ZoneInfo
from langdetect import detect
import pycountry
import json
from datetime import time as dt_time, date
import logging
import os
from typing import List, Dict, Any, Optional
import re
import shelve
from multiprocessing import Lock

from app.services.supabase_service import (
    is_supabase_enabled,
    get_previous_response_id as get_id_from_supabase,
    clear_previous_response_id as clear_id_in_supabase,
)

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from datetime import datetime
from zoneinfo import ZoneInfo

# Create a lock specifically for accessing the responses_db shelve file
_responses_db_lock = Lock()

def is_missing_email(value: str | None) -> bool:
    """
    Returns True when the given email-like value should be treated as missing.
    Normalizes common placeholders like "N/A", "NA", "None", "Null", and "-" to empty.
    """
    try:
        normalized = (value or "").strip().lower()
    except Exception:
        normalized = ""
    return normalized in ("", "n/a", "na", "none", "null", "-")

def store_current_response_id(db_directory: str, current_response_id: str, wa_id: str):
    """
    Almacena o actualiza el response_id asociado a un wa_id.

    Args:
        db_directory (str): Directorio donde se almacena la base de datos de responses.
        current_response_id (str): Identificador de la respuesta actual.
        wa_id (str): Identificador de WhatsApp.
    """
    db_path = os.path.join(db_directory, "responses_db")
    with _responses_db_lock:
        with shelve.open(db_path, writeback=True) as responses_shelf:
            # Se almacena en forma de diccionario para poder extender fácilmente la información
            responses_shelf[wa_id] = {"response_id": current_response_id}

def get_previous_response_id(db_directory: str, wa_id: str):
    """
    Retrieves the last response ID for a given wa_id, trying Supabase first
    and (if Supabase is enabled) NOT falling back to the local shelve database.
    When Supabase is disabled, uses local shelve.
    """
    if is_supabase_enabled():
        logging.info(f"Attempting to fetch previous_response_id for {wa_id} from Supabase.")
        response_id = get_id_from_supabase(wa_id)
        if response_id:
            logging.info(f"Successfully fetched previous_response_id from Supabase for {wa_id}.")
            return response_id
        # Do not fallback to shelve if Supabase is ON
        logging.info(f"No previous_response_id found in Supabase for {wa_id}. Skipping shelve fallback because SUPABASE_ON=True.")
        return None
    
    # Fallback to shelve if Supabase is disabled or returns nothing
    db_path = os.path.join(db_directory, 'responses_db')
    try:
        with shelve.open(db_path) as db:
            data = db.get(wa_id)
            if isinstance(data, dict):
                return data.get("response_id")
            return data
    except Exception as e:
        logging.error(f"Error accessing shelve database for previous_response_id: {e}")
        return None

def set_previous_response_id(db_directory: str, wa_id: str, response_id: str):
    db_path = os.path.join(db_directory, 'responses_db')
    with shelve.open(db_path, writeback=True) as db:
        db[wa_id] = {"response_id": response_id}

def delete_response_id(db_directory: str, wa_id: str):
    """
    Elimina el previous_response_id según el backend activo:
    - Si SUPABASE_ON=True: limpia en Supabase.
    - Si no: limpia en la base local (shelve).

    Args:
        db_directory (str): Directorio local donde se almacena la base de datos de responses.
        wa_id (str): Identificador de WhatsApp cuya entrada se eliminará.
    """
    if is_supabase_enabled():
        try:
            ok = clear_id_in_supabase(wa_id)
            if ok:
                logging.info(f"Cleared previous_response_id in Supabase for wa_id: {wa_id}")
            else:
                logging.warning(f"Requested clear of previous_response_id in Supabase but it did not confirm success for wa_id: {wa_id}")
        except Exception as e:
            logging.error(f"Error clearing previous_response_id in Supabase for wa_id {wa_id}: {e}")
        return

    # Fallback/local-only deletion
    db_path = os.path.join(db_directory, "responses_db")
    try:
        with _responses_db_lock:
            with shelve.open(db_path, writeback=True) as responses_shelf:
                if wa_id in responses_shelf:
                    del responses_shelf[wa_id]
                    logging.info(f"Deleted local response ID entry for wa_id: {wa_id}")
                else:
                    logging.warning(f"Attempted to delete non-existent local response ID entry for wa_id: {wa_id}")
    except Exception as e:
        logging.error(f"Error deleting local response ID entry for wa_id {wa_id}: {e}")

def parse_datetime(appointment_time, timezone="Europe/Madrid"):
    """
    Convierte una cadena de fecha y hora en formato ISO a un objeto datetime ajustado a la zona horaria especificada.
    Si la cadena no incluye información de zona horaria, se asume que es hora local y se le asigna la zona indicada.

    Parámetros:
        appointment_time (str): Cadena en formato ISO, por ejemplo '2025-01-30T14:15:00'.
        timezone (str): Zona horaria a asignar (por defecto "Europe/Madrid").
    
    Retorna:
        datetime: Objeto datetime con la zona horaria asignada.
    """
    tz = ZoneInfo(timezone)
    dt = datetime.fromisoformat(appointment_time)
    # Asumir que la hora del input es local, asignándole la zona Europe/Madrid
    return dt.replace(tzinfo=tz)


def round_up_time(dt, rounding_interval):
    """
    Redondea la fecha y hora 'dt' al siguiente múltiplo de 'rounding_interval' minutos.
    Si rounding_interval es 0 o menor, se utiliza 60 (redondeo a la hora en punto).

    Retorna:
      datetime: Fecha y hora redondeada.
    """
    if rounding_interval <= 0:
        rounding_interval = 60
    remainder = dt.minute % rounding_interval
    if remainder == 0 and dt.second == 0 and dt.microsecond == 0:
        return dt
    delta_minutes = rounding_interval - remainder
    return dt.replace(second=0, microsecond=0) + timedelta(minutes=delta_minutes)


def on_schedule(business_hours, start_dt, end_dt):
    """Verifica que el intervalo esté dentro del horario de negocio."""
    if start_dt.weekday() not in business_hours:
        return False
    day_schedule = business_hours[start_dt.weekday()]
    return (start_dt.time() >= day_schedule['start'] and 
            end_dt.time() <= day_schedule['end'] and 
            start_dt.date() == end_dt.date())


def detect_language_name(text):
    """
    Detecta el idioma de un texto y devuelve el nombre completo del idioma.
    
    Args:
        text (str): Texto a analizar.
    
    Returns:
        str: Nombre del idioma (por ejemplo, "Spanish") o "Idioma desconocido" si no se detecta.
    """
    language_code = detect(text)
    language = pycountry.languages.get(alpha_2=language_code)
    return language.name if language else "Idioma desconocido"

def load_business_hours(business_hours_str):
    """
    Carga la variable de entorno BUSINESS_HOURS y la convierte en un diccionario.
    
    Se espera que BUSINESS_HOURS en el .env sea una cadena JSON con el siguiente formato:
    {"0": [{"start": "09:30", "end": "11:00", "slots": 1},
           {"start": "11:00", "end": "13:30", "slots": 2},
           {"start": "13:30", "end": "21:30", "slots": 3}],
     "1": [{"start": "09:30", "end": "21:30", "slots": 3}],
     ...}
    
    Args:
        business_hours_str (str): Cadena en formato JSON con los horarios y slots de negocio.
    
    Returns:
        dict: Diccionario con claves numéricas (0, 1, ...) y valores que son listas de diccionarios.
              Cada diccionario en la lista contiene las claves "start" y "end" convertidas a objetos datetime.time,
              y la clave "slots" tal como se defina en la cadena JSON.
              Devuelve un diccionario vacío si ocurre algún error.
    """
    try:
        business_hours_json = json.loads(business_hours_str)
        business_hours = {}
        for day, intervals in business_hours_json.items():
            day_intervals = []
            for interval in intervals:
                day_intervals.append({
                    "start": dt_time.fromisoformat(interval["start"]),
                    "end": dt_time.fromisoformat(interval["end"]),
                    "slots": interval.get("slots")
                })
            business_hours[int(day)] = day_intervals
        return business_hours
    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"Error al cargar BUSINESS_HOURS: {e}")
        return {}
    

def load_holidays(holidays_str):
    """
    Carga la variable de entorno HOLIDAYS_2025_MADRID y la convierte en una lista de objetos date.

    Se espera que HOLIDAYS_2025_MADRID en el .env sea una cadena JSON con el siguiente formato:
    ["2025-01-01", "2025-01-06", "2025-04-17", ...]
    
    Args:
        holidays_str (str): Cadena en formato JSON con las fechas (en formato "YYYY-MM-DD") de los festivos.

    Returns:
        list: Lista de objetos date correspondientes a los festivos. Devuelve una lista vacía en caso de error.
    """
    try:
        holidays_json = json.loads(holidays_str)
        holidays = [date.fromisoformat(dt_str) for dt_str in holidays_json]
        return holidays
    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"Error al cargar HOLIDAYS: {e}")
        return []

def read_instructions(env_name: str) -> str:
    """
    Lee el contenido del archivo de instrucciones basado en el entorno.

    Args:
        env_name (str): Nombre del entorno (por ejemplo, "local", "staging", "production").

    Returns:
        str: Contenido del archivo de instrucciones.
    """
    try:
        instructions_folder = "instructions"
        instructions_filename = f"{env_name}.txt"
        instructions_path = os.path.join(instructions_folder, instructions_filename)
        with open(instructions_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        logging.error(f"Instructions file not found for environment: {env_name}")
        raise
    except Exception as e:
        logging.error(f"Error reading instructions: {e}")
        raise


def read_functions(env_name: str) -> List[Dict[str, Any]]:
    """
    Lee el archivo JSON central que contiene la definición de todas las funciones disponibles
    y filtra las funciones basándose en la variable de entorno OPENAI_FUNCTIONS.

    Args:
        env_name (str): Nombre del entorno (aunque no se usa directamente para el path,
                      se mantiene por consistencia y posible uso futuro).

    Returns:
        list: Lista de funciones seleccionadas para el entorno actual.
    """
    try:
        functions_folder = "instructions"
        functions_filename = "functions.json"  # Usar el archivo centralizado
        functions_path = os.path.join(functions_folder, functions_filename)
        with open(functions_path, 'r', encoding='utf-8') as file:
            all_functions = json.load(file)

        # Leer la variable de entorno para saber qué funciones usar
        enabled_functions_str = os.getenv("OPENAI_FUNCTIONS", "")
        if not enabled_functions_str:
            # logging.warning(f"OPENAI_FUNCTIONS environment variable is not set for {env_name}. No functions will be loaded.")
            return []
        
        enabled_function_names = {name.strip() for name in enabled_functions_str.split(',')}

        # Filtrar las funciones
        selected_functions = [
            func for func in all_functions 
            if func.get("name") in enabled_function_names
        ]

        # Verificar si todas las funciones especificadas se encontraron
        found_names = {func.get("name") for func in selected_functions}
        missing_names = enabled_function_names - found_names
        if missing_names:
            logging.warning(f"The following functions specified in OPENAI_FUNCTIONS were not found in {functions_filename}: {missing_names}")

        return selected_functions

    except FileNotFoundError:
        # Build the same path string used above for accurate logging
        functions_path = os.path.join("instructions", "functions.json")
        logging.error(f"Central functions file not found: {functions_path}")
        raise


def read_sector_definitions() -> str:
    """
    Lee el contenido del archivo de definiciones de sectores.
    """
    try:
        instructions_folder = "instructions"
        instructions_filename = "sector_definitions.txt"
        instructions_path = os.path.join(instructions_folder, instructions_filename)
        with open(instructions_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        logging.error(f"Sector definitions file not found: {instructions_path}")
        return "" # Return empty string as fallback
    except Exception as e:
        logging.error(f"Error reading sector definitions: {e}")
        return ""


# -------------------- Fast message routing helpers --------------------
_fast_rules_cache: Dict[str, Dict[str, Any]] = {}

def _fast_rules_path(env_name: str) -> str:
    """
    Resolve the fast messages rules file path for a given environment.

    Expected file name pattern inside `instructions/`:
    - "{env_name}_fast_messages.json" (e.g., "transfersandexperiences_fast_messages.json")
    """
    folder = "instructions"
    filename = f"{env_name}_fast_messages.json"
    return os.path.join(folder, filename)


def read_fast_message_rules(env_name: str) -> Dict[str, Any] | None:
    """
    Read fast-message routing rules for the given environment.

    Returns dict with keys: { "fast_model": str, "patterns": { "greetings": [...], "email": [...], "name_like": [...] } }
    Returns None if file not found or invalid.
    """
    try:
        global _fast_rules_cache
        if env_name in _fast_rules_cache:
            return _fast_rules_cache[env_name]
        path = _fast_rules_path(env_name)
        if not os.path.exists(path):
            logging.warning(f"Fast message rules file not found: {path}")
            _fast_rules_cache[env_name] = None
            return None
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Basic shape validation
            if not isinstance(data, dict) or "patterns" not in data:
                logging.error(f"Invalid fast rules format in {path}")
                _fast_rules_cache[env_name] = None
                return None
            _fast_rules_cache[env_name] = data
            return data
    except Exception as e:
        logging.error(f"Error reading fast message rules for {env_name}: {e}")
        _fast_rules_cache[env_name] = None
        return None


def get_fast_model_name(env_name: str, fallback: str = "gpt-4o-mini") -> str:
    """
    Resolve the fast model name with the following precedence:
      1) Environment variable OPENAI_FAST_MODEL_NAME (if set and non-empty)
      2) Fast rules file for the env (instructions/{env}_fast_messages.json -> fast_model)
      3) Provided fallback (defaults to "gpt-4o-mini")
    """
    try:
        env_override = (os.getenv("OPENAI_FAST_MODEL_NAME") or "").strip()
        if env_override:
            return env_override
    except Exception:
        pass

    rules = read_fast_message_rules(env_name)
    if rules and isinstance(rules.get("fast_model"), str) and rules.get("fast_model").strip():
        return rules.get("fast_model").strip()
    return fallback


def is_fast_message(text: str, env_name: str) -> bool:
    """
    Return True if the message should be routed to the fast model, based on
    greetings, email addresses, or "name-like" content as defined in rules.
    """
    if not text or not isinstance(text, str):
        return False
    rules = read_fast_message_rules(env_name)
    if not rules:
        return False
    patterns = rules.get("patterns", {}) or {}
    try:
        normalized = text.strip()
        # 1) Regex lists: greetings and email
        for key in ("greetings", "email"):
            regex_list = patterns.get(key) or []
            for pattern in regex_list:
                try:
                    if re.search(pattern, normalized, flags=re.IGNORECASE):
                        return True
                except re.error:
                    logging.warning(f"Invalid regex in fast rules '{key}': {pattern}")
                    continue

        # 2) Names list, matched with word boundaries (case-insensitive)
        names_list = [n.strip() for n in (patterns.get("names") or []) if isinstance(n, str) and n.strip()]
        if names_list:
            try:
                escaped_names = [re.escape(n) for n in names_list]
                # Use non-word boundaries to be robust around punctuation and whitespace
                names_regex = r"(?i)(?<!\w)(?:" + "|".join(escaped_names) + r")(?!\w)"
                if re.search(names_regex, normalized):
                    return True
            except re.error as _name_re_err:
                logging.error(f"Invalid names regex compiled from rules: {_name_re_err}")
        
        # 3) Provinces list, matched with word boundaries (case-insensitive)
        provinces_list = [p.strip() for p in (patterns.get("provinces") or []) if isinstance(p, str) and p.strip()]
        if provinces_list:
            try:
                escaped_prov = [re.escape(p) for p in provinces_list]
                provinces_regex = r"(?i)(?<!\w)(?:" + "|".join(escaped_prov) + r")(?!\w)"
                if re.search(provinces_regex, normalized):
                    return True
            except re.error as _prov_re_err:
                logging.error(f"Invalid provinces regex compiled from rules: {_prov_re_err}")
        return False
    except Exception as e:
        logging.error(f"Error evaluating fast message rules: {e}")
        return False


def _calculate_dni_letter(number_str: str) -> str:
    """
    Calcula la letra de control de un DNI/NIF español a partir de 8 dígitos.

    Tabla oficial: TRWAGMYFPDXBNJZSQVHLCKE
    """
    letters = "TRWAGMYFPDXBNJZSQVHLCKE"
    index = int(number_str) % 23
    return letters[index]


def _sum_digits(value: int) -> int:
    return sum(int(c) for c in str(value))


def _validate_cif(value: str) -> bool:
    """
    Valida un CIF español.
    Formato: Letra inicial + 7 dígitos + control (dígito o letra)
    Reglas de control:
      - Letras iniciales que exigen letra de control: K, P, Q, S, N, W
      - Letras iniciales que exigen dígito de control: A, B, E, H
      - Resto admiten ambos tipos de control
    Mapeo de control letra: 0..9 -> JABCDEFGHI
    """
    if len(value) != 9:
        return False
    first = value[0]
    digits = value[1:8]
    control = value[8]

    if first not in "ABCDEFGHJKLMNPQRSUVWK":
        return False
    if not digits.isdigit():
        return False

    even_sum = sum(int(digits[i]) for i in [1, 3, 5])
    odd_transformed_sum = sum(_sum_digits(int(digits[i]) * 2) for i in [0, 2, 4, 6])
    total = even_sum + odd_transformed_sum
    check_digit = (10 - (total % 10)) % 10
    check_letter_map = "JABCDEFGHI"
    expected_digit = str(check_digit)
    expected_letter = check_letter_map[check_digit]

    force_letter = first in "KPQSNW"
    force_digit = first in "ABEH"

    if control.isdigit():
        is_valid_digit = control == expected_digit
        return is_valid_digit and (not force_letter)
    else:
        is_valid_letter = control == expected_letter
        return is_valid_letter and (not force_digit)


def validate_and_normalize_spanish_tax_id(value: str) -> str | None:
    """
    Valida y normaliza un identificador fiscal español (NIF/DNI, NIE o CIF).

    - Elimina prefijo "ES" si existe y separadores (espacios/guiones)
    - Debe quedar con exactamente 9 caracteres
    - Comprueba el dígito/letra de control según normativa

    Retorna la cadena normalizada en mayúsculas si es válida; en caso contrario, None.
    """
    if not value:
        return None

    normalized = value.strip().upper().replace("-", "").replace(" ", "")
    if normalized.startswith("ES") and len(normalized) > 9:
        normalized = normalized[2:]

    if len(normalized) != 9:
        return None

    first = normalized[0]
    last = normalized[-1]

    # Caso DNI/NIF: 8 dígitos + letra
    if first.isdigit():
        number_part = normalized[:8]
        if not number_part.isdigit() or not last.isalpha():
            return None
        expected = _calculate_dni_letter(number_part)
        return normalized if last == expected else None

    # Caso NIE: X/Y/Z + 7 dígitos + letra
    if first in "XYZ":
        nie_number = {"X": "0", "Y": "1", "Z": "2"}[first] + normalized[1:8]
        if not nie_number.isdigit() or not last.isalpha():
            return None
        expected = _calculate_dni_letter(nie_number)
        return normalized if last == expected else None

    # Caso CIF
    if first.isalpha():
        return normalized if _validate_cif(normalized) else None

    return None


def log_sector_activity_choice(wa_id: str, activity_text: str, sector: str) -> None:
    """
    Registra temporalmente en CSV la relación entre la respuesta del usuario a
    "¿A qué se dedica tu empresa?" y el sector elegido por el modelo.

    Crea el archivo `logs/sector_activity_map.csv` si no existe y añade filas con:
    timestamp, wa_id, actividad (texto libre), sector elegido.
    """
    try:
        logs_dir = "logs"
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir, exist_ok=True)
        csv_path = os.path.join(logs_dir, "sector_activity_map.csv")

        # Normalizar valores a strings simples sin saltos de línea para CSV simple
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        wa = str(wa_id or "").replace("\n", " ").replace("\r", " ")
        act = str(activity_text or "").replace("\n", " ").replace("\r", " ")
        sec = str(sector or "").replace("\n", " ").replace("\r", " ")

        file_exists = os.path.exists(csv_path)
        with open(csv_path, "a", encoding="utf-8") as f:
            if not file_exists:
                f.write("timestamp,wa_id,actividad,sector\n")
            # CSV simple con coma; escapamos comas envolviendo con comillas si aparecen
            def _csv_escape(value: str) -> str:
                if "," in value or '"' in value:
                    return '"' + value.replace('"', '""') + '"'
                return value
            f.write(
                f"{_csv_escape(ts)},{_csv_escape(wa)},{_csv_escape(act)},{_csv_escape(sec)}\n"
            )
    except Exception as e:
        # No interrumpir el flujo por logging temporal
        logging.error(f"Error writing sector activity CSV: {e}")