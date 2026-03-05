from flask import current_app
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
from datetime import datetime, date, timedelta
from app.utils.openai_functions import GPTRequest # Import GPTRequest from its actual location
import gspread
import logging
import random
from unidecode import unidecode
import re
import heapq
import os # Added for os.path.join
from app.services.supabase_service import is_supabase_enabled
from app.utils.enrollment_state import get_enrollment_context, update_enrollment_context
import psutil

# Flag para habilitar lectura desde Supabase (tabla courses)
# Se puede desactivar temporalmente si hay problemas
USE_SUPABASE_COURSES = os.getenv('USE_SUPABASE_COURSES', 'true').lower() in ('1', 'true', 'yes')

_gs_client = None
_sheets_service = None
_drive_service = None
_docs_service = None
_google_creds = None

def _get_google_creds():
    """Gets cached Google credentials, initializing them if they don't exist."""
    global _google_creds
    if _google_creds is None:
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials_path = _get_credentials_path()
        _google_creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    return _google_creds

def get_gs_client():
    """Gets a singleton gspread client."""
    global _gs_client
    if _gs_client is None:
        creds = _get_google_creds()
        _gs_client = gspread.authorize(creds)
    return _gs_client

def get_sheets_service():
    """Gets a singleton Google Sheets API service."""
    global _sheets_service
    if _sheets_service is None:
        creds = _get_google_creds()
        _sheets_service = build('sheets', 'v4', credentials=creds)
    return _sheets_service

def get_drive_service():
    """Gets a singleton Google Drive API service."""
    global _drive_service
    if _drive_service is None:
        creds = _get_google_creds()
        _drive_service = build('drive', 'v3', credentials=creds)
    return _drive_service

def get_docs_service():
    """Gets a singleton Google Docs API service."""
    global _docs_service
    if _docs_service is None:
        creds = _get_google_creds()
        _docs_service = build('docs', 'v1', credentials=creds)
    return _docs_service

# --- Transient error handling helpers for Google APIs (gspread/Sheets) ---
def _is_transient_google_error(exc):
    """Return True if the exception looks transient (e.g., 5xx, 429, internalError)."""
    try:
        msg = str(exc or "").lower()

        # googleapiclient HttpError
        if isinstance(exc, HttpError):
            status = getattr(getattr(exc, 'resp', None), 'status', None)
            if status is not None:
                try:
                    status = int(status)
                except Exception:
                    status = None
            if status and (status == 429 or 500 <= status < 600):
                return True
            # Fallback: inspect reason in content
            try:
                content = exc.content.decode('utf-8') if isinstance(exc.content, (bytes, bytearray)) else str(exc.content)
                content_lower = (content or '').lower()
                if 'internal error' in content_lower or 'backendError'.lower() in content_lower:
                    return True
            except Exception:
                pass

        # gspread APIError often wraps a dict with code/message in args[0]
        import gspread
        if isinstance(exc, gspread.exceptions.APIError):
            payload = None
            try:
                payload = exc.response
            except Exception:
                payload = None
            if not payload and exc.args:
                try:
                    payload = exc.args[0]
                except Exception:
                    payload = None
            code = None
            if isinstance(payload, dict):
                code = payload.get('code') or payload.get('status')
                try:
                    code = int(code)
                except Exception:
                    code = None
                message = str(payload.get('message', '')).lower()
                reason = ''
                try:
                    errors = payload.get('errors') or []
                    if errors and isinstance(errors, list):
                        reason = str((errors[0] or {}).get('reason', '')).lower()
                except Exception:
                    pass
                if code and (code == 429 or 500 <= code < 600):
                    return True
                if 'internal error' in message or 'internalerror' in reason:
                    return True
        # Network hiccups etc. can be treated as transient if desired; keep conservative
        # Tratar errores típicos de red/SSL como transitorios (p.ej. "EOF occurred in violation of protocol")
        # sin acoplarse demasiado a tipos concretos (httplib2 / ssl / socket).
        network_transient_snippets = [
            "eof occurred in violation of protocol",
            "connection reset by peer",
            "tlsv1 alert internal error",
            "temporarily unavailable",
            "connection aborted",
            "remote end closed connection",
        ]
        if any(snippet in msg for snippet in network_transient_snippets):
            return True
    except Exception:
        pass
    return False

def _execute_google_call(callable_fn, action_desc: str, *, max_attempts: int = 4, base_backoff: float = 0.5):
    """Execute callable_fn() with exponential backoff on transient Google API errors."""
    last_exc = None
    had_transient_error = False
    for attempt in range(1, max_attempts + 1):
        try:
            result = callable_fn()
            if had_transient_error:
                logging.info(f"✅ Recuperado de error transitorio de Google API durante {action_desc} en el intento {attempt}.")
            return result
        except Exception as e:  # noqa: BLE001 - we classify below
            last_exc = e
            if _is_transient_google_error(e) and attempt < max_attempts:
                had_transient_error = True
                delay = min(4.0, base_backoff * (2 ** (attempt - 1))) + random.uniform(0, 0.25)
                logging.warning(f"Transient Google API error during {action_desc}: {e}. Retrying in {delay:.2f}s (attempt {attempt}/{max_attempts})...")
                time.sleep(delay)
                continue
            # Not transient or out of attempts → re-raise
            raise
    # Should not reach; re-raise last exception defensively
    if last_exc:
        raise last_exc
    return None

# Regular expression to extract URL from HYPERLINK formula
HYPERLINK_REGEX = re.compile(r'^=HYPERLINK\("([^"]+)".*\)', re.IGNORECASE)

SPECIAL_SHEETS = [
    # 'MICROCRÉDITOS CORRECTO',  # Hoja en desuso: se evita leerla explícitamente
    'OFERTA ESTATAL 24',
    # 'CLOUD COMPUTING',         # Hoja en desuso: se evita leerla explícitamente
    'EOI',
]

# Hojas regionales conocidas (para búsqueda sin restricción de origen)
REGIONAL_SHEETS = [
    'VALENCIA',
    'ANDALUCIA', 
    'MADRID',
    'MURCIA',
]

# Todas las hojas disponibles para búsqueda libre
ALL_SHEETS = REGIONAL_SHEETS + SPECIAL_SHEETS


# URL shortening removed per user request to reduce memory/HTTP overhead
try:
    import ctypes, os, gc as _gc
    def _malloc_trim():
        try:
            if os.name == 'posix':
                ctypes.CDLL('libc.so.6').malloc_trim(0)
        except Exception:
            pass
    def _free_mem_hint():
        try:
            _gc.collect()
            _malloc_trim()
        except Exception:
            pass
except Exception:
    def _free_mem_hint():
        pass

def _normalize_text(value):
    return unidecode(str(value or '').strip().lower())

def _parse_iso_datetime_safe(value):
    """
    Parsea timestamps ISO de forma tolerante (Z, fracciones variables, offset con/sin ':').
    Retorna datetime o None si no se puede parsear.
    """
    raw = str(value or '').strip()
    if not raw:
        return None

    candidates = [raw]
    # Caso común: sufijo Z
    if raw.endswith('Z'):
        candidates.append(raw[:-1] + '+00:00')
    # Offset sin dos puntos (+0100 / -0500) -> +01:00 / -05:00
    m = re.search(r'([+-]\d{2})(\d{2})$', raw)
    if m:
        candidates.append(raw[:-5] + f"{m.group(1)}:{m.group(2)}")

    for cand in candidates:
        try:
            return datetime.fromisoformat(cand)
        except Exception:
            pass

    # Fallback: normalizar precisión de microsegundos a 6 dígitos
    # p.ej. .52535 -> .525350 o .123456789 -> .123456
    try:
        base = raw[:-1] + '+00:00' if raw.endswith('Z') else raw
        m_frac = re.search(r'\.(\d+)(?=(?:[+-]\d{2}:?\d{2})$)', base)
        if m_frac:
            frac = m_frac.group(1)
            frac6 = (frac + '000000')[:6]
            normalized = base.replace(f".{frac}", f".{frac6}", 1)
            return datetime.fromisoformat(normalized)
    except Exception:
        pass

    return None

def _col_index_to_a1(zero_based_index):
    """Convierte un índice de columna (0-based) a letra(s) estilo A1 (A, B, ... AA)."""
    try:
        col = int(zero_based_index) + 1
    except Exception:
        col = 1
    letters = ''
    while col > 0:
        col, remainder = divmod(col - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters

# Provincias españolas a su Comunidad Autónoma (normalizadas)
PROVINCE_TO_COMMUNITY = {
    # Andalucía
    'almeria': 'andalucia', 'cadiz': 'andalucia', 'cordoba': 'andalucia', 'granada': 'andalucia',
    'huelva': 'andalucia', 'jaen': 'andalucia', 'malaga': 'andalucia', 'sevilla': 'andalucia',
    # Aragón
    'huesca': 'aragon', 'teruel': 'aragon', 'zaragoza': 'aragon',
    # Principado de Asturias
    'asturias': 'asturias',
    # Illes Balears
    'illes balears': 'baleares', 'islas baleares': 'baleares', 'baleares': 'baleares',
    # Canarias
    'las palmas': 'canarias', 'santa cruz de tenerife': 'canarias',
    # Cantabria
    'cantabria': 'cantabria',
    # Castilla-La Mancha
    'albacete': 'castilla la mancha', 'ciudad real': 'castilla la mancha', 'cuenca': 'castilla la mancha',
    'guadalajara': 'castilla la mancha', 'toledo': 'castilla la mancha',
    # Castilla y León
    'avila': 'castilla y leon', 'ávila': 'castilla y leon', 'burgos': 'castilla y leon', 'leon': 'castilla y leon',
    'palencia': 'castilla y leon', 'salamanca': 'castilla y leon', 'segovia': 'castilla y leon',
    'soria': 'castilla y leon', 'valladolid': 'castilla y leon', 'zamora': 'castilla y leon',
    # Cataluña
    'barcelona': 'cataluna', 'girona': 'cataluna', 'gerona': 'cataluna', 'lleida': 'cataluna', 'lerida': 'cataluna',
    'tarragona': 'cataluna',
    # Comunitat Valenciana
    'alicante': 'comunidad valenciana', 'castellon': 'comunidad valenciana', 'castellón': 'comunidad valenciana',
    'valencia': 'comunidad valenciana',
    # Extremadura
    'badajoz': 'extremadura', 'caceres': 'extremadura', 'cáceres': 'extremadura',
    # Galicia
    'a coruna': 'galicia', 'a coruña': 'galicia', 'la coruna': 'galicia', 'la coruña': 'galicia', 'coruna': 'galicia', 'coruña': 'galicia',
    'lugo': 'galicia', 'ourense': 'galicia', 'orense': 'galicia', 'pontevedra': 'galicia',
    # La Rioja
    'la rioja': 'la rioja', 'rioja': 'la rioja',
    # Comunidad de Madrid
    'madrid': 'madrid',
    # Región de Murcia
    'murcia': 'murcia',
    # Comunidad Foral de Navarra
    'navarra': 'navarra',
    # País Vasco
    'alava': 'pais vasco', 'álava': 'pais vasco', 'araba': 'pais vasco', 'bizkaia': 'pais vasco', 'vizcaya': 'pais vasco',
    'gipuzkoa': 'pais vasco', 'guipuzcoa': 'pais vasco', 'guipúzcoa': 'pais vasco',
    # Ceuta y Melilla
    'ceuta': 'ceuta', 'melilla': 'melilla'
}

COMMUNITIES = {
    'andalucia', 'aragon', 'asturias', 'baleares', 'canarias', 'cantabria', 'castilla la mancha',
    'castilla y leon', 'cataluna', 'comunidad valenciana', 'extremadura', 'galicia', 'la rioja',
    'madrid', 'murcia', 'navarra', 'pais vasco', 'ceuta', 'melilla'
}

# Palabras demasiado genéricas para matching de sector
_SECTOR_STOP_TOKENS = {
    'servicios', 'servicio', 'sector', 'profesional', 'profesionales', 'industria', 'industriales',
    'gran', 'general', 'otros', 'varios', 'y', 'e', 'de', 'del', 'la', 'el', 'las', 'los', 'en'
}

def _normalize_for_match(text):
    try:
        s = unidecode(str(text or '')).lower()
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s
    except Exception:
        return str(text or '').strip().lower()

def _is_course_blocked_for_recommendation(course_name):
    """
    Regla de exclusión para cursos que no deben recomendarse.
    """
    name_norm = _normalize_for_match(course_name)
    return 'prueba de nivel' in name_norm

def _sector_matches(chosen_sector_norm, sheet_sector_norm):
    """
    Matching robusto de sector evitando falsos positivos por tokens genéricos.
    Respeta casos de coincidencia parcial en ambas direcciones.
    """
    try:
        if not chosen_sector_norm or chosen_sector_norm == 'n/a':
            return True
        s_sheet = _normalize_for_match(sheet_sector_norm)
        if not s_sheet or s_sheet == 'intersectorial':
            return True
        s_chosen = _normalize_for_match(chosen_sector_norm)

        # Coincidencia directa por substring en ambas direcciones
        if s_chosen and (s_chosen in s_sheet or s_sheet in s_chosen):
            return True

        # Coincidencia por tokens significativos
        chosen_tokens = [t for t in s_chosen.split() if len(t) >= 4 and t not in _SECTOR_STOP_TOKENS]
        if not chosen_tokens:
            return False

        # Si hay 2+ tokens significativos, basta con que alguno relevante aparezca en el texto de la hoja
        for tok in chosen_tokens:
            if tok in s_sheet:
                return True
            # Tolerar singular/plural simples (p.ej., medioambiental/medioambientales)
            if len(tok) >= 7:
                if tok.endswith('es') and tok[:-2] in s_sheet:
                    return True
                if tok.endswith('s') and tok[:-1] in s_sheet:
                    return True
        return False
    except Exception:
        return False

# Aliases y abreviaturas comunes para Comunidades Autónomas (normalizadas)
ALIAS_TO_COMMUNITY = {
    # Comunitat Valenciana / C.Valenciana
    'comunitat valenciana': 'comunidad valenciana',
    'c valenciana': 'comunidad valenciana',
    'c valenc': 'comunidad valenciana',
    'c valencian': 'comunidad valenciana',
    'cvalenciana': 'comunidad valenciana',
    'cvalenc': 'comunidad valenciana',
    'c. valenciana': 'comunidad valenciana',
    'c.valenciana': 'comunidad valenciana',
    'c-valenciana': 'comunidad valenciana',

    # Castilla y León / C-L / CYL
    'c-l': 'castilla y leon',
    'c l': 'castilla y leon',
    'cyl': 'castilla y leon',
    'c y l': 'castilla y leon',
    'c-y-l': 'castilla y leon',

    # Castilla-La Mancha (variantes con guion)
    'castilla-la mancha': 'castilla la mancha',
    'castilla la-mancha': 'castilla la mancha',
    'castilla l a mancha': 'castilla la mancha',

    # País Vasco (variantes con guion o abreviación simple)
    'pais-vasco': 'pais vasco',
    'p vasco': 'pais vasco',
    'p. vasco': 'pais vasco',
}

def _province_to_community(user_province_norm):
    if not user_province_norm:
        return None
    # If already a community name, return itself
    if user_province_norm in COMMUNITIES:
        return user_province_norm
    return PROVINCE_TO_COMMUNITY.get(user_province_norm)

def _map_province_to_sheet(province):
    p = _normalize_text(province)
    # Comunidad Valenciana: provincias de Valencia, Alicante o Castellón
    if ('valencia' in p) or ('alicante' in p) or ('castellon' in p):
        return 'VALENCIA'
    if 'malaga' in p:
        return 'ANDALUCIA'
    if 'madrid' in p:
        return 'MADRID'
    if 'murcia' in p:
        return 'MURCIA'
    return None

def _map_user_situation(user_situation):
    s = _normalize_text(user_situation)
    if 'autonom' in s:
        return 'autonomo'
    if 'ocup' in s:
        return 'ocupado'
    if 'desemple' in s:
        return 'desempleado'
    return s

def _is_eoi_allowed_for_origin(origen):
    """Devuelve True si el origen pertenece a Andalucía (cualquier provincia), Ceuta o Melilla."""
    o = _normalize_text(origen)
    if not o:
        return False
    # Coincidencia directa por comunidad o ciudades autónomas
    if ('andalucia' in o) or ('ceuta' in o) or ('melilla' in o):
        return True
    # Coincidencia por provincias que pertenecen a Andalucía
    try:
        for prov, comm in PROVINCE_TO_COMMUNITY.items():
            if comm == 'andalucia' and prov in o:
                return True
    except Exception:
        pass
    return False

def _sheet_situation_allows_user(sheet_value, user_bucket, user_province):
    sv = _normalize_text(sheet_value)
    normalized_user_bucket = _map_user_situation(user_bucket)
    if normalized_user_bucket not in {'autonomo', 'ocupado', 'desempleado'}:
        normalized_user_bucket = _normalize_text(user_bucket)

    # 1) Evaluación base por bucket (autónomo/ocupado/desempleado)
    effective_bucket = normalized_user_bucket
    if not sv:
        # Si no se especifica, tratar como válido para ocupados; autónomos también pasan por la regla general
        if normalized_user_bucket == 'autonomo':
            base_ok = True
            effective_bucket = 'ocupado'
        else:
            base_ok = normalized_user_bucket in {'ocupado', 'desempleado'}
    else:
        if '(o/d)' in sv or ('ocup' in sv and 'desemple' in sv and 'autonom' not in sv):
            if normalized_user_bucket == 'autonomo':
                base_ok = True
                effective_bucket = 'ocupado'
            else:
                base_ok = normalized_user_bucket in {'ocupado', 'desempleado'}
        else:
            allows_autonomo = 'autonom' in sv or '(a)' in sv
            allows_ocupado = '(o)' in sv or 'ocup' in sv or ' os ' in f' {sv} '
            allows_desempleado = '(d)' in sv or 'desemple' in sv
            if normalized_user_bucket == 'autonomo':
                # Permitir a autónomos cursos marcados como "ocupados".
                base_ok = allows_autonomo or allows_ocupado
                # Si entra por la vía de "ocupados", usar bucket efectivo "ocupado" para geo-restricciones.
                if not allows_autonomo and allows_ocupado:
                    effective_bucket = 'ocupado'
            elif normalized_user_bucket == 'ocupado':
                base_ok = allows_ocupado
            elif normalized_user_bucket == 'desempleado':
                base_ok = allows_desempleado
            else:
                base_ok = normalized_user_bucket in sv

    if not base_ok:
        return False

    # 2) Geo-restricciones específicas por bucket
    def collect_regions(text):
        # Normalizar texto y también crear una versión sin puntuación común para alias
        text_norm = _normalize_text(text)
        simplified = re.sub(r"[\.,\-/()]+", " ", text_norm)
        simplified = re.sub(r"\s+", " ", simplified).strip()

        places = set()
        # Coincidencias directas de comunidades
        for comm in COMMUNITIES:
            if comm in text_norm or comm in simplified:
                places.add(comm)
        # Aliases de comunidades (abreviaturas)
        for alias, canonical in ALIAS_TO_COMMUNITY.items():
            if alias in text_norm or alias in simplified:
                places.add(canonical)
        # Provincias que mapean a comunidades
        for prov in PROVINCE_TO_COMMUNITY.keys():
            if prov in text_norm or prov in simplified:
                places.add(prov)
        return places

    # Buscar patrones "<bucket> de|en <regiones>" y cortar cuando empiece otro bucket
    # Soporta separadores: nada, "y", "o" o coma antes del siguiente bucket; y punto/;\n/fin
    pattern_next_bucket = r"(?:autonom\w*|ocup\w*|desemple\w*)\b"
    lookahead_sep = rf"(?=(?:\s+(?:y\s+|o\s+)?{pattern_next_bucket}|,\s*{pattern_next_bucket}|[.;\n]|$))"
    bucket_preposition = r"(?:de\s+la|de\s+las|de\s+los|del|de|en)"
    matches_aut = re.findall(rf"autonom\w*\s*{bucket_preposition}\s+([^.;\n]+?){lookahead_sep}", sv)
    matches_ocu = re.findall(rf"ocup\w*\s*{bucket_preposition}\s+([^.;\n]+?){lookahead_sep}", sv)
    matches_des = re.findall(rf"desemple\w*\s*{bucket_preposition}\s+([^.;\n]+?){lookahead_sep}", sv)

    aut_places = set().union(*(collect_regions(m) for m in matches_aut)) if matches_aut else set()
    ocu_places = set().union(*(collect_regions(m) for m in matches_ocu)) if matches_ocu else set()
    des_places = set().union(*(collect_regions(m) for m in matches_des)) if matches_des else set()

    # Soportar expresiones agrupadas tipo:
    # "autónomos y desempleados de Andalucía"
    # En ese caso, la geo-restricción aplica a todos los buckets mencionados en el grupo.
    grouped_bucket_pattern = rf"((?:autonom\w*|ocup\w*|desemple\w*)(?:\s*(?:,|y|o)\s*(?:autonom\w*|ocup\w*|desemple\w*))*)\s*{bucket_preposition}\s+([^.;\n]+?){lookahead_sep}"
    grouped_matches = re.findall(grouped_bucket_pattern, sv)
    for bucket_group, places_text in grouped_matches:
        group_places = collect_regions(places_text)
        if not group_places:
            continue
        if re.search(r"autonom\w*", bucket_group):
            aut_places.update(group_places)
        if re.search(r"ocup\w*", bucket_group):
            ocu_places.update(group_places)
        if re.search(r"desemple\w*", bucket_group):
            des_places.update(group_places)

    found_any_specific = bool(aut_places or ocu_places or des_places)

    # 3) Geo-restricciones genéricas (si no hay específicas por bucket)
    generic_places = set()
    if not found_any_specific:
        generic_places = collect_regions(sv)

    user_prov_norm = _normalize_text(user_province)
    user_comm_norm = _province_to_community(user_prov_norm)

    def check_geo(places):
        if not places:
            return True
        return (user_prov_norm in places) or (user_comm_norm in places)

    # Lógica final:
    # - Si hay restricciones específicas y existen para el bucket actual, se aplican.
    # - Si hay restricciones específicas pero no para este bucket, no se aplica restricción geográfica a este bucket.
    # - Si no hay específicas y hay genéricas, se aplican a todos.
    # Para usuarios autónomos, permitir por cualquiera de las dos vías de restricción específicas:
    # - Si hay "autónomos de ..." y/o "ocupados de ...", aceptar si alguna coincide
    if normalized_user_bucket == 'autonomo' and (aut_places or ocu_places):
        return (check_geo(aut_places) if aut_places else False) or (check_geo(ocu_places) if ocu_places else False)
    if effective_bucket == 'autonomo' and aut_places:
        return check_geo(aut_places)
    if effective_bucket == 'ocupado' and ocu_places:
        return check_geo(ocu_places)
    if effective_bucket == 'desempleado' and des_places:
        return check_geo(des_places)

    if found_any_specific:
        # Había restricciones, pero no para este bucket concreto
        return True

    # Aplicar genéricas si existen
    return check_geo(generic_places)

def _get_credentials_path():
    """Construye la ruta al archivo de credenciales basado en el entorno."""
    env_name = current_app.config.get("ENV_NAME")
    filename = f"client_secret_{env_name}.json" if env_name else "client_secret.json"
    return os.path.join('config', filename)

def insert_question(question):
    """
    Inserta una pregunta en una hoja de Google Sheets.
    
    Args:
        question (str): La pregunta a registrar.
    """
    gs_client = get_gs_client()
    def _open_preguntas():
        return gs_client.open('Preguntas')
    spreadsheet = _execute_google_call(_open_preguntas, "open 'Preguntas'")
    sheet = spreadsheet.sheet1
    def _count_rows():
        return len(sheet.get_all_values()) + 1
    row = _execute_google_call(_count_rows, "count rows in 'Preguntas'")
    def _insert_row():
        return sheet.insert_row([question], row)
    _execute_google_call(_insert_row, "insert row into 'Preguntas'")


def _filter_courses_by_theme_with_ai(courses, tematica, key_curso='curso'):
    """
    Filtra cursos usando IA para determinar cuáles coinciden con la temática solicitada.
    
    Args:
        courses (list): Lista de diccionarios con los cursos ya filtrados
        tematica (str): Temática de interés del usuario (ej: "idiomas", "gestión de riesgos")
        key_curso (str): Clave del diccionario que contiene el nombre del curso
        
    Returns:
        list: Lista de cursos filtrados que coinciden con la temática
    """
    import json
    import os
    
    if not courses:
        logging.info("📝 No hay cursos para filtrar por temática")
        return []
    
    # Extraer solo los nombres de los cursos para enviar a la IA
    course_names = []
    for idx, course in enumerate(courses):
        nombre = course.get(key_curso, '').strip()
        if nombre:
            course_names.append({"index": idx, "nombre": nombre})
    
    if not course_names:
        logging.warning("⚠️ No se encontraron nombres de cursos para filtrar")
        return courses
    
    logging.info(f"📤 Enviando {len(course_names)} nombres de cursos a la IA para filtrar por temática: '{tematica}'")
    
    # Preparar el prompt para la IA
    prompt = f"""Eres un asistente experto en clasificación de cursos de formación. 

Te voy a proporcionar una lista de nombres de cursos y una temática de interés. Tu tarea es identificar qué cursos están relacionados con esa temática.

TEMÁTICA DE INTERÉS: {tematica}

LISTA DE CURSOS:
{json.dumps(course_names, ensure_ascii=False, indent=2)}

INSTRUCCIONES:
1. Analiza cada nombre de curso y determina si está relacionado con la temática "{tematica}"
2. Sé flexible en la interpretación: considera sinónimos, temas relacionados y contextos similares
3. Devuelve ÚNICAMENTE un array JSON con los índices de los cursos que SÍ están relacionados con la temática
4. El formato debe ser exactamente: {{"indices": [0, 2, 5, ...]}}
5. Si ningún curso coincide, devuelve: {{"indices": []}}
6. NO incluyas explicaciones adicionales, SOLO el JSON

RESPUESTA (solo JSON):"""
    
    try:
        # Llamar a la API de OpenAI
        logging.info("🔄 Llamando a la API de ChatGPT para filtrar cursos...")
        from openai import OpenAI
        
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logging.error("❌ No se encontró OPENAI_API_KEY en las variables de entorno")
            return courses
        
        client = OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un asistente experto en clasificación de cursos. Respondes ÚNICAMENTE con JSON válido."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=1000
        )
        
        ai_response = response.choices[0].message.content.strip()
        logging.info(f"📥 Respuesta de la IA recibida: {ai_response[:200]}...")
        
        # Parsear la respuesta JSON
        try:
            result = json.loads(ai_response)
            matching_indices = result.get('indices', [])
        except json.JSONDecodeError as e:
            logging.error(f"❌ Error al parsear JSON de la IA: {e}")
            logging.error(f"Respuesta completa: {ai_response}")
            # Intentar extraer el JSON si está dentro de markdown code blocks
            if '```' in ai_response:
                import re
                json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', ai_response, re.DOTALL)
                if json_match:
                    try:
                        result = json.loads(json_match.group(1))
                        matching_indices = result.get('indices', [])
                        logging.info("✅ JSON extraído exitosamente de markdown code block")
                    except:
                        logging.error("❌ No se pudo extraer JSON válido")
                        return courses
                else:
                    return courses
            else:
                return courses
        
        logging.info(f"✅ IA identificó {len(matching_indices)} cursos relacionados con la temática '{tematica}'")
        
        # Filtrar los cursos usando los índices devueltos por la IA
        filtered_courses = []
        for idx in matching_indices:
            if 0 <= idx < len(courses):
                filtered_courses.append(courses[idx])
                nombre_curso = courses[idx].get(key_curso, 'Sin nombre')
                logging.info(f"  ✓ Curso #{idx}: {nombre_curso}")
        
        if not filtered_courses:
            logging.warning(f"⚠️ No se encontraron cursos relacionados con '{tematica}'.")
            return []
        
        return filtered_courses
        
    except Exception as e:
        logging.error(f"❌ Error inesperado al filtrar cursos con IA: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return courses


_sync_stale_email_sent = False  # Flag para evitar spam de emails

def _get_courses_from_supabase(
    origen, situacion_laboral, nivel_formacion, 
    pagina=1, page_size=5, sector="N/A", modalidad="N/A", 
    tematica="N/A", codigo="N/A", formacion="N/A", dashboard_strict: bool = False
):
    """
    Lee cursos desde Supabase y aplica los mismos filtros que get_and_filter_courses.
    Retorna None si no hay cursos en Supabase o si ocurre un error.
    """
    global _sync_stale_email_sent
    
    try:
        from app.services.supabase_client.courses import fetch_all_courses_for_filtering, count_courses, get_last_sync_time
        
        # Verificar si hay cursos en Supabase
        total = count_courses()
        if total == 0:
            logging.info("📊 No hay cursos en Supabase, usando Google Sheets...")
            return None
        
        # Verificar frescura de datos (máximo 2 horas)
        max_stale_hours = float(os.getenv('SUPABASE_MAX_STALE_HOURS', '2'))
        last_sync = get_last_sync_time()
        if last_sync:
            sync_time = _parse_iso_datetime_safe(last_sync)
            if sync_time:
                now = datetime.now(sync_time.tzinfo) if sync_time.tzinfo else datetime.now()
                hours_since_sync = (now - sync_time).total_seconds() / 3600
                
                if hours_since_sync > max_stale_hours:
                    logging.warning(f"⚠️ Datos de Supabase desactualizados ({hours_since_sync:.1f}h > {max_stale_hours}h)")
                    
                    # Enviar email de alerta (solo una vez por sesión)
                    if not _sync_stale_email_sent:
                        try:
                            from app.services.email_service import send_notification_email
                            alert_email = os.getenv('SYNC_ALERT_EMAIL', 'nachocmrl@gmail.com')
                            send_notification_email(
                                subject="⚠️ Alerta: Sincronización de cursos detenida",
                                body=f"""
Hola,

El sistema ha detectado que la sincronización de cursos desde Google Sheets a Supabase lleva más de {max_stale_hours} horas sin actualizarse.

Última sincronización: {last_sync}
Horas transcurridas: {hours_since_sync:.1f}

El bot está usando Google Sheets directamente como fallback.

Posibles causas:
- El Apps Script de Google Sheets ha dejado de funcionar
- Hubo un error en el webhook de sincronización
- El servidor no está recibiendo las actualizaciones

Por favor revisa el Apps Script en el Google Sheet.

Saludos,
ByTheBot
                                """,
                                to_addr=alert_email
                            )
                            logging.info(f"📧 Email de alerta enviado a {alert_email}")
                            _sync_stale_email_sent = True
                        except Exception as email_err:
                            logging.error(f"Error enviando email de alerta: {email_err}")
                    
                    return None  # Usar fallback a Google Sheets
                else:
                    # Reset flag si los datos están frescos
                    _sync_stale_email_sent = False
            else:
                # No contaminar la UI con un warning no crítico.
                logging.debug(f"No se pudo parsear synced_at='{last_sync}'")
        
        logging.info(f"📊 Leyendo {total} cursos desde Supabase...")
        start_time = time.time()
        
        all_courses = fetch_all_courses_for_filtering()
        
        if not all_courses:
            logging.warning("⚠️ No se pudieron obtener cursos de Supabase")
            return None
        
        read_time = time.time() - start_time
        logging.info(f"⏱️ Tiempo de lectura desde Supabase: {read_time:.2f} s")
        
        # Normalizar criterios de búsqueda
        origen_norm = unidecode(str(origen or '').lower())
        situacion_laboral_norm = unidecode(str(situacion_laboral or '').lower())
        nivel_formacion_norm = unidecode(str(nivel_formacion or '').lower())
        sector_lower = sector.lower() if isinstance(sector, str) else ''
        sector_norm = unidecode(sector_lower) if sector_lower and sector_lower != 'n/a' else None
        modalidad_lower = modalidad.lower() if isinstance(modalidad, str) else ''
        modalidad_norm = unidecode(modalidad_lower) if modalidad_lower and modalidad_lower != 'n/a' else None
        codigo_lower = codigo.lower() if isinstance(codigo, str) else ''
        codigo_norm = re.sub(r'[^a-z0-9]', '', unidecode(codigo_lower)) if codigo_lower and codigo_lower != 'n/a' else None
        apply_sector_filter = (situacion_laboral_norm == 'ocupado' and sector_norm is not None)
        
        # Normalizar formación del usuario
        formacion_lower = formacion.lower() if isinstance(formacion, str) else ''
        formacion_norm = formacion_lower if formacion_lower and formacion_lower != 'n/a' else None
        
        # Extraer nivel numérico del usuario
        user_level_match = re.search(r'(\d+)', nivel_formacion_norm)
        user_level = int(user_level_match.group(1)) if user_level_match else None
        
        # Determinar hojas válidas para el origen
        valid_sheets = set()
        origen_is_na = origen_norm in ('n/a', 'na', '')
        
        if origen_is_na:
            # Búsqueda libre: incluir todas las hojas
            valid_sheets = set(ALL_SHEETS)
            logging.info("🔍 Búsqueda libre (sin filtro de origen): leyendo todas las hojas")
        else:
            mapped = _map_province_to_sheet(origen)
            if mapped:
                valid_sheets.add(mapped)
            
            eoi_allowed = _is_eoi_allowed_for_origin(origen)
            for sheet_name in SPECIAL_SHEETS:
                if sheet_name == 'EOI' and not eoi_allowed:
                    continue
                valid_sheets.add(sheet_name)
        
        # Convertir cursos de formato Supabase a formato compatible
        KEY_CURSO = 'curso'
        KEY_PP = 'pp'
        KEY_PC = 'pc'
        
        filtered_courses = []
        
        for course in all_courses:
            # Helper para manejar None -> ''
            def _s(val):
                return val if val is not None else ''
            
            # Convertir formato de Supabase a formato de sheet
            course_data = {
                'codigo': _s(course.get('codigo')),
                'curso': _s(course.get('curso')),
                'modalidad': _s(course.get('modalidad')),
                'f.inicio': _s(course.get('fecha_inicio')),
                'horas': _s(course.get('horas')),
                'lugar': _s(course.get('lugar')),
                'horario': _s(course.get('horario')),
                'practicas': _s(course.get('practicas')),
                'localizacion': _s(course.get('localizacion')),
                'localidad / zona': _s(course.get('zona')),
                'situacion laboral': _s(course.get('situacion_laboral')),
                'requisitos academicos': _s(course.get('requisitos_academicos')),
                'sector': _s(course.get('sector')),
                'status': _s(course.get('status')),
                'pp': course.get('pp') or 0,
                'pc': course.get('pc') or 0,
                'que aprenderas': _s(course.get('que_aprenderas')),
                'salidas profesionales': _s(course.get('salidas_profesionales')),
                'enlace': _s(course.get('enlace')),
                'sheet_name': _s(course.get('sheet_name')),
            }
            
            # Añadir campos extra si existen
            extra = course.get('extra_data') or {}
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if k not in course_data:
                        course_data[k] = _s(v)
            
            # Filtrar por hoja origen
            sheet_name = course_data.get('sheet_name', '')
            if sheet_name and sheet_name not in valid_sheets:
                continue
            
            # Aplicar filtros (misma lógica que get_and_filter_courses)
            situacion_sheet_norm = unidecode((course_data.get('situacion laboral') or '').lower())
            status_sheet_norm = unidecode((course_data.get('status') or '').lower())
            sector_sheet_norm = unidecode((course_data.get('sector') or '').lower())
            modalidad_sheet_norm = unidecode((course_data.get('modalidad') or '').lower())
            
            # Status ok
            if "pausado" in status_sheet_norm:
                continue
            
            # Situación laboral (si es N/A, no filtrar)
            situacion_is_na = situacion_laboral_norm in ('n/a', 'na', '')
            if situacion_is_na:
                situacion_match = True
            else:
                situacion_match = _sheet_situation_allows_user(situacion_sheet_norm, situacion_laboral_norm, origen)
            if not situacion_match:
                continue
            
            # Sector
            if situacion_laboral_norm == 'autonomo':
                sector_match = True
            elif apply_sector_filter:
                if sector_sheet_norm and sector_sheet_norm != 'intersectorial':
                    sector_match = _sector_matches(sector_norm, sector_sheet_norm)
                else:
                    sector_match = True
            else:
                sector_match = True
            if not sector_match:
                continue
            
            # Modalidad
            if modalidad_norm:
                if modalidad_norm == 'online':
                    modalidad_match = 'online' in modalidad_sheet_norm
                elif modalidad_norm == 'aula virtual':
                    modalidad_match = 'aula virtual' in modalidad_sheet_norm
                elif modalidad_norm == 'presencial':
                    modalidad_match = ('presencial' in modalidad_sheet_norm) and ('online' not in modalidad_sheet_norm) and ('aula' not in modalidad_sheet_norm)
                else:
                    modalidad_match = True
            else:
                modalidad_match = True
            if not modalidad_match:
                continue
            
            # Nivel formación - dashboard estricto vs bot inclusivo
            nivel_is_na = nivel_formacion_norm in ('n/a', 'na', '')
            formacion_is_na = formacion_norm is None or formacion_norm in ('n/a', 'na', '')
            requisito_academico = course_data.get('requisitos academicos') or ''
            
            if dashboard_strict:
                # Dashboard:
                # - Si hay formación específica => filtrar SOLO por esa (ignorando nivel)
                # - Si hay solo nivel => exigir NIVEL explícito en requisitos (no inferir ESO->2)
                if not formacion_is_na:
                    nivel_match = _formacion_matches_only(formacion_norm, requisito_academico)
                elif nivel_is_na:
                    nivel_match = True
                else:
                    req_norm = _normalize_text(requisito_academico)
                    if not req_norm or "sin requisito" in req_norm:
                        nivel_match = True
                    else:
                        # En dashboard, un "CERT. PROF. NIVEL X" (CP-only) NO debe entrar por filtro de nivel genérico.
                        # Solo debe entrar si el usuario selecciona explícitamente esa titulación en "Titulación específica".
                        if _is_cp_only_requirement(requisito_academico):
                            nivel_match = False
                        else:
                            req_level = _extract_level_num(requisito_academico)
                            nivel_match = (req_level is not None) and (user_level is not None) and (req_level <= user_level)
            else:
                # Bot (inclusivo / actual)
                if nivel_is_na and formacion_is_na:
                    nivel_match = True
                elif nivel_is_na and not formacion_is_na:
                    nivel_match = _formacion_matches_only(formacion_norm, requisito_academico)
                else:
                    nivel_match = _formacion_cumple_requisito(formacion_norm, user_level, requisito_academico)
            if not nivel_match:
                continue
            
            # Código (si se especificó)
            if codigo_norm:
                codigo_val = course_data.get('codigo') or ''
                codigo_val_norm = re.sub(r'[^a-z0-9]', '', unidecode(codigo_val.lower())) if codigo_val else ''
                if codigo_val_norm != codigo_norm:
                    continue
            
            # Contenido mínimo - MISMA LÓGICA QUE GOOGLE SHEETS
            nombre_val = (course_data.get('curso') or '').strip()
            if _is_course_blocked_for_recommendation(nombre_val):
                continue
            codigo_val = (course_data.get('codigo') or '').strip()
            # No contar códigos autogenerados como código válido
            if codigo_val.startswith('SIN_CODIGO_'):
                codigo_val = ''
            link_val = (course_data.get('enlace') or '').strip()
            additional_fields = [
                course_data.get('f.inicio', ''), course_data.get('modalidad', ''),
                course_data.get('lugar', ''), course_data.get('que aprenderas', ''),
                course_data.get('salidas profesionales', '')
            ]
            additional_nonempty = sum(1 for v in additional_fields if str(v).strip())
            has_minimum_content = bool(nombre_val or codigo_val or link_val or additional_nonempty >= 2)
            if not has_minimum_content:
                continue
            
            # EOI check
            if course_data.get('sheet_name') == 'EOI' and not _is_eoi_allowed_for_origin(origen):
                continue
            
            filtered_courses.append(course_data)
        
        logging.info(f"✅ Filtrados {len(filtered_courses)} cursos desde Supabase")
        
        # Ordenar por PP+PC
        filtered_courses.sort(key=lambda c: (c.get(KEY_PP, 0) + c.get(KEY_PC, 0)), reverse=True)
        
        # Aplicar filtro de temática con IA si se especificó
        tematica_lower = tematica.lower() if isinstance(tematica, str) else ''
        tematica_norm = unidecode(tematica_lower) if tematica_lower and tematica_lower != 'n/a' else None
        
        if tematica_norm:
            logging.info(f"🤖 Aplicando filtro de temática con IA: '{tematica}'")
            try:
                themed_courses = _filter_courses_by_theme_with_ai(filtered_courses, tematica, KEY_CURSO)
                if themed_courses:
                    filtered_courses = themed_courses
                else:
                    filtered_courses = []
            except Exception as e:
                logging.error(f"❌ Error al aplicar filtro de temática con IA: {e}")
        
        # Paginar
        if page_size and page_size > 0:
            start_index = (pagina - 1) * page_size
            end_index = start_index + page_size
            paged_courses = filtered_courses[start_index:end_index]
        else:
            paged_courses = filtered_courses
        
        # Preparar estructura amigable para WhatsApp (misma lógica que la función original)
        prepared_courses = []
        for course in paged_courses:
            codigo_c = course.get('codigo', '')
            nombre = course.get('curso', '')
            modalidad_val = course.get('modalidad', '')
            fecha_ini = course.get('f.inicio', '')
            lugar = course.get('lugar', '')
            direccion_val = course.get('localizacion', '')
            zona_val = course.get('localidad / zona', '')
            horas = course.get('horas', '')
            practicas = course.get('practicas', '')
            horario = course.get('horario', '')
            que_aprenderas = course.get('que aprenderas', '')
            salidas_prof = course.get('salidas profesionales', '')
            enlace = course.get('enlace', '')
            
            parts = [f"{nombre}".strip()]
            if modalidad_val: parts.append(f"Modalidad: {modalidad_val}")
            if fecha_ini: parts.append(f"Inicio: {fecha_ini}")
            if horas: parts.append(f"Horas: {horas}")
            if practicas: parts.append(f"Prácticas: {practicas}")
            if horario: parts.append(f"Horario: {horario}")
            if lugar: parts.append(f"Lugar: {lugar}")
            if str(direccion_val).strip():
                modalidad_norm_for_label = unidecode(str(modalidad_val).lower())
                is_presencial = ('presencial' in modalidad_norm_for_label) and ('online' not in modalidad_norm_for_label) and ('aula' not in modalidad_norm_for_label)
                direccion_label = 'Dirección' if is_presencial else 'Dirección del examen'
                parts.append(f"{direccion_label}: {direccion_val}")
            if str(zona_val).strip():
                parts.append(f"Zona: {zona_val}")
            if que_aprenderas: parts.append(f"Qué aprenderás: {que_aprenderas}")
            if salidas_prof:
                try:
                    split_tokens = [t for t in re.split(r"\s*\d+\.\s*", str(salidas_prof).strip()) if t]
                    if len(split_tokens) > 1:
                        formatted_salidas = "Salidas profesionales:\n  - " + "\n  - ".join(split_tokens)
                    else:
                        alt = [t for t in re.split(r"\s*[;/]\s*", str(salidas_prof).strip()) if t]
                        formatted_salidas = "Salidas profesionales:\n  - " + "\n  - ".join(alt) if len(alt) > 1 else f"Salidas profesionales: {salidas_prof}"
                except Exception:
                    formatted_salidas = f"Salidas profesionales: {salidas_prof}"
                parts.append(formatted_salidas)
            
            dedup_parts = []
            seen_lines = set()
            for line in parts:
                if str(line).strip() and line not in seen_lines:
                    dedup_parts.append(line)
                    seen_lines.add(line)
            
            card_text = "\n- ".join([dedup_parts[0]] + dedup_parts[1:]) if dedup_parts else (nombre or "")
            button_id = f"curso_{codigo_c}" if codigo_c else f"curso_{abs(hash(nombre)) % 100000}"
            
            prepared_courses.append({
                **course,
                **({"direccion": direccion_val} if str(direccion_val).strip() else {}),
                **({"zona": zona_val} if str(zona_val).strip() else {}),
                "whatsapp_card_text": card_text,
                "whatsapp_button_id": button_id,
                "whatsapp_button_title": "Inscribirme"
            })
        
        total_time = time.time() - start_time
        logging.info(f"⏱️ Tiempo total recomendación cursos (Supabase): {total_time:.2f} s")
        
        return prepared_courses
        
    except Exception as e:
        logging.error(f"❌ Error leyendo cursos de Supabase: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return None


# Mapeo de titulaciones a sus niveles y si son Certificados de Profesionalidad
# IMPORTANTE:
# - Las claves se guardan NORMALIZADAS con _normalize_text() para que coincidan con valores del Sheet
#   aunque vengan en mayúsculas, con puntos o con tildes (p.ej. "MÁSTER" -> "master", "CERT. PROF." -> "cert prof").
_TITULACION_INFO_RAW = {
    # Valores reportados por cliente (y equivalentes comunes)
    "SIN ESTUDIOS": (0, False),
    "ESTUDIOS PRIMARIOS": (1, False),
    "FP GRADO MEDIO": (2, False),
    "ESO": (2, False),
    "BACHILLERATO": (3, False),
    "DOCTORADO": (3, False),
    "MÁSTER": (3, False),
    "FP GRADO SUPERIOR": (3, False),
    "GRADO UNIVERSITARIO": (3, False),
    "ACCESO UNI >25": (3, False),
    "ENSEÑ. PROF. MÚSICA Y DANZA": (0, False),
    # Certificados de Profesionalidad
    "CERT. PROF. NIVEL 1": (1, True),
    "CERT. PROF. NIVEL 2": (2, True),
    "CERT. PROF. NIVEL 3": (3, True),
    # Alias útiles
    "EST. PRIMARIOS": (1, False),
    "FP GR. MEDIO": (2, False),
    "FP GR. SUPERIOR": (3, False),
    "GRADO UNIV.": (3, False),
    "CP NIVEL 1": (1, True),
    "CP NIVEL 2": (2, True),
    "CP NIVEL 3": (3, True),
    "CERTIFICADO DE PROFESIONALIDAD DE NIVEL 1": (1, True),
    "CERTIFICADO DE PROFESIONALIDAD DE NIVEL 2": (2, True),
    "CERTIFICADO DE PROFESIONALIDAD DE NIVEL 3": (3, True),
    "ACCESO UNIVERSIDAD MAYORES 25": (3, False),
    "ENSEÑANZAS PROFESIONALES DE MÚSICA Y DANZA": (0, False),
    "PROF. MÚSICA/DANZA": (0, False),
}

TITULACION_INFO = {_normalize_text(k): v for k, v in _TITULACION_INFO_RAW.items()}

def _is_cert_profesionalidad(text):
    """Detecta si un texto se refiere a un Certificado de Profesionalidad."""
    if not text:
        return False
    t = _normalize_text(text)
    # Patrones que indican cert. prof.
    return ('cert' in t and 'prof' in t) or ('cp nivel' in t) or ('certificado de profesionalidad' in t)

def _is_cp_only_requirement(text):
    """
    Devuelve True SOLO cuando el requisito académico parece ser específicamente
    "Certificado de Profesionalidad" (CP) y no una lista mixta tipo:
    "NIVEL 2 (FP GRADO MEDIO, ESO, CERT. PROF. NIVEL 2)".
    """
    if not text:
        return False
    if not _is_cert_profesionalidad(text):
        return False
    t = _normalize_text(text)

    # Si hay una lista mixta de opciones (paréntesis tras "nivel X"), NO es CP-only.
    if re.search(r"\bnivel\s*(\d+|[ivx]+)\s*\(", t):
        return False

    # Si aparecen titulaciones no-CP en el requisito, NO es CP-only.
    non_cp_markers = [
        "eso",
        "fp",
        "bachillerato",
        "grado",
        "master",
        "máster",
        "doctorado",
        "estudios primarios",
        "primarios",
        "sin estudios",
    ]
    if any(m in t for m in non_cp_markers):
        return False

    return True

def _extract_level_num(text):
    """
    Extrae nivel numérico desde un texto de requisitos.
    Soporta:
    - "NIVEL 2", "nivel2", "nivel-2", "nivel: 2"
    - "NIVEL II", "Nivel iii" (romanos básicos I/II/III/IV/V)
    Devuelve int o None.
    """
    if not text:
        return None
    t = _normalize_text(text)
    # Normalizar separadores para capturar "nivel-2", "nivel:2", etc.
    t2 = re.sub(r"[^a-z0-9]+", " ", t).strip()
    # 1) Dígitos después de "nivel"
    m = re.search(r"\bnivel\s*(\d+)\b", t2)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    # 2) Romanos después de "nivel"
    m = re.search(r"\bnivel\s*([ivx]+)\b", t2)
    if m:
        roman = m.group(1)
        roman_map = {
            "i": 1,
            "ii": 2,
            "iii": 3,
            "iv": 4,
            "v": 5,
        }
        return roman_map.get(roman)
    # 3) Fallback: primer número en el texto
    m = re.search(r"\b(\d+)\b", t2)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None

def _formacion_matches_only(formacion_value, requisito_value) -> bool:
    """
    Matching estricto por titulación específica:
    - Si el curso no tiene requisito (o "sin requisito") => True
    - Si el requisito contiene la formación => True
    """
    req_norm = _normalize_text(requisito_value or "")
    if not req_norm or "sin requisito" in req_norm:
        return True
    form_norm = _normalize_text(formacion_value or "")
    if not form_norm or form_norm in ("n/a", "na"):
        return True

    # Caso especial: Certificados de Profesionalidad (CP)
    # En el dashboard, si el usuario selecciona "CP NIVEL X", debe matchear:
    # - "CERT. PROF. NIVEL X"
    # - "CERTIFICADO DE PROFESIONALIDAD DE NIVEL X"
    # - "NIVEL X (..., CERT. PROF. NIVEL X)" (listas mixtas)
    try:
        is_cp_selected = _is_cert_profesionalidad(formacion_value) or ("cp nivel" in form_norm)
        if is_cp_selected:
            sel_level = _extract_level_num(form_norm)
            req_level = _extract_level_num(req_norm)

            # Debe existir indicio CP en el requisito
            if not _is_cert_profesionalidad(requisito_value):
                return False

            # Si ambos tienen nivel detectable, exigir coincidencia exacta
            if sel_level is not None and req_level is not None:
                return sel_level == req_level

            # Fallback: substring (p.ej. "nivel 2" puede venir sin extraerse bien)
            if sel_level is not None:
                return f"nivel {sel_level}" in req_norm
            return True
    except Exception:
        pass

    return (form_norm in req_norm) or (req_norm in form_norm)

def _get_titulacion_info(formacion_name):
    """
    Obtiene (nivel, es_cert_prof) para una titulación dada.
    Si no se encuentra, intenta extraer el nivel del nombre.
    """
    if not formacion_name:
        return (None, False)
    
    f_norm = _normalize_text(formacion_name)
    
    # Buscar en el mapeo
    if f_norm in TITULACION_INFO:
        return TITULACION_INFO[f_norm]
    
    # Buscar coincidencia parcial
    for key, info in TITULACION_INFO.items():
        if key in f_norm or f_norm in key:
            return info
    
    # Si es un cert. prof., extraer nivel del nombre
    if _is_cert_profesionalidad(formacion_name):
        lvl = _extract_level_num(f_norm)
        if lvl is not None:
            return (lvl, True)
        return (None, True)
    
    # Intentar extraer nivel genérico
    lvl = _extract_level_num(f_norm)
    if lvl is not None:
        return (lvl, False)
    
    return (None, False)

def _formacion_cumple_requisito(user_formacion, user_nivel, requisito_sheet):
    """
    Determina si la formación del usuario cumple el requisito del curso.
    
    Args:
        user_formacion: Nombre de la titulación del usuario (ej: "ESO", "CP NIVEL 2")
        user_nivel: Nivel numérico del usuario (0, 1, 2, 3)
        requisito_sheet: Texto del requisito académico del curso
    
    Returns:
        bool: True si cumple el requisito
    """
    if not requisito_sheet or not requisito_sheet.strip():
        return True  # Sin requisito = todos pasan
    
    req_norm = _normalize_text(requisito_sheet)
    
    if 'sin requisito' in req_norm:
        return True
    
    # Verificar si el requisito ESPECÍFICAMENTE CP (no una lista mixta con otras titulaciones)
    requisito_es_cp = _is_cp_only_requirement(requisito_sheet)
    
    # Obtener info de la titulación del usuario
    user_info = _get_titulacion_info(user_formacion)
    user_es_cp = user_info[1] if user_info else False
    
    if requisito_es_cp:
        # El curso requiere un Cert. Prof. específico
        # El usuario DEBE tener un Cert. Prof. de nivel >= al requerido
        if not user_es_cp:
            # Usuario no tiene cert. prof., no cumple aunque tenga el nivel
            return False
        # Usuario tiene cert. prof., verificar nivel
        req_level = _extract_level_num(req_norm)
        if req_level is not None and user_nivel is not None:
            return user_nivel >= req_level
        return True
    
    # El requisito NO es un cert. prof.
    # Verificar si es una titulación específica o solo un nivel genérico
    
    # Buscar si el requisito menciona una titulación específica
    titulaciones_especificas = [
        'eso',
        'bachillerato',
        'fp grado medio', 'fp gr medio',
        'fp grado superior', 'fp gr superior',
        'grado universitario', 'grado univ',
        'master',
        'doctorado',
        'estudios primarios', 'est primarios',
        'ensen prof musica y danza', 'ensenanzas profesionales de musica y danza',
        'acceso uni 25', 'acceso universidad mayores 25',
    ]
    
    req_es_titulacion_especifica = any(tit in req_norm for tit in titulaciones_especificas)
    
    if req_es_titulacion_especifica and user_formacion:
        # Si el requisito menciona "nivel ..." lo tratamos como genérico (puede ser lista mixta),
        # no como titulación única tipo "ESO".
        if "nivel" not in req_norm:
            # Verificar si el usuario tiene ESA titulación específica.
            user_f_norm = _normalize_text(user_formacion)

            # Si el usuario tiene la misma titulación, pasa
            if user_f_norm in req_norm or req_norm in user_f_norm:
                return True

            # Si el usuario es Certificado de Profesionalidad (CP), NO debe cumplir por equivalencia
            # requisitos de titulaciones no-CP (p.ej. ESO) salvo match exacto.
            if user_es_cp:
                return False

            # Si el usuario NO es CP, permitir por nivel superior (comportamiento inclusivo del bot)
            req_info = _get_titulacion_info(requisito_sheet)
            req_nivel = req_info[0] if req_info else None
            if req_nivel is not None and user_nivel is not None:
                return user_nivel >= req_nivel
    
    # Requisito genérico (solo nivel) o fallback
    # Extraer nivel del requisito
    req_level = _extract_level_num(req_norm)
    if req_level is not None and user_nivel is not None:
        return user_nivel >= req_level
    
    # Si no podemos determinar, usar comparación de texto
    if user_formacion:
        user_f_norm = _normalize_text(user_formacion)
        return user_f_norm in req_norm or req_norm in user_f_norm
    
    return True  # Fallback permisivo


def get_and_filter_courses(origen, situacion_laboral, nivel_formacion, pagina=1, page_size=5, sector="N/A", modalidad="N/A", tematica="N/A", codigo="N/A", wa_id=None, formacion="N/A", dashboard_strict: bool = False):
    """Lee el spreadsheet y filtra cursos combinando varias hojas según provincia y orígenes especiales.
    
    Si USE_SUPABASE_COURSES está activado y hay cursos en Supabase, los lee de ahí (más rápido).
    Si no hay cursos en Supabase o ocurre un error, lee directamente de Google Sheets.
    
    Args:
        formacion: Nombre específico de la titulación del usuario (ej: "ESO", "CP NIVEL 2").
                   Se usa para filtrado avanzado junto con nivel_formacion.
        dashboard_strict: Si True, el filtrado por nivel/formación es estricto (dashboard).
                         Si False, el filtrado es inclusivo (bot).
    """
    start_time_overall = time.time()
    read_time_total = 0.0

    def _log_mem(stage):
        try:
            proc = psutil.Process()
            rss_mb = proc.memory_info().rss / (1024 * 1024)
            logging.info(f"💾 MEM [{stage}]: {rss_mb:.1f} MB")
        except Exception:
            pass
    
    # Actualizar contexto de enrollment si tenemos wa_id
    if wa_id and is_supabase_enabled():
        logging.info(f"🔄 Refreshing enrollment context for wa_id: {wa_id}")
        try:
            ctx = get_enrollment_context(wa_id) or {}
            if ctx:
                updates = {}
                ctx_prov = ctx.get("provincia")
                if ctx_prov:
                    if str(origen) != str(ctx_prov):
                        logging.info(f"  -> Refreshed 'origen' from '{origen}' to '{ctx_prov}'")
                    origen = ctx_prov
                elif origen:
                    updates["provincia"] = origen
                ctx_sit = ctx.get("situacion_laboral")
                if ctx_sit:
                    if str(situacion_laboral) != str(ctx_sit):
                        logging.info(f"  -> Refreshed 'situacion_laboral' from '{situacion_laboral}' to '{ctx_sit}'")
                    situacion_laboral = ctx_sit
                elif situacion_laboral:
                    updates["situacion_laboral"] = situacion_laboral
                ctx_nivel = ctx.get("nivel_formacion")
                if ctx_nivel:
                    if str(nivel_formacion) != str(ctx_nivel):
                        logging.info(f"  -> Refreshed 'nivel_formacion' from '{nivel_formacion}' to '{ctx_nivel}'")
                    nivel_formacion = ctx_nivel
                elif nivel_formacion:
                    updates["nivel_formacion"] = nivel_formacion
                # Recuperar formación (nombre de titulación) del contexto
                ctx_formacion = ctx.get("formacion")
                if ctx_formacion:
                    if formacion == "N/A" or not formacion:
                        logging.info(f"  -> Refreshed 'formacion' from '{formacion}' to '{ctx_formacion}'")
                    formacion = ctx_formacion
                elif formacion and formacion != "N/A":
                    updates["formacion"] = formacion
                if updates:
                    try:
                        if update_enrollment_context(wa_id, updates):
                            logging.info(f"  -> Persisted enrollment_context fields: {list(updates.keys())}")
                    except Exception as e:
                        logging.error(f"❌ Error persisting enrollment updates for {wa_id}: {e}")
        except Exception as e:
            logging.error(f"❌ Error refreshing enrollment context for {wa_id}: {e}")
    
    # INTENTAR LEER DE SUPABASE PRIMERO (más rápido)
    if USE_SUPABASE_COURSES and is_supabase_enabled():
        logging.info("🚀 Intentando leer cursos desde Supabase...")
        supabase_result = _get_courses_from_supabase(
            origen, situacion_laboral, nivel_formacion,
            pagina, page_size, sector, modalidad, tematica, codigo, formacion, dashboard_strict
        )
        if supabase_result is not None:
            logging.info(f"✅ Cursos obtenidos de Supabase: {len(supabase_result)}")
            return supabase_result
        logging.info("⚠️ Fallback a Google Sheets...")
    
    # FALLBACK: LEER DE GOOGLE SHEETS
    try:
        spreadsheet_name = current_app.config.get('SPREADSHEET_NAME')
        if not spreadsheet_name:
            logging.error("❌ Error: SPREADSHEET_NAME not configured in the environment.")
            return []
            
        gs_client = get_gs_client()
        
        today = date.today()

        logging.info(f"📊 Accediendo a la hoja de cálculo '{spreadsheet_name}'...")
        _log_mem("start")

        try:
            def _open_sheet():
                return gs_client.open(spreadsheet_name)
            t_open = time.time()
            spreadsheet = _execute_google_call(_open_sheet, f"open spreadsheet '{spreadsheet_name}'")
            read_time_total += time.time() - t_open
            spreadsheet_id = spreadsheet.id
        except gspread.SpreadsheetNotFound:
            logging.error(f"❌ Error: No se encontró la hoja de cálculo '{spreadsheet_name}'.")
            return []
        
        # Determinar hojas a leer
        worksheet_names = []
        # Verificar si origen es N/A usando el valor original (antes de normalizar)
        origen_lower = str(origen or '').lower().strip()
        origen_is_na = origen_lower in ('n/a', 'na', '')
        
        if origen_is_na:
            # Búsqueda libre: incluir todas las hojas
            worksheet_names = list(ALL_SHEETS)
            logging.info("🔍 Búsqueda libre (sin filtro de origen): leyendo todas las hojas")
        else:
            mapped = _map_province_to_sheet(origen)
            if mapped:
                worksheet_names.append(mapped)

            # Restricción: cursos de 'EOI' solo para Andalucía (todas las provincias), Ceuta y Melilla
            eoi_allowed = _is_eoi_allowed_for_origin(origen)

            for sheet_name in SPECIAL_SHEETS:
                if sheet_name == 'EOI' and not eoi_allowed:
                    logging.info("⛔ Omitiendo hoja 'EOI' por ubicación no permitida (solo Andalucía, Ceuta y Melilla).")
                    continue
                worksheet_names.append(sheet_name)

        sheets_service = get_sheets_service()

        # Claves canónicas usadas en filtrado y procesamiento
        KEY_STATUS = 'status'
        KEY_PP = 'pp'
        KEY_PC = 'pc'
        KEY_SITUACION_LABORAL = 'situacion laboral'
        KEY_REQUISITOS_ACADEMICOS = 'requisitos academicos'
        KEY_FECHA_INICIO = 'f.inicio'
        KEY_CURSO = 'curso'
        KEY_CODIGO = 'codigo'
        KEY_QUE_APRENDERAS = 'que aprenderas'
        KEY_SALIDAS_PROFESIONALES = 'salidas profesionales'
        KEY_SECTOR = 'sector'

        # Normalizar criterios de búsqueda una sola vez
        origen_norm = unidecode(str(origen or '').lower())
        situacion_laboral_norm = unidecode(str(situacion_laboral or '').lower())
        nivel_formacion_norm = unidecode(str(nivel_formacion or '').lower())
        sector_lower = sector.lower() if isinstance(sector, str) else ''
        sector_norm = unidecode(sector_lower) if sector_lower and sector_lower != 'n/a' else None
        modalidad_lower = modalidad.lower() if isinstance(modalidad, str) else ''
        modalidad_norm = unidecode(modalidad_lower) if modalidad_lower and modalidad_lower != 'n/a' else None
        codigo_lower = codigo.lower() if isinstance(codigo, str) else ''
        # Comparación exacta por código, ignorando mayúsculas y separadores no alfanuméricos
        codigo_norm = re.sub(r'[^a-z0-9]', '', unidecode(codigo_lower)) if codigo_lower and codigo_lower != 'n/a' else None
        apply_sector_filter = (situacion_laboral_norm == 'ocupado' and sector_norm is not None)
        
        # Normalizar formación del usuario (nombre de titulación específico)
        formacion_lower = formacion.lower() if isinstance(formacion, str) else ''
        formacion_norm = formacion_lower if formacion_lower and formacion_lower != 'n/a' else None
        
        # Extraer nivel numérico del usuario
        user_level_match_global = re.search(r'(\d+)', nivel_formacion_norm)
        user_level_global = int(user_level_match_global.group(1)) if user_level_match_global else None

        # Memory-optimized: keep only the top-K rows unless page_size <= 0 (no cap)
        no_cap = (page_size is None) or (int(page_size) <= 0)
        # Si se solicitó temática, necesitaremos el conjunto completo para que la IA
        # evalúe sobre todos los cursos filtrados (no solo top-K) antes de paginar
        tematica_lower_early = tematica.lower() if isinstance(tematica, str) else ''
        tematica_norm_early = unidecode(tematica_lower_early) if tematica_lower_early and tematica_lower_early != 'n/a' else None
        collect_all_for_theme = bool(tematica_norm_early)
        if no_cap:
            filtered_courses_no_cap = []  # collect all matches without heap cap
            top_k_size = None
            top_heap = None
        else:
            top_k_size = max(1, int(page_size or 20) * int(pagina or 1))
            top_heap = []  # min-heap of (score, counter, course_dict)
        heap_counter = 0
        total_filtered = 0
        # Si hay temática, recolectar también el conjunto completo sin límite
        all_filtered_for_theme = [] if collect_all_for_theme else None

        logging.debug(f"--- Start Filtering (streaming) ---")
        logging.debug(f"Normalized Criteria: origen='{origen_norm}', situacion='{situacion_laboral_norm}', formacion='{nivel_formacion_norm}', sector='{sector_norm}'")

        def _parse_int(value):
            if not value:
                return 0
            cleaned = re.sub(r"[^0-9.,]", "", str(value)).replace(',', '.')
            try:
                return int(float(cleaned))
            except (ValueError, TypeError):
                logging.debug(f"  Could not parse score '{value}'. Using 0.")
                return 0

        for ws_name in worksheet_names:
            # 1) Leer solo cabecera
            t_sheet_read_start = time.time()
            try:
                def _read_header():
                    return sheets_service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=f"{ws_name}!1:1",
                        valueRenderOption="FORMULA",
                        dateTimeRenderOption="FORMATTED_STRING"
                    ).execute()
                header_resp = _execute_google_call(_read_header, f"read header from '{ws_name}'")
            except Exception as e:
                logging.warning(f"⚠️ No se pudo leer cabecera de '{ws_name}': {e}. Se omite.")
                read_time_total += time.time() - t_sheet_read_start
                continue

            header_values = (header_resp.get('values') or [[]])[0]
            if not header_values:
                logging.warning(f"⚠️ La hoja '{ws_name}' no tiene cabecera.")
                read_time_total += time.time() - t_sheet_read_start
                continue

            num_columns = len(header_values)
            sheet_headers = [
                ' '.join(
                    _normalize_text(h)
                    .replace('_', ' ').replace('-', ' ')
                    .replace('📆','').replace('👔','')
                    .replace('⌛','').replace('📍','')
                    .replace('📅','').replace('💭','')
                    .split()
                ) for h in header_values
            ]

            # 2) Determinar columnas necesarias
            desired_headers = {
                KEY_STATUS, KEY_PP, KEY_PC, KEY_SITUACION_LABORAL, KEY_REQUISITOS_ACADEMICOS,
                KEY_FECHA_INICIO, KEY_CURSO, KEY_CODIGO, KEY_QUE_APRENDERAS, KEY_SALIDAS_PROFESIONALES,
                KEY_SECTOR, 'modalidad', 'lugar', 'horario', 'practicas',
                'localizacion', 'localidad / zona'
            }
            include_indices = set()
            for idx, name in enumerate(sheet_headers):
                if name in desired_headers:
                    include_indices.add(idx)
                if 'hora' in name:
                    include_indices.add(idx)

            link_col_idx = sheet_headers.index('link') if 'link' in sheet_headers else -1
            enlace_col_idx = sheet_headers.index('enlace') if 'enlace' in sheet_headers else -1
            link_like_idx = max(link_col_idx, enlace_col_idx)
            # Incluir explícitamente la columna de enlace en el rango compacto
            if link_like_idx >= 0:
                include_indices.add(link_like_idx)

            if not include_indices:
                include_indices = set(range(min(10, num_columns)))

            min_idx = min(include_indices)
            max_idx = max(include_indices)
            start_col = _col_index_to_a1(min_idx)
            end_col = _col_index_to_a1(max_idx)

            # 3) Leer rango compacto de columnas necesarias (todas las filas)
            try:
                def _read_values():
                    return sheets_service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=f"{ws_name}!{start_col}2:{end_col}",
                        valueRenderOption="FORMULA",
                        dateTimeRenderOption="FORMATTED_STRING"
                    ).execute()
                values_resp = _execute_google_call(_read_values, f"read values from '{ws_name}'")
            except Exception as e:
                logging.warning(f"⚠️ No se pudo leer rango compacto de '{ws_name}': {e}. Se omite.")
                read_time_total += time.time() - t_sheet_read_start
                continue

            data_rows = values_resp.get('values', []) or []
            non_empty_rows_count = 0
            _log_mem(f"{ws_name} values rows={len(data_rows)}")
            # 4) Extraer hyperlink real de la columna de enlace usando includeGridData solo para las filas con datos
            link_hrefs = []
            if link_like_idx >= 0:
                try:
                    col_letter = _col_index_to_a1(link_like_idx)
                    # Limitar el rango a las filas de datos conocidas para evitar cargar toda la columna
                    last_row = 1 + max(len(data_rows), 0)
                    if last_row > 1:
                        col_range = f"{ws_name}!{col_letter}2:{col_letter}{last_row}"
                        def _read_links():
                            return sheets_service.spreadsheets().get(
                                spreadsheetId=spreadsheet_id,
                                ranges=[col_range],
                                includeGridData=True,
                                fields="sheets.data.rowData.values(hyperlink,textFormatRuns,formattedValue)"
                            ).execute()
                        link_resp = _execute_google_call(_read_links, f"read link hrefs from '{ws_name}'")
                        row_data = (((link_resp.get('sheets') or [{}])[0].get('data') or [{}])[0].get('rowData')) or []
                        for rd in row_data:
                            cell = (rd.get('values') or [{}])[0] or {}
                            href = cell.get('hyperlink') or ''
                            if not href:
                                try:
                                    for run in cell.get('textFormatRuns') or []:
                                        fmt = run.get('format') or {}
                                        link_obj = fmt.get('link') or {}
                                        uri = link_obj.get('uri')
                                        if uri:
                                            href = uri
                                            break
                                except Exception:
                                    pass
                            if not href:
                                fv = (cell.get('formattedValue') or '').strip()
                                if isinstance(fv, str) and fv.lower().startswith(("http://", "https://")):
                                    href = fv
                            link_hrefs.append(href or '')
                except Exception as e:
                    logging.debug(f"No se pudo extraer hyperlink via gridData para '{ws_name}': {e}")
                    link_hrefs = []

            # Hasta este punto solo hemos hecho llamadas de lectura a Google Sheets
            read_time_total += time.time() - t_sheet_read_start

            # 5) Procesar filas del rango compacto y reconstruir por nombre de columna
            for row_idx, row in enumerate(data_rows):
                if not any(str(cell).strip() for cell in row):
                    continue
                non_empty_rows_count += 1

                current_row_map = {}
                for orig_idx in range(min_idx, max_idx + 1):
                    name = sheet_headers[orig_idx]
                    idx_in_range = orig_idx - min_idx
                    v = row[idx_in_range] if idx_in_range < len(row) else ''
                    if isinstance(v, str):
                        m = HYPERLINK_REGEX.match(v.strip())
                        if m:
                            v = m.group(1)
                    current_row_map[name] = v

                # Reemplazar 'link'/'enlace' con el href real extraído via includeGridData si está disponible
                try:
                    href = link_hrefs[row_idx] if row_idx < len(link_hrefs) else ''
                except Exception:
                    href = ''
                if link_col_idx >= 0:
                    if href:
                        current_row_map['link'] = href
                if enlace_col_idx >= 0:
                    if href:
                        current_row_map['enlace'] = href

                # Aplicar lógica de filtrado fila por fila
                course_lower_keys = {k: str(v).strip() for k, v in current_row_map.items()}
                course_lower_keys['sheet_name'] = ws_name

                situacion_sheet_norm = unidecode(course_lower_keys.get(KEY_SITUACION_LABORAL, '').lower())
                status_sheet_norm = unidecode(course_lower_keys.get(KEY_STATUS, '').lower())
                start_date_str = course_lower_keys.get(KEY_FECHA_INICIO, '')
                sector_sheet_norm = unidecode(course_lower_keys.get(KEY_SECTOR, '').lower()) if KEY_SECTOR in course_lower_keys else ''
                modalidad_sheet_norm = unidecode(course_lower_keys.get('modalidad', '').lower())

                course_lower_keys[KEY_PP] = _parse_int(course_lower_keys.get(KEY_PP, '0'))
                course_lower_keys[KEY_PC] = _parse_int(course_lower_keys.get(KEY_PC, '0'))
                
                # --- Validaciones ---
                origen_ok = True
                # Situación laboral (si es N/A, no filtrar)
                situacion_is_na = situacion_laboral_norm in ('n/a', 'na', '')
                if situacion_is_na:
                    situacion_match = True
                else:
                    situacion_match = _sheet_situation_allows_user(situacion_sheet_norm, situacion_laboral_norm, origen)
                status_ok = "pausado" not in status_sheet_norm
                
                # Sector
                if situacion_laboral_norm == 'autonomo':
                    sector_match = True
                elif apply_sector_filter:
                    if sector_sheet_norm and sector_sheet_norm != 'intersectorial':
                        sector_match = _sector_matches(sector_norm, sector_sheet_norm)
                    else:
                        sector_match = True
                else:
                    sector_match = True

                # Modalidad
                if modalidad_norm:
                    if modalidad_norm == 'online': modalidad_match = 'online' in modalidad_sheet_norm
                    elif modalidad_norm == 'aula virtual': modalidad_match = 'aula virtual' in modalidad_sheet_norm
                    elif modalidad_norm == 'presencial': modalidad_match = ('presencial' in modalidad_sheet_norm) and ('online' not in modalidad_sheet_norm) and ('aula' not in modalidad_sheet_norm)
                    else: modalidad_match = True
                else:
                    modalidad_match = True

                # Nivel formación - dashboard estricto vs bot inclusivo
                nivel_is_na = nivel_formacion_norm in ('n/a', 'na', '')
                formacion_is_na = formacion_norm is None or formacion_norm in ('n/a', 'na', '')
                requisito_academico = course_lower_keys.get(KEY_REQUISITOS_ACADEMICOS, '')
                
                if dashboard_strict:
                    if not formacion_is_na:
                        nivel_match = _formacion_matches_only(formacion_norm, requisito_academico)
                    elif nivel_is_na:
                        nivel_match = True
                    else:
                        req_norm = _normalize_text(requisito_academico)
                        if not req_norm or "sin requisito" in req_norm:
                            nivel_match = True
                        else:
                            # En dashboard, un "CERT. PROF. NIVEL X" (CP-only) NO debe entrar por filtro de nivel genérico.
                            # Solo debe entrar si el usuario selecciona explícitamente esa titulación en "Titulación específica".
                            if _is_cp_only_requirement(requisito_academico):
                                nivel_match = False
                            else:
                                req_level = _extract_level_num(requisito_academico)
                                nivel_match = (req_level is not None) and (user_level_global is not None) and (req_level <= user_level_global)
                else:
                    if nivel_is_na and formacion_is_na:
                        nivel_match = True
                    elif nivel_is_na and not formacion_is_na:
                        nivel_match = _formacion_matches_only(formacion_norm, requisito_academico)
                    else:
                        nivel_match = _formacion_cumple_requisito(formacion_norm, user_level_global, requisito_academico)

                # Contenido mínimo
                nombre_val = course_lower_keys.get(KEY_CURSO, '').strip()
                if _is_course_blocked_for_recommendation(nombre_val):
                    continue
                codigo_val = course_lower_keys.get(KEY_CODIGO, '').strip()
                # Normalizar código de la fila para comparación exacta ignorando separadores
                codigo_val_norm = re.sub(r'[^a-z0-9]', '', unidecode(codigo_val.lower())) if codigo_val else ''
                link_val = (course_lower_keys.get('enlace', '') or course_lower_keys.get('link', '')).strip()
                additional_fields = [
                    course_lower_keys.get(KEY_FECHA_INICIO, ''), course_lower_keys.get('modalidad', ''),
                    course_lower_keys.get('lugar', ''), course_lower_keys.get(KEY_QUE_APRENDERAS, ''),
                    course_lower_keys.get(KEY_SALIDAS_PROFESIONALES, '')
                ]
                additional_nonempty = sum(1 for v in additional_fields if str(v).strip())
                has_minimum_content = bool(nombre_val or codigo_val or link_val or additional_nonempty >= 2)

                if course_lower_keys.get('sheet_name') == 'EOI' and not _is_eoi_allowed_for_origin(origen):
                    continue

                # Filtro por código (si se solicitó)
                if codigo_norm:
                    codigo_match = (codigo_val_norm == codigo_norm)
                else:
                    codigo_match = True

                if (origen_ok and situacion_match and nivel_match and sector_match and codigo_match and
                    modalidad_match and status_ok and has_minimum_content):
                    total_filtered += 1
                    if no_cap:
                        filtered_courses_no_cap.append(course_lower_keys)
                    else:
                        score = course_lower_keys.get(KEY_PP, 0) + course_lower_keys.get(KEY_PC, 0)
                        entry = (score, heap_counter, course_lower_keys)
                        heap_counter += 1
                        if len(top_heap) < top_k_size:
                            heapq.heappush(top_heap, entry)
                        else:
                            if score > top_heap[0][0]:
                                heapq.heapreplace(top_heap, entry)
                    # Recolectar siempre para temática si está activa
                    if all_filtered_for_theme is not None:
                        all_filtered_for_theme.append(course_lower_keys)
            
            logging.info(f"🔎 Procesadas {non_empty_rows_count} filas no vacías de '{ws_name}'.")
            _free_mem_hint()
            _log_mem(f"{ws_name} processed")
        
        logging.info(f"✅ Filtrados {total_filtered} cursos en total de las hojas: {worksheet_names}")

        # Recuperar los Top-K ordenados desc por PP+PC (o conjunto completo si temática activa)
        if all_filtered_for_theme is not None:
            # Si hay temática, trabajamos con el conjunto completo ordenado
            top_courses = sorted(
                all_filtered_for_theme,
                key=lambda course: (course.get(KEY_PP, 0) + course.get(KEY_PC, 0)),
                reverse=True
            )
        else:
            if no_cap:
                top_courses = sorted(
                    filtered_courses_no_cap,
                    key=lambda course: (course.get(KEY_PP, 0) + course.get(KEY_PC, 0)),
                    reverse=True
                )
            else:
                top_courses = [entry[2] for entry in heapq.nlargest(top_k_size, top_heap)]
                top_courses.sort(key=lambda course: (course.get(KEY_PP, 0) + course.get(KEY_PC, 0)), reverse=True)

        # Aplicar filtro de temática con IA ANTES de paginar, si se especificó
        tematica_lower = tematica.lower() if isinstance(tematica, str) else ''
        tematica_norm = unidecode(tematica_lower) if tematica_lower and tematica_lower != 'n/a' else None

        if tematica_norm:
            logging.info(f"🤖 Aplicando filtro de temática con IA: '{tematica}'")
            logging.info(f"📊 Cursos antes del filtro de temática: {len(top_courses)}")
            try:
                themed_courses = _filter_courses_by_theme_with_ai(top_courses, tematica, KEY_CURSO)
                logging.info(f"✅ Cursos después del filtro de temática: {len(themed_courses)}")
                if not themed_courses:
                    logging.warning(f"⚠️ No se encontraron cursos para la temática '{tematica}'.")
                    top_courses = []
                else:
                    top_courses = themed_courses
            except Exception as e:
                logging.error(f"❌ Error al aplicar filtro de temática con IA: {e}")
                # Si falla el filtro de IA, continuar con los cursos sin filtrar
                logging.warning("⚠️ Continuando sin filtro de temática")

        # Ahora paginar
        if page_size and page_size > 0:
            start_index = (pagina - 1) * page_size
            end_index = start_index + page_size
            paged_courses = top_courses[start_index:end_index]
        else:
            paged_courses = top_courses

        logging.info(f"✅ Finalizados filtrado, ordenado y paginado de {len(paged_courses)} cursos (página {pagina}).")

        # Métricas de tiempo para mostrar en los logs del dashboard
        try:
            total_time = time.time() - start_time_overall
            recommend_time = max(0.0, total_time - read_time_total)
            logging.info(f"⏱️ Tiempo de lectura de hojas (Google Sheets): {read_time_total:.2f} s")
            logging.info(f"⏱️ Tiempo de recomendación de cursos (filtrado/IA/paginado): {recommend_time:.2f} s")
            logging.info(f"⏱️ Tiempo total recomendación cursos: {total_time:.2f} s")
        except Exception:
            pass
        _free_mem_hint()

        # Preparar estructura amigable para WhatsApp
        prepared_courses = []
        for course in paged_courses:
            codigo = course.get(KEY_CODIGO, '')
            nombre = course.get(KEY_CURSO, '')
            modalidad_val = course.get('modalidad', '')
            fecha_ini = course.get(KEY_FECHA_INICIO, '')
            lugar = course.get('lugar', '')
            # Nuevos campos desde el sheet si existen
            direccion_val = course.get('localizacion', '')
            zona_val = course.get('localidad / zona', '')
            horas = ''
            try:
                if isinstance(course, dict):
                    horas = course.get('no horas', '') or course.get('horas', '') or course.get('n horas', '')
                    if not str(horas).strip():
                        for k, v in course.items():
                            if 'hora' in str(k) and str(v).strip():
                                horas = str(v).strip()
                                break
            except Exception:
                pass
            practicas = course.get('practicas', '')
            horario = course.get('horario', '')
            que_aprenderas = course.get(KEY_QUE_APRENDERAS, '')
            salidas_prof = course.get(KEY_SALIDAS_PROFESIONALES, '')
            enlace = course.get('enlace', '') or course.get('link', '')

            parts = [f"{nombre}".strip()]
            if modalidad_val: parts.append(f"Modalidad: {modalidad_val}")
            if fecha_ini: parts.append(f"Inicio: {fecha_ini}")
            if horas: parts.append(f"Horas: {horas}")
            if practicas: parts.append(f"Prácticas: {practicas}")
            if horario: parts.append(f"Horario: {horario}")
            if lugar: parts.append(f"Lugar: {lugar}")
            # Dirección/Zona si existen
            if str(direccion_val).strip():
                modalidad_norm_for_label = unidecode(str(modalidad_val).lower())
                is_presencial = ('presencial' in modalidad_norm_for_label) and ('online' not in modalidad_norm_for_label) and ('aula' not in modalidad_norm_for_label)
                direccion_label = 'Dirección' if is_presencial else 'Dirección del examen'
                parts.append(f"{direccion_label}: {direccion_val}")
            if str(zona_val).strip():
                parts.append(f"Zona: {zona_val}")
            if que_aprenderas: parts.append(f"Qué aprenderás: {que_aprenderas}")
            if salidas_prof:
                try:
                    split_tokens = [t for t in re.split(r"\s*\d+\.\s*", str(salidas_prof).strip()) if t]
                    if len(split_tokens) > 1:
                        formatted_salidas = "Salidas profesionales:\n  - " + "\n  - ".join(split_tokens)
                    else:
                        alt = [t for t in re.split(r"\s*[;/]\s*", str(salidas_prof).strip()) if t]
                        formatted_salidas = "Salidas profesionales:\n  - " + "\n  - ".join(alt) if len(alt) > 1 else f"Salidas profesionales: {salidas_prof}"
                except Exception:
                    formatted_salidas = f"Salidas profesionales: {salidas_prof}"
                parts.append(formatted_salidas)

            dedup_parts = []
            seen_lines = set()
            for line in parts:
                if str(line).strip() and line not in seen_lines:
                    dedup_parts.append(line)
                    seen_lines.add(line)
            
            card_text = "\n- ".join([dedup_parts[0]] + dedup_parts[1:]) if dedup_parts else (nombre or "")
            button_id = f"curso_{codigo}" if codigo else f"curso_{abs(hash(nombre)) % 100000}"

            prepared_courses.append({
                **course,
                **({"direccion": direccion_val} if str(direccion_val).strip() else {}),
                **({"zona": zona_val} if str(zona_val).strip() else {}),
                "whatsapp_card_text": card_text,
                "whatsapp_button_id": button_id,
                "whatsapp_button_title": "Inscribirme"
            })

        return prepared_courses

    except gspread.exceptions.APIError as e:
        logging.error(f"❌ Error de API de Google Sheets/gspread: {e}")
        return []
    except Exception as e:
        import traceback
        logging.error(f"❌ Error inesperado al leer/filtrar cursos: {e}\n{traceback.format_exc()}")
        return []


# RECORD CONVERSATION IN GOOGLE DOCS
def search_doc(wa_id, folder_id, drive_service):
    # Perform a search for the document by name in the specified folder
    query = f"name='{wa_id}' and '{folder_id}' in parents and mimeType='application/vnd.google-apps.document'"
    
    response = drive_service.files().list(q=query).execute()
    files = response.get('files', [])
    
    if files:
        # Si se encuentra el documento, devuelve su ID
        return files[0]['id']
    return None

def create_document(wa_id, folder_id, drive_service):
    # Creates a new Google Docs document with the specified name (wa_id) in the given folder (folder_id).
    # The document's MIME type is set to Google Docs format.
    # Returns the ID of the newly created document.
    doc_metadata = {
    'name': wa_id,
    'parents': [folder_id],
    'mimeType': 'application/vnd.google-apps.document'
    }
    doc = drive_service.files().create(body=doc_metadata).execute()
    return doc['id']

def add_record(document_id, client_name, host_name, question, response):
    # Create a service client to interact with the Google Docs API.
    docs_service = get_docs_service()
    date = time.ctime()
    # Give format to record
    if question:
        record = f"Fecha: {date}\n{client_name}: {question}\n{host_name}: {response}\n\n"
    else:
        record = f"Fecha: {date}\n{host_name}: {response}\n\n"
    # Define the body of the request to insert the text
    requests = [
        {
            'insertText': {
                'location': {
                    'index': 1,  # La posición donde se insertará el texto
                },
                'text': record
            }
        }
    ]
    # Executes a request to update the document with the operations defined in 'requests'
    docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()

def record_conversation(wa_id, client_name, host_name, question, response):
    time.sleep(10)
    start_time_temp = time.time() 
    logging.info("5. Recording conversation in Google Docs") 
    # Folder where all the records are recorded.
    folder_id = "1Nm_IrA_W-HPS71hkRwWZmf4y8LcHy6G4"
    # Create a service client to interact with the Google Drive API. 
    drive_service = get_drive_service()
    # Create doc if it doesn't exist with the name of the phone number
    document_id = search_doc(wa_id, folder_id, drive_service)
    if not document_id:
        document_id = create_document(wa_id, folder_id, drive_service)
        print("Creating document")
    # Add record in contact doc with date, name of contact, question and response
    add_record(document_id, client_name, host_name, question, response)
    logging.info(f"Conversation recorded in -> {time.time() - start_time_temp :.2f} seconds\n")
    return 0 


# RECORD SUMMARIES IN GOOGLE SHEET

def insert_data(entry_id, name, question, response, summary, categories, interest):
    # Autenticarse usando las credenciales del archivo JSON
    client = get_gs_client()
    def _open_summaries():
        return client.open('Summaries')
    spreadsheet = _execute_google_call(_open_summaries, "open 'Summaries'")
    sheet = spreadsheet.sheet1
    # Obtener la fecha actual
    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Insertar una nueva fila con los datos proporcionados
    def _target_row():
        return len(sheet.get_all_values()) + 1
    target_row = _execute_google_call(_target_row, "count rows in 'Summaries'")
    def _insert_summary():
        return sheet.insert_row([entry_id, name, date, question, response, summary, categories, interest], target_row)
    _execute_google_call(_insert_summary, "insert row into 'Summaries'")

def record_summary(wa_id, name, question, response):
    start_time_temp = time.time() 
    logging.info("6. Recording conversation in Google Docs") 
    conversation_text = ""
    # thread_messages = get_thread_messages(wa_id)
    # thread_messages = thread_messages[:4]
    # Ordenar mensajes de más antiguo a más reciente
    # sorted_messages = sorted(thread_messages, key=lambda m: m.created_at)
    
    # for message in sorted_messages:
    #     author = "Asistente" if message.role == 'assistant' else "Usuario"
    #     content = message.content[0].text.value  # Obtener el texto del mensaje
    #     conversation_text += f"{author}: {content}\n"
    
    prompt = f"""
    Aquí tienes una conversación entre un usuario y un asistente virtual. 
    Por favor, proporciona un resumen de dos líneas de la conversación:
    {conversation_text}
    Resumen (no quiero que no respondas nada más que no sea el resumen):
    """
    summary = GPTRequest(prompt)

    prompt = f"""
    Según las siguientes categorías, ¿cuál es el tema principal de la pregunta? 
    Elige solo de las categorías listadas a continuación, sin agregar comentarios adicionales. 
    Si la pregunta corresponde a más de una categoría, sepáralas con comas.

    Categorías: 
    - Saludo
    - Precios
    - Diferencias de filtros
    - Instalación
    - Beneficios
    - Garantía
    - Consumo de luz
    - Agua alcalina
    - Cambio de filtro
    - Productos

    Pregunta:
    {question}

    Responde solo con las categorías correspondientes:
    """
    categories = GPTRequest(prompt)

    prompt = f"""
    Según la siguiente pregunta, ¿cuánto interés muestra el cliente en base a estas categorías: low, medium o high?

    Pregunta:
    {question}

    Responde solo con una de las tres categorías (low, medium o high) basándote en el nivel de interés percibido en la pregunta.
    """
    interest = GPTRequest(prompt)

    insert_data(wa_id, name, question, response, summary, categories, interest)
    logging.info(f"Summary recorded in -> {time.time() - start_time_temp :.2f} seconds\n")
