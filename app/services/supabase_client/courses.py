"""
Módulo para gestionar cursos en Supabase.
Proporciona operaciones CRUD y sincronización desde Google Sheets.
"""
import logging
import hashlib
import json
from typing import Any, Dict, List, Optional
import requests

from .core import (
    is_supabase_enabled,
    _get_supabase_headers,
    _rest_url,
    _get_phone_number_id,
)

# Mapeo de nombres de columnas normalizados del sheet a columnas de la tabla
SHEET_TO_DB_MAPPING = {
    'codigo': 'codigo',
    'curso': 'curso',
    'modalidad': 'modalidad',
    'f.inicio': 'fecha_inicio',
    'fecha inicio': 'fecha_inicio',
    'horas': 'horas',
    'no horas': 'horas',
    'n horas': 'horas',
    'lugar': 'lugar',
    'horario': 'horario',
    'practicas': 'practicas',
    'localizacion': 'localizacion',
    'localidad / zona': 'zona',
    'zona': 'zona',
    'situacion laboral': 'situacion_laboral',
    'requisitos academicos': 'requisitos_academicos',
    'sector': 'sector',
    'status': 'status',
    'pp': 'pp',
    'pc': 'pc',
    'que aprenderas': 'que_aprenderas',
    'salidas profesionales': 'salidas_profesionales',
    'enlace': 'enlace',
    'link': 'enlace',
    'sheet_name': 'sheet_name',
}

# Columnas conocidas de la tabla (para separar extra_data)
KNOWN_COLUMNS = set(SHEET_TO_DB_MAPPING.values()) | {'extra_data', 'row_hash', 'phone_number_id', 'synced_at'}


def _compute_row_hash(row_data: Dict[str, Any]) -> str:
    """Calcula un hash MD5 del contenido de la fila para detectar cambios."""
    try:
        # Ordenar claves para consistencia
        sorted_items = sorted(row_data.items(), key=lambda x: x[0])
        content = json.dumps(sorted_items, ensure_ascii=False, sort_keys=True)
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    except Exception:
        return ''


def _map_sheet_row_to_db(row: Dict[str, Any], sheet_name: str, row_index: int = 0) -> Dict[str, Any]:
    """
    Convierte una fila del sheet (con claves normalizadas) a formato de la tabla.
    Inicializa TODAS las columnas conocidas para asegurar consistencia en batch inserts.
    """
    import re
    
    # Inicializar TODAS las columnas conocidas con valores por defecto
    from datetime import datetime, timezone
    
    # Usar hora actual en UTC (PostgreSQL convertirá a la zona configurada)
    now_utc = datetime.now(timezone.utc).isoformat()
    
    db_row = {
        'sheet_name': sheet_name,
        'phone_number_id': _get_phone_number_id(),
        'codigo': None,
        'curso': None,
        'modalidad': None,
        'fecha_inicio': None,
        'horas': None,
        'lugar': None,
        'horario': None,
        'practicas': None,
        'localizacion': None,
        'zona': None,
        'situacion_laboral': None,
        'requisitos_academicos': None,
        'sector': None,
        'status': None,
        'pp': 0,
        'pc': 0,
        'que_aprenderas': None,
        'salidas_profesionales': None,
        'enlace': None,
        'extra_data': None,
        'row_hash': None,
        'synced_at': now_utc,  # Siempre actualizar con la hora actual
    }
    extra_data = {}
    
    for key, value in row.items():
        if key == 'sheet_name':
            continue
            
        # Normalizar el valor
        if value is None:
            value = ''
        elif not isinstance(value, (str, int, float, bool)):
            value = str(value)
        
        # Limpiar espacios extra
        if isinstance(value, str):
            value = value.strip()
        
        # Mapear a columna de DB o guardar en extra_data
        db_column = SHEET_TO_DB_MAPPING.get(key)
        if db_column:
            # Convertir PP y PC a enteros (solo si son valores razonables)
            if db_column in ('pp', 'pc'):
                try:
                    if isinstance(value, str):
                        cleaned = re.sub(r"[^0-9.,]", "", value).replace(',', '.')
                        if cleaned:
                            num_value = float(cleaned)
                            # Solo aceptar valores razonables para puntuación (0-10000)
                            if num_value > 10000:
                                value = 0  # Valor demasiado grande, probablemente es un código
                            else:
                                value = int(num_value)
                        else:
                            value = 0
                    else:
                        num_value = int(value) if value else 0
                        value = num_value if num_value <= 10000 else 0
                except (ValueError, TypeError):
                    value = 0
            db_row[db_column] = value if value else None
        else:
            # Columna no mapeada, guardar en extra_data
            if str(value).strip():
                extra_data[key] = value
    
    if extra_data:
        db_row['extra_data'] = extra_data
    
    # Calcular hash de la fila
    db_row['row_hash'] = _compute_row_hash(row)
    
    # Si no hay código, generar uno basado en contenido (determinístico)
    # Esto es necesario para el constraint UNIQUE de la BD
    if not db_row.get('codigo'):
        # Usar hash basado en contenido real para que sea determinístico
        curso_name = db_row.get('curso') or ''
        modalidad = db_row.get('modalidad') or ''
        fecha = db_row.get('fecha_inicio') or ''
        content_key = f"{sheet_name}|{curso_name}|{modalidad}|{fecha}|{row_index}"
        db_row['codigo'] = f"SIN_CODIGO_{hashlib.md5(content_key.encode()).hexdigest()[:10]}"
    
    return db_row


def upsert_course(course_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Inserta o actualiza un curso en Supabase.
    Usa codigo + sheet_name + phone_number_id como clave única.
    """
    if not is_supabase_enabled():
        return None
    
    try:
        headers = _get_supabase_headers(use_service_role=True)
        headers['Prefer'] = 'resolution=merge-duplicates,return=representation'
        
        url = _rest_url('courses')
        
        # Asegurar que extra_data es JSON string si existe
        if 'extra_data' in course_data and isinstance(course_data['extra_data'], dict):
            course_data['extra_data'] = json.dumps(course_data['extra_data'], ensure_ascii=False)
        
        resp = requests.post(url, headers=headers, json=course_data, timeout=30)
        
        if resp.status_code in (200, 201):
            result = resp.json()
            return result[0] if isinstance(result, list) and result else result
        else:
            logging.error(f"Error upserting course: {resp.status_code} - {resp.text}")
            return None
            
    except Exception as e:
        logging.error(f"Exception upserting course: {e}")
        return None


def upsert_courses_batch(courses: List[Dict[str, Any]], sheet_name: str) -> int:
    """
    Inserta o actualiza múltiples cursos en batch.
    Retorna el número de cursos procesados exitosamente.
    
    Maneja códigos duplicados añadiendo sufijos numéricos.
    Usa upsert con on_conflict para actualizar registros existentes.
    """
    if not is_supabase_enabled() or not courses:
        return 0
    
    try:
        headers = _get_supabase_headers(use_service_role=True)
        # Usar upsert con columnas de conflicto específicas
        headers['Prefer'] = 'resolution=merge-duplicates,return=minimal'
        
        url = _rest_url('courses')
        # Añadir on_conflict para especificar las columnas de upsert
        url += '?on_conflict=codigo,sheet_name,phone_number_id'
        
        # Preparar datos para batch insert
        db_rows = []
        seen_codes = set()  # Para detectar códigos duplicados en este batch
        
        skipped_count = 0
        for idx, course in enumerate(courses):
            db_row = _map_sheet_row_to_db(course, sheet_name, idx)
            
            # FILTRO 1: Saltar cursos sin título (no deben existir en la BD)
            curso_titulo = (db_row.get('curso') or '').strip()
            if not curso_titulo:
                skipped_count += 1
                logging.debug(f"Skipping course without title at index {idx}")
                continue
            
            # FILTRO 2: Saltar cursos con código que parece una URL (error de mapeo)
            codigo_val = db_row.get('codigo') or ''
            if codigo_val.startswith('http://') or codigo_val.startswith('https://'):
                skipped_count += 1
                logging.warning(f"Skipping course with URL as codigo: {codigo_val[:50]}...")
                continue
            
            # Manejar códigos duplicados DENTRO DEL BATCH añadiendo sufijo
            original_code = codigo_val
            code = original_code
            counter = 1
            while code in seen_codes:
                code = f"{original_code}_{counter}"
                counter += 1
            db_row['codigo'] = code
            seen_codes.add(code)
            
            # Convertir extra_data a JSON string
            if 'extra_data' in db_row and isinstance(db_row['extra_data'], dict):
                db_row['extra_data'] = json.dumps(db_row['extra_data'], ensure_ascii=False)
            
            db_rows.append(db_row)
        
        if skipped_count > 0:
            logging.info(f"Skipped {skipped_count} invalid courses (no title or URL as code)")
        
        # Dividir en chunks de 500 para evitar límites
        chunk_size = 500
        total_success = 0
        
        for i in range(0, len(db_rows), chunk_size):
            chunk = db_rows[i:i + chunk_size]
            resp = requests.post(url, headers=headers, json=chunk, timeout=60)
            
            if resp.status_code in (200, 201):
                total_success += len(chunk)
                logging.info(f"Upserted {len(chunk)} courses for sheet '{sheet_name}' (batch {i // chunk_size + 1})")
            else:
                logging.error(f"Error upserting batch chunk: {resp.status_code} - {resp.text[:500]}")
        
        return total_success
        
    except Exception as e:
        logging.error(f"Exception upserting courses batch: {e}")
        return 0


def delete_courses_by_sheet(sheet_name: str) -> bool:
    """
    Elimina todos los cursos de una hoja específica.
    Útil antes de una sincronización completa de esa hoja.
    """
    if not is_supabase_enabled():
        return False
    
    try:
        headers = _get_supabase_headers(use_service_role=True)
        phone_id = _get_phone_number_id()
        
        url = _rest_url('courses')
        url += f"?sheet_name=eq.{sheet_name}"
        if phone_id:
            url += f"&phone_number_id=eq.{phone_id}"
        
        resp = requests.delete(url, headers=headers, timeout=30)
        
        if resp.status_code in (200, 204):
            logging.info(f"Deleted courses from sheet '{sheet_name}'")
            return True
        else:
            logging.error(f"Error deleting courses: {resp.status_code} - {resp.text}")
            return False
            
    except Exception as e:
        logging.error(f"Exception deleting courses: {e}")
        return False


def delete_course_by_codigo(codigo: str, sheet_name: str) -> bool:
    """
    Elimina un curso específico por código y hoja.
    """
    if not is_supabase_enabled():
        return False
    
    try:
        headers = _get_supabase_headers(use_service_role=True)
        phone_id = _get_phone_number_id()
        
        url = _rest_url('courses')
        url += f"?codigo=eq.{codigo}&sheet_name=eq.{sheet_name}"
        if phone_id:
            url += f"&phone_number_id=eq.{phone_id}"
        
        resp = requests.delete(url, headers=headers, timeout=30)
        return resp.status_code in (200, 204)
        
    except Exception as e:
        logging.error(f"Exception deleting course: {e}")
        return False


def fetch_courses(
    sheet_names: Optional[List[str]] = None,
    modalidad: Optional[str] = None,
    sector: Optional[str] = None,
    situacion_laboral: Optional[str] = None,
    exclude_paused: bool = True,
    limit: int = 1000,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Obtiene cursos de Supabase con filtros opcionales.
    """
    if not is_supabase_enabled():
        return []
    
    try:
        headers = _get_supabase_headers(use_service_role=True)
        phone_id = _get_phone_number_id()
        
        url = _rest_url('courses')
        url += "?select=*"
        
        # Filtro por phone_number_id
        if phone_id:
            url += f"&phone_number_id=eq.{phone_id}"
        
        # Filtro por hojas
        if sheet_names:
            sheets_param = ','.join(sheet_names)
            url += f"&sheet_name=in.({sheets_param})"
        
        # Filtro por modalidad
        if modalidad:
            url += f"&modalidad=ilike.*{modalidad}*"
        
        # Filtro por sector
        if sector:
            url += f"&sector=ilike.*{sector}*"
        
        # Filtro por situación laboral
        if situacion_laboral:
            url += f"&situacion_laboral=ilike.*{situacion_laboral}*"
        
        # Excluir pausados
        if exclude_paused:
            url += "&status=not.ilike.*pausado*"
        
        # Ordenamiento por PP+PC descendente
        url += "&order=pp.desc,pc.desc"
        
        # Paginación
        url += f"&limit={limit}&offset={offset}"
        
        resp = requests.get(url, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            courses = resp.json()
            # Parsear extra_data de JSON string a dict
            for course in courses:
                if course.get('extra_data') and isinstance(course['extra_data'], str):
                    try:
                        course['extra_data'] = json.loads(course['extra_data'])
                    except json.JSONDecodeError:
                        course['extra_data'] = {}
            return courses
        else:
            logging.error(f"Error fetching courses: {resp.status_code} - {resp.text}")
            return []
            
    except Exception as e:
        logging.error(f"Exception fetching courses: {e}")
        return []


def fetch_all_courses_for_filtering() -> List[Dict[str, Any]]:
    """
    Obtiene todos los cursos para filtrado en memoria.
    Optimizado para el flujo de recomendación existente.
    """
    if not is_supabase_enabled():
        return []
    
    try:
        headers = _get_supabase_headers(use_service_role=True)
        phone_id = _get_phone_number_id()
        
        # Seleccionar solo las columnas necesarias para filtrado
        columns = [
            'id', 'codigo', 'sheet_name', 'curso', 'modalidad', 'fecha_inicio',
            'horas', 'lugar', 'horario', 'practicas', 'localizacion', 'zona',
            'situacion_laboral', 'requisitos_academicos', 'sector', 'status',
            'pp', 'pc', 'que_aprenderas', 'salidas_profesionales', 'enlace', 'extra_data'
        ]
        
        url = _rest_url('courses')
        url += f"?select={','.join(columns)}"
        
        if phone_id:
            url += f"&phone_number_id=eq.{phone_id}"
        
        # Excluir pausados
        url += "&status=not.ilike.*pausado*"
        
        # Ordenar por puntuación
        url += "&order=pp.desc,pc.desc"
        
        # Sin límite para obtener todos
        url += "&limit=10000"
        
        resp = requests.get(url, headers=headers, timeout=60)
        
        if resp.status_code == 200:
            courses = resp.json()
            logging.info(f"Fetched {len(courses)} courses from Supabase")
            
            # Parsear extra_data y convertir a formato compatible con drive_service
            for course in courses:
                if course.get('extra_data') and isinstance(course['extra_data'], str):
                    try:
                        course['extra_data'] = json.loads(course['extra_data'])
                    except json.JSONDecodeError:
                        course['extra_data'] = {}
            
            return courses
        else:
            logging.error(f"Error fetching all courses: {resp.status_code} - {resp.text}")
            return []
            
    except Exception as e:
        logging.error(f"Exception fetching all courses: {e}")
        return []


def get_last_sync_time() -> Optional[str]:
    """
    Obtiene la fecha/hora de la última sincronización.
    """
    if not is_supabase_enabled():
        return None
    
    try:
        headers = _get_supabase_headers(use_service_role=True)
        phone_id = _get_phone_number_id()
        
        url = _rest_url('courses')
        url += "?select=synced_at&order=synced_at.desc&limit=1"
        
        if phone_id:
            url += f"&phone_number_id=eq.{phone_id}"
        
        resp = requests.get(url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            result = resp.json()
            if result:
                return result[0].get('synced_at')
        return None
        
    except Exception as e:
        logging.error(f"Exception getting last sync time: {e}")
        return None


def count_courses() -> int:
    """
    Cuenta el número total de cursos en Supabase.
    """
    if not is_supabase_enabled():
        return 0
    
    try:
        headers = _get_supabase_headers(use_service_role=True)
        headers['Prefer'] = 'count=exact'
        phone_id = _get_phone_number_id()
        
        url = _rest_url('courses')
        url += "?select=id"
        
        if phone_id:
            url += f"&phone_number_id=eq.{phone_id}"
        
        resp = requests.head(url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            count_header = resp.headers.get('content-range', '')
            if '/' in count_header:
                total = count_header.split('/')[-1]
                return int(total) if total != '*' else 0
        return 0
        
    except Exception as e:
        logging.error(f"Exception counting courses: {e}")
        return 0


__all__ = [
    'upsert_course',
    'upsert_courses_batch',
    'delete_courses_by_sheet',
    'delete_course_by_codigo',
    'fetch_courses',
    'fetch_all_courses_for_filtering',
    'get_last_sync_time',
    'count_courses',
    'SHEET_TO_DB_MAPPING',
]


