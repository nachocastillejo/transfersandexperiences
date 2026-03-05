import json
import logging
import os
import re
import time
from typing import Any, Dict

import requests
from app.utils.extra_utils import validate_and_normalize_spanish_tax_id, is_missing_email
from unidecode import unidecode


def _load_crm_credentials() -> tuple[str, str]:
    """Load CRM API credentials from config/CRM_euroformac.json."""
    try:
        with open("config/CRM_euroformac.json", "r", encoding="utf-8") as f:
            credentials = json.load(f)
        client_id = credentials.get("client_id")
        client_secret = credentials.get("client_secret")
        if not client_id or not client_secret:
            raise ValueError("Missing client_id or client_secret in CRM_euroformac.json")
        return client_id, client_secret
    except Exception as e:
        logging.error(f"Error loading CRM credentials: {e}")
        raise


def _is_crm_test_mode() -> bool:
    """Check if CRM is in test mode based on environment variable."""
    return os.getenv("CRM_TEST_MODE", "False").lower() in ("true", "1", "yes")


def _get_access_token(client_id: str, client_secret: str, use_test_mode: bool = False) -> str:
    """Retrieve access token from CRM API with retries/backoff on transient errors."""
    login_url = "http://alumnos.grupoeuroformac.com/apicrm/login"
    params = {"client_id": client_id, "client_secret": client_secret}
    
    # Incluir testMode en el login si se solicita
    if use_test_mode:
        params["testMode"] = 1
    
    max_attempts = 4
    backoff_base_seconds = 0.5
    mode_text = "TEST" if use_test_mode else "PRODUCTION"
    logging.info(f"Requesting CRM access token [{mode_text}]...")
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(login_url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            if data.get("codigo") != 200 or not data.get("access_token"):
                raise RuntimeError(f"CRM login failed: {data}")
            return data["access_token"]
        except requests.RequestException as e:
            last_error = e
            # Network/transient error → retry with backoff
            if attempt < max_attempts:
                delay = min(4.0, backoff_base_seconds * (2 ** (attempt - 1)))
                logging.warning(f"CRM login attempt {attempt} failed due to communication error: {e}. Retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                logging.error(f"CRM login failed after {max_attempts} attempts: {e}")
        except Exception as e:
            # Non-transient error (e.g., invalid body) → do not retry
            last_error = e
            logging.error(f"Unexpected error during CRM login (no retry): {e}")
            break
    assert last_error is not None
    raise last_error


def _normalize_phone(phone: str | None) -> str | None:
    if not phone:
        return None
    digits = re.sub(r"\D", "", str(phone))
    # Remove country code 34 if present
    if digits.startswith("0034"):
        digits = digits[4:]
    elif digits.startswith("34"):
        digits = digits[2:]
    # Spanish national numbers are 9 digits; take last 9 as fallback
    if len(digits) > 9:
        digits = digits[-9:]
    return digits


# Province mapping as required by CRM (1..52)
PROVINCIAS_MAP: Dict[str, int] = {
    "Araba/Álava": 1,
    "Albacete": 2,
    "Alicante/Alacant": 3,
    "Almería": 4,
    "Ávila": 5,
    "Badajoz": 6,
    "Balears, Illes": 7,
    "Barcelona": 8,
    "Burgos": 9,
    "Cáceres": 10,
    "Cádiz": 11,
    "Castellón/Castelló": 12,
    "Ciudad Real": 13,
    "Córdoba": 14,
    "Coruña, A": 15,
    "Cuenca": 16,
    "Girona": 17,
    "Granada": 18,
    "Guadalajara": 19,
    "Gipuzkoa": 20,
    "Huelva": 21,
    "Huesca": 22,
    "Jaén": 23,
    "León": 24,
    "Lleida": 25,
    "Rioja, La": 26,
    "Lugo": 27,
    "Madrid": 28,
    "Málaga": 29,
    "Murcia": 30,
    "Navarra": 31,
    "Ourense": 32,
    "Asturias": 33,
    "Palencia": 34,
    "Palmas, Las": 35,
    "Pontevedra": 36,
    "Salamanca": 37,
    "Santa Cruz de Tenerife": 38,
    "Cantabria": 39,
    "Segovia": 40,
    "Sevilla": 41,
    "Soria": 42,
    "Tarragona": 43,
    "Teruel": 44,
    "Toledo": 45,
    "Valencia/València": 46,
    "Valladolid": 47,
    "Bizkaia": 48,
    "Zamora": 49,
    "Zaragoza": 50,
    "Ceuta": 51,
    "Melilla": 52,
    "Fuera de España": 53,
}


SITUACION_LABORAL_MAP: Dict[str, int] = {
    # Canonical values and synonyms
    "trabajador por cuenta ajena": 1,
    "ocupado": 1,
    "autónomo": 2,
    "autonomo": 2,
    "desempleado": 3,
}


def _map_situacion_laboral(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    value_str = str(value).strip().lower()
    return SITUACION_LABORAL_MAP.get(value_str)


def _map_titulacion(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    value_str = str(value).strip()
    # Accept "Nivel 1/2/3"
    if value_str.lower().startswith("nivel "):
        try:
            return int(value_str.split()[-1])
        except Exception:
            return None
    # Legacy 6-value mapping removed: expect CRM integer codes or "Nivel X"
    return None


def _map_provincia(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    # value may be "29" or name
    value_str = str(value).strip()
    try:
        return int(value_str)
    except ValueError:
        return PROVINCIAS_MAP.get(value_str)


# Sector mapping to CRM IDs (1..25). Keys are normalized (lowercase, accent-free)
SECTOR_MAP: Dict[str, int] = {
    "act. fisico deportivas": 1,
    "act fisico deportivas": 1,
    "administracion y gestion": 2,
    "agrario": 3,
    "autonomos": 4,
    "autónomos": 4,
    "comercio": 5,
    "comercio y marketing": 5,  # aceptar variante extendida
    "construccion": 6,
    "construcción": 6,
    "economia e industria digital (teleco)": 7,
    "economía e industria digital (teleco)": 7,
    "economia social (mutualistas y fundaciones)": 8,
    "economía social (mutualistas y fundaciones)": 8,
    "educacion": 9,
    "educación": 9,
    "energia": 10,
    "energía": 10,
    "finanzas": 11,
    "gran distribucion (almacenes)": 12,
    "gran distribución (almacenes)": 12,
    "industria alimentaria": 13,
    "informacion y comunicacion y artes graficas": 14,
    "información y comunicación y artes gráficas": 14,
    "maritima y actividades portuarias": 15,
    "marítima y actividades portuarias": 15,
    "metal": 16,
    "pesca": 17,
    "quimica / laboratorio": 18,
    "química / laboratorio": 18,
    "sanidad": 19,
    "servicios (otros)": 20,
    "servicios a las empresas": 21,
    "servicios medioambientales": 22,
    "textil y confeccion y piel": 23,
    "textil y confección y piel": 23,
    "transporte y logistica": 24,
    "transporte y logística": 24,
    "turismo": 25,
}


def _map_sector(value: Any) -> int | None:
    """Map input sector to CRM ID. Accepts int, canonical strings, and variants.

    Returns None when not provided or when value indicates not applicable (e.g., 'N/A').
    """
    if value is None:
        return None
    # If already an integer-like, return directly
    if isinstance(value, int):
        return value
    try:
        # Strings like '9' → 9
        maybe_int = int(str(value).strip())
        return maybe_int
    except (ValueError, TypeError):
        pass
    value_str = str(value).strip()
    if not value_str:
        return None
    # Treat special non-applicable markers as None
    if value_str.upper() in {"N/A", "NA", "NONE", "SIN", "NO APLICA"}:
        return None
    norm = unidecode(value_str).lower()
    return SECTOR_MAP.get(norm)



def inscribir_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inscribe un lead en el CRM (MOCKED PARA DEMO T&E).
    """
    logging.info(f"MOCKED: CRM inscription called with lead data: {lead}")
    return {"codigo": 200, "descripcion": "OK (MOCKED)"}


