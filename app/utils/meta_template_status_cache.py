import time
from typing import Dict, List, Optional, Any

import logging

_STATUS_CACHE: Dict[str, Dict[str, object]] = {}
_TTL_SECONDS = 120  # Mantener estados unos 2 minutos como máximo


def _now() -> float:
    return time.time()


def _purge_old(now: float | None = None) -> None:
    """Elimina entradas antiguas para evitar fugas de memoria."""
    if now is None:
        now = _now()
    try:
        to_delete = []
        for mid, entry in _STATUS_CACHE.items():
            created = float(entry.get("created_at") or 0)
            if created and now - created > _TTL_SECONDS:
                to_delete.append(mid)
        for mid in to_delete:
            _STATUS_CACHE.pop(mid, None)
    except Exception as exc:
        logging.warning(f"meta_template_status_cache purge failed: {exc}")


def record_message_ids(message_ids: List[str]) -> None:
    """
    Registra una lista de message_ids como 'pending' para poder
    seguir su estado más tarde desde el dashboard.
    """
    now = _now()
    if not message_ids:
        return
    for mid in message_ids:
        if not mid:
            continue
        entry = _STATUS_CACHE.get(mid)
        if entry is None:
            _STATUS_CACHE[mid] = {"status": "pending", "created_at": now}
        else:
            # Respetar un estado ya conocido (ej. si el webhook llegó antes)
            if "created_at" not in entry:
                entry["created_at"] = now
            _STATUS_CACHE[mid] = entry
    _purge_old(now)


def record_pending_template_message(
    message_id: str,
    wa_id: str,
    display_text: str,
    project_name: str,
    required_action: Optional[str] = None
) -> None:
    """
    Registra un mensaje de plantilla pendiente de confirmación.
    
    La conversación NO se crea hasta que llegue confirmación (sent/delivered).
    Si llega 'failed', simplemente se descarta sin crear nada.
    """
    if not message_id:
        return
    now = _now()
    _STATUS_CACHE[message_id] = {
        "status": "pending",
        "created_at": now,
        "pending_message": True,
        "wa_id": wa_id,
        "display_text": display_text,
        "project_name": project_name,
        "required_action": required_action,
    }
    _purge_old(now)


def get_pending_message_info(message_id: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene la info de un mensaje pendiente para poder crearlo
    cuando llegue la confirmación del webhook.
    
    Retorna None si no hay info pendiente o ya fue procesado.
    """
    if not message_id:
        return None
    entry = _STATUS_CACHE.get(message_id)
    if not entry or not entry.get("pending_message"):
        return None
    return {
        "wa_id": entry.get("wa_id"),
        "display_text": entry.get("display_text"),
        "project_name": entry.get("project_name"),
        "required_action": entry.get("required_action"),
    }


def mark_message_created(message_id: str) -> None:
    """
    Marca un mensaje como ya creado en la DB para evitar duplicados.
    """
    if not message_id:
        return
    entry = _STATUS_CACHE.get(message_id)
    if entry:
        entry["pending_message"] = False
        entry["message_created"] = True


def update_status(message_id: str, status: str, error: str | None = None) -> None:
    """
    Actualiza el estado de un message_id concreto.

    Se llama desde el webhook cuando llega un status update
    ('sent', 'delivered', 'failed', ...).
    """
    if not message_id or not status:
        return
    now = _now()
    entry = _STATUS_CACHE.get(message_id) or {}
    entry["status"] = status
    # Guardar texto de error si se proporciona (típicamente en estados 'failed')
    if error:
        entry["error"] = error
    if "created_at" not in entry:
        entry["created_at"] = now
    _STATUS_CACHE[message_id] = entry
    _purge_old(now)


def get_summary_for_message_ids(message_ids: List[str]) -> Dict[str, object]:
    """
    Devuelve un resumen {sent, failed, pending} para los message_ids dados.

    - 'sent' cuenta estados 'sent' o 'delivered'.
    - 'failed' cuenta estados 'failed'.
    - 'pending' es el resto (incluidos ids no vistos aún).
    """
    now = _now()
    sent = 0
    failed = 0
    pending = 0
    first_error: str | None = None
    sent_ids: List[str] = []
    failed_ids: List[str] = []
    pending_ids: List[str] = []
    errors_by_id: Dict[str, str] = {}

    for mid in message_ids or []:
        if not mid:
            continue
        entry = _STATUS_CACHE.get(mid)
        if not entry:
            pending += 1
            pending_ids.append(mid)
            continue
        st = str(entry.get("status") or "").lower()
        if st in ("sent", "delivered"):
            sent += 1
            sent_ids.append(mid)
        elif st == "failed":
            failed += 1
            failed_ids.append(mid)
            err_val = str(entry.get("error") or "")
            if err_val:
                errors_by_id[mid] = err_val
            if first_error is None and err_val:
                first_error = err_val
        else:
            pending += 1
            pending_ids.append(mid)

    _purge_old(now)
    return {
        "sent": sent,
        "failed": failed,
        "pending": pending,
        "first_error": first_error,
        "sent_ids": sent_ids,
        "failed_ids": failed_ids,
        "pending_ids": pending_ids,
        "errors_by_id": errors_by_id
    }


__all__ = [
    "record_message_ids",
    "update_status",
    "get_summary_for_message_ids",
    "record_pending_template_message",
    "get_pending_message_info",
    "mark_message_created",
]


