import os
import logging
from dotenv import load_dotenv
import os.path  # Asegúrate de importar os.path si no lo tienes ya

# Limitar arenas de glibc (equivalente a MALLOC_ARENA_MAX=2) sin usar variables de entorno
try:
    if os.name == "posix":
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        M_ARENA_MAX = 23  # mallopt constant
        libc.mallopt(M_ARENA_MAX, 2)
except Exception:
    pass

if __name__ == "__main__":

    # --- CARGA DE VARIABLES DE ENTORNO ---
    # Lee ENV_NAME; si no existe, usa 'transfersandexperiences' como valor por defecto.
    env = os.getenv("ENV_NAME", "transfersandexperiences")
    dotenv_path = os.path.join("envs", f"{env}.env")
    
    # Carga el archivo .env si existe.
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path, override=True)
    else:
        # Si no existe (común en producción/despliegues como Railway),
        # se asume que las variables están en el entorno del sistema.
        print(f"⚠️ WARNING: No se encontró el archivo de entorno '{dotenv_path}'. Se usarán las variables de entorno del sistema.")

from app import create_app  # Importar después de cargar las variables de entorno
app = create_app()

if __name__ == "__main__":
    # Configurar logging
    # Nota: en el f-string, usa comillas simples para evitar conflicto con las comillas dobles dentro de os.getenv
    logging.info("Application has started successfully! 🚀")
    logging.info(f"Project --> {env}")
    # Iniciar la app: try waitress (robusto en Windows), fallback a Flask
    try:
        from waitress import serve
        # waitress es multi-hilo por defecto; ajusta threads si lo necesitas
        serve(app, host="0.0.0.0", port=8000)
    except Exception as _waitress_err:
        logging.warning(f"Waitress not available or failed ({_waitress_err}), falling back to Flask dev server.")
        app.run(host="0.0.0.0", port=8000, threaded=True)
