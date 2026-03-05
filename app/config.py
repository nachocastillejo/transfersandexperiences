import sys
import os
import logging
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

# Cargar variables de entorno desde el archivo .env
load_dotenv()


def load_configurations(app):
    """
    Carga las configuraciones del entorno en la aplicación Flask.
    
    :param app: Objeto de la aplicación Flask.
    """
    # Forzar la carga del archivo .env de forma robusta.
    # Esto localiza el archivo .env en la raíz del proyecto, sin importar desde dónde se ejecute el script.
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    dotenv_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path=dotenv_path)

    # Additionally, attempt to load envs/{ENV_NAME}.env if present (helps when not started via run.py)
    try:
        envs_dir = os.path.join(project_root, 'envs')
        selected_env_name = os.getenv('ENV_NAME')
        if not selected_env_name:
            # If ENV_NAME wasn't set, try best-effort detection
            if os.path.isdir(envs_dir):
                env_files = [f for f in os.listdir(envs_dir) if f.endswith('.env')]
                if len(env_files) == 1:
                    selected_env_name = os.path.splitext(env_files[0])[0]
                else:
                    # Fallback sensible default used in this repo
                    if os.path.exists(os.path.join(envs_dir, 'transfersandexperiences.env')):
                        selected_env_name = 'transfersandexperiences'
        if selected_env_name:
            envs_file_path = os.path.join(envs_dir, f'{selected_env_name}.env')
            if os.path.exists(envs_file_path):
                # override=True ensures values from the env file are applied
                load_dotenv(dotenv_path=envs_file_path, override=True)
    except Exception:
        pass

    # Carga de variables de entorno. getenv devuelve 'None' si la variable no existe,
    # lo cual previene que la aplicación falle.
    app.config["ACCESS_TOKEN"] = os.getenv("ACCESS_TOKEN")
    app.config["YOUR_PHONE_NUMBER"] = os.getenv("YOUR_PHONE_NUMBER")
    app.config["APP_ID"] = os.getenv("APP_ID")
    app.config["APP_SECRET"] = os.getenv("APP_SECRET")
    app.config["RECIPIENT_WAID"] = os.getenv("RECIPIENT_WAID")
    app.config["VERSION"] = os.getenv("VERSION")
    app.config["PHONE_NUMBER_ID"] = os.getenv("PHONE_NUMBER_ID")
    app.config["VERIFY_TOKEN"] = os.getenv("VERIFY_TOKEN")
    app.config["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

    # --- Debugging: Log critical environment variables ---
    logging.info(f"DEBUG: ACCESS_TOKEN loaded: {'set' if app.config['ACCESS_TOKEN'] else 'not set'}")
    logging.info(f"DEBUG: VERSION loaded: {app.config['VERSION']}")
    logging.info(f"DEBUG: PHONE_NUMBER_ID loaded: {app.config['PHONE_NUMBER_ID']}")
    # --- End Debugging ---

    app.config["SLACK_TOKEN"] = os.getenv("SLACK_TOKEN")
    app.config["SIGNING_SECRET"] = os.getenv("SIGNING_SECRET")
    app.config["SLACK_USER_IDS"] = os.getenv("SLACK_USER_IDS")
    app.config["SPREADSHEET_NAME"] = os.getenv("SPREADSHEET_NAME")
    app.config["WORKSHEET_NAME"] = os.getenv("WORKSHEET_NAME")

    # Supabase settings
    app.config["SUPABASE_ON"] = os.getenv("SUPABASE_ON", "False").lower() == "true"
    app.config["SUPABASE_URL"] = os.getenv("SUPABASE_URL")
    app.config["SUPABASE_ANON_KEY"] = os.getenv("SUPABASE_ANON_KEY")
    app.config["SUPABASE_SERVICE_ROLE_KEY"] = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    allowed_emails_str = os.getenv("SUPABASE_ALLOWED_EMAILS", "")
    app.config["SUPABASE_ALLOWED_EMAILS"] = [email.strip().lower() for email in allowed_emails_str.split(',') if email.strip()]

    # Dashboard admin emails (comma-separated). Used to determine admin role.
    dashboard_admins_str = os.getenv("DASHBOARD_ADMINS", "")
    app.config["DASHBOARD_ADMINS"] = [email.strip().lower() for email in dashboard_admins_str.split(',') if email.strip()]


    # Casos con lógica específica
    app.config["ENV_NAME"] = os.getenv("ENV_NAME")
    # WhatsApp Template settings
    app.config["WHATSAPP_TEMPLATE_ENROLL"] = os.getenv("WHATSAPP_TEMPLATE_ENROLL", "cuestionario_inscripcion")
    app.config["WHATSAPP_TEMPLATE_LANG"] = os.getenv("WHATSAPP_TEMPLATE_LANG", "es_ES")
    app.config["WHATSAPP_TEMPLATE_COMPONENTS"] = os.getenv("WHATSAPP_TEMPLATE_COMPONENTS")
    app.config["WHATSAPP_TEMPLATE_FORCE_COMPONENTS"] = os.getenv("WHATSAPP_TEMPLATE_FORCE_COMPONENTS", "false").lower() == "true"
    app.config["WHATSAPP_TEMPLATE_ALLOW_ALT_LANG"] = os.getenv("WHATSAPP_TEMPLATE_ALLOW_ALT_LANG", "false").lower() == "true"
    # Input decoration mode: 'long' (multi-day), 'short' (today only), 'false' (disabled)
    _dates_mode = (os.getenv("DATES_IN_INPUT", "long") or "").strip().lower()
    if _dates_mode in ("false", "off", "0", "no"):
        app.config["DATES_IN_INPUT"] = "false"
    elif _dates_mode in ("short", "today", "hoy"):
        app.config["DATES_IN_INPUT"] = "short"
    else:
        # Default to 'long' if anything else
        app.config["DATES_IN_INPUT"] = "long"
    # WhatsApp Flows configuration (used when template CTA is a Flow)
    app.config["WHATSAPP_FLOW_ID_INSCRIPCION"] = os.getenv("WHATSAPP_FLOW_ID_INSCRIPCION")
    app.config["WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL"] = os.getenv("WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL")
    app.config["WHATSAPP_FLOW_TOKEN"] = os.getenv("WHATSAPP_FLOW_TOKEN")
    app.config["WHATSAPP_FLOW_CTA"] = os.getenv("WHATSAPP_FLOW_CTA", "Abrir formulario")
    app.config["WHATSAPP_FLOW_ACTION"] = os.getenv("WHATSAPP_FLOW_ACTION", "navigate")
    app.config["WHATSAPP_FLOW_MESSAGE_VERSION"] = os.getenv("WHATSAPP_FLOW_MESSAGE_VERSION", "3")
    app.config["WHATSAPP_FLOW_ACTION_SCREEN"] = os.getenv("WHATSAPP_FLOW_ACTION_SCREEN")
    app.config["WHATSAPP_FLOW_ACTION_PAYLOAD_JSON"] = os.getenv("WHATSAPP_FLOW_ACTION_PAYLOAD_JSON")
    # WhatsApp Business Account (WABA) ID, usado para operaciones como listar plantillas
    app.config["WHATSAPP_BUSINESS_ACCOUNT_ID"] = os.getenv("WHATSAPP_BUSINESS_ACCOUNT_ID")
    # La construcción de CLIENT_SECRET_FILE se mueve a drive_service.py
    
    app.config["ENABLE_DASHBOARD"] = os.getenv('ENABLE_DASHBOARD', 'False').lower() == 'true'
    # Feature flags
    app.config["ENABLE_SLACK"] = os.getenv('ENABLE_SLACK', 'False').lower() == 'true'

    # Dashboard basic auth credentials
    app.config["DASHBOARD_USERNAME"] = os.getenv("DASHBOARD_USERNAME", "admin")
    app.config["DASHBOARD_PASSWORD"] = os.getenv("DASHBOARD_PASSWORD")

    # Secret key for session management (e.g., flash messages)
    app.config['SECRET_KEY'] = os.getenv('DASHBOARD_SECRET_KEY')
    if not app.config['SECRET_KEY']:
        app.config['SECRET_KEY'] = os.urandom(32) # Fallback for dev if not set, 32 bytes is strong
        env_name = os.getenv('ENV_NAME', 'development')
        if env_name.lower() not in ['dev', 'development', 'local']:
            if app.logger:
                app.logger.critical("CRITICAL: DASHBOARD_SECRET_KEY environment variable is NOT SET. Using an insecure, temporary key.")
            else:
                logging.critical("CRITICAL: DASHBOARD_SECRET_KEY environment variable is NOT SET. Using an insecure, temporary key.")
        else:
            if app.logger:
                app.logger.warning("DASHBOARD_SECRET_KEY environment variable not set. Using a random key for this session (development only).")
            else:
                logging.warning("DASHBOARD_SECRET_KEY environment variable not set. Using a random key for this session (development only).")

    # CRM Auto-upload inactivity window (in minutes). Accepts numbers, or strings like '5', '5m', '0.5h'.
    app.config["CRM_AUTO_UPLOAD_INACTIVITY_MINUTES"] = os.getenv("CRM_AUTO_UPLOAD_INACTIVITY_MINUTES", "5")

    # Unified auto-reset inactivity window (minutes) for OpenAI conversation and enrollment context.
    # Accepts numbers or strings like '5', '5m', '0.5h', '2h'. Empty disables the feature.
    # Backward compatibility: if not set, fall back to prior specific keys.
    unified_reset = os.getenv("AUTO_RESET_INACTIVITY_MINUTES", "")
    if not (unified_reset or "").strip():
        # Fallback order: OPENAI_ first, then ENROLLMENT_
        unified_reset = os.getenv("OPENAI_AUTO_RESET_INACTIVITY_MINUTES", "") or os.getenv("ENROLLMENT_AUTO_RESET_INACTIVITY_MINUTES", "")
    app.config["AUTO_RESET_INACTIVITY_MINUTES"] = unified_reset

def configure_logging():
    """
    Configura el sistema de logging de la aplicación, incluyendo rotación de logs y salida en consola.
    """
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, f"{os.getenv('ENV_NAME')}_app.log")
    
    logger = logging.getLogger()
    
    # Limpiamos los handlers existentes para evitar logs duplicados.
    # Esto ocurre si el logging se inicializa antes de llamar a esta función.
    if logger.hasHandlers():
        logger.handlers.clear()
        
    logger.setLevel(logging.INFO)
    
    log_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )    
    # Configuración del archivo de log con rotación automática
    file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(log_format)
    
    # Configuración del stream handler para salida en consola
    stream_handler = logging.StreamHandler(sys.stdout)
    try:
        stream_handler.stream.reconfigure(encoding="utf-8")  # Python 3.7+
    except AttributeError:
        pass  # En versiones antiguas no es necesario
    
    stream_handler.setFormatter(log_format)
    
    # Silenciar logs demasiado verbosos de librerías externas
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.WARNING)
    logging.getLogger('oauth2client').setLevel(logging.WARNING)

    # Mantener los logs de la app en INFO, pero los de werkzeug (peticiones HTTP) en WARNING
    # Esto asegura que nuestros logs personalizados (como el monitor) se muestren.
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logger.setLevel(logging.INFO) # Aseguramos que el logger raíz de la app sí captura INFO
    
    # Añadir handlers al logger raíz
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    