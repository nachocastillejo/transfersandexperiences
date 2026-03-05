from flask import Flask
from app.config import load_configurations, configure_logging
from .views import webhook_blueprint
from .utils.slack_utils import create_slack_adapter
from app.utils.database_utils import init_app as init_db_app
from app.utils.inactivity_scheduler import init_scheduler

def create_app():
    """
    Crea y configura la aplicación Flask.
    
    - Carga las configuraciones desde las variables de entorno.
    - Configura el sistema de logging.
    - Inicializa la integración con Slack.
    - Registra los blueprints de la aplicación.
    - Inicializa la base de datos.
    
    :return: Instancia de la aplicación Flask.
    """
    app = Flask(__name__)
    
    load_configurations(app)  # Carga las configuraciones del entorno
    app.secret_key = app.config.get('SECRET_KEY') # Set secret_key after loading configs
    configure_logging()  # Configura el logging de la aplicación
    # Inicializa la integración con Slack si está habilitada
    if app.config.get('ENABLE_SLACK'):
        create_slack_adapter(app)
        app.logger.info("Slack adapter enabled and registered.")
    else:
        app.logger.info("Slack adapter disabled by ENABLE_SLACK flag.")
    
    init_db_app(app) # Initialize the database with the app

    app.register_blueprint(webhook_blueprint)  # Registra los blueprints necesarios
    
    if app.config.get('ENABLE_DASHBOARD'):
        from app.dashboard import dashboards_bp  # Corrected blueprint variable name
        app.register_blueprint(dashboards_bp, url_prefix='/dashboard')
        app.logger.info("Dashboard enabled and registered.")
        # Log that dashboard auth is configured (without secrets)
        try:
            has_pass = bool(app.config.get('DASHBOARD_PASSWORD'))
            app.logger.info(f"Dashboard auth configured: user='{app.config.get('DASHBOARD_USERNAME', 'admin')}', password_set={has_pass}")
        except Exception:
            pass

    # Initialize inactivity-based CRM auto-upload scheduler
    try:
        init_scheduler(app)
    except Exception as _sched_err:
        app.logger.error(f"Failed to initialize inactivity scheduler: {_sched_err}")

    return app
