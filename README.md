# Bot Multi-Integraciones con ByTheBot

Esta aplicación es un **bot construido sobre Flask utilizando el "Flask Factory Pattern". Además de gestionar mensajes en WhatsApp, integra funcionalidades avanzadas como comunicación con Slack, generación de respuestas mediante OpenAI, gestión de citas en Google Calendar y un panel de control (dashboard) para la visualización y gestión de conversaciones.

---

## Tabla de Contenidos

- [Estructura del Proyecto](#estructura-del-proyecto)
  - [app/dashboard/](#appdashboard)
- [Flujo de Ejecución del Bot](#flujo-de-ejecución-del-bot)
- [Ejecutando el Proyecto](#ejecutando-el-proyecto)
- [Resumen del Flujo Completo](#resumen-del-flujo-completo)
- [Licencia y Contacto](#licencia-y-contacto)

---

## Estructura del Proyecto

### app/
Este directorio es el núcleo de la aplicación y contiene todos los módulos esenciales para el funcionamiento de Flask. Aquí se centraliza la lógica de negocio, las configuraciones y las integraciones necesarias para que el bot opere de forma eficiente.

#### __init__.py
Inicializa la aplicación Flask utilizando el patrón de fábrica (Factory Pattern). Esto permite crear múltiples instancias de la aplicación, lo que facilita la realización de pruebas y la adaptación a diferentes configuraciones o entornos.

#### config.py
Se encarga de cargar todas las configuraciones del entorno en la aplicación. Extrae variables sensibles (como tokens y claves de acceso) y otros ajustes críticos, permitiendo que la aplicación se conecte a APIs externas y se ejecute de forma segura y personalizada.

#### decorators/
Almacena decoradores reutilizables que encapsulan funcionalidades comunes y ayudan a mantener el código limpio.  
- **security.py:** Incluye decoradores enfocados en la seguridad, como la validación de solicitudes entrantes, garantizando que solo se procesen peticiones legítimas.

#### services/
Reúne los servicios que se comunican con APIs externas y gestionan functionalities principales del bot.  
- **calendar_service.py:** Integra y administra eventos en Google Calendar. Ofrece funcionalidades avanzadas como la comprobación de disponibilidad (considerando horarios comerciales configurables, festivos y múltiples slots por intervalo), agendado y cancelación de citas, y envío de notificaciones por correo electrónico (con soporte multi-idioma) a los usuarios.
- **drive_service.py:** Gestiona la interacción con Google Drive, incluyendo Google Sheets y Google Docs. Permite registrar preguntas en Sheets, filtrar cursos desde una hoja de cálculo de cursos (utilizando la API de Google Sheets v4 para una correcta extracción de hipervínculos y con lógica avanzada de filtrado), y crear/actualizar documentos de Google Docs para registrar transcripciones de conversaciones.
- **openai_service.py:** Gestiona la comunicación con la API de OpenAI para la generación de respuestas. Utiliza la API `responses` de OpenAI (o una similar, gestionando `response_id` para la continuidad de la conversación), carga dinámicamente instrucciones y herramientas (definidas en archivos de configuración y habilitadas mediante variables de entorno), y maneja un sistema de llamadas a funciones (tool calls) con descubrimiento dinámico de procesadores. Incluye una característica para enviar mensajes preliminares al usuario antes de ejecutar acciones largas.
- **perplexity_service.py:** Se encarga de las interacciones con la API de Perplexity (usando el modelo `llama-3.1-sonar-small-128k-online`) para obtener respuestas basadas en información en tiempo real, añadiendo contexto de fecha/hora actual a las consultas y devolviendo citaciones.
- **extra_service.py:** Contiene funciones de utilidad para servicios, principalmente para añadir información contextual de fecha y hora (formateada en español) a los mensajes o preguntas que se procesarán.

#### utils/
Contiene funciones auxiliares y herramientas de soporte que simplifican tareas recurrentes en la aplicación.  
- **whatsapp_utils.py:** Proporciona funciones específicas para el procesamiento y formateo de mensajes de WhatsApp. Gestiona la lógica principal del procesamiento de mensajes entrantes, incluyendo la **transcripción de mensajes de audio mediante OpenAI Whisper**. Enriquece los mensajes con información de fecha (usando `app/services/extra_service.py`) y llama a `openai_service.py` para la generación de respuestas. Delega el envío de mensajes a `messaging_utils.py`. También incluye la función `resume_automation` que interactúa con `threads_db`.
- **slack_utils.py:** Administra la integración con Slack. Facilita el envío de notificaciones de interacciones de WhatsApp a canales de Slack (que se crean dinámicamente por conversación: `{wa_id}_{sender}`). Permite a los agentes responder a usuarios de WhatsApp directamente desde Slack. Gestiona comandos de barra para pausar (temporalmente o indefinidamente) y reanudar la automatización del bot para usuarios específicos, interactuando con `automation_status_db.dat`.
- **extra_utils.py:** Ofrece un conjunto expandido de funciones auxiliares críticas. Gestiona la **persistencia del `response_id` de OpenAI** en `responses_db.dat` (esencial para el flujo de conversación con la API `responses`). Carga y parsea configuraciones importantes como horarios de negocio (`BUSINESS_HOURS`), festivos (`HOLIDAYS`), las instrucciones personalizadas para el asistente de OpenAI (`instructions/{entorno}.txt`), y las definiciones de las herramientas (funciones) de OpenAI (`instructions/functions.json`, filtradas por la variable de entorno `OPENAI_FUNCTIONS`). También incluye utilidades para el manejo y formateo de fechas y detección de idioma (aunque esta última no esté prominentemente usada en el flujo principal actual).
- **messaging_utils.py:** Centraliza la comunicación de bajo nivel con la API de WhatsApp (Meta Graph API). Se encarga del envío de mensajes (texto, y potencialmente otros tipos como imágenes si se implementa) y notificaciones de estado (como el indicador de "escribiendo").
- **openai_functions.py:** Define las funciones específicas (conocidas como "tools" en el contexto de OpenAI) que el asistente de OpenAI puede solicitar ejecutar. Actúa como un puente entre las solicitudes de función del asistente y los servicios reales de la aplicación (ej., `calendar_service`, `drive_service`, `perplexity_service`). Cada función aquí procesa los argumentos proporcionados por el asistente y llama a la lógica de negocio correspondiente. También contiene una función `GPTRequest` para realizar llamadas directas a modelos GPT.

#### views.py
Define y organiza los endpoints principales mediante Blueprints, lo que permite una estructura modular y facilita la gestión de rutas y controladores dentro de la aplicación.

### app/dashboard/
Este directorio contiene el módulo para el panel de control (dashboard) de la aplicación, que permite visualizar métricas, supervisar conversaciones y realizar acciones administrativas.

#### __init__.py
Inicializa el Blueprint de Flask para el dashboard (`dashboards_bp`). Define filtros de plantilla personalizados para formatear timestamps en la interfaz de usuario del dashboard.

#### views.py
Define las rutas y la lógica para las diferentes secciones del dashboard:
- **Página Principal (`/`):** Muestra métricas generales del bot (total de mensajes, conversaciones, mensajes por dirección, tiempo promedio de respuesta del bot) y una línea de tiempo de la actividad de mensajes (diaria, semanal, mensual).
- **Interfaz de Conversaciones (`/conversations`, `/conversations/<wa_id>`):** Permite a los administradores ver una lista de todas las conversaciones. Al seleccionar una conversación específica, se pueden ver los mensajes intercambiados y se muestra el estado de automatización (pausada/activa).
- **Envío de Respuestas (`/conversations/<wa_id>/reply`):** Proporciona una interfaz para que un agente envíe mensajes directamente a un usuario de WhatsApp desde el dashboard.
- **Gestión de Automatización (`/conversations/<wa_id>/resume_bot`, `/conversations/<wa_id>/manual_pause`):** Permite pausar o reanudar manualmente la automatización del bot para un usuario específico.
- **API de Estado de Pausa (`/api/pause_status/<wa_id>`):** Endpoint para consultar el estado de pausa de la automatización para un usuario.

#### utils.py
Contiene funciones de utilidad para el dashboard:
- **Carga de Mensajes:** Lee y procesa los datos desde la base de datos SQLite (`db/conversations.db`) para obtener el historial de mensajes.
- **Agrupación de Mensajes:** Agrupa los mensajes por `wa_id` para reconstruir las conversaciones.
- **Generación de Resúmenes:** Crea resúmenes de conversaciones (último mensaje, conteo de mensajes, etc.).
- **Cálculo de Métricas:** Funciones para calcular las métricas que se muestran en la página principal del dashboard.
- **Gestión de Ventana de 24h:** Determina si la ventana de 24 horas de WhatsApp para respuestas libres está abierta.

#### static_dashboard/
Almacena archivos estáticos (CSS, JavaScript, imágenes) específicos para el dashboard.

#### templates/
Contiene las plantillas HTML (utilizando Jinja2) para renderizar las diferentes páginas del dashboard.

### config/
Este directorio almacena archivos JSON con las credenciales y configuraciones necesarias para integrar servicios de Google, simplificando la autenticación y el acceso a sus APIs.

### db/
Contiene archivos de bases de datos en formato `.dat` o `.bak` (gestionados con `shelve`) que se utilizan para almacenar estados persistentes:
- **threads_db.dat:** Usado por `whatsapp_utils.py` para el estado de pausa/reanudación de la automatización iniciado por el propio bot (uso menos frecuente o legado).
- **automation_status_db.dat:** Utilizado por `slack_utils.py` para gestionar el estado de pausa/reanudación de la automatización controlado por comandos de Slack.
- **responses_db.dat:** Utilizado por `extra_utils.py` para almacenar el `response_id` actual de la conversación con la API de OpenAI, crucial para mantener el contexto en el modelo de interacción por turnos.
Esto garantiza la persistencia de la información crítica del bot.

### docs/
Incluye documentación complementaria, notas y archivos README.md adicionales, proporcionando guías y detalles para desarrolladores y usuarios.

### envs/
Almacena archivos de variables de entorno para diferentes configuraciones, permitiendo personalizar cada proyecto. En estos archivos se definen:
- Nombre del proyecto.
- Tokens para la aplicación de Meta (WhatsApp).
- Tokens de OpenAI (`OPENAI_API_KEY`, `OPENAI_ASSISTANT_ID`).
- Tokens de Slack (`SLACK_TOKEN`, `SIGNING_SECRET`, `SLACK_USER_IDS`).
- Tokens y configuración de Google (`SERVICE_ACCOUNT_FILE`, `CALENDAR_ID`).
- Configuraciones para servicios:
    - `BUSINESS_HOURS`: JSON string definiendo horarios de atención y slots para `calendar_service`.
    - `HOLIDAYS`: JSON string con lista de fechas festivas para `calendar_service`.
    - `ROUNDING_INTERVAL`: Intervalo de redondeo para citas.
    - `MAIN_LANGUAGE`: Idioma principal para comunicaciones (ej. emails de calendario).
    - `SMTP_SERVER`, `SMTP_PORT`, `EMAIL_USER`, `EMAIL_PASSWORD`: Para envío de emails.
    - `OPENAI_FUNCTIONS`: Lista separada por comas de las funciones/tools de OpenAI habilitadas para el entorno.
    - `PERPLEXITY_API_KEY`.
Esto facilita la adaptación de la aplicación a distintos entornos sin modificar el código fuente.

### logs/
Almacena archivos de registro que capturan eventos de ejecución y mensajes del sistema. Estos logs son fundamentales para el monitoreo y la depuración de la aplicación.

### scripts/
Contiene scripts auxiliares diseñados para automatizar tareas de mantenimiento y operación:
- **always_on.py:** Probablemente se encarga de mantener la aplicación en ejecución continua.
- **csv_logger.py:** Gestiona el registro de eventos en formato CSV, lo que resulta útil para análisis y auditoría.

### Archivos Principales

- **run.py:** Es el archivo de entrada principal para iniciar la aplicación Flask. Se encarga de cargar las configuraciones, iniciar los servicios y levantar el servidor.
- **requirements.txt:** Lista todas las dependencias necesarias para instalar y ejecutar el proyecto, asegurando que el entorno de ejecución esté correctamente configurado.


---
---

## Flujo de Ejecución del Bot

El funcionamiento del bot se puede dividir en varias fases:

### 1. Inicialización

- **run.py**  
  - **Carga de Variables de Entorno:**  
    Se carga el archivo envs/{env}.env (donde {env} identifica el entorno, por ejemplo, draelenaberezo) para obtener configuraciones y tokens.
  - **Creación de la Aplicación:**  
    Se invoca create_app() (en app/__init__.py), que:
    - Carga las configuraciones (con load_configurations).
    - Configura el logging (con configure_logging).
    - Inicializa la integración con Slack (con create_slack_adapter).
    - Registra el Blueprint que expone el endpoint /webhook.

### 2. Recepción de Mensajes a través del Webhook

- **Endpoint /webhook:**
  - **GET:**  
    Se utiliza para verificar el estado del webhook (mediante un proceso _challenge_).
  - **POST:**  
    Recibe mensajes entrantes:
    - Valida la autenticidad de la solicitud usando @signature_required.
    - Distingue entre mensajes de WhatsApp y otros eventos.
    - Registra el mensaje y llama a handle_message_async para procesarlo de forma asíncrona.

### 3. Procesamiento Asincrónico del Mensaje

- **handle_message_async (en views.py):**  
  Lanza un **thread** (o proceso, en Linux) que llama a `handle_message`, pasando la instancia de la aplicación y el mensaje recibido.

- **handle_message:**  
  Dentro del contexto de la aplicación:
  - Se extraen datos importantes del mensaje (por ejemplo, el texto, número de teléfono, nombre, y **tipo de mensaje, incluyendo audio**).
  - Se invoca `process_whatsapp_message` (en `app/utils/whatsapp_utils.py`) para procesar el mensaje.

### 4. Procesamiento del Mensaje y Generación de Respuesta

- **process_whatsapp_message (en `app/utils/whatsapp_utils.py`):**
  - **Extracción y Pre-procesamiento:**  
    Se extraen datos del remitente (`wa_id`, nombre) y el contenido del mensaje.
    Si el mensaje es de **audio**, se descarga y transcribe usando OpenAI Whisper.
    El mensaje (texto original o transcrito) se enriquece con información de fecha/hora actual (usando `app/services/extra_service.py`).
  - **Verificación del Estado de Automatización:**  
    Se comprueba si la automatización está pausada para ese usuario (vía `slack_utils.py` y `automation_status_db.dat`). Si es así, se notifica a Slack y se detiene el procesamiento.
  - **Generación de Respuesta con OpenAI:**  
    Se llama a `generate_response` (definido en `app/services/openai_service.py`). Este proceso ha sido actualizado:
    - **Gestión de Conversación mediante `response_id`:** En lugar de la gestión explícita de hilos (threads) de OpenAI, se utiliza un `response_id` (almacenado en `responses_db.dat` por `app/utils/extra_utils.py`) para mantener el contexto de la conversación con la API de OpenAI (posiblemente la API `responses` o una similar).
    - **Carga Dinámica de Instrucciones y Herramientas:** Las instrucciones para el asistente y las definiciones de las herramientas (funciones) disponibles se cargan desde archivos de configuración (`instructions/`) y se habilitan según el entorno.
    - **Ejecución de Herramientas (Tool Calling):** Si el asistente solicita ejecutar una función, `openai_service.py` identifica la función y llama al procesador correspondiente en `app/utils/openai_functions.py`. Este módulo actúa como intermediario para ejecutar la lógica de negocio real (ej., interactuar con Google Calendar, Drive, o Perplexity). Se pueden enviar mensajes preliminares al usuario (ej., "Consultando disponibilidad...") antes de que la función se complete.
    - **Formateo de la Respuesta:** La respuesta de texto generada por el asistente se procesa (ej., adaptando el formato de markdown para WhatsApp con `process_text_for_whatsapp`) y se devuelve.

### 5. Envío de la Respuesta y Notificaciones

- **Envío a WhatsApp:**  
  La respuesta formateada se envía al usuario a través de la función `send_message` (en `app/utils/messaging_utils.py`), que interactúa directamente con la API de Meta Graph. Se pueden enviar indicadores de "escribiendo" (`send_typing_indicator` desde `messaging_utils.py`) para mejorar la UX. Se registran logs y se guarda información en un archivo CSV (a través de `scripts/csv_logger.py`).

- **Notificación a Slack:**  
  Se invoca send_message_slack (en slack_utils.py) para enviar un resumen del intercambio (mensaje original y respuesta) a un canal en Slack, lo que facilita la supervisión y auditoría.

### 6. Integraciones y Funciones Adicionales

- **Google Calendar (`app/services/calendar_service.py`):**  
  El bot puede:
  - **Verificar Disponibilidad y Agendar Citas:** De forma avanzada, considerando horarios comerciales (con múltiples slots), festivos, duración de la cita, y redondeo de horas. Propone alternativas si es necesario.
  - **Cancelar Citas.**
  - **Enviar Notificaciones por Correo Electrónico:** Se envían confirmaciones y cancelaciones de citas por email, con plantillas en español e inglés.
  
- **Google Drive (`app/services/drive_service.py`):**
  - **Gestión de Cursos (Google Sheets):** Lee y filtra información de cursos desde una hoja de cálculo, manejando correctamente hipervínculos gracias al uso de la API de Google Sheets v4.
  - **Registro de Preguntas (Google Sheets):** Guarda preguntas de los usuarios.
  - **Registro de Conversaciones (Google Docs):** Crea o actualiza documentos para almacenar el historial de interacciones.

- **Gestión de Estados y Logs:**  
  Se utilizan bases de datos locales (`.dat` gestionadas con `shelve`):
  - `automation_status_db.dat`: Para el estado de pausa de automatización controlado desde Slack.
  - `responses_db.dat`: Para el ID de la última respuesta de OpenAI, manteniendo el contexto de la conversación.
  - `threads_db.dat`: Su uso principal actual parece ser para la función `resume_automation` en `whatsapp_utils.py`, posiblemente un remanente o para una lógica de pausa específica.
  Los logs y el registro en CSV facilitan la depuración y el monitoreo de la aplicación.

---

## Ejecutando el Proyecto

Para iniciar la aplicación en un entorno de desarrollo, basta con ejecutar:

bash
python run.py

y en run.py o WSGI se elige el proyecto.env del que se leen las variables.

## Árbol de Directorios

├── app/
│   ├── __init__.py
│   ├── config.py
│   ├── decorators/
│   │   └── security.py
│   ├── services/
│   │   ├── calendar_service.py
│   │   ├── drive_service.py
│   │   ├── openai_service.py
│   │   ├── perplexity_service.py
│   │   └── extra_service.py
│   ├── utils/
│   │   ├── whatsapp_utils.py
│   │   ├── slack_utils.py
│   │   ├── extra_utils.py
│   │   ├── messaging_utils.py
│   │   └── openai_functions.py
│   ├── views.py
│   └── dashboard/
│       ├── __init__.py
│       ├── views.py
│       ├── utils.py
│       ├── static_dashboard/
│       └── templates/
├── config/
│   └── client_secret.json
├── db/
│   ├── threads_db.dat
│   ├── threads_db.bak
│   ├── automation_status_db.dat
│   └── responses_db.dat
├── docs/
├── envs/
│   └── <entornos>.env
├── logs/
├── scripts/
│   ├── always_on.py
│   └── csv_logger.py
├── run.py
├── requirements.txt
└── README.md
