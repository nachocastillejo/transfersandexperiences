from slackeventsapi import SlackEventAdapter
from flask import Flask, current_app, jsonify, request
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from .whatsapp_utils import send_message, get_text_message_input
from threading import Timer
import shelve
from datetime import datetime, timedelta
import logging
import time
import re
import os

DB_DIRECTORY = "db"
if not os.path.exists(DB_DIRECTORY):
    os.makedirs(DB_DIRECTORY)

def get_channel_id_by_name(client, channel_name, is_private=False):
    try:
        # El parámetro 'types' permite especificar el tipo de canales a listar.
        types = "private_channel" if is_private else "public_channel"
        response = client.conversations_list(types=types, limit=1000)
        channels = response["channels"]
        for channel in channels:
            # Compara ignorando mayúsculas y minúsculas
            if channel["name"].lower() == channel_name.lower():
                return channel["id"]
    except SlackApiError as e:
        print(f"Error al obtener la lista de canales: {e.response['error']}")

    return None

def fetch_all_messages(channel_id, client):
    messages = []
    cursor = None

    try:
        while True:
            response = client.conversations_history(
                channel=channel_id,
                cursor=cursor,
                limit=200  # Puedes ajustar este valor
            )
            messages.extend(response["messages"])

            if response.get("has_more"):
                cursor = response["response_metadata"]["next_cursor"]
            else:
                break

    except SlackApiError as e:
        print(f"Error al obtener mensajes del canal {channel_id}: {e.response['error']}")

    return messages

def get_history(wa_id, sender):
    client = WebClient(token=current_app.config['SLACK_TOKEN'])
    # Reemplaza con el nombre del canal (sin el símbolo '#')
    channel_name = f"{wa_id}_{sender}"
    channel_name = re.sub(r'[^a-z0-9_-]', '', channel_name.lower())

    # Especifica si es un canal privado o público
    is_private = False  # Cambia a True si el canal es privado

    channel_id = get_channel_id_by_name(client, channel_name, is_private=is_private)
    if not channel_id:
        logging.info(f"No se encontró el canal con el nombre '{channel_name}'.")
        return ""
    else:
        logging.info(f"El ID del canal '{channel_name}' es: {channel_id}")
        all_messages = fetch_all_messages(channel_id, client)
        logging.info(f"Se han obtenido {len(all_messages)} mensajes del canal '{channel_name}'.")

        # Seleccionamos los 10 mensajes más recientes (la API devuelve los mensajes de más reciente a menos reciente)
        latest_messages = all_messages[:10]

        # Invertimos el orden para que el mensaje más antiguo se imprima primero
        latest_messages_in_order = list(reversed(latest_messages))
        # También podrías usar: latest_messages_in_order = latest_messages[::-1]

        # Construimos un string único que contenga todos los mensajes, separados por saltos de línea.
        messages_text = ""
        for msg in latest_messages_in_order:
            if "text" in msg:
                messages_text += msg["text"] + "\n"

        # Imprime el string completo
        # print("\nÚltimos 10 mensajes (ordenados de antiguo a reciente):\n")
        # print(messages_text)
        return messages_text



# Conjunto para almacenar IDs de mensajes ya procesados
processed_messages = set()

def create_channel_if_it_doesnt_exist(channel_name, client):
    try:
        start_time_temp = time.time()
        # 1. Obtener la lista de canales
        response = client.conversations_list(types="public_channel", limit=1000)
        channel_names = [channel['name'] for channel in response['channels']]

        # 2. Verificar si el canal ya existe
        if channel_name in channel_names:
            pass
            # logging.info("Slack channel already exists.")
        else:
            # a. Crear canal
            response = client.conversations_create(name=channel_name, is_private=False)
            created_channel_id = response['channel']['id']
            logging.info(f"Creating Slack channel: {created_channel_id}")
            # b. Invitar usuarios
            response_invite = client.conversations_invite(channel=created_channel_id, users=current_app.config['SLACK_USER_IDS'])
            if response_invite['ok']:
                print("Usuarios invitados correctamente.")
            else:
                print(f"Error al invitar usuarios: {response_invite['error']}")
            logging.info(f"Slack channel created in -> {time.time() - start_time_temp :.2f} seconds\n")

    except SlackApiError as e:
        print(f"Error al interactuar con la API de Slack: {e.response['error']}")



def send_message_slack(wa_id, sender, question, response, bot_active=True):
    # If Slack is disabled, do nothing
    try:
        if not current_app.config.get('ENABLE_SLACK'):
            return
    except Exception:
        # In case current_app is not available, fail closed (no send)
        return
    start_time_temp = time.time()
    # logging.info(f"📤 4. Sending message to Slack")
    client = WebClient(token=current_app.config['SLACK_TOKEN'])
    channel_name = f"{wa_id}_{sender}"
    # Eliminar cualquier carácter que no sea alfanumérico o guion bajo
    channel_name = re.sub(r'[^a-z0-9_-]', '', channel_name.lower())
    create_channel_if_it_doesnt_exist(channel_name, client)
    if bot_active:
        message = f"*{sender}*: {question}\n\n*ByTheBot*: {response}\n----------------------------------"
    else:
        message = f"*{sender}*: {question}"
    client.chat_postMessage(channel=f"#{channel_name}", text=message)
    logging.info(f"📤 3. Message sent to slack in -> {time.time() - start_time_temp :.2f} seconds\n")

def create_slack_adapter(app: Flask):
    # Crea el adaptador de Slack con la clave de firma y la ruta deseada para recibir eventos de Slack
    slack_event_adapter = SlackEventAdapter(app.config["SIGNING_SECRET"], "/slack/events", app)    
    with app.app_context():
        client = WebClient(token=app.config['SLACK_TOKEN'])
    # Manejar el evento de mensaje de Slack
    @slack_event_adapter.on("message")
    def handle_slack_message(payload):
        event = payload.get("event", {})
        # logging.info(f"Tipo de evento recibido: {payload.get('type', 'Desconocido')}")
        # logging.info("Event RECEIVED in Slack")

        # print("EVENT: ", event)
        # print("Procceesed messages: ", processed_messages)
        user_id = event.get("user")
        channel_id = event.get("channel")
        message_text = event.get("text")
        client_msg_id = event.get("client_msg_id")
        # print("client_msg_id: ", client_msg_id)

        # Respuesta rápida
        response = jsonify({'status': 'received'})
        response.status_code = 200

        # Comprobar si el mensaje ya fue procesado
        if client_msg_id in processed_messages:
            return response  # Ignorar el mensaje duplicado

        # Agregar el ID del mensaje al conjunto de procesados
        processed_messages.add(client_msg_id)

        if user_id in current_app.config['SLACK_USER_IDS']:                
            # Intentar obtener el nombre del canal a partir del ID
            try:
                channel_info = client.conversations_info(channel=channel_id)
                channel_name = channel_info['channel']['name']
                print(f"Nombre del canal: {channel_name}")
                
                # Procesar el mensaje
                data = get_text_message_input(channel_name, message_text)
                send_message(data)
                # record_conversation(channel_name.split('_')[0], "", "SLACK", "", message_text)
                # record_summary(channel_name.split('_')[0], "SLACK", "", message_text)           

            except SlackApiError as e:
                print(f"Error al obtener información del canal: {e.response['error']}")

        return response  # Devolver respuesta rápida aquí
    
    # Manejar comandos de Slack
    @app.route('/slack/commands', methods=['POST'])
    def handle_slack_command():
        data = request.form
        command = data.get('command')
        channel_id = data.get('channel_id')

        if command == '/pararautomatizacion1min':
            channel_info = client.conversations_info(channel=channel_id)
            channel_name = channel_info['channel']['name']
            wa_id_part = channel_name.split('_')[0] # Extract the part used as key
            pause_automation(wa_id_part, 1)
            # Pass the correct key (wa_id_part) to resume_automation in the Timer
            Timer(60, lambda: resume_automation(wa_id_part)).start() 
            return jsonify(response_type="ephemeral", text="La automatización se ha pausado por 1 minuto."), 200

        elif command == '/reanudar':
            channel_info = client.conversations_info(channel=channel_id)
            channel_name = channel_info['channel']['name']
            resume_automation(channel_name.split('_')[0])
            return jsonify(response_type="ephemeral", text="La automatización se ha reanudado."), 200

        return jsonify(response_type="ephemeral", text="Comando no reconocido."), 400
    
    return slack_event_adapter


# Funciones para manejar el estado de la automatización
def is_automation_paused(channel_name):
    with shelve.open(os.path.join(DB_DIRECTORY, "automation_status_db")) as db:
        pause_info = db.get(f"paused_{channel_name}", None)
        if pause_info:
            # Verificar si la pausa ya expiró
            resume_time = pause_info.get("resume_time")
            if resume_time and datetime.now() >= resume_time:
                # La pausa ha expirado, reanudar automáticamente
                resume_automation(channel_name)
                return False
            return True
        return False

def pause_automation(channel_name, duration_minutes):
    with shelve.open(os.path.join(DB_DIRECTORY, "automation_status_db"), writeback=True) as db:
        resume_time = datetime.now() + timedelta(minutes=duration_minutes)
        db[f"paused_{channel_name}"] = {"paused": True, "resume_time": resume_time}
        print(f"Automatización pausada para el canal {channel_name} hasta {resume_time}.")

def resume_automation(channel_name):
    with shelve.open(os.path.join(DB_DIRECTORY, "automation_status_db"), writeback=True) as db:
        if f"paused_{channel_name}" in db:
            del db[f"paused_{channel_name}"]
            print(f"Automatización reanudada para el canal {channel_name}.")


# Ejemplos de Eventos de Slack
# EVENT:  {'user': 'U07NGSA7P0R', 'type': 'message', 'ts': '1735751278.997079', 'client_msg_id': '58284bda-d2ec-40da-9385-606ba8d7e37e', 'text': 'hey', 'team': 'T07N1A6EBC5', 'blocks': [{'type': 'rich_text', 'block_id': '7JVNO', 'elements': [{'type': 'rich_text_section', 'elements': [{'type': 'text', 'text': 'hey'}]}]}], 'channel': 'C086NE8B6TG', 'event_ts': '1735751278.997079', 'channel_type': 'channel'}
# EVENT:  {'user': 'U07NN4PUT7Y', 'type': 'message', 'ts': '1735751314.805169', 'bot_id': 'B07NULASXUK', 'app_id': 'A07NUCR2398', 'text': '*Nacho Castillejo*: hola\n\n*ByTheBot*: ¡Hola de nuevo! :wave: ¿Qué tal? Si necesitas información o tienes alguna pregunta, ¡aquí estoy para ayudarte! :blossom:\n----------------------------------', 'team': 'T07N1A6EBC5', 'bot_profile': {'id': 'B07NULASXUK', 'deleted': False, 'name': 'WA Bot App 2', 'updated': 1727198841, 'app_id': 'A07NUCR2398', 'icons': {'image_36': 'https://a.slack-edge.com/80588/img/plugins/app/bot_36.png', 'image_48': 'https://a.slack-edge.com/80588/img/plugins/app/bot_48.png', 'image_72': 'https://a.slack-edge.com/80588/img/plugins/app/service_72.png'}, 'team_id': 'T07N1A6EBC5'}, 'blocks': [{'type': 'rich_text', 'block_id': 'EE4', 'elements': [{'type': 'rich_text_section', 'elements': [{'type': 'text', 'text': 'Nacho Castillejo', 'style': {'bold': True}}, {'type': 'text', 'text': ': hola\n\n'}, {'type': 'text', 'text': 'ByTheBot', 'style': {'bold': True}}, {'type': 'text', 'text': ': ¡Hola de nuevo! '}, {'type': 'emoji', 'name': 'wave', 'unicode': '1f44b'}, {'type': 'text', 'text': ' ¿Qué tal? Si necesitas información o tienes alguna pregunta, ¡aquí estoy para ayudarte! '}, {'type': 'emoji', 'name': 'blossom', 'unicode': '1f33c'}, {'type': 'text', 'text': '\n----------------------------------'}]}]}], 'channel': 'C086NE8B6TG', 'event_ts': '1735751314.805169', 'channel_type': 'channel'}



