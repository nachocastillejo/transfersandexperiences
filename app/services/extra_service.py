from datetime import datetime, timedelta
import pytz
import locale

def add_current_date_to_question(question):
    zona_horaria = pytz.timezone("Europe/Madrid")

    # Intentar establecer la configuración regional en español
    try:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')  # Linux/Unix
    except locale.Error:
        try:
            locale.setlocale(locale.LC_TIME, 'es_ES')    # Alternativa
        except locale.Error:
            try:
                locale.setlocale(locale.LC_TIME, 'Spanish_Spain.1252')  # Windows
            except locale.Error:
                print("Locale en español no disponible. Se usará formato numérico.")

    # Obtener fecha y hora actual
    now = datetime.now(zona_horaria)

    # Formatear fecha en “30 de enero de 2025, a las 22:28”
    try:
        fecha_hora_str = now.strftime("%A %d de %B de %Y, a las %H:%M")
    except:
        fecha_hora_str = now.strftime("%d/%m/%Y, %H:%M")

    # Construir la pregunta con la fecha y hora
    question = f"(Hoy es {fecha_hora_str}) Usuario: {question}"
    return question

def add_dates_to_question(question):
    zona_horaria = pytz.timezone("Europe/Madrid")

    # Intentar establecer la configuración regional en español
    try:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')  # Linux/Unix
    except locale.Error:
        try:
            locale.setlocale(locale.LC_TIME, 'es_ES')    # Alternativa
        except locale.Error:
            try:
                locale.setlocale(locale.LC_TIME, 'Spanish_Spain.1252')  # Windows
            except locale.Error:
                print("Locale en español no disponible. Se usará formato numérico.")

    # Función para formatear fecha en “lunes 03 de febrero de 2025” (si locale en ES funciona)
    def formatear_fecha(fecha):
        try:
            return fecha.strftime("%A %d de %B de %Y")
        except:
            # Si falla locale, formato numérico “dd/mm/yyyy”
            return fecha.strftime("%d/%m/%Y")

    # Obtener fecha/hora actual
    now = datetime.now(zona_horaria)

    # Cálculo de días
    hoy = now
    manana = hoy + timedelta(days=1)
    pasado_manana = hoy + timedelta(days=2)

    # Convertir cada uno a texto
    hoy_str = formatear_fecha(hoy)
    manana_str = formatear_fecha(manana)
    pasado_manana_str = formatear_fecha(pasado_manana)

    # Generar automáticamente los días siguientes (desde el día +3 hasta el +6)
    dias_siguientes = []
    for i in range(3, 7):
        fecha_obj = hoy + timedelta(days=i)
        # Día de la semana en español (ej.: 'miércoles')
        nombre_dia = fecha_obj.strftime("%A")
        # Solo la fecha para este ejemplo: “5 de febrero de 2025”
        fecha_str = fecha_obj.strftime("%d de %B de %Y")
        # Añadir al listado la parte: “el miércoles es 5 de febrero de 2025”
        dias_siguientes.append(f"el {nombre_dia} es {fecha_str}")

    # Unir todo en una sola cadena
    # Añadimos los días siguientes separados por comas, excepto que podríamos añadir "y" antes del último
    # Para simplificar, usamos simplemente comas aquí.
    frase_dias_siguientes = ", ".join(dias_siguientes)

    fechas = (
        f"(Hoy es {hoy_str}, "
        f"mañana es {manana_str}, "
        f"pasado mañana es {pasado_manana_str}, "
        f"{frase_dias_siguientes})"
    )

    question = f"{fechas} " + question
    return question


# def check_image(data, recipient):
#     # Convertir el string JSON a diccionario de Python
#     data_dict = json.loads(data)
#     # Obtener el cuerpo del texto
#     text_body = data_dict.get("text", {}).get("body", "")
#     # Expresión regular para buscar URLs que terminen en .jpg o .png
#     url_pattern = r'(https?://\S+\.(?:jpg|png))|(https://drive\.usercontent\.google\.com/download\?id=\S+&.*)'

#     # Buscar todas las coincidencias de URLs
#     matches = re.findall(url_pattern, text_body)
#     urls = [match[0] if match[0] else match[1] for match in matches]
#     print(urls)
#     # Para cada URL encontrada
#     for url in urls:

#         print("hola")
#         print(url)
#         send_message(json.dumps(
#             {
#                 "messaging_product": "whatsapp",
#                 "to": recipient,
#                 "type": "image",
#                 "image": {"link": url},
#             })
#         )