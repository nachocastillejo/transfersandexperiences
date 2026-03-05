from datetime import datetime
import requests
import os
import pytz

def get_day_time_text():
    # Zona horaria de España
    spain_tz = pytz.timezone("Europe/Madrid")
    # Obtener la hora actual en la zona horaria de España
    now = datetime.now(spain_tz)
    # Formatear la fecha y hora en un texto corto
    day = now.strftime("%d")  # Día del mes
    month = now.strftime("%B")  # Nombre del mes en inglés
    year = now.strftime("%Y")  # Año
    time = now.strftime("%H:%M")  # Hora en formato 24 horas
    text = f" (Today in Málaga is {day} of {month} of {year} at {time})"
    return text

def generate_response_perplexity(message):
    PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")
    # Display the text in the prompt
    date = get_day_time_text()
    print(str(message + date))
    url = "https://api.perplexity.ai/chat/completions"
    print(url)

    payload = {
        "model": "llama-3.1-sonar-small-128k-online",
        "messages": [
            {
                "role": "system",
                "content": "Be precise and concise and answer in user language"
            },
            {
                "role": "user",
                "content": str(message + date)
            }
        ],
        "max_tokens": 1000,
        "temperature": 0.5,
        "top_p": 0.9,
        "search_domain_filter": ["perplexity.ai"],
        "return_images": False,
        "return_related_questions": False,
        "search_recency_filter": "month",
        "top_k": 0,
        "stream": False,
        "presence_penalty": 0,
        "frequency_penalty": 1
    }
    headers = {
        "Authorization": "Bearer " + PERPLEXITY_API_KEY,
        "Content-Type": "application/json"
    }

    try:
        response = requests.request("POST", url, json=payload, headers=headers)
        response_json = response.json()
        text_response = str(response_json["choices"][0]["message"]["content"])
        citations = str(response_json["citations"])
        
        final_response = (
            f"{text_response}\n\n"
            f"Citations:\n{citations}"
            )
    except:
        final_response = "Perplexity not working"
    return final_response
