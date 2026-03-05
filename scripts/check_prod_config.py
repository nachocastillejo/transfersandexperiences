import os
import sys
import logging

# Configurar logging básico para ver errores
logging.basicConfig(level=logging.INFO)

# Agregar el directorio raíz al path para poder importar la app
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

print(f"🔍 Directorio raíz detectado: {project_root}")
print(f"🔍 Variable ENV_NAME en el sistema: {os.getenv('ENV_NAME')}")

# Listar archivos en envs/ para ver qué hay
envs_dir = os.path.join(project_root, 'envs')
if os.path.exists(envs_dir):
    print(f"📂 Archivos en {envs_dir}: {os.listdir(envs_dir)}")
else:
    print(f"❌ No existe la carpeta {envs_dir}")

try:
    from app import create_app
    app = create_app()
    
    print("\n--- Verificación de Configuración ---")
    token = app.config.get('VERIFY_TOKEN')
    print(f"✅ VERIFY_TOKEN cargado: '{token}'")
    print(f"✅ PHONE_NUMBER_ID: {app.config.get('PHONE_NUMBER_ID')}")
    print(f"✅ ACCESS_TOKEN: {'[CONFIGURADO]' if app.config.get('ACCESS_TOKEN') else '[VACÍO!!!]'}")
    
    if token == '12345':
        print("\n✨ El token es CORRECTO ('12345').")
    else:
        print(f"\n⚠️ ERROR: El token es '{token}', pero en Meta has puesto '12345'. No coinciden.")

    # Simular una petición de verificación (GET /webhook)
    with app.test_client() as client:
        params = {
            'hub.mode': 'subscribe',
            'hub.verify_token': '12345',
            'hub.challenge': 'test_challenge'
        }
        response = client.get('/webhook', query_string=params)
        print(f"\n--- Simulación de Validación Webhook ---")
        print(f"Status Code: {response.status_code}")
        print(f"Response Body: {response.data.decode()}")
        
        if response.status_code == 200 and response.data.decode() == 'test_challenge':
            print("🚀 LA VALIDACIÓN LOCAL FUNCIONA. El problema debe ser la URL en Meta o el servidor bloqueando peticiones externas.")
        else:
            print("❌ LA VALIDACIÓN LOCAL FALLÓ. Revisa la lógica en app/views.py.")

except Exception as e:
    print(f"\n❌ Error crítico durante el diagnóstico: {e}")
    import traceback
    traceback.print_exc()
