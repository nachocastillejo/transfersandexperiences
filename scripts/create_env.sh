#!/usr/bin/env bash
set -eu
set -o pipefail 2>/dev/null || true

# ==============================================================================
# Configuración de entorno para Transfers & Experiences
# ==============================================================================

# 1) RUTAS
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/.." && pwd -P)}"
TARGET_ENV_FILE="$REPO_DIR/envs/transfersandexperiences.env"

echo "==> Repositorio: $REPO_DIR"
echo "==> Archivo destino: $TARGET_ENV_FILE"

# 2) CARGAR SECRETOS LOCALES
# Crea 'scripts/secrets.sh' en el servidor con los valores reales.
# Ese archivo NO se sube a Git (está en .gitignore).
SECRETS_FILE="$SCRIPT_DIR/secrets.sh"
if [ -f "$SECRETS_FILE" ]; then
    echo "🔐 Cargando secretos desde $SECRETS_FILE..."
    source "$SECRETS_FILE"
else
    echo "⚠️  No se encontró $SECRETS_FILE"
    echo "    Crea ese archivo con tus credenciales reales (ver instrucciones abajo)"
fi

# 3) VALORES (se sobreescriben si vienen de secrets.sh)
ACCESS_TOKEN="${ACCESS_TOKEN:-CAMBIAR_POR_EL_REAL}"
APP_SECRET="${APP_SECRET:-CAMBIAR_POR_EL_REAL}"
PHONE_NUMBER_ID="${PHONE_NUMBER_ID:-746827528508620}"
WHATSAPP_BUSINESS_ACCOUNT_ID="${WHATSAPP_BUSINESS_ACCOUNT_ID:-23906409222373242}"
WHATSAPP_FLOW_ID_INSCRIPCION="${WHATSAPP_FLOW_ID_INSCRIPCION:-1558492295532781}"
WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL="${WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL:-778766405119715}"
OPENAI_API_KEY="${OPENAI_API_KEY:-CAMBIAR_POR_EL_REAL}"
VERIFY_TOKEN="${VERIFY_TOKEN:-12345}"

# Función para insertar o actualizar variable en el .env
upsert_env() {
  local var="$1" val="$2" file="$3"
  mkdir -p "$(dirname "$file")"
  touch "$file"
  awk -v var="$var" -v val="$val" '
    BEGIN{updated=0}
    $0 ~ "^"var"=" {print var"=\""val"\""; updated=1; next}
    {print}
    END{if(!updated) print var"=\""val"\""}
  ' "$file" > "$file.tmp" && mv "$file.tmp" "$file"
}

# 4) CREAR DIRECTORIOS
echo "==> Creando directorios..."
mkdir -p "$REPO_DIR/db" "$REPO_DIR/logs" "$REPO_DIR/envs"

# 5) CREAR ARCHIVO BASE si no existe
if [ ! -f "$TARGET_ENV_FILE" ]; then
  echo "==> Creando $TARGET_ENV_FILE desde plantilla..."
  cat > "$TARGET_ENV_FILE" << 'EOF'
# VARIABLES DE BY THE BOT
ENV_NAME="transfersandexperiences"

# DASHBOARD
ENABLE_DASHBOARD=False
DASHBOARD_SECRET_KEY="1234"
DASHBOARD_USERNAME="admin"
DASHBOARD_PASSWORD="btb_Admin_2025!"
USE_SUPABASE_COURSES=false
SUPABASE_ON=False

# APP SETTINGS
MAIN_LANGUAGE="spanish"

# META
VERSION="v23.0"

# OPENAI
OPENAI_FUNCTIONS=""
OPENAI_MODEL_NAME="gpt-4o-mini"
OPENAI_FAST_MODEL_NAME="gpt-4o-mini"

# AUTO-RESET
CRM_AUTO_UPLOAD_INACTIVITY_MINUTES=""
AUTO_RESET_INACTIVITY_MINUTES=72h
EOF
fi

# 6) APLICAR VALORES
echo "==> Aplicando credenciales..."
upsert_env "ACCESS_TOKEN"                       "$ACCESS_TOKEN"                       "$TARGET_ENV_FILE"
upsert_env "APP_SECRET"                         "$APP_SECRET"                         "$TARGET_ENV_FILE"
upsert_env "PHONE_NUMBER_ID"                    "$PHONE_NUMBER_ID"                    "$TARGET_ENV_FILE"
upsert_env "WHATSAPP_BUSINESS_ACCOUNT_ID"      "$WHATSAPP_BUSINESS_ACCOUNT_ID"      "$TARGET_ENV_FILE"
upsert_env "OPENAI_API_KEY"                     "$OPENAI_API_KEY"                     "$TARGET_ENV_FILE"
upsert_env "VERIFY_TOKEN"                       "$VERIFY_TOKEN"                       "$TARGET_ENV_FILE"
upsert_env "WHATSAPP_FLOW_ID_INSCRIPCION"       "$WHATSAPP_FLOW_ID_INSCRIPCION"       "$TARGET_ENV_FILE"
upsert_env "WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL" "$WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL" "$TARGET_ENV_FILE"

chmod 644 "$TARGET_ENV_FILE"
chmod 755 "$REPO_DIR/db" "$REPO_DIR/logs"

echo ""
echo "✨ Listo. Variables configuradas:"
grep -E '^(ENV_NAME|PHONE_NUMBER_ID|VERIFY_TOKEN|OPENAI_MODEL_NAME)=' "$TARGET_ENV_FILE" || true
echo ""
echo "📌 Recuerda: los secretos reales deben estar en scripts/secrets.sh (gitignoreado)"
