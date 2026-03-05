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

# 2) VALORES DE PRODUCCIÓN
# Actualiza aquí si cambian los tokens
ACCESS_TOKEN="EAAcgjYL7vh8BPPqGnw31diUNvx7cES3O3ZC2LCnHZCflBV6fRg9ZCaGhIZA9JOAYZBzggjp6ZA3ZB43R36J2mVHzM9cRgqhppMBeDF0ygINxefpZBfSW1Rg4fOxMCRZAwZBeozxQhQEYYm3ves8qDhLRIFZClgR6eiJ3CJuujO7O6vPRU73ccyjqFsA9OMV6xz69NDWqAZDZD"
APP_SECRET="ad3949c248d22a30e79058e45f6386f2"
PHONE_NUMBER_ID="760980557098833"
WHATSAPP_BUSINESS_ACCOUNT_ID="23906409222373242"
WHATSAPP_FLOW_ID_INSCRIPCION="1558492295532781"
WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL="778766405119715"
OPENAI_API_KEY="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"
VERIFY_TOKEN="12345"

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

# 3) CREAR DIRECTORIOS
echo "==> Creando directorios..."
mkdir -p "$REPO_DIR/db" "$REPO_DIR/logs" "$REPO_DIR/envs"

# 4) CREAR ARCHIVO BASE si no existe
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

# 5) APLICAR VALORES
echo "==> Aplicando credenciales..."
upsert_env "ACCESS_TOKEN"                    "$ACCESS_TOKEN"                    "$TARGET_ENV_FILE"
upsert_env "APP_SECRET"                      "$APP_SECRET"                      "$TARGET_ENV_FILE"
upsert_env "PHONE_NUMBER_ID"                 "$PHONE_NUMBER_ID"                 "$TARGET_ENV_FILE"
upsert_env "WHATSAPP_BUSINESS_ACCOUNT_ID"   "$WHATSAPP_BUSINESS_ACCOUNT_ID"   "$TARGET_ENV_FILE"
upsert_env "OPENAI_API_KEY"                  "$OPENAI_API_KEY"                  "$TARGET_ENV_FILE"
upsert_env "VERIFY_TOKEN"                    "$VERIFY_TOKEN"                    "$TARGET_ENV_FILE"
upsert_env "WHATSAPP_FLOW_ID_INSCRIPCION"    "$WHATSAPP_FLOW_ID_INSCRIPCION"    "$TARGET_ENV_FILE"
upsert_env "WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL" "$WHATSAPP_FLOW_ID_INSCRIPCION_EMAIL" "$TARGET_ENV_FILE"

chmod 644 "$TARGET_ENV_FILE"
chmod 755 "$REPO_DIR/db" "$REPO_DIR/logs"

echo "✨ Listo. Variables configuradas:"
grep -E '^(ENV_NAME|PHONE_NUMBER_ID|VERIFY_TOKEN|OPENAI_MODEL_NAME)=' "$TARGET_ENV_FILE" || true
