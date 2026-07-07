import os

# Anthropic
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Wazzup
WAZZUP_API_KEY       = os.getenv("WAZZUP_API_KEY",       "")
WAZZUP_CHANNEL_ID    = os.getenv("WAZZUP_CHANNEL_ID",    "")

# Wazzup channel IDs (hardcoded, не env — значение фиксировано)
WAZZUP_INSTAGRAM_CHANNEL_ID = "1e7bbcd8-62d2-459d-a863-06f4f5bf2860"

# EnvyCRM
ENVY_CRM_URL      = os.getenv("ENVY_CRM_URL",      "")   # https://shkolaobucheniya.envycrm.com
ENVY_API_KEY      = os.getenv("ENVY_API_KEY",       "")   # 6ba0dee6ca1df43f2eff1441912d9c6884e7dc01
ENVY_CUSTOM_URL   = os.getenv("ENVY_CUSTOM_URL",    "")
ENVY_CHANNEL_KEY  = os.getenv("ENVY_CHANNEL_KEY",   "")
ENVY_FORWARD_URL  = os.getenv("ENVY_FORWARD_URL",   "")
ENVY_OPERATOR_KEY = os.getenv("ENVY_OPERATOR_KEY",  "")
ENVY_FUNNEL_NAME  = os.getenv("ENVY_FUNNEL_NAME",   "Входящие")

# OpenAI (для эмбеддингов RAG)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Google Sheets (синхронизация базы знаний)
# GOOGLE_CREDENTIALS_JSON — весь JSON сервисного аккаунта одной строкой (Railway env var)
SHEETS_SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID", "1aN89a8YEqVbHsPW1hdKp1-Sgay7LhQD6")

# Railway / PostgreSQL
DATABASE_URL = os.getenv("DATABASE_URL", "")
PORT         = int(os.getenv("PORT", "8080"))

# Пауза бота по каналу (через env в Railway)
BOT_PAUSED_INSTAGRAM = os.getenv("BOT_PAUSED_INSTAGRAM", "false").lower() == "true"
