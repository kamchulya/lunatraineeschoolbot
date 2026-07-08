import os

# Anthropic
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Wazzup
WAZZUP_API_KEY       = os.getenv("WAZZUP_API_KEY",       "")
WAZZUP_CHANNEL_ID    = os.getenv("WAZZUP_CHANNEL_ID",    "")

# Wazzup channel IDs (hardcoded, не env — значение фиксировано)
WAZZUP_INSTAGRAM_CHANNEL_ID = "1e7bbcd8-62d2-459d-a863-06f4f5bf2860"

# Эскалация триггерных тем (перенос/возврат/оплата/сертификат) — направляем клиента
# на личный WhatsApp Артёма, пока не построен отдельный WhatsApp-бот школы.
ARTYOM_WHATSAPP_PERSONAL = "+77713974199"
# Рабочий WABA-номер, привязанный к CRM/Wazzup — задел на будущее, когда появится
# полноценный WhatsApp-бот школы (по аналогии с Лолой). Пока не используется в коде.
ARTYOM_WHATSAPP_WABA = "+77471494815"
# Wazzup channelId активного WhatsApp-канала (+77471494815, transport=wapi, state=active).
# Через него бот шлёт Артёму уведомления напрямую в WhatsApp.
WAZZUP_WHATSAPP_CHANNEL_ID = "ac2d5b3f-bc14-4465-9134-5bc81eb0d736"

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
SHEETS_SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID", "13zyjRX1e_Z3x92PUWLKoMynGMFBWzvZS9oPNm7XRQ_Y")

# Railway / PostgreSQL
DATABASE_URL = os.getenv("DATABASE_URL", "")
PORT         = int(os.getenv("PORT", "8080"))

# Пауза бота по каналу (через env в Railway)
BOT_PAUSED_INSTAGRAM = os.getenv("BOT_PAUSED_INSTAGRAM", "false").lower() == "true"
