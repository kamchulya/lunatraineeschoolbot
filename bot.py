import asyncio
import hashlib
import json
import os
import re
import logging
import random
from collections import deque, OrderedDict
from datetime import datetime, timezone, timedelta

import io

import aiohttp
import asyncpg
import anthropic
import httpx
from knowledge_base import get_knowledge_base
import sheets_sync
from rag import init_rag, get_context
import openai
from aiohttp import web

from config import (
    ANTHROPIC_API_KEY,
    WAZZUP_API_KEY,
    WAZZUP_WHATSAPP_CHANNEL_ID,
    WAZZUP_INSTAGRAM_CHANNEL_ID,
    OPENAI_API_KEY,
    ENVY_OPERATOR_KEY,
    ENVY_API_KEY,
    ENVY_CRM_URL,
    DATABASE_URL,
    PORT,
    BOT_PAUSED_WHATSAPP,
    BOT_PAUSED_INSTAGRAM,
    SIPUNI_USER,
    SIPUNI_SECRET,
    SIPUNI_CALLS_ENABLED,
    SIPUNI_EMPLOYEE_SIPNUMBERS,
)
from prompt import SYSTEM_PROMPT, WHATSAPP_PROMPT

REAL_MANAGERS = [
    1165916,  # Расул Ильясов
    1166309,  # Тамирлан Бауржанов
    1109958,  # Джони
    1158023,  # Кайратулы Нурзат
    1164185,  # Жайсангалиева Диляра
    1163510,  # Каирлымова Дамира
    1088614,  # Сейфуллина Алуа
]

# ---------- States ----------
STATE_NEW     = "new"
STATE_ACTIVE  = "active"
STATE_DONE    = "done"
STATE_MANAGER = "manager"
STATE_REFUSED = "refused"
STATE_SMM     = "smm"

SILENT_STATES = {STATE_DONE, STATE_MANAGER, STATE_REFUSED, STATE_SMM}


CLAUDE_FALLBACK = {
    "ru": "Извините, небольшой сбой. Напишите позже или менеджер свяжется с Вами 😊",
    "kz": "Кешіріңіз, қате болды. Кейінірек жазыңыз 😊",
}

THANKS_MSGS = {
    "ru": "Спасибо! Передала Ваш номер менеджеру — скоро свяжутся 🙌",
    "kz": "Рахмет! Нөміріңізді менеджерімізге бердім, жақын арада байланысады 🙌",
}

FAREWELL_MSGS = {
    "ru": "Хорошо, не буду беспокоить 😊 Если надумаете — всегда рады помочь!",
    "kz": "Жақсы, мазаламаймын 😊 Ойланып қалсаңыз — әрқашан қош келдіңіз!",
}

REFUSE_WORDS = [
    # RU
    "не надо", "не интересно", "нет спасибо", "не хочу", "не нужно",
    # KZ
    "қажет емес", "жоқ рахмет",
    # EN
    "no thanks", "not interested", "don't need",
]

PHONE_RE = re.compile(
    r'(?:\+7|8|\b7)[\s\-\(\)]*\d{3}[\s\-\(\)]*\d{3}[\s\-]*\d{2}[\s\-]*\d{2}'
    r'|\b\d{10,11}\b'
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

db_pool: asyncpg.Pool | None = None
processed_message_ids: deque = deque(maxlen=1000)
call_triggered_chat_ids: deque[str] = deque(maxlen=5000)
sent_texts: dict[str, dict[str, datetime]] = {}  # chat_id → {text: added_at}
dialog_locks: OrderedDict = OrderedDict()
last_notify: dict[str, datetime] = {}  # chat_id → время последнего notify_manager
last_bot_reply: dict[str, str] = {}   # chat_id → последний текст, отправленный Лолой

ASTANA_TZ = timezone(timedelta(hours=5))  # Казахстан UTC+5, без перехода на летнее время
# Авто-пауза WhatsApp по ночному расписанию (23:00–09:00 Алматы). Отдельный
# флаг от ручного BOT_PAUSED_WHATSAPP (env) — объединяются через OR в
# envy_hook_handler, поэтому эта задача никогда не снимает ручную дневную
# паузу раньше времени, а только управляет своим собственным флагом.
whatsapp_auto_paused = False


def should_notify(chat_id: str, cooldown_seconds: int = 300) -> bool:
    now = datetime.now(timezone.utc)
    last = last_notify.get(chat_id)
    if last and (now - last).total_seconds() < cooldown_seconds:
        return False
    if len(last_notify) >= 10000:
        oldest_key = min(last_notify, key=last_notify.get)
        del last_notify[oldest_key]
    last_notify[chat_id] = now
    return True


def get_lock(chat_id: str) -> asyncio.Lock:
    if chat_id not in dialog_locks:
        if len(dialog_locks) >= 10000:
            dialog_locks.popitem(last=False)
        dialog_locks[chat_id] = asyncio.Lock()
    return dialog_locks[chat_id]


# ---------- DB helpers ----------
async def get_state(chat_id: str) -> tuple[str | None, list, datetime | None, int | None]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, history, updated_at, deal_id FROM dialogs WHERE chat_id=$1", chat_id
        )
    if row:
        history = json.loads(row["history"]) if row["history"] else []
        return row["state"], history, row["updated_at"], row["deal_id"]
    return None, [], None, None


async def set_state(
    chat_id: str,
    state: str,
    history: list | None = None,
    deal_id: int | None = None,
) -> None:
    history_json = json.dumps(history, ensure_ascii=False) if history is not None else None
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO dialogs (chat_id, state, history, deal_id, updated_at, awaiting_reply)
            VALUES ($1, $2, $3::jsonb, $4, NOW(), FALSE)
            ON CONFLICT (chat_id) DO UPDATE
                SET state          = EXCLUDED.state,
                    history        = COALESCE(EXCLUDED.history, dialogs.history),
                    deal_id        = COALESCE(EXCLUDED.deal_id, dialogs.deal_id),
                    updated_at     = NOW(),
                    awaiting_reply = FALSE
            """,
            chat_id, state, history_json, deal_id,
        )


async def set_state_guarded(
    chat_id: str,
    state: str,
    history: list | None = None,
    deal_id: int | None = None,
) -> None:
    """Like set_state but won't overwrite a row already in SILENT_STATES."""
    history_json = json.dumps(history, ensure_ascii=False) if history is not None else None
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO dialogs (chat_id, state, history, deal_id, updated_at, awaiting_reply)
            VALUES ($1, $2, $3::jsonb, $4, NOW(), FALSE)
            ON CONFLICT (chat_id) DO UPDATE
                SET state          = EXCLUDED.state,
                    history        = COALESCE(EXCLUDED.history, dialogs.history),
                    deal_id        = COALESCE(EXCLUDED.deal_id, dialogs.deal_id),
                    updated_at     = NOW(),
                    awaiting_reply = FALSE
                WHERE dialogs.state NOT IN ('manager', 'done', 'refused', 'smm')
                   OR dialogs.updated_at <= NOW() - INTERVAL '30 minutes'
            """,
            chat_id, state, history_json, deal_id,
        )
        # command tag вида "INSERT 0 N" — N=0 означает, что WHERE заблокировал
        # UPDATE на конфликте (строка уже была manager/done/refused/smm), и
        # ни state, ни history в БД в этот раз НЕ сохранились
        try:
            affected = int(result.rsplit(" ", 1)[-1])
        except (ValueError, AttributeError):
            affected = None
        if affected == 0:
            row = await conn.fetchrow(
                "SELECT state, updated_at FROM dialogs WHERE chat_id=$1", chat_id
            )
            log.warning(
                f"⚠️ set_state_guarded заблокировал запись для {chat_id} "
                f"(попытка state={state!r}) — в БД state={row['state'] if row else None!r} "
                f"updated_at={row['updated_at'] if row else None}, "
                f"история диалога за это сообщение не сохранена"
            )


async def save_pending_message(chat_id: str, text: str) -> None:
    """Сохраняет сообщение клиента, пришедшее пока бот молчит (STATE_MANAGER, таймаут ещё не истёк).
    Специально НЕ трогает updated_at — иначе сбросится таймер ожидания менеджера,
    и фоновая задача resume_unanswered_manager_chats никогда не подхватит этот чат."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT history FROM dialogs WHERE chat_id=$1", chat_id)
        history = json.loads(row["history"]) if row and row["history"] else []
        history.append({"role": "user", "content": text})
        history = history[-MAX_HISTORY:]
        await conn.execute(
            "UPDATE dialogs SET history=$1::jsonb, awaiting_reply=TRUE WHERE chat_id=$2",
            json.dumps(history, ensure_ascii=False), chat_id,
        )


async def clear_awaiting_reply(chat_id: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE dialogs SET awaiting_reply=FALSE WHERE chat_id=$1", chat_id)


async def save_deal_id(chat_id: str, deal_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE dialogs SET deal_id = $1 WHERE chat_id = $2",
            deal_id, chat_id,
        )


# ---------- Wazzup API (3 попытки: 0 / 2 / 4 сек) ----------
async def send_wazzup(chat_id: str, text: str) -> None:
    url = "https://api.wazzup24.com/v3/message"
    headers = {
        "Authorization": f"Bearer {WAZZUP_API_KEY}",
        "Content-Type": "application/json",
    }
    if chat_id.startswith("wapp-"):
        channel_id = WAZZUP_WHATSAPP_CHANNEL_ID
        chat_type = "whatsapp"
        chat_id = chat_id[5:]  # strip wapp- prefix for Wazzup API
    else:
        channel_id = WAZZUP_INSTAGRAM_CHANNEL_ID
        chat_type = "instagram"
    # chatId — всегда username как есть (строка), никогда не конвертировать
    body = {
        "channelId": channel_id,
        "chatId": chat_id,
        "chatType": chat_type,
        "text": text,
    }
    delays = [0, 2, 4]
    for attempt, delay in enumerate(delays):
        if delay:
            await asyncio.sleep(delay)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=body, headers=headers) as resp:
                    result = await resp.text()
                    log.info(
                        f"📤 Wazzup → {chat_id} attempt={attempt + 1} "
                        f"[{resp.status}]: {result[:200]}"
                    )
                    if resp.status < 500:
                        now = datetime.now(timezone.utc)
                        bucket = sent_texts.setdefault(chat_id, {})
                        expired = [t for t, ts in bucket.items() if (now - ts).total_seconds() > 3600]
                        for t in expired:
                            del bucket[t]
                        if len(bucket) >= 1000:
                            oldest = sorted(bucket, key=lambda t: bucket[t])[:len(bucket) - 999]
                            for t in oldest:
                                del bucket[t]
                        bucket[text] = now
                        return
                    log.warning(f"⚠️ Wazzup attempt {attempt + 1} status={resp.status}")
        except Exception as e:
            log.warning(f"⚠️ Wazzup attempt {attempt + 1} error: {e}")
    log.error(f"❌ Wazzup: все 3 попытки провалились для {chat_id}")


# ---------- EnvyCRM API ----------
async def find_lead(username: str, phone: str | None = None, retries: int = 3, delay: float = 3.0) -> int | None:
    url = f"{ENVY_CRM_URL}/openapi/v1/lead/list?api_key={ENVY_API_KEY}"
    headers = {"Content-Type": "application/json"}
    body = {"limit": 1, "inputs": {"phone": phone}} if phone else {"limit": 1, "keyword": username}
    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=body, headers=headers) as resp:
                    raw = await resp.text()
                    log.info(f"🔍 find_lead attempt={attempt+1} raw [{resp.status}]: {raw[:300]}")
                    data = json.loads(raw) if raw else {}
                    leads_data = data.get("leads") or {}
                    result = leads_data.get("result") or []
                    if result:
                        lead_id = result[0]["id"]
                        log.info(f"🔍 find_lead username={username} phone={phone} → lead_id={lead_id}")
                        return lead_id
                    all_ids = leads_data.get("all_ids") or []
                    if all_ids and attempt == retries - 1:
                        log.warning(f"⚠️ find_lead: result пуст, но all_ids есть ({all_ids[0]}), используем как fallback")
                        return all_ids[0]
        except Exception as e:
            log.error(f"❌ find_lead error attempt={attempt+1}: {e}")
        if attempt < retries - 1:
            await asyncio.sleep(delay)
    log.warning(f"⚠️ find_lead: лид не найден после {retries} попыток для username={username}")
    return None


async def create_lead_log(lead_id: int, comment: str) -> None:
    try:
        url = f"{ENVY_CRM_URL}/openapi/v1/lead/log/create?api_key={ENVY_API_KEY}"
        headers = {"Content-Type": "application/json"}
        body = {"lead_id": lead_id, "type_id": 10, "data": {"comment": comment}}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body, headers=headers) as resp:
                result = await resp.text()
                log.info(f"📝 create_lead_log lead_id={lead_id} [{resp.status}]: {result[:200]}")
    except Exception as e:
        log.error(f"❌ create_lead_log error: {e}")


async def lead_to_inbox(lead_id: int, chat_id: str, known_deal_id: int | None = None) -> None:
    try:
        headers = {"Content-Type": "application/json"}
        deal_id = known_deal_id

        if not deal_id:
            url1 = f"{ENVY_CRM_URL}/openapi/v1/lead/get?api_key={ENVY_API_KEY}"
            body1 = {"lead_id": lead_id}
            async with aiohttp.ClientSession() as session:
                async with session.post(url1, json=body1, headers=headers) as resp:
                    data = await resp.json()
                    log.info(f"🔎 lead/get [{resp.status}]: {json.dumps(data, ensure_ascii=False)[:1000]}")
                    deals = data.get("result", {}).get("deals") or []
                    if deals:
                        deal_id = deals[0]
                        log.info(f"✅ lead/get извлечён deal_id={deal_id}")
                        await save_deal_id(chat_id, deal_id)

        if not deal_id:
            random_employee_id = random.choice(REAL_MANAGERS)
            url_start = f"{ENVY_CRM_URL}/openapi/v1/lead/start?api_key={ENVY_API_KEY}"
            body_start = {"lead_id": lead_id, "user_id": 346511, "employee_id": random_employee_id}
            log.info(f"🎲 Случайный менеджер для lead/start: {random_employee_id}")
            async with aiohttp.ClientSession() as session:
                async with session.post(url_start, json=body_start, headers=headers) as resp:
                    data = await resp.json()
                    log.info(f"🚀 lead/start [{resp.status}]: {json.dumps(data, ensure_ascii=False)[:500]}")
                    new_deal_id = (data.get("result") or {}).get("deal_id")
                    if new_deal_id:
                        deal_id = new_deal_id
                        log.info(f"✅ lead/start deal_id={deal_id}")
                        await save_deal_id(chat_id, deal_id)

        if not deal_id:
            log.warning(f"⚠️ lead_to_inbox: нет deal_id для lead_id={lead_id}, пропускаем toInbox")
            return

        url2 = f"{ENVY_CRM_URL}/openapi/v1/deal/toInbox?api_key={ENVY_API_KEY}"
        body2 = {"deal_id": deal_id}
        async with aiohttp.ClientSession() as session:
            async with session.post(url2, json=body2, headers=headers) as resp:
                result = await resp.text()
                log.info(f"📥 deal/toInbox deal_id={deal_id} [{resp.status}]: {result[:200]}")
    except Exception as e:
        log.error(f"❌ lead_to_inbox error: {e}")


# ---------- Sipuni (callback-звонок при новом лиде, очередь "Горячая база") ----------
# call_tree не используется: код схемы живёт в недокументированном формате (000-XXXXXX),
# у нас его нет, и метод не запускал реальных звонков несмотря на success:true от API
# (проверено эмпирически через statistic/export — звонков с этими id не существовало).
# Вместо этого — call_number (задокументирован, подтверждён рабочим вживую) с ручным
# retry-циклом по очереди менеджеров.
SIPUNI_QUEUE_SIPNUMBERS = ["250", "265", "273", "277", "282", "287", "201"]  # Алуа → Нурзат → Дамира → Диляра → Расул → Тамирлан → Нина
SIPUNI_RING_WAIT_SECONDS = 20      # сколько ждём ответа одного менеджера, прежде чем пробовать следующего
SIPUNI_STATS_CHECK_DELAY = 45      # задержка перед проверкой statistic/export (индексации нужно время — проверено эмпирически, 25 сек не хватает)


async def sipuni_call_number(client_phone: str, sipnumber: str) -> bool:
    """Один звонок через call_number: sipnumber → после ответа → client_phone.
    Возвращает True, если Sipuni приняла запрос (HTTP 200, success=true).
    Это НЕ значит, что трубку взяли — только что запрос ушёл корректно."""
    antiaon = "0"
    reverse = "0"
    hash_string = f"{antiaon}+{client_phone}+{reverse}+{sipnumber}+{SIPUNI_USER}+{SIPUNI_SECRET}"
    call_hash = hashlib.md5(hash_string.encode("utf-8")).hexdigest()
    data = {
        "antiaon": antiaon,
        "phone": client_phone,
        "reverse": reverse,
        "sipnumber": sipnumber,
        "user": SIPUNI_USER,
        "hash": call_hash,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post("https://sipuni.com/api/callback/call_number", data=data)
            log.info(f"📞 Sipuni call_number sipnumber={sipnumber} client={client_phone} [{resp.status_code}]: {resp.text[:200]}")
            return resp.status_code == 200
    except Exception as e:
        log.error(f"❌ Sipuni call_number error sipnumber={sipnumber}: {e}")
        return False


async def sipuni_check_answered(sipnumber: str, client_phone: str, since_minutes: int = 3) -> bool:
    """Проверяет через statistic/export, был ли звонок sipnumber → client_phone
    принят за последние since_minutes минут. Критерий приёма — непустая
    колонка "Кто ответил" (8-я по счёту, индекс 8, в CSV с разделителем ';').
    Матчим строку по паттерну "{sipnumber} (" в поле "Откуда" и точному
    совпадению "Куда" == client_phone."""
    today = datetime.now(ASTANA_TZ).strftime("%d.%m.%Y")
    params = {
        "anonymous": "0", "crmLinks": "0", "dtmfUserAnswer": "0", "firstTime": "0",
        "from": today, "fromNumber": "", "hangupinitor": "0", "ignoreSpecChar": "0",
        "names": "1", "numbersInvolved": "1", "numbersRinged": "1", "outgoingLine": "0",
        "rating": "0", "showTreeId": "0", "state": "0", "timeFrom": "", "timeTo": "",
        "to": today, "toAnswer": "", "toNumber": "", "tree": "", "type": "0",
        "user": SIPUNI_USER,
    }
    order = ["anonymous", "crmLinks", "dtmfUserAnswer", "firstTime", "from", "fromNumber",
             "hangupinitor", "ignoreSpecChar", "names", "numbersInvolved", "numbersRinged",
             "outgoingLine", "rating", "showTreeId", "state", "timeFrom", "timeTo", "to",
             "toAnswer", "toNumber", "tree", "type", "user"]
    hash_string = "+".join(params[k] for k in order) + f"+{SIPUNI_SECRET}"
    params["hash"] = hashlib.md5(hash_string.encode("utf-8")).hexdigest()
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post("https://sipuni.com/api/statistic/export", data=params)
        cutoff = datetime.now(ASTANA_TZ) - timedelta(minutes=since_minutes)
        for line in resp.text.splitlines()[1:]:  # первая строка — заголовок
            cols = line.split(";")
            if len(cols) < 9:
                continue
            call_time_str, otkuda, kuda, kto_otvetil = cols[2], cols[4], cols[5], cols[8]
            if f"{sipnumber} (" not in otkuda:
                continue
            if kuda.strip() != client_phone:
                continue
            try:
                call_time = datetime.strptime(call_time_str, "%d.%m.%Y %H:%M:%S").replace(tzinfo=ASTANA_TZ)
            except ValueError:
                continue
            if call_time < cutoff:
                continue
            if kto_otvetil.strip():
                return True
        return False
    except Exception as e:
        log.error(f"❌ Sipuni statistic/export error: {e}")
        return False  # при ошибке проверки считаем "не ответили" — пойдём к следующему в очереди, не зависнем


def is_within_callback_hours() -> bool:
    """Callback-звонки работают только 09:00–22:00 Astana time."""
    now = datetime.now(ASTANA_TZ)
    return 9 <= now.hour < 22


def should_trigger_call(chat_id: str) -> bool:
    """Возвращает True и атомарно помечает chat_id как обработанный, если
    звонок для этого лида ещё не запускали. Не зависит от state/history
    диалога — работает и во время паузы бота, когда _handle_incoming не
    выполняется и не может сам это зафиксировать.

    Важная оговорка: это in-memory структура — при рестарте/редеплое на
    Railway она обнуляется. Если редеплой произойдёт ровно в момент, когда
    лид уже "видел" хук, но не успел получить звонок — теоретически
    возможен один лишний повторный звонок после рестарта. Это разовый,
    редкий, не критичный сценарий — осознанно принимаем этот компромисс
    ради простоты, не городим ради этого отдельную таблицу в БД.
    """
    if chat_id in call_triggered_chat_ids:
        return False
    call_triggered_chat_ids.append(chat_id)
    return True


async def has_existing_lead_in_crm(client_phone: str) -> bool:
    """Проверяет через EnvyCRM /lead/search, есть ли уже лид на этот
    номер телефона — независимо от того, писал ли человек раньше именно
    в наш WhatsApp/Instagram бот. Нужно, чтобы не звонить как "новому"
    тем, кто уже клиент компании, но впервые написал боту.

    Формат подтверждён вручную через Swagger: тело запроса — ТОЛЬКО
    phone, без email/name (даже пустыми — иначе метод возвращает
    нерелевантные результаты). Ответ: {"leads": [...]} если найдено,
    {"leads": []} если нет.
    При ошибке запроса — возвращает True (осторожная сторона: лучше
    один раз не позвонить настоящему новому лиду из-за сетевого сбоя,
    чем повторно спровоцировать жалобу "звоните нашим клиентам")."""
    url = f"{ENVY_CRM_URL}/openapi/v1/lead/search?api_key={ENVY_API_KEY}"
    body = {"phone": client_phone}  # ТОЛЬКО phone, не добавлять email/name даже пустыми строками
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
                leads = data.get("leads") or []
                found = bool(leads)
                log.info(f"🔍 EnvyCRM lead/search phone={client_phone}: {'найден существующий лид' if found else 'не найден, реально новый'}")
                return found
    except Exception as e:
        log.error(f"❌ EnvyCRM lead/search error phone={client_phone}: {e} — считаем как 'уже есть', звонок не запускаем на всякий случай")
        return True


async def trigger_new_lead_callback(chat_id: str, client_phone: str) -> None:
    """Обходит очередь "Горячая база" по порядку (250→265→273→277→282→287→201).
    Звонит каждому, ждёт SIPUNI_RING_WAIT_SECONDS, проверяет через
    statistic/export — ответили или нет. Останавливается на первом, кто
    ответил, либо после того как обзвонит всех 7."""
    if not SIPUNI_CALLS_ENABLED:
        log.info(f"🔕 {chat_id} — звонки отключены (SIPUNI_CALLS_ENABLED=false), пропускаем")
        return
    if not is_within_callback_hours():
        log.info(f"🌙 {chat_id} — новый лид вне окна 09:00–22:00, Sipuni-обзвон пропущен")
        return

    queue_order = SIPUNI_QUEUE_SIPNUMBERS.copy()
    random.shuffle(queue_order)
    for sipnumber in queue_order:
        ok = await sipuni_call_number(client_phone, sipnumber)
        if not ok:
            log.warning(f"⚠️ {chat_id}: запрос на sipnumber={sipnumber} не принят Sipuni, пробуем следующего")
            continue
        await asyncio.sleep(SIPUNI_RING_WAIT_SECONDS)
        # индексации в statistic/export нужно время — ждём отдельно перед проверкой
        await asyncio.sleep(max(0, SIPUNI_STATS_CHECK_DELAY - SIPUNI_RING_WAIT_SECONDS))
        answered = await sipuni_check_answered(sipnumber, client_phone)
        if answered:
            log.info(f"✅ {chat_id}: звонок принят sipnumber={sipnumber}, клиент={client_phone}")
            return
        log.info(f"➡️ {chat_id}: sipnumber={sipnumber} не ответил, следующий в очереди")
    log.warning(f"❌ {chat_id}: никто из 7 менеджеров не принял звонок, клиент={client_phone}")


# ---------- Темы, требующие вовлечения человека: перевод сделки на отдельный этап ----------
# TODO: вписать реальный stage_id этапа "Требует вовлечения" (воронка "Сделки админов")
# Узнать так же, как раньше находили 699734: через /deal/list в Swagger или переместив
# тестовую сделку в этот этап руками и посмотрев её stage_id в ответе.
INVOLVEMENT_STAGE_ID: int | None = 1780442  # этап "Требует вовлечения", воронка 279133 — подтверждено через /deal/get

# Каждая категория — список ФРАЗ (не отдельных слов), чтобы не ловить обычные
# разговоры о тарифах/тренерах. Категория матчится, если ЛЮБАЯ из её фраз
# встречается в тексте клиента (простое вхождение подстроки, без стемминга).
INVOLVEMENT_TRIGGERS: dict[str, list[str]] = {
    "заморозка абонемента": ["заморо", "мұздату", "тоңазыт"],
    "срок действия абонемента": [
        "сколько осталось", "до какого числа", "когда заканчивается",
        "срок действия абонемента", "когда истекает", "когда закончится абонемент",
    ],
    "проверка гостевых визитов": [
        "сколько гостевых", "остаток гостевых", "гостевые визиты остались",
        "проверить гостевые",
    ],
    "переоформление абонемента": [
        "переоформ", "переписать карту", "перевести карту", "передать карту",
        "перевести абонемент на", "отдать абонемент",
    ],
    "возврат абонемента": [
        "возврат", "вернуть абонемент", "хочу вернуть деньги",
        "верните деньги", "верните мне деньги", "забрать деньги назад",
    ],
    "коммунальные проблемы на филиале": [
        "нет воды", "нет света", "отключили воду", "отключили свет",
        "без воды", "без света",
    ],
    "подбор тренера (действующий клиент)": [
        "подобрать тренера", "подберите тренера", "какой тренер свободен",
        "записаться к тренеру", "хочу сменить тренера",
    ],
    "потерянные вещи на филиале": [
        "потерял в зале", "потеряла в зале", "забыл на филиале", "забыла на филиале",
        "забыл в зале", "забыла в зале", "потерял на филиале", "потеряла на филиале",
    ],
}


def detect_involvement_category(text: str) -> str | None:
    """Возвращает название категории, если текст клиента попадает под один
    из триггеров, требующих подключения человека. Иначе None."""
    t = text.lower()
    for category, phrases in INVOLVEMENT_TRIGGERS.items():
        if any(p in t for p in phrases):
            return category
    return None


# Промпт (стоп-правило "ТЕМЫ, ТРЕБУЮЩИЕ ПОДКЛЮЧЕНИЯ ЧЕЛОВЕКА") велит Лоле
# говорить клиенту, что запрос уже передан администратору — но
# INVOLVEMENT_TRIGGERS матчит по ФИКСИРОВАННЫМ фразам КЛИЕНТА и не покрывает
# все формулировки (например общую жалобу на филиал без слов "нет воды/света").
# Если Лола всё равно даёт это обещание, а keyword-детект по тексту клиента не
# сработал — эскалируем по факту самого обещания, чтобы оно не было пустым.
ADMIN_HANDOFF_RE = re.compile(
    r"запрос\w*\s+(уже\s+)?пере[дт]а\w*|пере[дт]а\w*\s+(ваш\w*\s+)?запрос\w*",
    re.IGNORECASE,
)


async def update_deal_stage(deal_id: int, stage_id: int, employee_id: int | None = None) -> bool:
    """Переводит сделку на указанный этап воронки через /deal/updateDealStage.
    Возвращает True при успехе (200), False при ошибке."""
    try:
        headers = {"Content-Type": "application/json"}
        url = f"{ENVY_CRM_URL}/openapi/v1/deal/updateDealStage?api_key={ENVY_API_KEY}"
        body = {"deal_id": deal_id, "stage_id": stage_id}
        if employee_id is not None:
            body["employee_id"] = employee_id
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body, headers=headers) as resp:
                result = await resp.text()
                log.info(f"🙋 deal/updateDealStage deal_id={deal_id} stage_id={stage_id} [{resp.status}]: {result[:200]}")
                return resp.status == 200
    except Exception as e:
        log.error(f"❌ update_deal_stage error: {e}")
        return False


async def escalate_to_involvement(chat_id: str, username: str, client_text: str, category: str, known_deal_id: int | None = None) -> None:
    """Переводит сделку на этап 'Требует вовлечения' СРАЗУ, в обход обычного
    5-минутного отката уведомлений, и пишет в карточку сам текст вопроса клиента
    и распознанную категорию — чтобы менеджер сразу понимал суть, не читая весь чат."""
    if INVOLVEMENT_STAGE_ID is None:
        log.warning("⚠️ escalate_to_involvement: INVOLVEMENT_STAGE_ID не задан, пропускаю перевод в воронку")
        return
    try:
        lead_id = await find_lead(username)
        if lead_id is None:
            log.warning(f"⚠️ escalate_to_involvement: лид не найден для username={username}")
            return
        await create_lead_log(lead_id, f"🙋 Лола: требует вовлечения ({category}) — «{client_text[:200]}»")
        headers = {"Content-Type": "application/json"}
        deal_id = known_deal_id

        if not deal_id:
            # тот же путь получения deal_id, что и в lead_to_inbox
            url1 = f"{ENVY_CRM_URL}/openapi/v1/lead/get?api_key={ENVY_API_KEY}"
            async with aiohttp.ClientSession() as session:
                async with session.post(url1, json={"lead_id": lead_id}, headers=headers) as resp:
                    data = await resp.json()
                    deals = data.get("result", {}).get("deals") or []
                    if deals:
                        deal_id = deals[0]
                        await save_deal_id(chat_id, deal_id)

        if not deal_id:
            # сделки ещё нет вообще (первое сообщение клиента) — создаём её,
            # так же как это делает lead_to_inbox, иначе updateDealStage не на чем вызывать
            random_employee_id = random.choice(REAL_MANAGERS)
            url_start = f"{ENVY_CRM_URL}/openapi/v1/lead/start?api_key={ENVY_API_KEY}"
            body_start = {"lead_id": lead_id, "user_id": 346511, "employee_id": random_employee_id}
            async with aiohttp.ClientSession() as session:
                async with session.post(url_start, json=body_start, headers=headers) as resp:
                    data = await resp.json()
                    new_deal_id = (data.get("result") or {}).get("deal_id")
                    if new_deal_id:
                        deal_id = new_deal_id
                        log.info(f"✅ escalate_to_involvement: lead/start создал deal_id={deal_id}")
                        await save_deal_id(chat_id, deal_id)

        if deal_id:
            await update_deal_stage(deal_id, INVOLVEMENT_STAGE_ID)
        else:
            log.warning(f"⚠️ escalate_to_involvement: нет deal_id для lead_id={lead_id} даже после lead/start, этап не переключаю")
    except Exception as e:
        log.error(f"❌ escalate_to_involvement error: {e}")


async def notify_manager(chat_id: str, username: str, phone: str | None = None, known_deal_id: int | None = None) -> None:
    try:
        lead_id = await find_lead(username, phone)
        if lead_id is None:
            log.warning(f"⚠️ notify_manager: лид не найден для username={username} phone={phone}")
            return
        if phone:
            await create_lead_log(lead_id, f"🤖 Лола: клиент {username} оставил номер {phone}. Позвонить!")
        else:
            await create_lead_log(lead_id, f"🤖 Лола: новый клиент {username} написал в Instagram. Проверить диалог.")
        asyncio.create_task(lead_to_inbox(lead_id, chat_id, known_deal_id))
    except Exception as e:
        log.error(f"❌ notify_manager error: {e}")


# ---------- Helpers ----------
def extract_phone(text: str) -> str | None:
    m = PHONE_RE.search(text)
    if m:
        if len(re.sub(r"\D", "", m.group())) >= 10:
            return m.group()
    return None


def is_refusal(text: str) -> bool:
    lower = text.lower()
    for phrase in REFUSE_WORDS:
        pattern = r'\b' + re.escape(phrase) + r'\b'
        if re.search(pattern, lower):
            return True
    return False


def detect_lang(text: str) -> str:
    """Простое определение языка по символам и целым словам."""
    kz_chars = set("әіңғүұқөһ")
    kz_words = {
        "керек", "емес", "жоқ", "бар", "барып", "журсек",
        "болмаима", "сурап", "озимиз", "қайда", "қалай",
        "рахмет", "сәлем", "жақсы", "бұл", "мен", "сен",
    }
    lower_text = text.lower()
    if any(c in kz_chars for c in lower_text):
        return "kz"
    words_in_text = set(re.findall(r"[а-яәіңғүұқөһa-z]+", lower_text))
    if words_in_text & kz_words:
        return "kz"
    return "ru"


# Оба канала на Haiku 4.5 — дешевле, меньше объём. Sonnet возвращён обратно
# из-за более высокой стоимости; при необходимости можно завести отдельную
# логику выбора модели по chat_id.
def pick_model(chat_id: str) -> str:
    return "claude-haiku-4-5"


async def claude_reply(messages: list[dict], static_prompt: str | None = None, dynamic_context: str = "", model: str = "claude-haiku-4-5") -> str:
    while messages and messages[0].get("role") == "assistant":
        messages = messages[1:]
    cleaned = []
    for msg in messages:
        if cleaned and cleaned[-1]["role"] == msg["role"]:
            continue
        cleaned.append(msg)
    messages = cleaned
    while messages and messages[0].get("role") == "assistant":
        messages = messages[1:]
    if not messages:
        messages = [{"role": "user", "content": "Здравствуйте"}]
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    # Блок 1 — ПОСТОЯННЫЙ текст (правила + база знаний), байт-в-байт одинаковый
    # на каждом сообщении → кэшируется, читается за 10% цены после первой записи.
    # Блок 2 — ПЕРЕМЕННЫЙ RAG-контекст, меняется почти каждый раз → без кэша,
    # но он маленький (0-4 чанка), так что дорого не выходит.
    system_blocks = [
        {
            "type": "text",
            "text": static_prompt or SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }
    ]
    if dynamic_context:
        system_blocks.append({"type": "text", "text": dynamic_context})
    # ВАЖНО: claude-sonnet-5 отклоняет параметр temperature (400 "deprecated for
    # this model") — передаём его только для Haiku, для Sonnet вообще не указываем.
    create_kwargs = dict(model=model, max_tokens=1024, system=system_blocks, messages=messages)
    if model == "claude-haiku-4-5":
        create_kwargs["temperature"] = 0.2
    msg = await client.messages.create(**create_kwargs)
    # ВАЖНО: Sonnet иногда возвращает ThinkingBlock первым элементом content
    # (режим рассуждения), а не сразу текст — ищем именно текстовый блок,
    # а не берём content[0] вслепую (это ломало КАЖДЫЙ такой ответ Sonnet).
    for block in msg.content:
        if getattr(block, "type", None) == "text":
            return block.text
    raise ValueError(f"В ответе Claude не найден текстовый блок: {[getattr(b,'type',None) for b in msg.content]}")


PRICE_LIKE_RE = re.compile(r"\d{2,3}[.,]?\s?\d{3}\s*тг", re.IGNORECASE)

INSTAGRAM_GATE_FALLBACK = {
    "ru": "Привет! 👋 Да, у нас как раз есть отличные варианты. Давайте познакомимся — как зовут и номер телефона? 😊",
    "kz": "Сәлем! 👋 Иә, бізде тиімді ұсыныстар бар. Танысайық — атыңыз және телефон нөміріңіз қандай? 😊",
}


def _history_has_phone(history: list[dict], current_text: str | None = None) -> bool:
    """Прокси для «контакты уже получены»: был ли номер телефона хоть раз в
    диалоге (включая текущее сообщение клиента)."""
    if current_text and extract_phone(current_text):
        return True
    for msg in history:
        if msg.get("role") == "user" and extract_phone(str(msg.get("content", ""))):
            return True
    return False


async def enforce_instagram_price_gate(
    chat_id: str,
    reply: str,
    history: list[dict],
    text: str | None,
    static_prompt: str,
    dynamic_context: str,
) -> str:
    """Программный предохранитель поверх правила «цена только после имени+телефона»
    для Instagram. Инцидент 07.07 (chat k_akhmetovaaaa и ещё один) показал, что
    модель иногда нарушает это правило на маркетинговых триггерах, несмотря на то
    что оно явно прописано в системном промпте с примером ошибки — на промпт
    полагаться недостаточно. Перехватываем уже сгенерированный ответ: если в нём
    есть цифры похожие на цену, а контакты (телефон) ещё не получены — просим
    Claude перегенерировать без цены, а если не получилось — отдаём безопасный
    заглушечный ответ с вопросом про имя и телефон."""
    if chat_id.startswith("wapp-"):
        return reply
    if not PRICE_LIKE_RE.search(reply) or _history_has_phone(history, text):
        return reply

    log.warning(f"🚧 INSTAGRAM_PRICE_GATE перехват (цена без контактов) → {chat_id}: {reply[:300]}")
    reinforced_prompt = static_prompt + (
        "\n\nСРОЧНОЕ НАПОМИНАНИЕ (сработала программная проверка): твой предыдущий "
        "вариант ответа содержал цену/цифры до получения имени и номера телефона "
        "клиента в Instagram — это запрещено. Ответь ЗАНОВО, БЕЗ единой цифры цены "
        "или названия тарифа — только вопрос про имя и номер телефона."
    )
    try:
        retry_reply = await claude_reply(history, reinforced_prompt, dynamic_context, model=pick_model(chat_id))
    except Exception as e:
        log.error(f"❌ INSTAGRAM_PRICE_GATE: ошибка перегенерации для {chat_id}: {e}")
        retry_reply = ""

    if retry_reply and retry_reply.strip() and not PRICE_LIKE_RE.search(retry_reply):
        log.info(f"✅ INSTAGRAM_PRICE_GATE перегенерация без цены прошла успешно → {chat_id}")
        return retry_reply

    log.warning(f"🚧 INSTAGRAM_PRICE_GATE перегенерация не помогла, отправляю safe-fallback → {chat_id}")
    return INSTAGRAM_GATE_FALLBACK.get(detect_lang(text or ""), INSTAGRAM_GATE_FALLBACK["ru"])


MAX_HISTORY = 20


async def transcribe_audio(url: str) -> str | None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    log.warning(f"⚠️ transcribe: не удалось скачать аудио [{resp.status}] {url[:100]}")
                    return None
                audio_bytes = await resp.read()
        buf = io.BytesIO(audio_bytes)
        buf.name = "audio.ogg"
        client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
        result = await client.audio.transcriptions.create(model="whisper-1", file=buf)
        text = result.text.strip()
        log.info(f"🎤 Whisper транскрипция: {text[:100]!r}")
        return text or None
    except Exception as e:
        log.error(f"❌ transcribe_audio error: {e}")
        return None


# ---------- Основная логика диалога ----------
async def handle_incoming(chat_id: str, text: str | None, client_phone: str | None = None) -> None:
    async with get_lock(chat_id):
        await _handle_incoming(chat_id, text, client_phone)


async def _handle_incoming(chat_id: str, text: str | None, client_phone: str | None = None) -> None:
    base_prompt = WHATSAPP_PROMPT if chat_id.startswith("wapp-") else SYSTEM_PROMPT
    rag_context = await get_context(text) if text else ""
    static_prompt = base_prompt + "\n\n<knowledge_base>\n" + get_knowledge_base() + "\n</knowledge_base>"
    dynamic_context = ("<retrieved_context>\n" + rag_context + "\n</retrieved_context>") if rag_context else ""
    state, history, updated_at, deal_id = await get_state(chat_id)

    if state == STATE_NEW and history and history[0].get("content") == text:
        log.info(f"♻️ Дубль сообщения в STATE_NEW {chat_id}, пропускаем")
        return

    if state == STATE_SMM:
        log.info(f"🔇 {chat_id} state=smm, молчим навсегда")
        return

    if state in SILENT_STATES:
        now = datetime.now(timezone.utc)
        if updated_at:
            elapsed = (now - updated_at).total_seconds()
            if state == STATE_MANAGER and elapsed >= 1800:  # 30 минут
                log.info(f"🔄 {chat_id} state=manager устарел (>30мин), сбрасываем")
                state = None
                # ВАЖНО: пишем STATE_ACTIVE, а не None — колонка state объявлена
                # NOT NULL, и запись None здесь всегда падала с NotNullViolationError
                # (тихо проглатывалась внешним try/except в вебхуке), из-за чего
                # сброс никогда не сохранялся и история диалога терялась. Запись
                # валидного состояния сразу же также не даёт повторной проверке
                # ниже (после ответа Claude) увидеть ту же самую устаревшую запись
                # "manager" и ложно решить, что менеджер только что взял чат.
                await set_state(chat_id, STATE_ACTIVE)
            elif state in {STATE_DONE, STATE_REFUSED} and elapsed >= 3600:  # 1 час
                log.info(f"🔄 {chat_id} state={state} устарел (>1 часа), сбрасываем")
                state = None
                await set_state(chat_id, STATE_ACTIVE)  # то же обоснование, см. выше
            else:
                if state == STATE_MANAGER and text:
                    await save_pending_message(chat_id, text)
                    log.info(f"💤 {chat_id} state=manager, сообщение сохранено — ответим сами, если менеджер не подключится за 30 мин")
                else:
                    log.info(f"🔇 {chat_id} state={state}, молчим")
                return
        else:
            log.info(f"🔇 {chat_id} state={state}, молчим")
            return

    # Новый диалог — передаём первое сообщение клиента в Claude
    if state is None:
        if history is None:
            history = []
        is_truly_new = len(history) == 0
        # Звонок теперь запускается только один раз, из envy_hook_handler
        # (до пауза-чека, через should_trigger_call) — независимо от паузы.
        if text:
            history.append({"role": "user", "content": text})
        else:
            history.append({"role": "user", "content": "Здравствуйте"})
        try:
            reply = await claude_reply(history, static_prompt, dynamic_context, model=pick_model(chat_id))
            if not reply or not reply.strip():
                raise ValueError("пустой ответ")
        except Exception as e:
            log.error(f"❌ Claude error on greeting: {e}")
            lang = detect_lang(text or "")
            reply = CLAUDE_FALLBACK.get(lang, CLAUDE_FALLBACK["ru"])
        reply = await enforce_instagram_price_gate(chat_id, reply, history, text, static_prompt, dynamic_context)
        history.append({"role": "assistant", "content": reply})
        current_state, _, _, _ = await get_state(chat_id)
        if current_state in SILENT_STATES:
            if current_state == STATE_MANAGER:
                log.info(f"🚫 {chat_id} — менеджер взял чат пока Claude думал, отмена отправки")
            else:
                log.info(f"🛑 {chat_id} state={current_state} пока Claude отвечал, не отправляем")
            return
        if last_bot_reply.get(chat_id) == reply:
            log.warning(f"⚠️ Повторная отправка того же приветствия подряд для {chat_id}, пропускаем send_wazzup")
        else:
            log.info(f"💬 Ответ Лолы → {chat_id}: {reply[:300]}")
            await send_wazzup(chat_id, reply)
            if len(last_bot_reply) >= 10000:
                del last_bot_reply[next(iter(last_bot_reply))]
            last_bot_reply[chat_id] = reply
        new_state = STATE_NEW if is_truly_new else STATE_ACTIVE
        await set_state_guarded(chat_id, new_state, history=history)
        if should_notify(chat_id):
            asyncio.create_task(notify_manager(chat_id, chat_id, known_deal_id=deal_id))
        escalated = False
        if text:
            phone = extract_phone(text)
            if phone:
                asyncio.create_task(notify_manager(chat_id, chat_id, phone, known_deal_id=deal_id))
                log.info(f"📞 {chat_id} дал телефон={phone} первым сообщением")
            involvement_category = detect_involvement_category(text)
            if involvement_category:
                asyncio.create_task(escalate_to_involvement(chat_id, chat_id, text, involvement_category, known_deal_id=deal_id))
                log.info(f"🙋 {chat_id} требует вовлечения ({involvement_category}) — первым сообщением, эскалирую")
                escalated = True
        if not escalated and ADMIN_HANDOFF_RE.search(reply):
            asyncio.create_task(escalate_to_involvement(
                chat_id, chat_id, text or reply, "Лола пообещала передачу администратору", known_deal_id=deal_id
            ))
            log.info(f"🙋 {chat_id} Лола пообещала передачу администратору — эскалирую по факту ответа")
        log.info(f"👋 {'Новый' if is_truly_new else 'Возобновлённый'} диалог {chat_id} → {new_state}")
        return

    # state = "new" или "active" — нужен текст клиента
    if not text:
        return

    phone = extract_phone(text)
    if phone:
        asyncio.create_task(notify_manager(chat_id, chat_id, phone, known_deal_id=deal_id))
        log.info(f"📞 {chat_id} дал телефон={phone}, продолжаем воронку")

    involvement_category = detect_involvement_category(text)
    if involvement_category:
        asyncio.create_task(escalate_to_involvement(chat_id, chat_id, text, involvement_category, known_deal_id=deal_id))
        log.info(f"🙋 {chat_id} требует вовлечения ({involvement_category}) — эскалирую")

    if is_refusal(text):
        lang = detect_lang(text)
        farewell = FAREWELL_MSGS.get(lang, FAREWELL_MSGS["ru"])
        history.append({"role": "user", "content": text})
        history.append({"role": "assistant", "content": farewell})
        await send_wazzup(chat_id, farewell)
        await set_state(chat_id, STATE_REFUSED, history=history)
        log.info(f"🚫 {chat_id} отказался lang={lang} → STATE_REFUSED")
        return

    # Добавляем сообщение клиента, обрезаем до MAX_HISTORY перед отправкой в Claude
    history.append({"role": "user", "content": text})
    history = history[-MAX_HISTORY:]

    try:
        reply = await claude_reply(history, static_prompt, dynamic_context, model=pick_model(chat_id))
        if not reply or not reply.strip():
            raise ValueError("пустой ответ")
    except Exception as e:
        log.error(f"❌ Claude error: {e}")
        reply = CLAUDE_FALLBACK.get(detect_lang(text or ""), CLAUDE_FALLBACK["ru"])

    reply = await enforce_instagram_price_gate(chat_id, reply, history, text, static_prompt, dynamic_context)

    # Добавляем ответ Лолы и сохраняем
    history.append({"role": "assistant", "content": reply})
    history = history[-MAX_HISTORY:]

    current_state, _, _, _ = await get_state(chat_id)
    if current_state in SILENT_STATES:
        if current_state == STATE_MANAGER:
            log.info(f"🚫 {chat_id} — менеджер взял чат пока Claude думал, отмена отправки")
        else:
            log.info(f"🛑 {chat_id} state={current_state} пока Claude отвечал, не отправляем")
        return

    log.info(f"💬 Ответ Лолы → {chat_id}: {reply[:300]}")
    await send_wazzup(chat_id, reply)
    if len(last_bot_reply) >= 10000:
        del last_bot_reply[next(iter(last_bot_reply))]
    last_bot_reply[chat_id] = reply
    await set_state_guarded(chat_id, STATE_ACTIVE, history=history)
    if should_notify(chat_id):
        asyncio.create_task(notify_manager(chat_id, chat_id, known_deal_id=deal_id))
    if not involvement_category and ADMIN_HANDOFF_RE.search(reply):
        asyncio.create_task(escalate_to_involvement(
            chat_id, chat_id, text, "Лола пообещала передачу администратору", known_deal_id=deal_id
        ))
        log.info(f"🙋 {chat_id} Лола пообещала передачу администратору — эскалирую по факту ответа")
    log.info(f"🤖 {chat_id} ответ Claude → STATE_ACTIVE (history={len(history)})")


# ---------- Автоответ на сообщения, зависшие в STATE_MANAGER ----------
async def _answer_pending(chat_id: str) -> None:
    """Отвечает на сообщение клиента, которое пришло пока бот молчал (STATE_MANAGER),
    и менеджер за 30 минут так и не подключился. Вызывается из фоновой задачи,
    а не из вебхука — поэтому клиенту не нужно писать что-то ещё, чтобы получить ответ."""
    state, history, updated_at, deal_id = await get_state(chat_id)
    if state != STATE_MANAGER:
        return  # кто-то уже обработал этот чат другим путём

    if not history or history[-1].get("role") != "user":
        # нечего отвечать — просто снимаем флаг, чтобы не проверять чат заново
        await clear_awaiting_reply(chat_id)
        return

    text = history[-1].get("content", "")
    base_prompt = WHATSAPP_PROMPT if chat_id.startswith("wapp-") else SYSTEM_PROMPT
    rag_context = await get_context(text) if text else ""
    static_prompt = base_prompt + "\n\n<knowledge_base>\n" + get_knowledge_base() + "\n</knowledge_base>"
    dynamic_context = ("<retrieved_context>\n" + rag_context + "\n</retrieved_context>") if rag_context else ""

    try:
        reply = await claude_reply(history, static_prompt, dynamic_context, model=pick_model(chat_id))
        if not reply or not reply.strip():
            raise ValueError("пустой ответ")
    except Exception as e:
        log.error(f"❌ Claude error on pending reply: {e}")
        reply = CLAUDE_FALLBACK.get(detect_lang(text or ""), CLAUDE_FALLBACK["ru"])

    reply = await enforce_instagram_price_gate(chat_id, reply, history, text, static_prompt, dynamic_context)

    history.append({"role": "assistant", "content": reply})
    history = history[-MAX_HISTORY:]

    # менеджер мог перехватить чат ровно пока Claude готовил ответ — перепроверяем
    current_state, _, _, _ = await get_state(chat_id)
    if current_state != STATE_MANAGER:
        log.info(f"🛑 {chat_id} — стейт изменился пока готовили отложенный ответ, не отправляем")
        return

    log.info(f"💬 Отложенный ответ Лолы (менеджер не подключился за 30 мин) → {chat_id}: {reply[:300]}")
    await send_wazzup(chat_id, reply)
    if len(last_bot_reply) >= 10000:
        del last_bot_reply[next(iter(last_bot_reply))]
    last_bot_reply[chat_id] = reply
    await set_state(chat_id, STATE_ACTIVE, history=history)
    log.info(f"⏰ {chat_id} — неотвеченное сообщение обработано → STATE_ACTIVE")


async def resume_unanswered_manager_chats() -> None:
    """Раз в минуту проверяет чаты, застрявшие в STATE_MANAGER дольше 30 минут
    с неотвеченным сообщением клиента, и отвечает сама — не дожидаясь нового сообщения."""
    while True:
        await asyncio.sleep(60)
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT chat_id FROM dialogs
                    WHERE state = 'manager'
                      AND awaiting_reply = TRUE
                      AND updated_at <= NOW() - INTERVAL '30 minutes'
                """)
            for row in rows:
                chat_id = row["chat_id"]
                async with get_lock(chat_id):
                    await _answer_pending(chat_id)
        except Exception as e:
            log.error(f"⚠️ Ошибка в resume_unanswered_manager_chats: {e}")


async def whatsapp_night_schedule_manager() -> None:
    """Раз в минуту проверяет время по Алматы и включает/выключает дневную
    авто-паузу WhatsApp (09:00–23:00) — бот работает только 23:00–09:00.
    Instagram не затрагивает."""
    global whatsapp_auto_paused
    while True:
        try:
            now = datetime.now(ASTANA_TZ)
            should_pause = 9 <= now.hour < 23
            if should_pause != whatsapp_auto_paused:
                whatsapp_auto_paused = should_pause
                if should_pause:
                    log.info(f"WhatsApp авто-пауза включена (Алматы {now.strftime('%H:%M')})")
                else:
                    log.info(f"WhatsApp авто-пауза снята (Алматы {now.strftime('%H:%M')})")
        except Exception as e:
            log.error(f"⚠️ Ошибка в whatsapp_night_schedule_manager: {e}")
        await asyncio.sleep(60)


# ---------- Эндпоинты ----------
async def envy_hook_handler(request: web.Request) -> web.Response:
    if os.getenv("BOT_PAUSED", "false").lower() == "true":
        log.info("⏸️ Бот на паузе, игнорируем")
        return web.Response(text="ok")

    try:
        payload = await request.json()
    except Exception:
        return web.Response(text="ok")

    # Звонок должен уйти независимо от паузы текстовых ответов —
    # см. комментарий в should_trigger_call выше. Извлекаем облегчённо,
    # не дублируя полную логику ниже (echo-фильтры, дедуп по message_id
    # и т.д. тут не нужны — should_trigger_call сама защищает от дублей).
    if payload.get("event_type") == "message":
        _contact = payload.get("contact") or {}
        _chat_id = str(_contact.get("external_id") or "").strip()
        if _chat_id.startswith("inst-"):
            _chat_id = _chat_id[5:]
        _client_phone = _contact.get("phone") or None
        if not _client_phone and _chat_id.startswith("wapp-"):
            _client_phone = _chat_id[5:]
        if _chat_id and _client_phone and should_trigger_call(_chat_id):
            _state, _history, _, _ = await get_state(_chat_id)
            _is_new_in_our_db = _state is None and not _history
            if _is_new_in_our_db:
                _already_in_crm = await has_existing_lead_in_crm(_client_phone)
                if not _already_in_crm:
                    asyncio.create_task(trigger_new_lead_callback(_chat_id, _client_phone))
                    log.info(f"📞 {_chat_id} — звонок запущен (новый и у нас, и в CRM)")
                else:
                    log.info(f"🔕 {_chat_id} — новый в нашей базе, но уже есть в CRM, звонок не запускаем")
            else:
                log.info(f"🔕 {_chat_id} — уже писал боту раньше, звонок не запускаем")

    is_whatsapp = payload.get("integration", {}).get("service") in ("whatsapp", "wapi")
    if is_whatsapp and (BOT_PAUSED_WHATSAPP or whatsapp_auto_paused):
        reason = "вручную" if BOT_PAUSED_WHATSAPP else "ночное расписание"
        log.info(f"⏸️ WhatsApp бот на паузе ({reason}), игнорируем")
        return web.Response(text="ok")
    if not is_whatsapp and BOT_PAUSED_INSTAGRAM:
        log.info("⏸️ Instagram бот на паузе, игнорируем")
        return web.Response(text="ok")

    log.info(f"📨 envy_hook payload: {json.dumps(payload, ensure_ascii=False)[:1000]}")

    event_type = payload.get("event_type")

    if event_type == "message_reply":
        message_data = payload.get("message_data") or {}
        message_text = message_data.get("text") or ""
        has_attachment = bool(message_data.get("attachments"))
        contact_check = payload.get("contact") or {}
        chat_id_check = str(contact_check.get("external_id") or "").strip()
        if chat_id_check.startswith("inst-"):
            chat_id_check = chat_id_check[5:]
        if chat_id_check.startswith("wapp-"):
            chat_id_check = chat_id_check[5:]  # normalize for sent_texts lookup
        if message_text and chat_id_check in sent_texts and message_text in sent_texts[chat_id_check]:
            log.info(f"🔄 Эхо Лолы text={message_text[:50]!r}, игнорируем")
            return web.Response(text="ok")
        SMM_KEYWORDS = ["штат моделей", "съёмк", "съемк", "смм менеджер", "сотрудничеств", "исходник", "модел"]
        if any(kw in message_text.lower() for kw in SMM_KEYWORDS):
            if chat_id_check:
                await set_state(chat_id_check, STATE_SMM)
                log.info(f"📸 SMM-рассылка → {chat_id_check} STATE_SMM (навсегда)")
            return web.Response(text="ok")
        from_user = payload.get("from_user") or {}
        crm_employee_id = from_user.get("crm_employee_id")
        if crm_employee_id and crm_employee_id != 0 and crm_employee_id > 100000:
            contact = payload.get("contact") or {}
            chat_id = str(contact.get("external_id") or "").strip()
            if chat_id.startswith("inst-"):
                chat_id = chat_id[5:]
            if chat_id:
                stored_last = last_bot_reply.get(chat_id, "")
                if message_text and stored_last and message_text.strip() == stored_last.strip():
                    log.info(
                        f"🔄 Эхо с crm_employee_id={crm_employee_id} — игнорируем, это наш ответ"
                    )
                elif not message_text and not has_attachment:
                    log.info(
                        f"⏭️ message_reply без текста и без вложений от crm_employee_id={crm_employee_id} — игнорируем системное событие"
                    )
                else:
                    async with get_lock(chat_id):
                        await set_state(chat_id, STATE_MANAGER)
                    reason = "текст" if message_text else "вложение (фото/файл)"
                    log.info(f"👨‍💼 Менеджер (crm_employee_id={crm_employee_id}, {reason}) взял {chat_id} → STATE_MANAGER")
        else:
            log.info("⏭️ message_reply от системы, игнорируем")
        return web.Response(text="ok")

    if event_type != "message":
        log.info(f"⏭️ event_type={event_type!r}, игнорируем")
        return web.Response(text="ok")

    # Дедупликация по message_id
    message_id = payload.get("message_id")
    if message_id is not None:
        if message_id in processed_message_ids:
            log.info(f"♻️ Дубль message_id={message_id}, пропускаем")
            return web.Response(text="ok")
        processed_message_ids.append(message_id)

    contact = payload.get("contact") or {}
    chat_id = str(contact.get("external_id") or "").strip()
    if chat_id.startswith("inst-"):
        chat_id = chat_id[5:]
    if not chat_id:
        log.warning("⚠️ Нет contact.external_id в payload, пропускаем")
        return web.Response(text="ok")
    log.info(f"📌 chat_id={chat_id}")

    # Номер клиента для Sipuni-звонка: сначала из пейлоада хука, иначе для
    # WhatsApp сам chat_id и есть номер (после "wapp-"), иначе — из текста
    # сообщения, если клиент прислал номер прямо в нём.
    client_phone = contact.get("phone") or None
    if not client_phone and chat_id.startswith("wapp-"):
        client_phone = chat_id[5:]

    from_user = payload.get("from_user") or {}
    crm_employee_id = from_user.get("crm_employee_id")

    if crm_employee_id is not None and crm_employee_id != 0 and crm_employee_id > 100000:
        await set_state(chat_id, STATE_MANAGER)
        log.info(f"👨‍💼 Менеджер (crm_employee_id={crm_employee_id}) взял {chat_id} → STATE_MANAGER")
        return web.Response(text="ok")

    # Сообщение от клиента
    message_data = payload.get("message_data") or {}
    raw_text = message_data.get("text") or ""
    attachments = message_data.get("attachments") or []
    if raw_text.strip() == "You mentioned in the story":
        log.info("⏭️ Отметка в сторис, игнорируем")
        return web.Response(text="ok")
    if any(a.get("type") in ("story", "video") and not raw_text.strip() for a in attachments):
        log.info("⏭️ Вложение сторис без текста, игнорируем")
        return web.Response(text="ok")
    if any(a.get("type") in ("audio", "voice") and not raw_text.strip() for a in attachments):
        audio_url = next(
            (a.get("link") or a.get("url") for a in attachments if a.get("type") in ("audio", "voice")),
            None,
        )
        if audio_url:
            log.info(f"🎤 Голосовое от {chat_id}, транскрибируем: {audio_url[:100]}")
            text = await transcribe_audio(audio_url) or "[клиент отправил голосовое сообщение]"
        else:
            log.info(f"🎤 Голосовое от {chat_id}, нет ссылки на файл — используем заглушку")
            text = "[клиент отправил голосовое сообщение]"
    else:
        if raw_text:
            text: str | None = raw_text.strip()
        elif any(a.get("type") == "doc" for a in attachments):
            text = "[клиент отправил файл/документ — вероятно чек оплаты]"
            log.info(f"📄 Документ без подписи от {chat_id}, передаю Claude с пометкой")
        elif any(a.get("type") == "photo" for a in attachments):
            text = "[клиент отправил фото]"
            log.info(f"🖼️ Фото без подписи от {chat_id}, передаю Claude с пометкой")
        else:
            text = None

    if not client_phone and text:
        client_phone = extract_phone(text)

    try:
        await handle_incoming(chat_id, text, client_phone)
    except Exception as e:
        log.error(f"❌ handle_incoming error {chat_id}: {e}", exc_info=True)

    return web.Response(text="ok")


async def webhook_handler(request: web.Request) -> web.Response:
    try:
        body = await request.read()
        if not body:
            return web.Response(text="ok")
        try:
            payload = json.loads(body)
        except Exception:
            log.warning(f"📡 /webhook non-JSON body: {body[:200]}")
            return web.Response(text="ok")

        log.info(f"📡 /webhook raw: {json.dumps(payload, ensure_ascii=False)[:500]}")

        for entry in payload.get("entry") or []:
            for event in entry.get("messaging") or []:
                sender_id = (event.get("sender") or {}).get("id", "-")
                recipient_id = (event.get("recipient") or {}).get("id", "-")
                if "message" in event:
                    event_type = "message"
                    text = (event.get("message") or {}).get("text") or ""
                elif "read" in event:
                    event_type = "read"
                    text = ""
                elif "reaction" in event:
                    event_type = "reaction"
                    text = (event.get("reaction") or {}).get("emoji") or ""
                else:
                    event_type = list(event.keys() - {"sender", "recipient", "timestamp"})
                    event_type = event_type[0] if event_type else "unknown"
                    text = ""
                log.info(
                    f"📡 /webhook event: sender={sender_id} recipient={recipient_id} "
                    f"type={event_type} text={text[:50] if text else '-'}"
                )
    except Exception as e:
        log.error(f"❌ /webhook parse error: {e}", exc_info=True)

    return web.Response(text="ok")


async def wazzup_handler(request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "bot_enabled": True})



# ---------- DB ----------
async def init_db(app: web.Application) -> None:
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dialogs (
                chat_id    TEXT PRIMARY KEY,
                state      TEXT NOT NULL DEFAULT 'new',
                lead_id    TEXT,
                history    JSONB NOT NULL DEFAULT '[]',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            ALTER TABLE dialogs
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
        """)
        await conn.execute("""
            ALTER TABLE dialogs
            ADD COLUMN IF NOT EXISTS history JSONB NOT NULL DEFAULT '[]'
        """)
        await conn.execute("""
            ALTER TABLE dialogs
            ADD COLUMN IF NOT EXISTS deal_id BIGINT
        """)
        await conn.execute("""
            ALTER TABLE dialogs
            ADD COLUMN IF NOT EXISTS awaiting_reply BOOLEAN NOT NULL DEFAULT FALSE
        """)
    try:
        await init_rag(db_pool)
    except Exception as e:
        log.error(f"⚠️ RAG не инициализирован ({e}) — бот продолжит работу на статичной базе знаний")
    log.info("✅ DB готова")


async def close_db(app: web.Application) -> None:
    for task_name in ("resume_task", "whatsapp_schedule_task"):
        task = app.get(task_name)
        if task:
            task.cancel()
    if db_pool:
        await db_pool.close()


async def start_background_tasks(app: web.Application) -> None:
    app["resume_task"] = asyncio.create_task(resume_unanswered_manager_chats())
    app["sheets_sync_task"] = asyncio.create_task(sheets_sync.start_daily_sync())
    app["whatsapp_schedule_task"] = asyncio.create_task(whatsapp_night_schedule_manager())


# ---------- App factory ----------
def create_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/webhook",   webhook_handler)
    app.router.add_get( "/webhook",   lambda r: web.Response(text="ok"))
    app.router.add_post("/envy_hook", envy_hook_handler)
    app.router.add_post("/wazzup",    wazzup_handler)
    app.router.add_get( "/health",    health_handler)
    app.on_startup.append(init_db)
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(close_db)
    return app


if __name__ == "__main__":
    app = create_app()
    log.info("🚀 Champion Bot запущен")
    web.run_app(app, host="0.0.0.0", port=PORT)
