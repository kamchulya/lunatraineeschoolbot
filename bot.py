import asyncio
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
from knowledge_base import KNOWLEDGE_BASE
import openai
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import (
    ANTHROPIC_API_KEY,
    WAZZUP_API_KEY,
    WAZZUP_INSTAGRAM_CHANNEL_ID,
    OPENAI_API_KEY,
    ENVY_OPERATOR_KEY,
    ENVY_API_KEY,
    ENVY_CRM_URL,
    DATABASE_URL,
    PORT,
    BOT_PAUSED_INSTAGRAM,
    LUNA_SCHEDULE_ENABLED,
    ARTYOM_WHATSAPP_PERSONAL,
    WAZZUP_WHATSAPP_CHANNEL_ID,
)
from prompt import SYSTEM_PROMPT
import sheets_sync

# ---------- Менеджеры EnvyCRM (shkolaobucheniya.envycrm.com) ----------
REAL_MANAGERS: list[int] = [
    1046932,  # Дмитрий
    1101942,  # Артём ШЕФ
    1127631,  # Диана
    1139532,  # Димаш
    1151420,  # Алдияр
    1112091,  # Луна (живой менеджер, теперь тоже получает лиды наравне со всеми)
]

# ---------- States ----------
STATE_NEW       = "new"
STATE_ACTIVE    = "active"
STATE_DEMO_SENT = "demo_sent"
STATE_DONE      = "done"
STATE_MANAGER   = "manager"
STATE_REFUSED   = "refused"
STATE_SMM       = "smm"

SILENT_STATES = {STATE_DONE, STATE_MANAGER, STATE_REFUSED, STATE_SMM}

# ---------- Involvement / эскалация ----------
# Раньше пытались двигать сделку в CRM по этой стадии, но stage_id так и не был найден.
# Эскалация теперь идёт напрямую в WhatsApp Артёму (send_whatsapp_escalation) — CRM-стадия не нужна.

INVOLVEMENT_TRIGGERS: dict[str, list[str]] = {
    "перенос даты обучения": [
        "перенести дату", "поменять дату", "другой поток", "следующий поток",
        "сдвинуть дату", "перевести на другой поток",
    ],
    "возврат оплаты за курс": [
        "вернуть деньги", "возврат денег", "возврат оплаты",
        "хочу вернуть", "верните деньги", "вернуть оплату",
    ],
    "вопрос по уже оплаченному курсу": [
        "уже оплатил", "уже оплатила", "я оплатил курс",
        "я уже записан", "уже записана на курс",
    ],
    "жалоба на преподавателя": [
        "жалоба на", "недоволен преподавателем", "недовольна преподавателем",
        "претензия к", "плохой лектор", "преподаватель грубит",
    ],
    "не одобрили рассрочку": [
        "не одобрили", "не одобрил", "отказали в рассрочке",
        "не дали рассрочку", "рассрочку не дали", "каспи отказал",
    ],
}

# Отдельно от вовлечения — клиент утверждает, что оплатил (нужна ручная проверка Kaspi/CRM)
PAYMENT_CLAIM_KEYWORDS = [
    "оплатил", "оплатила", "оплатили", "оплата прошла",
    "перевела деньги", "перевёл деньги", "закинула деньги", "закинул деньги",
    "скинула деньги", "скинул деньги", "отправил чек", "отправила чек",
    "вот чек", "деньги отправила", "деньги отправил",
]


def detect_payment_claim(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in PAYMENT_CLAIM_KEYWORDS)

# ---------- Цепочка напоминаний при молчании клиента ----------
# Запускается после ЛЮБОГО нашего исходящего сообщения (бота или менеджера) —
# не привязана к демо-доступу. Каждая дельта отсчитывается от предыдущего
# события в цепочке (отправленное напоминание либо "мягкий" ответ клиента на
# него) — НЕ накопительно от изначального момента молчания.
REMINDER_CHAIN: list[tuple[int, str]] = [
    (3600, (
        "Иногда одно решение меняет гораздо больше, чем кажется… ✨\n"
        "Мы недавно обсуждали обучение🙂\n"
        "Если вдруг \"закрутился день\" или сейчас неудобно ответить — это абсолютно нормально.\n"
        "Напишите, пожалуйста, когда Вам будет удобно, мы вам позвоним или напишем 🌿"
    )),
    (10800, (
        "Вчера заметили одну интересную закономерность🤔\n"
        "Большинство людей откладывают обучение не потому, что оно им не нужно.\n"
        "А потому что кажется, что \"ещё не время\".\n"
        "Если захотите посмотреть, как проходит обучение у нас — вот видео школы.\n"
        "https://www.instagram.com/reel/DWrG4oXiKvR/?igsh=NHVxOHI0ZWQ2amE="
    )),
    (18000, (
        "Иногда достаточно двух минут, чтобы многое стало понятнее… 🎥\n"
        "Отправляю короткое видео о нашей школе👇\n"
        "https://www.instagram.com/reel/DI3_-a3I4zA/?igsh=MmNzanl1Z2hmc2lr"
    )),
    (36000, (
        "Многие наши ученики перед обучением говорили одну и ту же фразу… 🤔\n"
        "«Я думал(а), что без опыта вообще ничего не получится.»\n"
        "Оказалось, всё намного проще, чем казалось в начале 💪\n"
        "Посмотрите, как это получилось у наших выпускников 👇\n"
        "https://www.instagram.com/reel/DW6WaodCI32/?igsh=bDkxODlvMGtmbXl1"
    )),
    (75600, (
        "Можно задам один короткий вопрос? 🙂\n"
        "Что для Вас сейчас самое важное при выборе школы?\n"
        "1️⃣ Наполняемость курса.\n"
        "2️⃣ Поддержка лектора.\n"
        "3️⃣ Цена.\n"
        "4️⃣ Чтобы обучение было удобным по графику.\n"
        "5️⃣ Пока просто изучаю разные варианты.\n"
        "Можно просто написать цифру — так мне будет проще понять, что для Вас "
        "действительно важно 🌿"
    )),
]

# Состояния, в которых цепочка напоминаний никогда не запускается/не срабатывает.
# STATE_MANAGER сюда специально НЕ входит — по ТЗ цепочка обязана сработать,
# даже если клиент молчит именно после сообщения менеджера, а не бота.
REMINDER_CHAIN_BLOCKED_STATES = {STATE_DONE, STATE_REFUSED, STATE_SMM}

CLAUDE_FALLBACK = {
    "ru": "Извините, небольшой сбой. Напишите позже или менеджер свяжется с Вами 😊",
    "kz": "Кешіріңіз, қате болды. Кейінірек жазыңыз 😊",
}

FAREWELL_MSGS = {
    "ru": "Хорошо, не буду беспокоить 😊 Если надумаете — всегда рады помочь!",
    "kz": "Жақсы, мазаламаймын 😊 Ойланып қалсаңыз — әрқашан қош келдіңіз!",
}

REFUSE_WORDS = [
    "не надо", "не интересно", "нет спасибо", "не хочу", "не нужно",
    "отвали", "отстань", "отстаньте", "не актуально",
    "қажет емес", "жоқ рахмет",
]

PHONE_RE = re.compile(
    r'(?:\+7|8|\b7)[\s\-\(\)]*\d{3}[\s\-\(\)]*\d{3}[\s\-]*\d{2}[\s\-]*\d{2}'
    r'|\b\d{10,11}\b'
)

SMM_KEYWORDS = [
    "штат моделей", "съёмк", "съемк", "смм менеджер",
    "сотрудничеств", "исходник", "модел",
]

# ---------- Таргет-реклама: комментарий "хочу" → авто-DM ----------
# Маркетинг запускает таргет с призывом написать "хочу" в комментариях к посту.
# Instagram/Wazzup при этом создаёт автоматическое сообщение в Direct с текстом
# комментария. Требование: отвечать ТОЛЬКО если в исходном комментарии есть слово
# "хочу" — остальные комментарии игнорировать молча, не заводя воронку.
AD_COMMENT_TRIGGER_WORD = "хочу"

AD_COMMENT_OPENERS = [
    "Привет! 👋\n"
    "Спасибо за комментарий ❤️\n"
    "Скажите, пожалуйста, Вы сейчас живёте в Казахстане или в другой стране? 😊\n"
    "🎓 Учиться можно из любой точки мира.\n"
    "📱 Все уроки доступны онлайн в удобное время.\n"
    "📜 После успешного окончания Вы получите сертификат.\n"
    "Подскажите, Вас интересует фитнес-тренер или тренер групповых программ?",

    "Привет! 👋\n"
    "Очень приятно, что Вас заинтересовало обучение.\n"
    "Скажите, пожалуйста, Вы сейчас живёте в Казахстане или в другой стране? 😊\n"
    "Самое классное в онлайн-формате — можно проходить обучение тогда, когда удобно "
    "именно Вам, независимо от города или страны 🌍\n"
    "Подскажите, Вас интересует фитнес-тренер или тренер групповых программ?",

    "Привет! 👋\n"
    "Спасибо за интерес к нашей школе ❤️\n"
    "Скажите, пожалуйста, Вы сейчас живёте в Казахстане или в другой стране? 😊\n"
    "Уже более 2500 человек прошли обучение у нас, и многие сейчас успешно работают "
    "фитнес-тренерами 💪\n"
    "Подскажите, Вас интересует фитнес-тренер или тренер групповых программ?",
]


def detect_ad_comment_text(payload: dict) -> str | None:
    """Вытаскивает текст исходного комментария из вебхука Wazzup, если входящее
    сообщение — это автоматический DM, созданный Instagram/Wazzup в ответ на
    комментарий к посту (а не сообщение, написанное клиентом напрямую в директ).

    ПОДТВЕРЖДЕНО на реальном payload (Railway log, 14.07.2026): у обычного DM
    message_data.data == null. У сообщения, созданного из комментария к посту,
    message_data.data — это объект с метаданными самого поста (src/likes/
    comments/description и т.п.), а message_data.text — это ТЕКСТ КОММЕНТАРИЯ.
    Пример реального payload:
        "message_data": {
            "text": "Когда начала смотреть аж поясница заболела...",
            "data": {"src": "https://www.instagram.com/p/...", "likes": 2807,
                      "comments": 298, "description": "...", ...}
        }
    Для обычного DM: "message_data": {"text": "...", "data": null}
    """
    message_data = payload.get("message_data") or {}
    data = message_data.get("data")
    if isinstance(data, dict) and ("src" in data or "comments" in data or "likes" in data):
        return message_data.get("text")
    return None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

db_pool: asyncpg.Pool | None = None
scheduler: AsyncIOScheduler | None = None
http_session: aiohttp.ClientSession | None = None
processed_message_ids: deque = deque(maxlen=1000)
sent_texts: dict[str, dict[str, datetime]] = {}
dialog_locks: OrderedDict = OrderedDict()
last_notify: dict[str, datetime] = {}
last_bot_reply: dict[str, str] = {}

# Дедап пачки сообщений, пришедших почти одновременно от одного контакта
# (например Instagram шлёт текст + вложение + повтор отдельными вебхуками)
DEBOUNCE_SECONDS = 4.0
pending_buffer: dict[str, list[str]] = {}
pending_tasks: dict[str, asyncio.Task] = {}

MAX_HISTORY = 20

# ---------- Автографик Луны: 20:00-11:00 Астана (работает ПОСЛЕ менеджеров) ----------
# Днём (11:00-20:00) клиентов ведут живые менеджеры — бот молчит, чтобы не мешать.
# Аналог ночной паузы у Лолы, только окно инвертировано (Луна активна ночью).
ASTANA_TZ = timezone(timedelta(hours=5))
LUNA_WORK_START_HOUR = 20  # начало смены Луны, Астана
LUNA_WORK_END_HOUR = 11    # конец смены Луны, Астана


def is_luna_working_hours() -> bool:
    """True, если сейчас окно 20:00-11:00 по Астане (окно переходит через полночь)."""
    hour = datetime.now(ASTANA_TZ).hour
    return hour >= LUNA_WORK_START_HOUR or hour < LUNA_WORK_END_HOUR


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


# ---------- DB ----------
async def log_message(chat_id: str, role: str, text: str | None) -> None:
    if not text:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO message_log (chat_id, role, text) VALUES ($1, $2, $3)",
                chat_id, role, text,
            )
    except Exception as e:
        log.error(f"❌ log_message error {chat_id}: {e}")


async def get_state(chat_id: str) -> tuple[str | None, list, datetime | None, int | None]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, history, updated_at, deal_id FROM dialogs WHERE chat_id=$1", chat_id
        )
    if row:
        history = json.loads(row["history"]) if row["history"] else []
        return row["state"], history, row["updated_at"], row["deal_id"]
    return None, [], None, None


async def get_reminder_step(chat_id: str) -> int | None:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT reminder_step FROM dialogs WHERE chat_id=$1", chat_id)
    return row["reminder_step"] if row else None


async def save_reminder_step(chat_id: str, step: int | None) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE dialogs SET reminder_step=$1 WHERE chat_id=$2", step, chat_id)


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
            INSERT INTO dialogs (chat_id, state, history, deal_id, updated_at)
            VALUES ($1, $2, COALESCE($3::jsonb, '[]'::jsonb), $4, NOW())
            ON CONFLICT (chat_id) DO UPDATE
                SET state      = EXCLUDED.state,
                    history    = COALESCE($3::jsonb, dialogs.history),
                    deal_id    = COALESCE(EXCLUDED.deal_id, dialogs.deal_id),
                    updated_at = NOW()
            """,
            chat_id, state, history_json, deal_id,
        )


async def set_state_guarded(
    chat_id: str,
    state: str,
    history: list | None = None,
    deal_id: int | None = None,
) -> None:
    history_json = json.dumps(history, ensure_ascii=False) if history is not None else None
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO dialogs (chat_id, state, history, deal_id, updated_at)
            VALUES ($1, $2, COALESCE($3::jsonb, '[]'::jsonb), $4, NOW())
            ON CONFLICT (chat_id) DO UPDATE
                SET state      = EXCLUDED.state,
                    history    = COALESCE($3::jsonb, dialogs.history),
                    deal_id    = COALESCE(EXCLUDED.deal_id, dialogs.deal_id),
                    updated_at = NOW()
                WHERE dialogs.state NOT IN ('manager', 'done', 'refused', 'smm')
            """,
            chat_id, state, history_json, deal_id,
        )


async def save_pending_message(chat_id: str, text: str) -> None:
    """Сохраняет сообщение клиента пришедшее пока бот молчит (STATE_MANAGER).
    НЕ трогает updated_at — иначе сбросится таймер ожидания менеджера."""
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
            "UPDATE dialogs SET deal_id = $1 WHERE chat_id = $2", deal_id, chat_id,
        )


# ---------- Wazzup ----------
async def send_wazzup(chat_id: str, text: str) -> None:
    url = "https://api.wazzup24.com/v3/message"
    headers = {
        "Authorization": f"Bearer {WAZZUP_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "channelId": WAZZUP_INSTAGRAM_CHANNEL_ID,
        "chatId": chat_id,
        "chatType": "instagram",
        "text": text,
    }
    delays = [0, 2, 4]
    for attempt, delay in enumerate(delays):
        if delay:
            await asyncio.sleep(delay)
        try:
            async with http_session.post(url, json=body, headers=headers) as resp:
                result = await resp.text()
                log.info(f"📤 Wazzup → {chat_id} attempt={attempt+1} [{resp.status}]: {result[:200]}")
                if resp.status < 500:
                    now = datetime.now(timezone.utc)
                    bucket = sent_texts.setdefault(chat_id, {})
                    expired = [t for t, ts in bucket.items() if (now - ts).total_seconds() > 3600]
                    for t in expired:
                        del bucket[t]
                    bucket[text] = now
                    return
        except Exception as e:
            log.warning(f"⚠️ Wazzup attempt {attempt+1} error: {e}")
    log.error(f"❌ Wazzup: все 3 попытки провалились для {chat_id}")


# ---------- APScheduler: цепочка напоминаний при молчании клиента ----------
def schedule_reminder_chain(chat_id: str, step: int, anchor: datetime | None = None) -> None:
    """Планирует шаг step цепочки (0-based) через REMINDER_CHAIN[step] секунд
    от anchor (или от текущего момента, если anchor не передан). У каждого
    чата в любой момент времени — максимум одна запланированная задача
    цепочки (id детерминирован по chat_id, replace_existing=True)."""
    if scheduler is None or step >= len(REMINDER_CHAIN):
        return
    base = anchor or datetime.now(timezone.utc)
    delay_sec, _ = REMINDER_CHAIN[step]
    run_time = base + timedelta(seconds=delay_sec)
    job_id = f"{chat_id}_reminder_chain"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    scheduler.add_job(
        send_reminder_chain_step,
        "date",
        run_date=run_time,
        args=[chat_id, step],
        id=job_id,
        replace_existing=True,
    )


def cancel_reminder_chain(chat_id: str) -> None:
    if scheduler is None:
        return
    job_id = f"{chat_id}_reminder_chain"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        log.info(f"🗑️ Отменена запланированная цепочка напоминаний для {chat_id}")


async def start_reminder_chain(chat_id: str, anchor: datetime | None = None, start_step: int = 0) -> None:
    """Вызывается после ЛЮБОГО нашего исходящего сообщения (бот или менеджер) —
    запускает/перезапускает цепочку с шага start_step (по умолчанию 0 — первое
    напоминание через 1 час). Если анкер — сообщение МЕНЕДЖЕРА, а не бота,
    вызывающий код передаёт start_step=REMINDER_CHAIN_MANAGER_START_STEP —
    цепочка тогда стартует сразу с 3-го шага (5 часов), пропуская ранние 1ч/3ч
    напоминания, которые для менеджерских рассылок ощущаются слишком навязчиво."""
    schedule_reminder_chain(chat_id, start_step, anchor=anchor)
    await save_reminder_step(chat_id, start_step)


# 3-й шаг цепочки (индекс 2, считая с 0) = 5 часов — именно с него стартуем,
# если последним написал менеджер, а не бот.
REMINDER_CHAIN_MANAGER_START_STEP = 2


async def send_reminder_chain_step(chat_id: str, step: int) -> None:
    # ВАЖНО: берём тот же per-chat lock, что использует обработка входящих
    # сообщений (_handle_incoming). Без этого возможна гонка: клиент пишет
    # ровно в момент срабатывания таймера напоминания, обе стороны почти
    # одновременно читают/пишут reminder_step и планируют следующий шаг под
    # одним и тем же job_id — в итоге может "выиграть" не та операция, и
    # цепочка откатывается / повторно отправляет уже отправленный шаг.
    async with get_lock(chat_id):
        try:
            # Доп. предохранитель: пока мы ждали lock, кто-то (входящее
            # сообщение клиента) мог уже изменить reminder_step — например,
            # отменить цепочку или продвинуть её дальше. Если текущий шаг в
            # БД больше не совпадает с тем, что должны сейчас отправить —
            # значит этот запуск устарел, ничего не шлём.
            current_step = await get_reminder_step(chat_id)
            if current_step != step:
                log.info(
                    f"⏭️ Цепочка напоминаний {chat_id} шаг {step} устарел "
                    f"(текущий reminder_step={current_step}), пропускаем"
                )
                return

            state, history, _, deal_id = await get_state(chat_id)
            if state in REMINDER_CHAIN_BLOCKED_STATES:
                log.info(f"⏭️ Цепочка напоминаний {chat_id} шаг {step} отменена (state={state})")
                await save_reminder_step(chat_id, None)
                return
            if step >= len(REMINDER_CHAIN):
                return
            _, text = REMINDER_CHAIN[step]
            log.info(f"🔔 Цепочка напоминаний: отправляю шаг {step} → {chat_id}")
            await send_wazzup(chat_id, text)
            history.append({"role": "assistant", "content": text})
            await set_state(chat_id, STATE_ACTIVE, history=history, deal_id=deal_id)
            asyncio.create_task(log_message(chat_id, "assistant", text))
            next_step = step + 1
            if next_step < len(REMINDER_CHAIN):
                await save_reminder_step(chat_id, next_step)
                schedule_reminder_chain(chat_id, next_step)
            else:
                log.info(f"🏁 Цепочка напоминаний для {chat_id} завершена (все 5 шагов отправлены)")
                await save_reminder_step(chat_id, None)
        except Exception as e:
            log.error(f"❌ send_reminder_chain_step {step} {chat_id}: {e}")


async def classify_reminder_response(text: str) -> str:
    """Классифицирует ответ клиента на уже отправленное напоминание.
    Вызывается ТОЛЬКО когда клиент отвечает после того, как ему уже ушёл
    хотя бы один шаг цепочки (reminder_step >= 1) — то есть он реагирует
    именно на напоминание, а не продолжает обычный диалог.

    'soft'      — расплывчатый/отложенный ответ без конкретики
                  ("я подумаю", "ок", "хорошо", эмодзи, "занята сейчас" и т.п.)
    'critical'  — реальный вопрос, возражение, готовность обсуждать детали —
                  требует содержательного ответа по существу, интерес сохраняется.
    'disengage' — клиент вежливо, но ясно даёт понять, что предложение ему
                  больше не актуально/не по адресу ("я уже обучен(а)", "я уже
                  ваш клиент", "не туда попал", "уже прошёл курс в другом
                  месте") — не грубый отказ, но и не интерес, продолжать
                  цепочку не нужно.
    При ошибке классификации безопасный дефолт — 'critical' (лучше ответить
    по существу, чем молча продолжить автоматическую рассылку)."""
    try:
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        msg = await client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=10,
            temperature=0,
            system=(
                "Клиент отвечает на автоматическое напоминание от школы фитнеса. "
                "Определи тип ответа. 'soft' — расплывчатый/отложенный ответ без "
                "конкретики (например 'я подумаю', 'ок', 'хорошо', 'сейчас занята', "
                "просто эмодзи, любое подтверждение без вопроса по существу). "
                "'critical' — содержит реальный вопрос, возражение, готовность "
                "обсуждать детали — интерес к обучению сохраняется. "
                "'disengage' — клиент вежливо даёт понять, что предложение больше "
                "не актуально или не по адресу (например 'я уже обучен(а)', 'я уже "
                "прошёл курс', 'я уже ваш клиент', 'не туда попал', 'уже записан "
                "в другом месте') — не грубый отказ, но продолжать не нужно. "
                "Ответь ОДНИМ словом: soft, critical или disengage. Больше ничего не пиши."
            ),
            messages=[{"role": "user", "content": text}],
        )
        result = msg.content[0].text.strip().lower()
        if "disengage" in result:
            return "disengage"
        return "soft" if "soft" in result else "critical"
    except Exception as e:
        log.error(f"❌ classify_reminder_response error: {e}")
        return "critical"


# ---------- EnvyCRM ----------
async def find_lead(username: str, phone: str | None = None, retries: int = 3, delay: float = 3.0) -> int | None:
    url = f"{ENVY_CRM_URL}/openapi/v1/lead/list?api_key={ENVY_API_KEY}"
    headers = {"Content-Type": "application/json"}
    body = {"limit": 1, "inputs": {"phone": phone}} if phone else {"limit": 1, "keyword": username}
    for attempt in range(retries):
        try:
            async with http_session.post(url, json=body, headers=headers) as resp:
                raw = await resp.text()
                data = json.loads(raw) if raw else {}
                leads_data = data.get("leads") or {}
                result = leads_data.get("result") or []
                if result:
                    return result[0]["id"]
                all_ids = leads_data.get("all_ids") or []
                if all_ids and attempt == retries - 1:
                    return all_ids[0]
        except Exception as e:
            log.error(f"❌ find_lead error attempt={attempt+1}: {e}")
        if attempt < retries - 1:
            await asyncio.sleep(delay)
    return None


async def create_lead_log(lead_id: int, comment: str) -> None:
    try:
        url = f"{ENVY_CRM_URL}/openapi/v1/lead/log/create?api_key={ENVY_API_KEY}"
        headers = {"Content-Type": "application/json"}
        body = {"lead_id": lead_id, "type_id": 10, "data": {"comment": comment}}
        async with http_session.post(url, json=body, headers=headers) as resp:
            log.info(f"📝 create_lead_log lead_id={lead_id} [{resp.status}]")
    except Exception as e:
        log.error(f"❌ create_lead_log error: {e}")


async def lead_to_inbox(lead_id: int, chat_id: str, known_deal_id: int | None = None) -> None:
    try:
        headers = {"Content-Type": "application/json"}
        deal_id = known_deal_id

        if not deal_id:
            url1 = f"{ENVY_CRM_URL}/openapi/v1/lead/get?api_key={ENVY_API_KEY}"
            async with http_session.post(url1, json={"lead_id": lead_id}, headers=headers) as resp:
                data = await resp.json()
                deals = data.get("result", {}).get("deals") or []
                if deals:
                    deal_id = deals[0]
                    await save_deal_id(chat_id, deal_id)

        if not deal_id and REAL_MANAGERS:
            random_employee_id = random.choice(REAL_MANAGERS)
            url_start = f"{ENVY_CRM_URL}/openapi/v1/lead/start?api_key={ENVY_API_KEY}"
            body_start = {"lead_id": lead_id, "employee_id": random_employee_id}
            async with http_session.post(url_start, json=body_start, headers=headers) as resp:
                data = await resp.json()
                new_deal_id = (data.get("result") or {}).get("deal_id")
                if new_deal_id:
                    deal_id = new_deal_id
                    await save_deal_id(chat_id, deal_id)

        if not deal_id:
            log.warning(f"⚠️ lead_to_inbox: нет deal_id для lead_id={lead_id}")
            return

        url2 = f"{ENVY_CRM_URL}/openapi/v1/deal/toInbox?api_key={ENVY_API_KEY}"
        async with http_session.post(url2, json={"deal_id": deal_id}, headers=headers) as resp:
            log.info(f"📥 deal/toInbox deal_id={deal_id} [{resp.status}]")
    except Exception as e:
        log.error(f"❌ lead_to_inbox error: {e}")



# ---------- Wazzup: WhatsApp-уведомления Артёму ----------
async def send_whatsapp_to_manager(text: str) -> None:
    url = "https://api.wazzup24.com/v3/message"
    headers = {
        "Authorization": f"Bearer {WAZZUP_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "channelId": WAZZUP_WHATSAPP_CHANNEL_ID,
        "chatId": ARTYOM_WHATSAPP_PERSONAL.lstrip("+"),
        "chatType": "whatsapp",
        "text": text,
    }
    delays = [0, 2, 4]
    for attempt, delay in enumerate(delays):
        if delay:
            await asyncio.sleep(delay)
        try:
            async with http_session.post(url, json=body, headers=headers) as resp:
                result = await resp.text()
                log.info(f"📲 WhatsApp → Артём attempt={attempt+1} [{resp.status}]: {result[:200]}")
                if resp.status < 500:
                    return
        except Exception as e:
            log.warning(f"⚠️ WhatsApp → Артём attempt {attempt+1} error: {e}")
    log.error("❌ WhatsApp → Артём: все 3 попытки провалились")


UNKNOWN_ANSWER_MARKER = "передала ваш вопрос менеджеру"


def detect_unknown_answer(reply: str) -> bool:
    return UNKNOWN_ANSWER_MARKER in (reply or "").lower()


async def extract_client_info(history: list) -> tuple[str | None, str | None]:
    """Пытается вытащить имя и телефон клиента из истории диалога через Claude
    (дёшево — Haiku, вызывается только когда реально нужно для уведомления)."""
    if not history:
        return None, None
    transcript = "\n".join(
        f"{'Клиент' if m.get('role') == 'user' else 'Луна'}: {m.get('content', '')}"
        for m in history[-20:]
    )
    try:
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        msg = await client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=100,
            temperature=0,
            system=(
                "Извлеки из диалога имя клиента и номер телефона, если они там названы. "
                'Ответь СТРОГО в формате JSON: {"name": "...", "phone": "..."} '
                'Если чего-то нет — null вместо значения. Больше ничего не пиши.'
            ),
            messages=[{"role": "user", "content": transcript}],
        )
        raw = msg.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.MULTILINE).strip()
        data = json.loads(raw)
        return data.get("name"), data.get("phone")
    except Exception as e:
        log.error(f"❌ extract_client_info error: {e}")
        return None, None


async def send_unknown_question_notification(chat_id: str, client_text: str, history: list) -> None:
    """Луна не знает ответа и пообещала клиенту связь с менеджером — уведомляем Артёма."""
    name, phone = await extract_client_info(history)
    ig_link = f"https://instagram.com/{chat_id}"
    lines = [
        "❓ Луна не смогла ответить — обещала связь с менеджером",
        "",
        f"Instagram: @{chat_id} ({ig_link})",
    ]
    if name:
        lines.append(f"Имя: {name}")
    if phone:
        lines.append(f"Телефон: {phone}")
    lines.append(f"Вопрос клиента: «{client_text[:300]}»")
    await send_whatsapp_to_manager("\n".join(lines))


whatsapp_escalated: set[str] = set()  # чтобы не слать уведомление повторно по одной и той же теме в диалоге


async def send_whatsapp_escalation(chat_id: str, category: str, client_text: str) -> None:
    """Гарантированно (не полагаясь на LLM) уведомляет Артёма в WhatsApp
    при триггерной теме — перенос/возврат/оплата/сертификат."""
    dedup_key = f"{chat_id}:{category}"
    if dedup_key in whatsapp_escalated:
        return
    whatsapp_escalated.add(dedup_key)
    if len(whatsapp_escalated) > 10000:
        whatsapp_escalated.clear()

    ig_link = f"https://instagram.com/{chat_id}"
    text = (
        f"🙋 Луна: клиент требует вовлечения ({category})\n\n"
        f"Instagram: @{chat_id} ({ig_link})\n"
        f"Сообщение клиента: «{client_text[:300]}»"
    )
    await send_whatsapp_to_manager(text)


payment_notified: set[str] = set()  # чтобы не слать повторно про оплату в одном диалоге


async def send_payment_notification(chat_id: str, client_text: str) -> None:
    """Клиент написал, что оплатил — уведомляем Артёма для ручной проверки в Kaspi/CRM."""
    if chat_id in payment_notified:
        return
    payment_notified.add(chat_id)
    if len(payment_notified) > 10000:
        payment_notified.clear()

    ig_link = f"https://instagram.com/{chat_id}"
    text = (
        f"💰 Луна: клиент утверждает, что оплатил(а) — нужна проверка!\n\n"
        f"Instagram: @{chat_id} ({ig_link})\n"
        f"Сообщение клиента: «{client_text[:300]}»\n\n"
        f"Проверьте поступление в Kaspi/CRM и подтвердите клиенту."
    )
    await send_whatsapp_to_manager(text)


def detect_involvement_category(text: str) -> str | None:
    t = text.lower()
    for category, phrases in INVOLVEMENT_TRIGGERS.items():
        if any(p in t for p in phrases):
            return category
    return None


async def notify_manager(
    chat_id: str, username: str, phone: str | None = None, known_deal_id: int | None = None
) -> None:
    try:
        lead_id = await find_lead(username, phone)
        if lead_id is None:
            log.warning(f"⚠️ notify_manager: лид не найден username={username}")
            return
        if phone:
            await create_lead_log(lead_id, f"🤖 Луна: клиент {username} оставил номер {phone}. Позвонить!")
        else:
            await create_lead_log(lead_id, f"🤖 Луна: новый клиент {username} написал в Instagram.")
        asyncio.create_task(lead_to_inbox(lead_id, chat_id, known_deal_id))
    except Exception as e:
        log.error(f"❌ notify_manager error: {e}")


# ---------- Helpers ----------
def extract_phone(text: str) -> str | None:
    m = PHONE_RE.search(text)
    if m and len(re.sub(r"\D", "", m.group())) >= 10:
        return m.group()
    return None


def is_refusal(text: str) -> bool:
    lower = text.lower().strip()
    # "нет" само по себе слишком частое слово в обычных фразах ("нет времени",
    # "нет, не в этом дело") — засчитываем его как отказ ТОЛЬКО если это весь
    # ответ целиком (короткое "нет" / "Нет." / "нет!!"), а не подстрока внутри
    # длинного сообщения.
    stripped = lower.rstrip(".!? ")
    if stripped in ("нет", "жоқ"):
        return True
    if "заеб" in lower:  # без \b — "заебал"/"заебала" не имеют границы слова после корня
        return True
    return any(re.search(r'\b' + re.escape(p) + r'\b', lower) for p in REFUSE_WORDS)


WHATSAPP_HANDOFF_KEYWORDS = ["ватсап", "вацап", "вотсап", "whatsapp", "вотсапп", "ватцап"]


def mentions_whatsapp(text: str) -> bool:
    """Дешёвая предфильтрация: упоминается ли WhatsApp вообще. Используется
    только чтобы решить, стоит ли вызывать классификатор ниже — сама по себе
    НЕ означает подтверждение переноса разговора (см. classify_whatsapp_mention)."""
    lower = (text or "").lower()
    return any(kw in lower for kw in WHATSAPP_HANDOFF_KEYWORDS)


async def classify_whatsapp_mention(text: str) -> bool:
    """Разделяет ДВА разных случая, которые по ключевым словам выглядят
    одинаково: менеджер ЗАПРОСИЛ номер WhatsApp у клиента ("поделитесь,
    пожалуйста, номером ватсап") — тогда клиент мог не ответить, и разговор
    в Instagram ещё не завершён, напоминания нужны как обычно — vs менеджер
    ПОДТВЕРДИЛ перенос разговора ("менеджер напишет Вам на ватсап") — тогда
    Instagram-тред закрыт, автоматические напоминания здесь больше не нужны.
    Возвращает True только во втором случае (handoff)."""
    try:
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        msg = await client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=5,
            temperature=0,
            system=(
                "Менеджер фитнес-школы написал клиенту сообщение, упоминающее "
                "WhatsApp. Определи тип сообщения. 'handoff' — менеджер "
                "ПОДТВЕРЖДАЕТ, что сам напишет/свяжется с клиентом в WhatsApp "
                "(разговор переносится туда, здесь его можно закрывать). "
                "'request' — менеджер ЗАПРАШИВАЕТ номер WhatsApp у клиента, "
                "задаёт вопрос или уточняет (клиент мог не ответить, разговор "
                "в этом канале ещё не завершён). Ответь ОДНИМ словом: "
                "handoff или request."
            ),
            messages=[{"role": "user", "content": text}],
        )
        result = msg.content[0].text.strip().lower()
        return "handoff" in result
    except Exception as e:
        log.error(f"❌ classify_whatsapp_mention error: {e}")
        return False  # безопасный дефолт — не отменяем напоминания, если не уверены


def detect_lang(text: str) -> str:
    kz_chars = set("әіңғүұқөһ")
    kz_words = {"керек", "емес", "жоқ", "бар", "қайда", "қалай", "рахмет", "сәлем", "жақсы", "бұл"}
    lower_text = text.lower()
    if any(c in kz_chars for c in lower_text):
        return "kz"
    words_in_text = set(re.findall(r"[а-яәіңғүұқөһa-z]+", lower_text))
    if words_in_text & kz_words:
        return "kz"
    return "ru"


def detect_demo_sent(reply: str) -> bool:
    # ВАЖНО: ловим только реально отправленную ссылку на платформу (домен из базы
    # знаний), а не просто упоминание слова "демо" — иначе фраза-предложение вида
    # "Могу отправить бесплатный демо-доступ..." (шаг 8 промпта, до согласия клиента
    # и до самой ссылки) уже засчитывалась бы как "демо отправлено" и включала
    # напоминания, хотя ссылка ещё не отправлена.
    demo_markers = ["skillspace.ru"]
    return any(marker in reply.lower() for marker in demo_markers)


async def claude_reply(messages: list[dict], system_prompt: str | None = None, kb: str | None = None) -> str:
    # Убираем ведущие assistant-сообщения
    while messages and messages[0].get("role") == "assistant":
        messages = messages[1:]
    # Убираем подряд идущие одинаковые роли
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

    # Два отдельных кешируемых блока:
    # Блок 1 — промпт (меняется редко, кеш живёт долго)
    # Блок 2 — база знаний из Sheets (меняется каждые 30 мин, свой кеш)
    system_blocks = [
        {
            "type": "text",
            "text": system_prompt or SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        },
    ]
    if kb:
        system_blocks.append({
            "type": "text",
            "text": "<knowledge_base>\n" + kb + "\n</knowledge_base>",
            "cache_control": {"type": "ephemeral"},
        })

    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    msg = await client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=1024,
        temperature=0.2,
        system=system_blocks,
        messages=messages,
    )
    return msg.content[0].text


async def transcribe_audio(url: str) -> str | None:
    try:
        async with http_session.get(url) as resp:
            if resp.status != 200:
                return None
            audio_bytes = await resp.read()
        buf = io.BytesIO(audio_bytes)
        buf.name = "audio.ogg"
        client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
        result = await client.audio.transcriptions.create(model="whisper-1", file=buf)
        return result.text.strip() or None
    except Exception as e:
        log.error(f"❌ transcribe_audio error: {e}")
        return None


# ---------- Основная логика ----------
async def handle_incoming(chat_id: str, text: str | None) -> None:
    async with get_lock(chat_id):
        await _handle_incoming(chat_id, text)


async def _handle_incoming(chat_id: str, text: str | None) -> None:
    # Автографик Луны: 20:00-11:00 Астана. В окно 11:00-20:00 работают живые
    # менеджеры — бот молчит, но сообщение сохраняем в историю, чтобы контекст
    # не терялся к моменту, когда Луна снова включится.
    if LUNA_SCHEDULE_ENABLED and not is_luna_working_hours():
        if text:
            state, history, _, deal_id = await get_state(chat_id)
            if state is None:
                # Совсем новый лид, диалога в БД ещё нет. Не создаём строку с
                # каким-то промежуточным state — иначе сломается ветка
                # "state is None" (первое приветствие) при следующем сообщении.
                # Сообщение всё равно видно менеджеру напрямую в EnvyCRM.
                log.info(f"🌙 {chat_id}: новый лид вне графика Луны — не создаём диалог, ждём следующего сообщения")
            else:
                history = (history or []) + [{"role": "user", "content": text}]
                history = history[-MAX_HISTORY:]
                await set_state(chat_id, state, history=history, deal_id=deal_id)
                asyncio.create_task(log_message(chat_id, "user", text))
                log.info(f"🌙 {chat_id}: вне графика Луны (11:00-20:00 Астана — работают менеджеры), сообщение сохранено в историю, не отвечаем")
                return
        log.info(f"🌙 {chat_id}: вне графика Луны (11:00-20:00 Астана — работают менеджеры), не отвечаем")
        return

    # Два отдельных кешируемых блока: промпт + база знаний
    kb = sheets_sync.get_cached_knowledge_base(fallback=KNOWLEDGE_BASE)

    state, history, updated_at, deal_id = await get_state(chat_id)
    reminder_step_before = await get_reminder_step(chat_id) if text else None

    if text:
        # Клиент написал — в любом случае гасим запланированный шаг цепочки
        # напоминаний, независимо от текущего state (даже если чат сейчас
        # закреплён за менеджером — клиент всё равно уже не молчит).
        cancel_reminder_chain(chat_id)

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
            if state == STATE_MANAGER and elapsed >= 1800:  # 30 мин
                log.info(f"🔄 {chat_id} state=manager устарел (>30мин), сбрасываем")
                # ВАЖНО: сразу пишем сброс в БД (обычным set_state, без guard).
                # Раньше state сбрасывался только в локальной переменной — реальная
                # запись в БД оставалась 'manager', и последующий set_state_guarded()
                # в конце функции (у которого условие WHERE state NOT IN ('manager',...))
                # молча не срабатывал. В итоге диалог навсегда застревал в 'manager'.
                await set_state(chat_id, STATE_ACTIVE, history=history, deal_id=deal_id)
                state = None
            elif state in {STATE_DONE, STATE_REFUSED} and elapsed >= 3600:
                log.info(f"🔄 {chat_id} state={state} устарел, сбрасываем")
                await set_state(chat_id, STATE_ACTIVE, history=history, deal_id=deal_id)
                state = None
            else:
                if state == STATE_MANAGER and text:
                    # Lesson 11: сохраняем сообщение — ответим сами если менеджер не подключится
                    await save_pending_message(chat_id, text)
                    log.info(f"💤 {chat_id} state=manager, сообщение сохранено (ответим через 30 мин)")
                else:
                    log.info(f"🔇 {chat_id} state={state}, молчим")
                return
        else:
            log.info(f"🔇 {chat_id} state={state}, молчим")
            return

    if text:
        asyncio.create_task(log_message(chat_id, "user", text))

    # Новый диалог
    if state is None:
        if history is None:
            history = []
        is_truly_new = len(history) == 0
        if text:
            history.append({"role": "user", "content": text})
        else:
            history.append({"role": "user", "content": "Здравствуйте"})
        try:
            reply = await claude_reply(history, SYSTEM_PROMPT, kb=kb)
            if not reply or not reply.strip():
                raise ValueError("пустой ответ")
        except Exception as e:
            log.error(f"❌ Claude error on greeting: {e}")
            reply = CLAUDE_FALLBACK.get(detect_lang(text or ""), CLAUDE_FALLBACK["ru"])

        history.append({"role": "assistant", "content": reply})

        # Lesson 11: перепроверяем state ПОСЛЕ генерации (гонка с менеджером)
        current_state, _, _, _ = await get_state(chat_id)
        if current_state in SILENT_STATES:
            log.info(f"🚫 {chat_id} — менеджер взял чат пока Claude думал, отмена отправки")
            return

        log.info(f"💬 Ответ Луны → {chat_id}: {reply[:300]}")
        await send_wazzup(chat_id, reply)
        asyncio.create_task(log_message(chat_id, "assistant", reply))
        if detect_unknown_answer(reply):
            asyncio.create_task(
                send_unknown_question_notification(chat_id, text or "(первое сообщение)", history)
            )
        if len(last_bot_reply) >= 10000:
            del last_bot_reply[next(iter(last_bot_reply))]
        last_bot_reply[chat_id] = reply

        new_state = STATE_NEW if is_truly_new else STATE_ACTIVE
        if detect_demo_sent(reply):
            new_state = STATE_DEMO_SENT
        await set_state_guarded(chat_id, new_state, history=history)
        # Цепочка напоминаний запускается после ЛЮБОГО нашего сообщения, в том
        # числе после самого первого приветствия — если лид вообще не ответил.
        await start_reminder_chain(chat_id)

        if should_notify(chat_id):
            asyncio.create_task(notify_manager(chat_id, chat_id, known_deal_id=deal_id))

        # Lesson 9.5: эскалация работает и для ПЕРВОГО сообщения
        if text:
            phone = extract_phone(text)
            if phone:
                asyncio.create_task(notify_manager(chat_id, chat_id, phone, known_deal_id=deal_id))
            involvement_category = detect_involvement_category(text)
            if involvement_category:
                asyncio.create_task(send_whatsapp_escalation(chat_id, involvement_category, text))
                log.info(f"🙋 {chat_id} требует вовлечения ({involvement_category}) — первым сообщением")
            if detect_payment_claim(text):
                asyncio.create_task(send_payment_notification(chat_id, text))
                log.info(f"💰 {chat_id} утверждает, что оплатил — первым сообщением")

        log.info(f"👋 {'Новый' if is_truly_new else 'Возобновлённый'} диалог {chat_id} → {new_state}")
        return

    # state = new / active / demo_sent — нужен текст
    if not text:
        return

    phone = extract_phone(text)
    if phone:
        asyncio.create_task(notify_manager(chat_id, chat_id, phone, known_deal_id=deal_id))

    # Lesson 9.5: эскалация для всех состояний
    involvement_category = detect_involvement_category(text)
    if involvement_category:
        asyncio.create_task(send_whatsapp_escalation(chat_id, involvement_category, text))
        log.info(f"🙋 {chat_id} требует вовлечения ({involvement_category})")

    if detect_payment_claim(text):
        asyncio.create_task(send_payment_notification(chat_id, text))
        log.info(f"💰 {chat_id} утверждает, что оплатил")

    if is_refusal(text):
        lang = detect_lang(text)
        farewell = FAREWELL_MSGS.get(lang, FAREWELL_MSGS["ru"])
        history.append({"role": "user", "content": text})
        history.append({"role": "assistant", "content": farewell})
        await send_wazzup(chat_id, farewell)
        asyncio.create_task(log_message(chat_id, "assistant", farewell))
        await set_state(chat_id, STATE_REFUSED, history=history)
        cancel_reminder_chain(chat_id)
        await save_reminder_step(chat_id, None)
        return

    # Клиент отвечает именно на уже отправленное напоминание (reminder_step_before
    # >= 1 означает, что хотя бы один шаг цепочки реально ушёл клиенту) — нужно
    # понять, "мягкий" это ответ (тогда просто коротко реагируем и переносим
    # СЛЕДУЮЩИЙ шаг цепочки на его собственную дельту от этого момента, не сбрасывая
    # всю цепочку) или "критичный" (тогда отвечаем по существу как обычно, и уже
    # обычная логика ниже перезапустит цепочку с нуля после отправки ответа).
    disengaged = False
    if reminder_step_before is not None and reminder_step_before >= 1:
        response_type = await classify_reminder_response(text)
        if response_type == "disengage":
            # Клиент вежливо даёт понять, что предложение больше не актуально
            # ("я уже обучен(а)", "уже ваш клиент" и т.п.) — не грубый отказ,
            # но продолжать цепочку не нужно. Отвечаем один раз по существу
            # через обычный Claude-флоу, а цепочку останавливаем насовсем
            # (как при явном отказе), а не перезапускаем с нуля.
            cancel_reminder_chain(chat_id)
            await save_reminder_step(chat_id, None)
            disengaged = True
            log.info(f"🚪 {chat_id}: клиент вне ЦА по ответу на напоминание ({text[:100]!r}), цепочка остановлена насовсем")
            # Дальше просто продолжаем обычной веткой ниже — Claude ответит по
            # существу, но start_reminder_chain() в конце не будет вызван,
            # т.к. ниже это условие проверяется через флаг disengaged.
        elif response_type == "soft":
            history.append({"role": "user", "content": text})
            history = history[-MAX_HISTORY:]
            try:
                soft_reply = await claude_reply(
                    history,
                    SYSTEM_PROMPT + (
                        "\n\nВАЖНО: клиент только что ответил расплывчато/отложенно на "
                        "автоматическое напоминание (например \"я подумаю\", \"ок\", "
                        "\"хорошо\"). Отреагируй коротко и тепло (1-2 предложения), без "
                        "давления и не повторяя вопросы воронки — просто дай понять, что "
                        "ты на связи и не торопишь."
                    ),
                    kb=kb,
                )
                if not soft_reply or not soft_reply.strip():
                    raise ValueError("пустой ответ")
            except Exception as e:
                log.error(f"❌ Claude error on soft reminder reply: {e}")
                soft_reply = CLAUDE_FALLBACK.get(detect_lang(text), CLAUDE_FALLBACK["ru"])
            history.append({"role": "assistant", "content": soft_reply})
            history = history[-MAX_HISTORY:]

            current_state, _, _, _ = await get_state(chat_id)
            if current_state in SILENT_STATES:
                log.info(f"🚫 {chat_id} — менеджер взял чат пока Claude думал, отмена отправки")
                return

            log.info(f"💬 Мягкий ответ на напоминание → {chat_id}: {soft_reply[:200]}")
            await send_wazzup(chat_id, soft_reply)
            asyncio.create_task(log_message(chat_id, "assistant", soft_reply))
            await set_state_guarded(chat_id, STATE_ACTIVE, history=history)
            # Не сбрасываем цепочку на шаг 0 — переносим ИМЕННО следующий
            # запланированный шаг на его собственную дельту от текущего момента.
            anchor_now = datetime.now(timezone.utc)
            schedule_reminder_chain(chat_id, reminder_step_before, anchor=anchor_now)
            await save_reminder_step(chat_id, reminder_step_before)
            if should_notify(chat_id):
                asyncio.create_task(notify_manager(chat_id, chat_id, known_deal_id=deal_id))
            return
        # response_type == "critical" — просто продолжаем обычной веткой ниже

    history.append({"role": "user", "content": text})
    history = history[-MAX_HISTORY:]

    try:
        reply = await claude_reply(history, SYSTEM_PROMPT, kb=kb)
        if not reply or not reply.strip():
            raise ValueError("пустой ответ")
    except Exception as e:
        log.error(f"❌ Claude error: {e}")
        reply = CLAUDE_FALLBACK.get(detect_lang(text or ""), CLAUDE_FALLBACK["ru"])

    history.append({"role": "assistant", "content": reply})
    history = history[-MAX_HISTORY:]

    # Lesson 11: перепроверяем state после генерации
    current_state, _, _, _ = await get_state(chat_id)
    if current_state in SILENT_STATES:
        log.info(f"🚫 {chat_id} — менеджер взял чат пока Claude думал, отмена отправки")
        return

    log.info(f"💬 Ответ Луны → {chat_id}: {reply[:300]}")
    await send_wazzup(chat_id, reply)
    asyncio.create_task(log_message(chat_id, "assistant", reply))
    if detect_unknown_answer(reply):
        asyncio.create_task(send_unknown_question_notification(chat_id, text, history))
    if len(last_bot_reply) >= 10000:
        del last_bot_reply[next(iter(last_bot_reply))]
    last_bot_reply[chat_id] = reply

    if detect_demo_sent(reply):
        await set_state_guarded(chat_id, STATE_DEMO_SENT, history=history)
    else:
        await set_state_guarded(chat_id, STATE_ACTIVE, history=history)
    if not disengaged:
        # Любое исходящее сообщение (в т.ч. этот обычный/критичный ответ) заново
        # запускает цепочку напоминаний с шага 0 (первое напоминание — через 1 час).
        await start_reminder_chain(chat_id)
    else:
        log.info(f"🚪 {chat_id}: цепочка не перезапускается (клиент вне ЦА)")

    if should_notify(chat_id):
        asyncio.create_task(notify_manager(chat_id, chat_id, known_deal_id=deal_id))


# ---------- Автоответ на зависшие в STATE_MANAGER чаты (30 мин) ----------
async def _answer_pending(chat_id: str) -> None:
    state, history, updated_at, deal_id = await get_state(chat_id)
    if state != STATE_MANAGER:
        return
    if not history or history[-1].get("role") != "user":
        await clear_awaiting_reply(chat_id)
        return

    text = history[-1].get("content", "")
    kb = sheets_sync.get_cached_knowledge_base(fallback=KNOWLEDGE_BASE)

    try:
        reply = await claude_reply(history, SYSTEM_PROMPT, kb=kb)
        if not reply or not reply.strip():
            raise ValueError("пустой ответ")
    except Exception as e:
        log.error(f"❌ Claude error on pending reply: {e}")
        reply = CLAUDE_FALLBACK.get(detect_lang(text or ""), CLAUDE_FALLBACK["ru"])

    history.append({"role": "assistant", "content": reply})
    history = history[-MAX_HISTORY:]

    # Lesson 11: перепроверяем — менеджер мог взять чат пока Claude думал
    current_state, _, _, _ = await get_state(chat_id)
    if current_state != STATE_MANAGER:
        log.info(f"🛑 {chat_id} — стейт изменился пока готовили отложенный ответ, не отправляем")
        return

    log.info(f"💬 Отложенный ответ Луны (менеджер не подключился за 30 мин) → {chat_id}: {reply[:300]}")
    await send_wazzup(chat_id, reply)
    asyncio.create_task(log_message(chat_id, "assistant", reply))
    if detect_unknown_answer(reply):
        asyncio.create_task(send_unknown_question_notification(chat_id, text, history))
    if len(last_bot_reply) >= 10000:
        del last_bot_reply[next(iter(last_bot_reply))]
    last_bot_reply[chat_id] = reply
    await set_state(chat_id, STATE_ACTIVE, history=history)
    await start_reminder_chain(chat_id)


async def resume_unanswered_manager_chats() -> None:
    """Раз в минуту проверяет чаты в STATE_MANAGER дольше 30 минут с unanswered сообщением."""
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
                cid = row["chat_id"]
                async with get_lock(cid):
                    await _answer_pending(cid)
        except Exception as e:
            log.error(f"⚠️ resume_unanswered_manager_chats: {e}")


# ---------- Эндпоинты ----------
async def _debounced_handle_incoming(chat_id: str) -> None:
    """Ждёт DEBOUNCE_SECONDS без новых сообщений от chat_id, затем склеивает
    всё что накопилось в один текст и обрабатывает разом (одним ответом)."""
    try:
        await asyncio.sleep(DEBOUNCE_SECONDS)
    except asyncio.CancelledError:
        return  # пришло новое сообщение — таймер перезапущен снаружи

    texts = pending_buffer.pop(chat_id, [])
    pending_tasks.pop(chat_id, None)
    if not texts:
        return

    combined = "\n".join(t for t in texts if t).strip() or None
    if len(texts) > 1:
        log.info(f"🧩 Склеено {len(texts)} сообщений от {chat_id} в одно")

    try:
        await handle_incoming(chat_id, combined)
    except Exception as e:
        log.error(f"❌ handle_incoming error {chat_id}: {e}", exc_info=True)


async def envy_hook_handler(request: web.Request) -> web.Response:
    if BOT_PAUSED_INSTAGRAM:
        return web.Response(text="ok")

    try:
        payload = await request.json()
    except Exception:
        return web.Response(text="ok")

    log.info(f"📨 envy_hook: {json.dumps(payload, ensure_ascii=False)[:1000]}")

    event_type = payload.get("event_type")

    if event_type == "message_reply":
        message_text = (payload.get("message_data") or {}).get("text") or ""
        contact_check = payload.get("contact") or {}
        chat_id_check = str(contact_check.get("external_id") or "").strip()
        if chat_id_check.startswith("inst-"):
            chat_id_check = chat_id_check[5:]

        # Lesson 10: вложение без подписи — не игнорируем, пишем что получили
        if not message_text and chat_id_check:
            attachments = (payload.get("message_data") or {}).get("attachments") or []
            if attachments:
                log.info(f"📎 {chat_id_check} прислал вложение без подписи (message_reply)")

        if message_text and chat_id_check in sent_texts and message_text in sent_texts.get(chat_id_check, {}):
            log.info(f"🔄 Эхо Луны text={message_text[:50]!r}, игнорируем")
            return web.Response(text="ok")

        if any(kw in message_text.lower() for kw in SMM_KEYWORDS):
            if chat_id_check:
                await set_state(chat_id_check, STATE_SMM)
            return web.Response(text="ok")

        from_user = payload.get("from_user") or {}
        crm_employee_id = from_user.get("crm_employee_id")
        if crm_employee_id and crm_employee_id != 0 and crm_employee_id > 100000:
            # Lesson (перенесено с Лолы): EnvyCRM иногда шлёт message_reply с пустым
            # message_text, когда менеджер просто открыл карточку клиента, ничего не
            # печатая. Это не реальный ответ — не переводим чат в STATE_MANAGER за это.
            if not message_text.strip():
                log.info(f"👀 message_reply с пустым текстом (крм_employee_id={crm_employee_id}) — вероятно открытие карточки, не реальный ответ, игнорируем")
                return web.Response(text="ok")

            contact = payload.get("contact") or {}
            chat_id = str(contact.get("external_id") or "").strip()
            if chat_id.startswith("inst-"):
                chat_id = chat_id[5:]
            if chat_id:
                stored_last = last_bot_reply.get(chat_id, "")
                if message_text and stored_last and message_text.strip() == stored_last.strip():
                    log.info(f"🔄 Эхо с crm_employee_id={crm_employee_id} — игнорируем")
                else:
                    # Lesson: НЕ используем общий per-chat lock здесь — если бот сейчас
                    # генерирует ответ через Claude (держит get_lock несколько секунд),
                    # запись STATE_MANAGER вставала бы в очередь за этим локом и не
                    # успевала примениться до того, как бот уже отправит сообщение.
                    # Прямая запись в БД без ожидания общего лока решает эту гонку.
                    await set_state(chat_id, STATE_MANAGER)
                    await clear_awaiting_reply(chat_id)
                    is_handoff = mentions_whatsapp(message_text) and await classify_whatsapp_mention(message_text)
                    if is_handoff:
                        cancel_reminder_chain(chat_id)
                        await save_reminder_step(chat_id, None)
                        log.info(f"👨‍💼 Менеджер взял {chat_id} → STATE_MANAGER (подтверждён перенос в WhatsApp, напоминания в Instagram отключены)")
                    else:
                        await start_reminder_chain(chat_id, start_step=REMINDER_CHAIN_MANAGER_START_STEP)
                        log.info(f"👨‍💼 Менеджер взял {chat_id} → STATE_MANAGER (реальный ответ, цепочка напоминаний запущена с 3-го шага/5ч от сообщения менеджера)")
        return web.Response(text="ok")

    if event_type != "message":
        return web.Response(text="ok")

    # Дедупликация
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
        return web.Response(text="ok")

    from_user = payload.get("from_user") or {}
    crm_employee_id = from_user.get("crm_employee_id")
    if crm_employee_id is not None and crm_employee_id != 0 and crm_employee_id > 100000:
        await set_state(chat_id, STATE_MANAGER)
        manager_text = (payload.get("message_data") or {}).get("text") or ""
        is_handoff = mentions_whatsapp(manager_text) and await classify_whatsapp_mention(manager_text)
        if is_handoff:
            cancel_reminder_chain(chat_id)
            await save_reminder_step(chat_id, None)
            log.info(f"👨‍💼 {chat_id} → STATE_MANAGER (подтверждён перенос в WhatsApp, напоминания в Instagram отключены)")
        else:
            await start_reminder_chain(chat_id, start_step=REMINDER_CHAIN_MANAGER_START_STEP)
        return web.Response(text="ok")

    message_data = payload.get("message_data") or {}
    raw_text = message_data.get("text") or ""
    attachments = message_data.get("attachments") or []

    if raw_text.strip() == "You mentioned in the story":
        return web.Response(text="ok")
    if any(a.get("type") in ("story", "video") for a in attachments):
        # Реклама Reels/сторис в Instagram при "Написать" шлёт отдельным сообщением
        # весь текст объявления (с эмодзи/видео) — это не то, что печатает клиент.
        # Реальный вопрос клиента всегда приходит отдельным сообщением следом.
        log.info(f"📹 Пропускаем автоцитату рекламы (video/story) от chat_id={chat_id}")
        return web.Response(text="ok")

    if any(a.get("type") in ("audio", "voice") and not raw_text.strip() for a in attachments):
        audio_url = next(
            (a.get("link") or a.get("url") for a in attachments if a.get("type") in ("audio", "voice")),
            None,
        )
        text = await transcribe_audio(audio_url) or "[клиент отправил голосовое сообщение]"
    elif any(a.get("type") in ("image", "file") and not raw_text.strip() for a in attachments):
        # Lesson 10: клиент прислал фото/файл без подписи (например, чек) — не молчим
        text = "[клиент отправил вложение]"
        # Луна не читает содержимое файлов (PDF/фото) — подстраховываемся и всегда
        # шлём Артёму на проверку, вдруг это чек об оплате
        attach_link = next(
            (a.get("link") or a.get("url") for a in attachments if a.get("type") in ("image", "file")),
            None,
        )
        asyncio.create_task(
            send_payment_notification(
                chat_id,
                f"[файл без подписи, возможно чек]" + (f"\nСсылка: {attach_link}" if attach_link else ""),
            )
        )
    else:
        text = raw_text.strip() if raw_text else None

    if any(kw in (text or "").lower() for kw in SMM_KEYWORDS):
        await set_state(chat_id, STATE_SMM)
        return web.Response(text="ok")

    # ---------- Таргет-реклама: комментарий "хочу" → авто-DM ----------
    # Применяем только к новым диалогам (ещё нет state в БД) — чтобы случайно
    # не перехватить обработку у уже идущего разговора.
    ad_comment_text = detect_ad_comment_text(payload)
    if ad_comment_text is not None:
        existing_state, _, _, _ = await get_state(chat_id)
        if existing_state is None:
            if AD_COMMENT_TRIGGER_WORD not in ad_comment_text.lower():
                log.info(
                    f"🔕 {chat_id}: комментарий без слова "
                    f"'{AD_COMMENT_TRIGGER_WORD}' ({ad_comment_text[:80]!r}), игнорируем"
                )
                return web.Response(text="ok")
            opener = random.choice(AD_COMMENT_OPENERS)
            await send_wazzup(chat_id, opener)
            history = [
                {"role": "user", "content": ad_comment_text},
                {"role": "assistant", "content": opener},
            ]
            await set_state(chat_id, STATE_ACTIVE, history=history)
            await start_reminder_chain(chat_id)
            asyncio.create_task(log_message(chat_id, "user", ad_comment_text))
            asyncio.create_task(log_message(chat_id, "assistant", opener))
            log.info(f"🎯 {chat_id}: ответила на рекламный комментарий 'хочу' фиксированным приветствием")
            if should_notify(chat_id):
                asyncio.create_task(notify_manager(chat_id, chat_id))
            return web.Response(text="ok")

    try:
        pending_buffer.setdefault(chat_id, []).append(text or "")
        old_task = pending_tasks.get(chat_id)
        if old_task and not old_task.done():
            old_task.cancel()
        pending_tasks[chat_id] = asyncio.create_task(_debounced_handle_incoming(chat_id))
    except Exception as e:
        log.error(f"❌ debounce schedule error {chat_id}: {e}", exc_info=True)

    return web.Response(text="ok")


async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "bot": "Luna", "school": "Champion School"})


# ---------- DB init ----------
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
        await conn.execute("ALTER TABLE dialogs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
        await conn.execute("ALTER TABLE dialogs ADD COLUMN IF NOT EXISTS history JSONB NOT NULL DEFAULT '[]'")
        await conn.execute("ALTER TABLE dialogs ADD COLUMN IF NOT EXISTS deal_id BIGINT")
        await conn.execute("ALTER TABLE dialogs ADD COLUMN IF NOT EXISTS awaiting_reply BOOLEAN NOT NULL DEFAULT FALSE")
        # Индекс шага цепочки напоминаний (0-4, NULL = цепочка не запланирована/завершена)
        await conn.execute("ALTER TABLE dialogs ADD COLUMN IF NOT EXISTS reminder_step INTEGER")
        # Лог сообщений клиентов — для ежедневного отчёта Артёму (кол-во клиентов, топ-темы)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS message_log (
                id         BIGSERIAL PRIMARY KEY,
                chat_id    TEXT NOT NULL,
                role       TEXT NOT NULL,
                text       TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_message_log_created_at ON message_log (created_at)")
    log.info("✅ DB готова")


async def close_db(app: web.Application) -> None:
    if db_pool:
        await db_pool.close()


# ---------- HTTP-сессия (одна на всё приложение вместо новой на каждый запрос) ----------
async def init_http_session(app: web.Application) -> None:
    global http_session
    http_session = aiohttp.ClientSession()
    log.info("✅ HTTP-сессия инициализирована")


async def close_http_session(app: web.Application) -> None:
    if http_session:
        await http_session.close()


# ---------- Ежедневный отчёт Артёму ----------
async def summarize_top_questions(texts: list[str]) -> str:
    if not texts:
        return "— нет данных —"
    joined = "\n".join(f"- {t}" for t in texts)
    try:
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        msg = await client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=400,
            temperature=0.2,
            system=(
                "Ты анализируешь сообщения клиентов фитнес-школы за сутки. "
                "Выдели 5 самых частых тем/вопросов коротким списком на русском, "
                "без вступлений и заключений. Каждая тема с новой строки, начинай с «•»."
            ),
            messages=[{"role": "user", "content": joined[:20000]}],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        log.error(f"❌ summarize_top_questions error: {e}")
        return "— не удалось проанализировать —"


LUNA_SHIFT_HOURS = 15  # рабочее окно Луны: 20:00-11:00 Астана = 15 часов


async def build_and_send_daily_report() -> None:
    try:
        # Отчёт шлётся в конце смены (11:00 Астана) — окно строго 20:00-11:00,
        # а не произвольные последние 24 часа (которые раньше захватывали и
        # нерабочее для Луны время суток).
        since = datetime.now(timezone.utc) - timedelta(hours=LUNA_SHIFT_HOURS)
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT chat_id, text FROM message_log WHERE role='user' AND created_at >= $1",
                since,
            )
        # Дата смены — день, когда началось окно 20:00 (т.е. "вчера" относительно
        # момента отправки отчёта в 11:00 Астана).
        shift_date = (datetime.now(ASTANA_TZ) - timedelta(days=1)).strftime("%d.%m")
        period_label = f"{shift_date} (20:00-11:00)"

        if not rows:
            await send_whatsapp_to_manager(f"📊 Отчёт Луны за {period_label}: новых обращений не было.")
            return

        unique_clients = len({r["chat_id"] for r in rows})
        total_messages = len(rows)
        sample_texts = [r["text"] for r in rows if r["text"]][:200]
        topics_summary = await summarize_top_questions(sample_texts)

        report = (
            f"📊 Отчёт Луны за {period_label}\n\n"
            f"👥 Уникальных клиентов: {unique_clients}\n"
            f"💬 Сообщений от клиентов: {total_messages}\n\n"
            f"🔝 Частые темы:\n{topics_summary}"
        )
        await send_whatsapp_to_manager(report)
        log.info("📊 Отчёт за смену отправлен Артёму")
    except Exception as e:
        log.error(f"❌ build_and_send_daily_report error: {e}")


# ---------- Scheduler init ----------
async def init_scheduler(app: web.Application) -> None:
    global scheduler
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.start()
    log.info("✅ APScheduler запущен")

    # Первая синхронизация Google Sheets
    loop = asyncio.get_event_loop()
    ok = await loop.run_in_executor(None, sheets_sync.refresh_cache)
    log.info(f"📊 Sheets sync при старте: {'✅' if ok else '⚠️ fallback на knowledge_base.py'}")

    # 2 раза в день — 09:00 и 21:00 по Астане (UTC+5) = 04:00 и 16:00 UTC
    async def _sync_sheets_job() -> None:
        loop = asyncio.get_running_loop()
        ok = await loop.run_in_executor(None, sheets_sync.refresh_cache)
        log.info(f"📊 Sheets sync (по расписанию): {'✅' if ok else '⚠️ fallback на knowledge_base.py'}")

    scheduler.add_job(
        _sync_sheets_job,
        CronTrigger(hour="4,16", minute=0),
        id="sheets_sync",
        replace_existing=True,
    )

    # Отчёт Артёму по итогам смены Луны (20:00-11:00 Астана) — шлётся в КОНЦЕ
    # смены, 11:00 по Астане (UTC+5) = 06:00 UTC. Раньше шёл в 20:00 (начало
    # смены) с окном "последние 24 часа" — оба момента были некорректны.
    scheduler.add_job(
        build_and_send_daily_report,
        CronTrigger(hour=6, minute=0),
        id="daily_report",
        replace_existing=True,
    )


async def close_scheduler(app: web.Application) -> None:
    if scheduler and scheduler.running:
        scheduler.shutdown()


# ---------- Background tasks ----------
async def start_background_tasks(app: web.Application) -> None:
    asyncio.create_task(resume_unanswered_manager_chats())
    log.info("✅ Background task: resume_unanswered_manager_chats запущен")


# ---------- App factory ----------
def create_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/envy_hook", envy_hook_handler)
    app.router.add_post("/wazzup",    lambda r: web.Response(text="ok"))
    app.router.add_get("/health",     health_handler)
    app.on_startup.append(init_http_session)
    app.on_startup.append(init_db)
    app.on_startup.append(init_scheduler)
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(close_db)
    app.on_cleanup.append(close_scheduler)
    app.on_cleanup.append(close_http_session)
    return app


if __name__ == "__main__":
    app = create_app()
    log.info("🚀 Luna Bot (Champion School) запущена")
    web.run_app(app, host="0.0.0.0", port=PORT)
