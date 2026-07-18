"""
Ежедневный разбор диалогов, в которых участвовал живой менеджер.

Логика:
1. За последние сутки находим все chat_id, где в message_log встречается
   хотя бы одно сообщение с role='manager' (то есть менеджер реально писал
   клиенту, а не просто открыл карточку).
2. Для каждого такого chat_id собираем ПОЛНУЮ расшифровку диалога — все
   сообщения (клиент / Луна / менеджер) в хронологическом порядке.
3. Каждую расшифровку отдаём Claude на разбор по чек-листу продаж — модель
   возвращает структурированную оценку с рекомендациями.
4. Результаты складываются в таблицу dialog_analysis (для истории/экспорта —
   например как основа платной доп-услуги для клиентов), а сводка по всем
   менеджерам за день уходит текстом в WhatsApp Артёму, как и обычный
   ежедневный отчёт.

Модель специально НЕ видит и не оценивает сообщения Луны как отдельный
объект критики — фокус на живом менеджере, Луна тут просто часть контекста
диалога (то, что уже было сказано клиенту до подключения менеджера).
"""

import json
import logging
import re
from datetime import datetime, timezone, timedelta

import anthropic

log = logging.getLogger(__name__)

SALES_ANALYSIS_SYSTEM_PROMPT = """
Ты — эксперт по продажам, который разбирает переписку менеджера фитнес-школы
Champion School (Instagram Direct) с клиентом и даёт честную, конкретную
обратную связь.

В расшифровке роли обозначены так:
- "Клиент:" — сообщения потенциального покупателя
- "Луна:" — автоматические сообщения бота (до момента, когда менеджер забрал
  диалог — это уже пройденный этап, не часть работы менеджера)
- "Менеджер {имя}:" — сообщения живого менеджера, это и есть объект разбора

Оцени РАБОТУ МЕНЕДЖЕРА (не Луны) по следующим критериям:
1. Скорость и своевременность ответов (если это видно по контексту)
2. Задавал ли уточняющие вопросы, чтобы понять потребность клиента, или сразу
   продавливал продажу
3. Как отработаны возражения (если они были) — по существу или отписками
4. Была ли попытка довести до следующего шага (оплата, бронь места, звонок) —
   или разговор просто угас
5. Тон и грамотность: вежливость, уверенность, отсутствие давления
6. Не наврал ли менеджер что-то, что противоречит официальным данным школы
   (цены, гарантии трудоустройства и т.п.) — если видно из контекста

Ответь СТРОГО в формате JSON, без markdown-разметки и пояснений вне JSON:
{
  "outcome": "оплата|назначен звонок|клиент думает|отказ|диалог оборвался|неясно",
  "score": <целое число 1-10, 10 — образцовая работа менеджера>,
  "strengths": ["конкретная сильная сторона", "..."],
  "issues": ["конкретная проблема с примером из диалога", "..."],
  "recommendation": "одна конкретная рекомендация, что сделать по-другому в следующий раз"
}
Если менеджер почти не участвовал (одно короткое сообщение без реального
диалога) — так и укажи в outcome и issues, не придумывай оценку из воздуха.
"""


def _role_label(role: str, manager_name: str | None) -> str:
    if role == "user":
        return "Клиент"
    if role == "assistant":
        return "Луна"
    if role == "manager":
        return f"Менеджер {manager_name}" if manager_name else "Менеджер"
    return role


async def get_manager_touched_chat_ids(db_pool, since: datetime) -> list[str]:
    """chat_id, где менеджер реально писал клиенту за последние сутки."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT chat_id FROM message_log WHERE role='manager' AND created_at >= $1",
            since,
        )
    return [r["chat_id"] for r in rows]


async def build_transcript(db_pool, chat_id: str) -> tuple[str, str | None]:
    """Возвращает (расшифровка_текстом, имя_менеджера) — вся история по
    chat_id в хронологическом порядке. Имя менеджера берётся из последнего
    сообщения с role='manager' (если участвовало несколько — берём того, кто
    писал последним, обычно это и есть основной менеджер по сделке)."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT role, text, manager_name, created_at FROM message_log "
            "WHERE chat_id=$1 ORDER BY created_at ASC",
            chat_id,
        )
    lines = []
    manager_name = None
    for r in rows:
        if r["role"] == "manager" and r["manager_name"]:
            manager_name = r["manager_name"]
        label = _role_label(r["role"], r["manager_name"])
        lines.append(f"{label}: {r['text']}")
    return "\n".join(lines), manager_name


async def analyze_dialog(anthropic_api_key: str, chat_id: str, transcript: str) -> dict:
    try:
        client = anthropic.AsyncAnthropic(api_key=anthropic_api_key)
        msg = await client.messages.create(
            model="claude-sonnet-5",
            max_tokens=800,
            temperature=0.2,
            system=SALES_ANALYSIS_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": transcript[:15000]}],
        )
        raw = msg.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.MULTILINE).strip()
        return json.loads(raw)
    except Exception as e:
        log.error(f"❌ analyze_dialog error {chat_id}: {e}")
        return {
            "outcome": "ошибка анализа",
            "score": None,
            "strengths": [],
            "issues": [f"Не удалось проанализировать: {e}"],
            "recommendation": "",
        }


async def run_daily_sales_analysis(db_pool, anthropic_api_key: str) -> str:
    """Главная функция — вызывается по расписанию раз в сутки. Возвращает
    готовый текст сводки для отправки в WhatsApp (или None, если за сутки
    менеджеры ни разу не подключались к диалогам)."""
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    chat_ids = await get_manager_touched_chat_ids(db_pool, since)
    if not chat_ids:
        return None

    results = []
    for chat_id in chat_ids:
        transcript, manager_name = await build_transcript(db_pool, chat_id)
        if not transcript.strip():
            continue
        analysis = await analyze_dialog(anthropic_api_key, chat_id, transcript)
        results.append({"chat_id": chat_id, "manager_name": manager_name, "analysis": analysis})

        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO dialog_analysis (chat_id, manager_name, analysis) VALUES ($1, $2, $3::jsonb)",
                    chat_id, manager_name, json.dumps(analysis, ensure_ascii=False),
                )
        except Exception as e:
            log.error(f"❌ dialog_analysis insert error {chat_id}: {e}")

    if not results:
        return None

    # Группировка по менеджеру для сводки
    by_manager: dict[str, list[dict]] = {}
    for r in results:
        name = r["manager_name"] or "неизвестный менеджер"
        by_manager.setdefault(name, []).append(r["analysis"])

    lines = ["📈 Разбор диалогов менеджеров за сутки\n"]
    for name, analyses in by_manager.items():
        scores = [a["score"] for a in analyses if isinstance(a.get("score"), (int, float))]
        avg_score = round(sum(scores) / len(scores), 1) if scores else "—"
        lines.append(f"👤 {name} — диалогов: {len(analyses)}, средняя оценка: {avg_score}/10")
        # Топ-1 проблема по менеджеру — самая частая или первая содержательная
        all_issues = [i for a in analyses for i in a.get("issues", []) if i]
        if all_issues:
            lines.append(f"   ⚠️ Пример проблемы: {all_issues[0][:180]}")
        all_recs = [a.get("recommendation") for a in analyses if a.get("recommendation")]
        if all_recs:
            lines.append(f"   💡 Рекомендация: {all_recs[0][:180]}")
        lines.append("")

    lines.append(f"Всего разобрано диалогов: {len(results)}. Полные разборы — в таблице dialog_analysis.")
    return "\n".join(lines)
