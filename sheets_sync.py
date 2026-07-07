"""
Синхронизация базы знаний Луны с Google Sheets.

Таблица: https://docs.google.com/spreadsheets/d/1aN89a8YEqVbHsPW1hdKp1-Sgay7LhQD6
Листы: "Курсы и тарифы", "Программа курсов", "Демо-доступ", "О школе и FAQ"

Аутентификация — через сервисный аккаунт Google (GOOGLE_CREDENTIALS_JSON в env,
содержит весь JSON ключа одной строкой).
"""

import json
import logging
import os

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

log = logging.getLogger(__name__)

SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID", "1aN89a8YEqVbHsPW1hdKp1-Sgay7LhQD6")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

SHEET_TARIFFS = "Курсы и тарифы"
SHEET_PROGRAM = "Программа курсов"
SHEET_DEMO    = "Демо-доступ"
SHEET_FAQ     = "О школе и FAQ"

# Кеш в памяти — обновляется по расписанию через APScheduler в bot.py
_cached_knowledge_base: str | None = None


def _get_service():
    creds_raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_raw:
        log.warning("⚠️ GOOGLE_CREDENTIALS_JSON не задан — Sheets sync отключён")
        return None
    try:
        creds_info = json.loads(creds_raw)
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds, cache_discovery=False)
    except Exception as e:
        log.error(f"❌ Ошибка инициализации Google Sheets credentials: {e}")
        return None


def _fetch_sheet_values(service, sheet_name: str) -> list[list[str]]:
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=SPREADSHEET_ID, range=sheet_name)
            .execute()
        )
        return result.get("values", [])
    except Exception as e:
        log.error(f"❌ Ошибка чтения листа {sheet_name!r}: {e}")
        return []


def _rows_to_dicts(values: list[list[str]]) -> list[dict]:
    if not values or len(values) < 2:
        return []
    headers = [h.strip() for h in values[0]]
    records = []
    for row in values[1:]:
        row = row + [""] * (len(headers) - len(row))  # добить пустыми если короче
        record = dict(zip(headers, row))
        if any(v.strip() for v in record.values()):
            records.append(record)
    return records


def _format_tariffs(records: list[dict]) -> str:
    if not records:
        return ""
    lines = ["=== КУРСЫ И ТАРИФЫ (актуальные данные из таблицы) ===\n"]
    for r in records:
        old_price = r.get("цена_старая", "").strip()
        new_price = r.get("цена_новая", "").strip()
        installment = r.get("рассрочка_мес", "").strip()
        seats = r.get("мест_осталось", "").strip()

        price_line = ""
        if old_price and new_price:
            price_line = f"Цена: {old_price} тг → {new_price} тг"
        elif new_price:
            price_line = f"Цена: {new_price} тг"
        if installment:
            price_line += f" | Рассрочка Kaspi 0-0-12-24: ~{installment} тг/мес"

        block = [
            f"--- {r.get('курс', '')} | {r.get('формат', '')} | {r.get('город', '')} | {r.get('поток_название', '')} ---",
        ]
        if r.get("адрес_зал", "").strip() not in ("", "—"):
            block.append(f"Адрес: {r.get('адрес_зал')}")
        if r.get("лектор", "").strip() not in ("", "—"):
            block.append(f"Лектор: {r.get('лектор')}")
        if r.get("расписание", "").strip():
            block.append(f"Расписание: {r.get('расписание')}")
        if r.get("дата_старта", "").strip() not in ("", "—"):
            block.append(f"Дата старта: {r.get('дата_старта')}")
        if r.get("следующий_старт", "").strip() not in ("", "—"):
            block.append(f"Следующий старт: {r.get('следующий_старт')}")
        if price_line:
            block.append(price_line)
        if seats and seats not in ("", "—"):
            block.append(f"Мест осталось: {seats}")
        if r.get("что_входит", "").strip():
            block.append(f"Что входит: {r.get('что_входит')}")
        if r.get("ссылка_регистрация", "").strip():
            block.append(f"Ссылка на регистрацию: {r.get('ссылка_регистрация')}")
        lines.append("\n".join(block) + "\n")
    return "\n".join(lines)


def _format_program(records: list[dict]) -> str:
    if not records:
        return ""
    lines = ["=== ПРОГРАММА КУРСОВ (темы по направлениям) ===\n"]
    for r in records:
        lines.append(f"{r.get('направление', '')}: {r.get('темы курса', '')}\n")
    return "\n".join(lines)


def _format_demo(records: list[dict]) -> str:
    if not records:
        return ""
    lines = ["=== ДЕМО-ДОСТУП ===\n"]
    for r in records:
        lines.append(f"{r.get('параметр', '')}: {r.get('значение', '')}")
    return "\n".join(lines) + "\n"


def _format_faq(records: list[dict]) -> str:
    if not records:
        return ""
    lines = ["=== О ШКОЛЕ И ШАБЛОНЫ ОТВЕТОВ ===\n"]
    for r in records:
        lines.append(f"--- {r.get('тема', '')} ---\n{r.get('текст ответа', '')}\n")
    return "\n".join(lines)


def fetch_knowledge_base_from_sheets() -> str | None:
    """Тянет все 4 листа таблицы и собирает единую строку базы знаний.
    Возвращает None при ошибке (тогда вызывающий код должен использовать fallback)."""
    service = _get_service()
    if service is None:
        return None

    try:
        tariffs = _rows_to_dicts(_fetch_sheet_values(service, SHEET_TARIFFS))
        program = _rows_to_dicts(_fetch_sheet_values(service, SHEET_PROGRAM))
        demo    = _rows_to_dicts(_fetch_sheet_values(service, SHEET_DEMO))
        faq     = _rows_to_dicts(_fetch_sheet_values(service, SHEET_FAQ))

        parts = [
            _format_tariffs(tariffs),
            _format_program(program),
            _format_demo(demo),
            _format_faq(faq),
        ]
        combined = "\n".join(p for p in parts if p.strip())

        if not combined.strip():
            log.warning("⚠️ Sheets вернули пустые данные — не обновляем базу знаний")
            return None

        log.info(
            f"✅ Sheets sync: тарифов={len(tariffs)} программ={len(program)} "
            f"демо={len(demo)} faq={len(faq)} символов={len(combined)}"
        )
        return combined
    except Exception as e:
        log.error(f"❌ fetch_knowledge_base_from_sheets ошибка: {e}")
        return None


def get_cached_knowledge_base(fallback: str) -> str:
    """Возвращает данные из кеша (обновляется по расписанию), либо fallback если кеш пуст."""
    global _cached_knowledge_base
    return _cached_knowledge_base if _cached_knowledge_base else fallback


def refresh_cache() -> bool:
    """Синхронный вызов — обновляет глобальный кеш. Возвращает True при успехе."""
    global _cached_knowledge_base
    result = fetch_knowledge_base_from_sheets()
    if result:
        _cached_knowledge_base = result
        return True
    return False
