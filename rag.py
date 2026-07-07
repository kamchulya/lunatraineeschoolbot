"""
rag.py — модуль RAG для бота Лола.
Подключается к bot.py вместо KNOWLEDGE_BASE.
"""

import os
from openai import AsyncOpenAI
import asyncpg

EMBEDDING_MODEL = "text-embedding-3-small"
TOP_K = 4           # сколько чанков брать (можно 3-5)
MIN_SCORE = 0.3     # минимальный порог релевантности (0–1)

_oai_client = None
_db_pool = None


async def init_rag(db_pool: asyncpg.Pool):
    """Вызвать один раз при старте бота."""
    global _oai_client, _db_pool
    _oai_client = AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"])
    _db_pool = db_pool
    print("✅ RAG модуль инициализирован")


async def get_context(query: str, category_filter: str = None) -> str:
    """
    Ищет TOP_K релевантных чанков по запросу клиента.
    Возвращает строку для вставки в системный промпт.
    Фолбэк: если что-то пошло не так — возвращает пустую строку.
    """
    try:
        # 1. Эмбеддинг запроса
        resp = await _oai_client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=query
        )
        emb = resp.data[0].embedding
        emb_str = "[" + ",".join(str(x) for x in emb) + "]"

        # 2. Поиск в pgvector
        if category_filter:
            rows = await _db_pool.fetch("""
                SELECT content, category,
                       1 - (embedding <=> $1::vector) AS score
                FROM knowledge_chunks
                WHERE category = $2
                  AND 1 - (embedding <=> $1::vector) > $3
                ORDER BY embedding <=> $1::vector
                LIMIT $4
            """, emb_str, category_filter, MIN_SCORE, TOP_K)
        else:
            rows = await _db_pool.fetch("""
                SELECT content, category,
                       1 - (embedding <=> $1::vector) AS score
                FROM knowledge_chunks
                WHERE 1 - (embedding <=> $1::vector) > $2
                ORDER BY embedding <=> $1::vector
                LIMIT $3
            """, emb_str, MIN_SCORE, TOP_K)

        if not rows:
            return ""

        # 3. Собираем контекст
        parts = []
        for row in rows:
            parts.append(row["content"])

        return "РЕЛЕВАНТНАЯ ИНФОРМАЦИЯ:\n" + "\n\n---\n\n".join(parts)

    except Exception as e:
        print(f"⚠️ RAG ошибка: {e}")
        return ""  # фолбэк — бот продолжит работу без RAG-контекста
