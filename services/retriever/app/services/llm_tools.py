# ==============================================================
# LLM tools
# ==============================================================
import structlog
from openai import OpenAI
from starlette.concurrency import run_in_threadpool

from app.schemas import LLMConfig, RetrievalResult

log = structlog.get_logger(__name__)


async def _call_llm(
    client: OpenAI,
    system_prompt: str,
    user_content: str,
    temperature: float = 0.2,
    max_tokens: int = 256,
) -> str | None:
    try:
        resp = await run_in_threadpool(
            client.chat.completions.create,
            model=LLMConfig.REWRITER_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return (resp.choices[0].message.content or "").strip() or None
    except Exception as ex:
        log.error("llm_call_error", error=str(ex))
        return None


async def rewrite_query(
    client: OpenAI,
    query: str,
    system_prompt: str,
) -> tuple[str, bool]:
    rewritten = await _call_llm(
        client, system_prompt, query, temperature=0.2, max_tokens=256
    )
    if not rewritten:
        return query, False
    return rewritten, True


async def compose_answer(
    client: OpenAI,
    query: str,
    effective_query: str,
    system_prompt: str,
    results: list[RetrievalResult],
) -> str:
    if not results:
        return (
            "## Ответ\n\n"
            "По этому запросу ничего не найдено в базе.\n\n"
            "Попробуйте уточнить формулировку, номер документа или нужный раздел."
        )

    context = _build_context(results)
    user_prompt = (
        f"Исходный запрос пользователя:\n{query}\n\n"
        f"Поисковый запрос после переформулирования:\n{effective_query}\n\n"
        f"Контекст из ретривера:\n{context}"
    )
    answer = await _call_llm(
        client, system_prompt, user_prompt, temperature=0.15, max_tokens=900
    )
    return answer or _fallback_answer(results)


def _build_context(results: list[RetrievalResult]) -> str:
    parts: list[str] = []
    for idx, result in enumerate(results[: LLMConfig.ANSWER_CONTEXT_LIMIT], start=1):
        headings = " > ".join(result.headings or []) or "—"
        section_path = result.section_path or "—"
        refs = ", ".join(result.man_refs or result.cross_refs or []) or "—"
        parts.append(
            "\n".join(
                [
                    f"Фрагмент {idx}",
                    f"Документ: {result.filename}",
                    f"Раздел: {section_path}",
                    f"Заголовки: {headings}",
                    f"Тип: {'таблица' if result.is_table else 'текст'}",
                    f"Ссылки: {refs}",
                    "Текст:",
                    result.text.strip(),
                ]
            )
        )
    return "\n\n---\n\n".join(parts)


def _fallback_answer(results: list[RetrievalResult]) -> str:
    bullets: list[str] = []
    basis: list[str] = []
    for result in results[:3]:
        snippet = " ".join(result.text.strip().split())[:280]
        if snippet:
            bullets.append(f"- {snippet}...")
        label = result.filename
        if result.section_path:
            label += f", раздел {result.section_path}"
        basis.append(f"- {label}")

    answer_parts = [
        "## Ответ:\n",
        "\tНе удалось сформировать полноценный ответ через модель, поэтому ниже показана краткая сводка по найденным фрагментам.",
        "## Что удалось найти\n",
        *(
            bullets
            or [
                "- В релевантных фрагментах нет достаточного объёма данных для краткой сводки."
            ]
        ),
        "## Основание",
        *(basis or ["- Подходящие фрагменты не найдены."]),
    ]
    return "\n".join(answer_parts)
