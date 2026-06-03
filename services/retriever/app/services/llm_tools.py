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
    for idx, result in enumerate(_select_context_results(results), start=1):
        headings = " > ".join(result.headings or []) or "—"
        section_path = result.section_path or "—"
        refs = _refs_context_line(result)
        table_context = _table_context_line(result)
        parts.append(
            "\n".join(
                [
                    f"Фрагмент {idx}",
                    f"Документ: {result.filename}",
                    f"Связь: {_relation_context_line(result)}",
                    f"Раздел: {section_path}",
                    f"Заголовки: {headings}",
                    f"Тип: {'таблица' if result.is_table else 'текст'}",
                    table_context,
                    f"Ссылки: {refs}",
                    "Текст:",
                    result.text.strip(),
                ]
            )
        )
    return "\n\n---\n\n".join(parts)


def _select_context_results(results: list[RetrievalResult]) -> list[RetrievalResult]:
    selected: list[RetrievalResult] = []
    seen: set[str] = set()
    primary_count = 0
    for result in results:
        if result.id in seen or not _should_include_context_result(result, primary_count):
            continue
        if len(selected) >= LLMConfig.ANSWER_CONTEXT_HARD_LIMIT:
            break
        selected.append(result)
        seen.add(result.id)
        if not result.expanded_from:
            primary_count += 1
    return selected


def _should_include_context_result(
    result: RetrievalResult,
    primary_count: int,
) -> bool:
    if _is_required_reference_result(result):
        return True
    if result.expanded_from:
        return primary_count < LLMConfig.ANSWER_CONTEXT_LIMIT
    return primary_count < LLMConfig.ANSWER_CONTEXT_LIMIT


def _is_required_reference_result(result: RetrievalResult) -> bool:
    if not result.expanded_from:
        return False
    return (
        result.is_table
        or result.expanded_from.startswith("table:")
        or result.expanded_from.startswith("table_id:")
    )


def _relation_context_line(result: RetrievalResult) -> str:
    if result.expanded_from:
        return f"подтянут по внутренней ссылке {result.expanded_from}"
    return "основной результат поиска"


def _refs_context_line(result: RetrievalResult) -> str:
    refs = []
    refs.extend(f"external:{ref}" for ref in result.man_refs)
    refs.extend(result.cross_refs)
    refs.extend(f"anchor:{ref}" for ref in result.anchor_refs)
    return ", ".join(refs) or "—"


def _table_context_line(result: RetrievalResult) -> str:
    if not result.is_table:
        return "Таблица: —"

    caption = result.table_caption or result.leaf_heading or "—"
    part = _format_index(result.table_part_index, result.table_part_total)
    window = _format_index(result.table_window_index, result.table_window_total)
    return f"Таблица: {caption}; часть: {part}; окно: {window}"


def _format_index(index: int | None, total: int | None) -> str:
    if index is None or total is None:
        return "—"
    return f"{index}/{total}"


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
