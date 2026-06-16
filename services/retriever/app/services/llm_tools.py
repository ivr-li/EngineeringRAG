import structlog
from openai import OpenAI
from starlette.concurrency import run_in_threadpool

from app.pipeline.schemas import ExpandedChunk
from app.schemas import LLMConfig, RetrievalResult
from app.services.context_packer import PackedContext, build_packed_context

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
    trace_metadata: dict | None = None,
    packed_context: PackedContext | None = None,
    static_prompt: str | None = None,
    expanded_chunks: list[ExpandedChunk] | None = None,
) -> str:
    if not results:
        return _empty_answer()

    static_prompt, packed = prepare_answer_context(
        results=results,
        query=query,
        effective_query=effective_query,
        system_prompt=system_prompt,
        packed_context=packed_context,
        static_prompt=static_prompt,
        expanded_chunks=expanded_chunks,
    )
    _update_trace_metadata(trace_metadata, packed)
    _log_packed_context(packed)

    answer = await _call_answer_llm(client, system_prompt, static_prompt, packed)

    return answer or _fallback_answer(results)


def prepare_answer_context(
    results: list[RetrievalResult],
    query: str,
    effective_query: str,
    system_prompt: str,
    packed_context: PackedContext | None = None,
    static_prompt: str | None = None,
    expanded_chunks: list[ExpandedChunk] | None = None,
) -> tuple[str, PackedContext]:
    prompt = static_prompt or _build_static_prompt(query, effective_query, results)
    expanded_relations = {item.chunk.id: item.relation for item in expanded_chunks or []}
    packed = packed_context or build_packed_context(
        results=results,
        query=query,
        effective_query=effective_query,
        static_prompt=prompt,
        system_prompt=system_prompt,
        expanded_relations=expanded_relations,
    )

    return prompt, packed


def _empty_answer() -> str:
    return (
        "## Ответ\n\n"
        "По этому запросу ничего не найдено в базе.\n\n"
        "Попробуйте уточнить формулировку, номер документа или нужный раздел."
    )


def _build_static_prompt(
    query: str,
    effective_query: str,
    results: list[RetrievalResult],
) -> str:
    usage_rules = _context_usage_rules(results)

    return (
        f"Исходный запрос пользователя:\n{query}\n\n"
        f"Поисковый запрос после переформулирования:\n{effective_query}\n\n"
        f"Правила использования найденного контекста:\n{usage_rules}"
    )


def _update_trace_metadata(
    trace_metadata: dict | None,
    packed: PackedContext,
) -> None:
    if trace_metadata is None:
        return

    trace_metadata["context_packing"] = {
        "included_count": packed.included_count,
        "dropped_count": packed.dropped_count,
        "used_tokens": packed.used_tokens,
        "budget_tokens": packed.budget_tokens,
        "max_output_tokens": packed.max_output_tokens,
    }


def _log_packed_context(packed: PackedContext) -> None:
    log.info(
        "llm_context_packed",
        included=packed.included_count,
        dropped=packed.dropped_count,
        used_tokens=packed.used_tokens,
        budget_tokens=packed.budget_tokens,
        max_output_tokens=packed.max_output_tokens,
    )


async def _call_answer_llm(
    client: OpenAI,
    system_prompt: str,
    static_prompt: str,
    packed: PackedContext,
) -> str | None:
    user_prompt = f"{static_prompt}\n\nКонтекст из ретривера:\n{packed.text}"
    return await _call_llm(
        client,
        system_prompt,
        user_prompt,
        temperature=0.15,
        max_tokens=packed.max_output_tokens,
    )


def _build_context(results: list[RetrievalResult]) -> str:
    packed = build_packed_context(
        results=results,
        query="",
        effective_query="",
        static_prompt="",
        system_prompt="",
    )
    return packed.text


def _context_usage_rules(results: list[RetrievalResult]) -> str:
    rules = [
        "- Фрагменты, подтянутые по внутренней ссылке, являются обязательным контекстом.",
        "- Если текст ссылается на таблицу и таблица есть в контексте, извлеки из нее конкретные значения.",
        "- Не отвечай только фразой, что значения указаны в таблице, если строки таблицы переданы ниже.",
        "- Если нужную строку или колонку нельзя однозначно восстановить, прямо укажи это ограничение.",
    ]

    if any(result.is_table for result in results):
        rules.append(
            "- Для таблиц сохраняй подпись, часть/окно и приводимые значения в разделе "
            '"Что удалось найти".'
        )
    return "\n".join(rules)


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
