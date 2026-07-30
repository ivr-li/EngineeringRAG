import structlog
from openai import OpenAI
from starlette.concurrency import run_in_threadpool

from app.pipeline.schemas import EvidenceItem, ExpandedChunk, QueryAspect
from app.pipeline.services.context_packer import (
    PackedContext,
    build_packed_context,
    estimate_text_tokens,
)
from app.pipeline.services.research import (
    basis_block,
    evidence_block,
    plan_block,
    source_group,
)
from app.pipeline.services.text_terms import term_covered, term_items, term_keys
from app.schemas import LLMConfig, RetrievalResult

log = structlog.get_logger(__name__)


async def _call_llm(
    client: OpenAI,
    model: str,
    system_prompt: str,
    user_content: str,
    temperature: float = 0.2,
    max_tokens: int = 256,
) -> str | None:
    try:
        resp = await run_in_threadpool(
            client.chat.completions.create,
            model=model,
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
        client,
        LLMConfig.REWRITER_MODEL,
        system_prompt,
        query,
        temperature=0.2,
        max_tokens=256,
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
    trace_metadata: dict | None = None, packed_context: PackedContext | None = None,
    static_prompt: str | None = None, expanded_chunks: list[ExpandedChunk] | None = None,
    answer_mode: str | None = None, evidence_items: list[EvidenceItem] | None = None,
    query_plan: list[QueryAspect] | None = None,
) -> str:
    if not results:
        return _empty_answer()

    static_prompt, packed = prepare_answer_context(
        results, query, effective_query, system_prompt, packed_context, static_prompt,
        expanded_chunks, answer_mode, evidence_items, query_plan
    )
    _update_trace_metadata(trace_metadata, packed)
    _log_packed_context(packed)

    answer = await _call_answer_llm(
        client,
        system_prompt,
        static_prompt,
        packed,
        trace_metadata,
    )

    return answer or _fallback_answer(results)


def prepare_answer_context(
    results: list[RetrievalResult],
    query: str,
    effective_query: str,
    system_prompt: str,
    packed_context: PackedContext | None = None, static_prompt: str | None = None,
    expanded_chunks: list[ExpandedChunk] | None = None, answer_mode: str | None = None,
    evidence_items: list[EvidenceItem] | None = None,
    query_plan: list[QueryAspect] | None = None,
) -> tuple[str, PackedContext]:
    prompt = static_prompt or _build_static_prompt(
        query,
        effective_query,
        results,
        answer_mode or "direct_supported",
        evidence_items or [],
        query_plan or [],
    )
    expanded_relations = {item.chunk.id: item.relation for item in expanded_chunks or []}
    packed = packed_context or build_packed_context(
        results=results,
        query=query,
        effective_query=effective_query,
        static_prompt=prompt,
        system_prompt=system_prompt,
        expanded_relations=expanded_relations,
        evidence_items=evidence_items,
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
    answer_mode: str,
    evidence_items: list[EvidenceItem],
    query_plan: list[QueryAspect],
) -> str:
    usage_rules = _context_usage_rules(results)
    coverage_note = _query_coverage_note(query, results)
    plan = plan_block(query_plan)
    evidence = evidence_block(evidence_items, answer_mode)
    basis = basis_block(evidence_items)
    mode_rules = _mode_rules(answer_mode)
    format_rules = _answer_format_rules(answer_mode)

    return (
        f"Исходный запрос пользователя:\n{query}\n\n"
        f"Поисковый запрос после переформулирования:\n{effective_query}\n\n"
        f"План поиска:\n{plan}\n\n"
        f"Структурированные evidence:\n{evidence}\n\n"
        f"Уникальные основания:\n{basis}\n\n"
        f"Режим ответа:\n{mode_rules}\n\n"
        f"Проверка покрытия исходного вопроса:\n{coverage_note}\n\n"
        f"Правила использования найденного контекста:\n{usage_rules}\n\n"
        f"Требуемый формат ответа:\n{format_rules}"
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
        "input_tokens": packed.input_tokens,
        "model_max_len": packed.model_max_tokens,
        "dropped_by_budget": packed.dropped_by_budget,
        "dropped_by_relevance": packed.dropped_by_relevance,
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
    trace_metadata: dict | None,
) -> str | None:
    user_prompt, max_tokens, input_tokens = _fit_answer_prompt(
        system_prompt,
        static_prompt,
        packed.text,
        packed.max_output_tokens,
    )
    _update_preflight_stats(trace_metadata, input_tokens, max_tokens)
    log.info(
        "llm_prompt_preflight",
        input_tokens=input_tokens,
        max_output_tokens=max_tokens,
        model_max_len=LLMConfig.ANSWER_MODEL_MAX_LEN,
    )

    return await _call_llm(
        client,
        LLMConfig.ANSWER_MODEL,
        system_prompt,
        user_prompt,
        temperature=0.15,
        max_tokens=max_tokens,
    )


def _update_preflight_stats(
    trace_metadata: dict | None,
    input_tokens: int,
    max_tokens: int,
) -> None:
    if trace_metadata is None:
        return

    packing = trace_metadata.setdefault("context_packing", {})
    packing["input_tokens"] = input_tokens
    packing["max_output_tokens"] = max_tokens
    packing["model_max_len"] = LLMConfig.ANSWER_MODEL_MAX_LEN


def _fit_answer_prompt(
    system_prompt: str,
    static_prompt: str,
    context_text: str,
    max_output_tokens: int,
) -> tuple[str, int, int]:
    max_tokens = max_output_tokens
    context = context_text
    user_prompt = _answer_user_prompt(static_prompt, context)
    input_tokens = _prompt_tokens(system_prompt, user_prompt)

    while _over_model_limit(input_tokens, max_tokens) and context:
        context = _shrink_context_text(context)
        user_prompt = _answer_user_prompt(static_prompt, context)
        input_tokens = _prompt_tokens(system_prompt, user_prompt)

    if _over_model_limit(input_tokens, max_tokens):
        max_tokens = _safe_output_tokens(input_tokens)

    return user_prompt, max_tokens, input_tokens


def _answer_user_prompt(static_prompt: str, context_text: str) -> str:
    return f"{static_prompt}\n\nКонтекст из ретривера:\n{context_text}"


def _prompt_tokens(system_prompt: str, user_prompt: str) -> int:
    return estimate_text_tokens(f"{system_prompt}\n{user_prompt}")


def _over_model_limit(input_tokens: int, output_tokens: int) -> bool:
    total = input_tokens + output_tokens + LLMConfig.ANSWER_TOKEN_SAFETY_MARGIN

    return total > LLMConfig.ANSWER_MODEL_MAX_LEN


def _shrink_context_text(context_text: str) -> str:
    if len(context_text) <= 800:
        return ""

    max_chars = max(800, int(len(context_text) * 0.8))
    return context_text[:max_chars].rsplit("\n---\n", 1)[0].strip()


def _safe_output_tokens(input_tokens: int) -> int:
    available = LLMConfig.ANSWER_MODEL_MAX_LEN - input_tokens
    available -= LLMConfig.ANSWER_TOKEN_SAFETY_MARGIN

    return max(1, min(LLMConfig.ANSWER_MAX_TOKENS, available))


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
        "- Сначала проверь, подтверждают ли найденные фрагменты полный исходный вопрос, а не только отдельные слова или условия.",
        "- Не выдавай смежный нормативный смысл за прямой ответ на исходный вопрос.",
        "- Основные результаты поиска имеют приоритет перед фрагментами, подтянутыми по внутренним ссылкам.",
        "- Фрагменты из ссылок и таблиц используй как уточняющий контекст, если они подтверждают или ограничивают primary evidence.",
        "- Если найдено несколько условий применения, не своди их к одному универсальному числу или одному правилу.",
        "- Если есть только частичные или смежные данные, дай инженерную сводку найденного и явно напиши, чего не хватает.",
        "- Если текст ссылается на таблицу и таблица есть в контексте, извлеки из нее конкретные значения.",
        "- Не отвечай только фразой, что значения указаны в таблице, если строки таблицы переданы ниже.",
        "- Если нужную строку или колонку нельзя однозначно восстановить, прямо укажи это ограничение.",
        "- Контекст уже очищен до уникальных нормативных единиц; не превращай соседние окна одного пункта или таблицы в разные основания.",
        "- Раздел 'Основание' формируй только по блоку 'Уникальные основания' и не дублируй одинаковый source_group.",
    ]

    if any(result.is_table for result in results):
        rules.append(
            "- Для таблиц сохраняй подпись, часть/окно и приводимые значения в разделе "
            '"Что удалось найти".'
        )
    return "\n".join(rules)


def _mode_rules(answer_mode: str) -> str:
    if answer_mode == "not_found":
        return (
            "Прямой ответ в найденных действующих нормах не подтвержден. "
            "Не подменяй его смежными нормами; покажи, что искали и что найдено."
        )
    if answer_mode == "partial_supported":
        return (
            "Дай частичную сводку: отдели прямой недостаток данных от смежных "
            "нормативных фрагментов и объясни, почему они не являются полным ответом."
        )
    if answer_mode == "multi_path":
        return (
            "Дай инженерную сводку по нескольким условиям применимости. "
            "Сначала сформулируй общий вывод, затем перечисли нормативные пути. "
            "Не объединяй разные условия в одно правило; отделяй отсутствие "
            "универсального значения от найденных применимых норм."
        )

    return (
        "Дай прямой ответ, но сохрани условия применимости, ограничения и "
        "дополнительные найденные нормы, если они есть в context."
    )


def _answer_format_rules(answer_mode: str) -> str:
    if answer_mode == "direct_supported":
        return _direct_format()
    if answer_mode == "not_found":
        return _not_found_format()

    return _summary_format()


def _summary_format() -> str:
    return (
        "Форматируй ответ в Markdown. Используй разделы:\n"
        "#### Краткая инженерная сводка\n"
        "1-3 предложения: есть ли одно универсальное правило или набор условий.\n"
        "#### По условиям применения\n"
        "Список вариантов: условие -> требование/значение -> ссылка на фрагмент.\n"
        "#### Что говорят нормы\n"
        "Короткая группировка по документам и пунктам.\n"
        "#### Ограничения / что уточнить\n"
        "Только реальные ограничения из evidence и context.\n"
        "#### Основание\n"
        "Таблица: Документ | Раздел | Что подтверждает."
    )


def _direct_format() -> str:
    return (
        "Форматируй ответ в Markdown. Используй разделы:\n"
        "#### Краткая инженерная сводка\n"
        "Дай общий вывод в 1-2 предложениях.\n"
        "#### По условиям применения\n"
        "Укажи условия, при которых действует найденное правило.\n"
        "#### Что говорят нормы\n"
        "Коротко сгруппируй прямые и смежные нормы по документам и пунктам.\n"
        "#### Ограничения / что уточнить\n"
        "Укажи только реальные ограничения из evidence и context.\n"
        "#### Основание\n"
        "Таблица: Документ | Раздел | Что подтверждает."
    )


def _not_found_format() -> str:
    return (
        "Форматируй ответ в Markdown. Используй разделы:\n"
        "#### Краткая инженерная сводка\n"
        "Напиши, что прямой ответ не подтвержден найденными нормами.\n"
        "#### Что удалось найти\n"
        "Перечисли только полезные смежные фрагменты.\n"
        "#### Что уточнить\n"
        "Укажи, какие исходные данные или документы нужны.\n"
        "#### Основание\n"
        "Таблица: Документ | Раздел | Что подтверждает."
    )


def _query_coverage_note(query: str, results: list[RetrievalResult]) -> str:
    terms = term_items(query)
    if not terms:
        return "Ключевые термины исходного вопроса не выделены."

    context_keys = _context_term_keys(results)
    missing = [
        term.raw for term in terms if not term_covered(term.key, context_keys)
    ]

    if not missing:
        return "Все ключевые термины исходного вопроса встречаются в контексте."

    missing_terms = ", ".join(missing)
    return (
        f"В контексте не найдены ключевые термины: {missing_terms}. "
        "Если они относятся к основному объекту или действию вопроса, ответь, "
        "что в найденных источниках нет достаточных данных для прямого ответа."
    )


def _context_term_keys(results: list[RetrievalResult]) -> set[str]:
    context = " ".join(_result_search_text(result) for result in results).lower()
    return term_keys(context)


def _result_search_text(result: RetrievalResult) -> str:
    return " ".join(
        [
            result.text,
            result.filename,
            result.section_path,
            result.parent_heading or "",
            result.leaf_heading or "",
        ]
    )


def _fallback_answer(results: list[RetrievalResult]) -> str:
    bullets: list[str] = []
    basis: list[str] = []
    seen_groups: set[str] = set()

    for result in results:
        group = source_group(result)
        if group in seen_groups:
            continue

        seen_groups.add(group)
        snippet = " ".join(result.text.strip().split())[:280]
        if snippet:
            bullets.append(f"- {snippet}...")

        label = result.filename
        if result.section_path:
            label += f", раздел {result.section_path}"

        basis.append(f"- {label}")
        if len(bullets) >= 3:
            break

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
