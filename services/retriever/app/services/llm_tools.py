import re

import structlog
from openai import OpenAI
from starlette.concurrency import run_in_threadpool

from app.pipeline.schemas import EvidenceItem, ExpandedChunk, QueryAspect
from app.schemas import LLMConfig, RetrievalResult
from app.services.context_packer import PackedContext, build_packed_context
from app.services.research import evidence_block, plan_block

log = structlog.get_logger(__name__)
_WORD_RE = re.compile(r"[а-яёa-z0-9][а-яёa-z0-9.\-]*", re.IGNORECASE)
_COVERAGE_STOP_TERMS = {
    "какие",
    "какой",
    "какая",
    "какое",
    "требования",
    "требование",
    "предъявляются",
    "предъявлять",
    "нужно",
    "нужны",
    "должен",
    "должна",
    "должно",
    "должны",
    "можно",
    "норматив",
    "нормы",
}


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

    answer = await _call_answer_llm(client, system_prompt, static_prompt, packed)

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
    mode_rules = _mode_rules(answer_mode)

    return (
        f"Исходный запрос пользователя:\n{query}\n\n"
        f"Поисковый запрос после переформулирования:\n{effective_query}\n\n"
        f"План поиска:\n{plan}\n\n"
        f"Извлеченные evidence:\n{evidence}\n\n"
        f"Режим ответа:\n{mode_rules}\n\n"
        f"Проверка покрытия исходного вопроса:\n{coverage_note}\n\n"
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
        LLMConfig.ANSWER_MODEL,
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
        "- Сначала проверь, подтверждают ли найденные фрагменты полный исходный вопрос, а не только отдельные слова или условия.",
        "- Не выдавай смежный нормативный смысл за прямой ответ на исходный вопрос.",
        "- Если есть только частичные или смежные данные, дай сводку найденного и явно напиши, что прямой ответ не подтвержден.",
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
            "Собери возможные пути решения по условиям применимости, затем дай "
            "итоговую инженерную сводку. Если evidence содержит direct-фрагменты "
            "со значениями или формулами, не пиши, что прямой ответ не найден: "
            "отдели отсутствие одного универсального значения от найденных "
            "условий применимости."
        )

    return "Дай прямой ответ с условиями применения и нормативным основанием."


def _query_coverage_note(query: str, results: list[RetrievalResult]) -> str:
    terms = _coverage_terms(query)
    if not terms:
        return "Ключевые термины исходного вопроса не выделены."

    context_keys = _context_term_keys(results)
    missing = [
        term for term in terms if not _is_term_covered(_term_key(term), context_keys)
    ]

    if not missing:
        return "Все ключевые термины исходного вопроса встречаются в контексте."

    missing_terms = ", ".join(missing)
    return (
        f"В контексте не найдены ключевые термины: {missing_terms}. "
        "Если они относятся к основному объекту или действию вопроса, ответь, "
        "что в найденных источниках нет достаточных данных для прямого ответа."
    )


def _coverage_terms(text: str) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()

    for raw in _WORD_RE.findall(text.lower()):
        term = raw.strip(".-")
        key = _term_key(term)
        if _is_coverage_term(term, key) and key not in seen:
            terms.append(term)
            seen.add(key)

    return terms


def _context_term_keys(results: list[RetrievalResult]) -> set[str]:
    context = " ".join(_result_search_text(result) for result in results).lower()
    return {_term_key(raw.strip(".-")) for raw in _WORD_RE.findall(context)}


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


def _is_coverage_term(term: str, key: str) -> bool:
    if not key or term in _COVERAGE_STOP_TERMS:
        return False

    return any(ch.isdigit() for ch in term) or len(key) >= 4


def _is_term_covered(key: str, context_keys: set[str]) -> bool:
    if key in context_keys:
        return True

    if len(key) < 5:
        return False

    prefix = key[:5]
    return any(context_key.startswith(prefix) for context_key in context_keys)


def _term_key(term: str) -> str:
    if any(ch.isdigit() for ch in term):
        return term

    for suffix in _TERM_SUFFIXES:
        if len(term) > len(suffix) + 3 and term.endswith(suffix):
            return term[: -len(suffix)]

    return term


_TERM_SUFFIXES = (
    "овать",
    "ировать",
    "аться",
    "яться",
    "ыми",
    "ими",
    "ого",
    "ему",
    "ами",
    "ями",
    "иях",
    "ых",
    "их",
    "ом",
    "ем",
    "ой",
    "ый",
    "ий",
    "ая",
    "ое",
    "ые",
    "ие",
    "ов",
    "ев",
    "ей",
    "ам",
    "ям",
    "ах",
    "ях",
    "ать",
    "ять",
    "ить",
    "еть",
    "а",
    "я",
    "ы",
    "и",
    "е",
    "у",
    "ю",
)


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
