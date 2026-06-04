import re
from dataclasses import dataclass

from app.schemas import LLMConfig, RetrievalResult

_WORD_RE = re.compile(r"[а-яёa-z0-9][а-яёa-z0-9.\-]*", re.IGNORECASE)
_TABLE_HEADER_RE = re.compile(
    r"(?P<prefix>.*?)Заголовки таблицы:\s*(?P<header>.*?)(?:Строки таблицы:|$)",
    re.IGNORECASE | re.DOTALL,
)
_TABLE_ROWS_RE = re.compile(r"Строки таблицы:\s*(?P<rows>.*)", re.IGNORECASE | re.DOTALL)
_STOP_TERMS = {
    "какие",
    "какой",
    "какая",
    "какое",
    "нужно",
    "нужны",
    "принимать",
    "должны",
    "между",
    "таблица",
    "таблице",
    "пункт",
    "пункте",
    "раздел",
    "разделе",
    "расстояния",
    "минимальные",
}


@dataclass(frozen=True)
class PackedContext:
    text: str
    included_count: int
    dropped_count: int
    used_tokens: int
    budget_tokens: int
    max_output_tokens: int


@dataclass(frozen=True)
class _Budget:
    context_tokens: int
    output_tokens: int


def build_packed_context(
    results: list[RetrievalResult],
    query: str,
    effective_query: str,
    static_prompt: str,
    system_prompt: str,
) -> PackedContext:
    terms = _query_terms(f"{query} {effective_query}")
    budget = _build_budget(static_prompt, system_prompt)
    selected = _select_candidates(results, terms)

    return _pack_candidates(selected, terms, budget)


def _estimate_tokens(text: str) -> int:
    if not text:
        return 0

    words = len(text.split())
    chars = len(text)

    return max(int(words * 1.7), int(chars / 3.2), 1)


def _build_budget(static_prompt: str, system_prompt: str) -> _Budget:
    fixed_tokens = _estimate_tokens(static_prompt) + _estimate_tokens(system_prompt)
    limit = LLMConfig.ANSWER_MODEL_CONTEXT_TOKENS
    output_tokens = min(LLMConfig.ANSWER_MAX_TOKENS, limit // 3)
    context_tokens = limit - output_tokens - LLMConfig.ANSWER_TOKEN_SAFETY_MARGIN
    context_tokens -= fixed_tokens

    if context_tokens < LLMConfig.ANSWER_MIN_CONTEXT_TOKENS:
        output_tokens = _shrink_output_tokens(limit, fixed_tokens)
        context_tokens = limit - output_tokens - LLMConfig.ANSWER_TOKEN_SAFETY_MARGIN
        context_tokens -= fixed_tokens

    return _Budget(max(context_tokens, 0), output_tokens)


def _shrink_output_tokens(limit: int, fixed_tokens: int) -> int:
    available = limit - fixed_tokens - LLMConfig.ANSWER_TOKEN_SAFETY_MARGIN
    available -= LLMConfig.ANSWER_MIN_CONTEXT_TOKENS

    return max(LLMConfig.ANSWER_MIN_TOKENS, min(LLMConfig.ANSWER_MAX_TOKENS, available))


def _query_terms(text: str) -> set[str]:
    terms = set()

    for raw in _WORD_RE.findall(text.lower()):
        term = raw.strip(".-")
        if _is_query_term(term):
            terms.add(term)

    return terms


def _is_query_term(term: str) -> bool:
    if not term or term in _STOP_TERMS:
        return False

    return any(ch.isdigit() for ch in term) or len(term) >= 4


def _select_candidates(
    results: list[RetrievalResult],
    terms: set[str],
) -> list[RetrievalResult]:
    candidates: list[RetrievalResult] = []
    seen: set[str] = set()
    primary_count = 0

    for result in results:
        if result.id in seen or not _is_candidate_allowed(result, primary_count, terms):
            continue

        candidates.append(result)
        seen.add(result.id)

        if not result.expanded_from:
            primary_count += 1

        if len(candidates) >= LLMConfig.ANSWER_CONTEXT_HARD_LIMIT:
            break

    return candidates


def _is_candidate_allowed(
    result: RetrievalResult,
    primary_count: int,
    terms: set[str],
) -> bool:
    if result.is_table:
        return _is_table_candidate_allowed(result, terms)

    if _is_required_reference_result(result):
        return True

    return primary_count < LLMConfig.ANSWER_CONTEXT_LIMIT


def _is_table_candidate_allowed(result: RetrievalResult, terms: set[str]) -> bool:
    if _table_has_row_match(result.text, terms):
        return True

    if not result.expanded_from:
        return True

    return (result.table_window_index or 1) == 1


def _is_required_reference_result(result: RetrievalResult) -> bool:
    if not result.expanded_from:
        return False

    return result.expanded_from.startswith(
        ("table:", "table_id:", "section:", "appendix:")
    )


def _pack_candidates(
    candidates: list[RetrievalResult],
    terms: set[str],
    budget: _Budget,
) -> PackedContext:
    blocks: list[str] = []
    used = 0
    dropped = 0

    for result in candidates:
        block = _format_block(len(blocks) + 1, result, terms)
        cost = _estimate_tokens(block)

        if used + cost > budget.context_tokens:
            dropped += 1
            continue

        blocks.append(block)
        used += cost

    text = _append_budget_note("\n\n---\n\n".join(blocks), dropped, budget, used)

    return PackedContext(
        text, len(blocks), dropped, used, budget.context_tokens, budget.output_tokens
    )


def _format_block(index: int, result: RetrievalResult, terms: set[str]) -> str:
    text = _compact_result_text(result, terms)
    fields = [
        f"Фрагмент {index}",
        f"Документ: {result.filename}",
        f"Связь: {_relation_line(result)}",
        f"Раздел: {result.section_path or '—'}",
        f"Тип: {'таблица' if result.is_table else 'текст'}",
        _table_line(result),
        f"Ссылки: {_refs_line(result)}",
        "Текст:",
        text,
    ]

    return "\n".join(field for field in fields if field)


def _compact_result_text(result: RetrievalResult, terms: set[str]) -> str:
    if result.is_table:
        return _compact_table_text(result.text, terms)

    return _trim_to_tokens(result.text.strip(), LLMConfig.ANSWER_MAX_TEXT_BLOCK_TOKENS)


def _compact_table_text(text: str, terms: set[str]) -> str:
    caption, header, rows = _table_sections(text)
    snippets = _matched_snippets(rows, terms)

    if not snippets:
        snippets = [_trim_to_tokens(rows, LLMConfig.ANSWER_MAX_TABLE_ROW_TOKENS)]

    blocks = [
        caption,
        f"Заголовки таблицы: {_trim_to_tokens(header, LLMConfig.ANSWER_MAX_TABLE_HEADER_TOKENS)}",
        "Релевантные строки/фрагменты таблицы:",
        *snippets[: LLMConfig.ANSWER_MAX_TABLE_SNIPPETS],
    ]

    return "\n".join(block for block in blocks if block).strip()


def _table_sections(text: str) -> tuple[str, str, str]:
    header_match = _TABLE_HEADER_RE.search(text)
    rows_match = _TABLE_ROWS_RE.search(text)

    if not header_match:
        return "", "", text.strip()

    caption = header_match.group("prefix").strip()
    header = header_match.group("header").strip()
    rows = rows_match.group("rows").strip() if rows_match else ""

    return caption, header, rows


def _matched_snippets(rows: str, terms: set[str]) -> list[str]:
    if not rows or not terms:
        return []

    lowered = rows.lower()
    ranges = _snippet_ranges(lowered, terms, LLMConfig.ANSWER_TABLE_SNIPPET_CHARS)

    return [_clean_snippet(rows[start:end]) for start, end in ranges]


def _snippet_ranges(text: str, terms: set[str], window: int) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []

    for term in terms:
        pos = text.find(term)
        if pos < 0:
            continue

        start = max(0, pos - window // 3)
        end = min(len(text), pos + window)
        ranges.append((start, end))

    return _merge_ranges(sorted(ranges))[: LLMConfig.ANSWER_MAX_TABLE_SNIPPETS]


def _merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []

    for start, end in ranges:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))

    return merged


def _clean_snippet(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip(" |")


def _table_has_row_match(text: str, terms: set[str]) -> bool:
    _, _, rows = _table_sections(text)
    lowered = rows.lower()

    return bool(terms and any(term in lowered for term in terms))


def _trim_to_tokens(text: str, max_tokens: int) -> str:
    text = text.strip()

    if _estimate_tokens(text) <= max_tokens:
        return text

    max_chars = max(120, int(max_tokens * 3.0))
    trimmed = text[:max_chars].rsplit(" ", 1)[0].strip()

    return f"{trimmed}\n[Фрагмент обрезан по token budget]"


def _append_budget_note(text: str, dropped: int, budget: _Budget, used: int) -> str:
    if not dropped:
        return text

    note = (
        f"Примечание упаковки контекста: отброшено {dropped} фрагментов; "
        f"использовано примерно {used}/{budget.context_tokens} токенов контекста."
    )

    return f"{text}\n\n---\n\n{note}" if text else note


def _relation_line(result: RetrievalResult) -> str:
    if result.expanded_from:
        return f"подтянут по внутренней ссылке {result.expanded_from}"

    return "основной результат поиска"


def _refs_line(result: RetrievalResult) -> str:
    refs = [f"external:{ref}" for ref in result.man_refs]
    refs.extend(result.cross_refs)
    refs.extend(f"anchor:{ref}" for ref in result.anchor_refs)

    return ", ".join(refs) or "—"


def _table_line(result: RetrievalResult) -> str:
    if not result.is_table:
        return "Таблица: —"

    caption = result.table_caption or result.leaf_heading or "—"
    part = _format_index(result.table_part_index, result.table_part_total)
    window = _format_index(result.table_window_index, result.table_window_total)
    required = "; обязательный контекст по ссылке" if result.expanded_from else ""

    return f"Таблица: {caption}; часть: {part}; окно: {window}{required}"


def _format_index(index: int | None, total: int | None) -> str:
    if index is None or total is None:
        return "—"

    return f"{index}/{total}"
