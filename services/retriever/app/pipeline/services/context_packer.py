import re
from dataclasses import dataclass
from functools import lru_cache

from app.pipeline.schemas import EvidenceItem
from app.pipeline.services.text_terms import (
    query_terms,
    term_coverage,
    term_hit,
)
from app.pipeline.services.research import asks_norm_refs, is_norm_refs, source_group
from app.schemas import LLMConfig, RetrievalResult

_EVIDENCE_PRIORITY = {
    "direct_rule": 0,
    "calculation_table": 1,
    "applicability": 2,
    "background": 3,
    "noise": 5,
}
_TABLE_HEADER_RE = re.compile(
    r"(?P<prefix>.*?)Заголовки таблицы:\s*(?P<header>.*?)(?:Строки таблицы:|$)",
    re.IGNORECASE | re.DOTALL,
)
_TABLE_ROWS_RE = re.compile(r"Строки таблицы:\s*(?P<rows>.*)", re.IGNORECASE | re.DOTALL)


@dataclass(frozen=True)
class PackedContext:
    text: str
    included_count: int
    dropped_count: int
    used_tokens: int
    budget_tokens: int
    max_output_tokens: int
    input_tokens: int = 0
    model_max_tokens: int = 0
    dropped_by_budget: int = 0
    dropped_by_relevance: int = 0
    candidates: tuple[RetrievalResult, ...] = ()
    included: tuple[RetrievalResult, ...] = ()
    excluded: tuple[RetrievalResult, ...] = ()
    selection_excluded: tuple[RetrievalResult, ...] = ()


@dataclass(frozen=True)
class _Budget:
    context_tokens: int
    output_tokens: int


@dataclass
class _SelectCounts:
    primary: int = 0
    expanded: int = 0
    tables: int = 0


def build_packed_context(
    results: list[RetrievalResult],
    query: str,
    effective_query: str,
    static_prompt: str,
    system_prompt: str,
    expanded_relations: dict[str, str] | None = None,
    evidence_items: list[EvidenceItem] | None = None,
) -> PackedContext:
    terms = query_terms(f"{query} {effective_query}")
    budget = _build_budget(static_prompt, system_prompt)
    relations = expanded_relations or {}
    evidence_by_id = _evidence_by_id(evidence_items)
    allow_norm_refs = asks_norm_refs(f"{query} {effective_query}")
    selected = _select_candidates(
        results, terms, relations, allow_norm_refs, evidence_by_id
    )
    selected_ids = {result.id for result in selected}
    selection_excluded = [result for result in results if result.id not in selected_ids]
    fixed_tokens = _estimate_tokens(static_prompt) + _estimate_tokens(system_prompt)

    return _pack_candidates(
        selected,
        selection_excluded,
        terms,
        budget,
        relations,
        fixed_tokens,
    )


def _estimate_tokens(text: str) -> int:
    if not text:
        return 0

    tokenizer = _answer_tokenizer()
    if tokenizer is not None:
        return len(tokenizer.encode(text, add_special_tokens=False))

    words = len(text.split())
    chars = len(text)

    return max(int(words * 2.0), int(chars / 1.7), 1)


def estimate_text_tokens(text: str) -> int:
    return _estimate_tokens(text)


@lru_cache(maxsize=1)
def _answer_tokenizer():
    try:
        from transformers import AutoTokenizer

        return AutoTokenizer.from_pretrained(
            LLMConfig.ANSWER_TOKENIZER_MODEL,
            local_files_only=True,
        )
    except Exception:
        return None


def _build_budget(static_prompt: str, system_prompt: str) -> _Budget:
    fixed_tokens = _estimate_tokens(static_prompt) + _estimate_tokens(system_prompt)
    input_limit = _input_token_limit()
    output_tokens = min(LLMConfig.ANSWER_MAX_TOKENS, LLMConfig.ANSWER_MODEL_MAX_LEN // 3)
    context_tokens = _context_budget(input_limit, output_tokens, fixed_tokens)

    if context_tokens < LLMConfig.ANSWER_MIN_CONTEXT_TOKENS:
        output_tokens = _shrink_output_tokens(fixed_tokens)
        context_tokens = _context_budget(input_limit, output_tokens, fixed_tokens)

    return _Budget(max(context_tokens, 0), output_tokens)


def _input_token_limit() -> int:
    return min(LLMConfig.ANSWER_MODEL_CONTEXT_TOKENS, LLMConfig.ANSWER_MAX_INPUT_TOKENS)


def _context_budget(input_limit: int, output_tokens: int, fixed_tokens: int) -> int:
    hard_input = LLMConfig.ANSWER_MODEL_MAX_LEN - output_tokens
    hard_input -= LLMConfig.ANSWER_TOKEN_SAFETY_MARGIN

    return min(input_limit, hard_input) - fixed_tokens


def _shrink_output_tokens(fixed_tokens: int) -> int:
    available = LLMConfig.ANSWER_MODEL_MAX_LEN - fixed_tokens
    available -= LLMConfig.ANSWER_TOKEN_SAFETY_MARGIN
    available -= LLMConfig.ANSWER_MIN_CONTEXT_TOKENS

    return max(LLMConfig.ANSWER_MIN_TOKENS, min(LLMConfig.ANSWER_MAX_TOKENS, available))


def _select_candidates(
    results: list[RetrievalResult],
    terms: set[str],
    expanded_relations: dict[str, str],
    allow_norm_refs: bool,
    evidence_by_id: dict[str, EvidenceItem],
) -> list[RetrievalResult]:
    candidates: list[RetrievalResult] = []
    seen: set[str] = set()
    group_counts: dict[str, int] = {}
    counts = _SelectCounts()

    for result in _ordered_candidates(
        results, terms, expanded_relations, evidence_by_id
    ):
        relation = expanded_relations.get(result.id)
        if result.id in seen or not _allow_candidate(
            result,
            counts.primary,
            terms,
            expanded_relations,
            allow_norm_refs,
            evidence_by_id,
        ):
            continue
        if not _allow_source_group(result, group_counts):
            continue
        if not _within_context_caps(result, relation, counts):
            continue

        candidates.append(result)
        seen.add(result.id)
        _increment_group(result, group_counts)
        _increment_counts(result, relation, counts)

        if len(candidates) >= LLMConfig.ANSWER_CONTEXT_HARD_LIMIT:
            break

    return candidates


def _ordered_candidates(
    results: list[RetrievalResult],
    terms: set[str],
    expanded_relations: dict[str, str],
    evidence_by_id: dict[str, EvidenceItem],
) -> list[RetrievalResult]:
    if evidence_by_id:
        return _evidence_ordered(results, expanded_relations, evidence_by_id)

    primary = [result for result in results if result.id not in expanded_relations]
    expanded = [result for result in results if result.id in expanded_relations]
    direct, related = _split_direct(primary, terms)

    return _diverse_first(direct) + _diverse_first(related) + _sort_expanded(
        expanded, terms
    )


def _evidence_ordered(
    results: list[RetrievalResult],
    expanded_relations: dict[str, str],
    evidence_by_id: dict[str, EvidenceItem],
) -> list[RetrievalResult]:
    support = [result for result in results if _evidence_rank(result, evidence_by_id) < 3]
    background = [
        result for result in results if _evidence_rank(result, evidence_by_id) == 3
    ]
    rest = [result for result in results if _evidence_rank(result, evidence_by_id) > 3]

    return (
        _preserve_diverse_first(
            _sort_by_evidence(support, expanded_relations, evidence_by_id)
        )
        + _preserve_diverse_first(
            _sort_by_evidence(background, expanded_relations, evidence_by_id)
        )
        + _preserve_diverse_first(
            _sort_by_evidence(rest, expanded_relations, evidence_by_id)
        )
    )


def _sort_by_evidence(
    results: list[RetrievalResult],
    expanded_relations: dict[str, str],
    evidence_by_id: dict[str, EvidenceItem],
) -> list[RetrievalResult]:
    return sorted(
        results,
        key=lambda item: _evidence_sort(item, expanded_relations, evidence_by_id),
    )


def _sort_expanded(
    results: list[RetrievalResult],
    terms: set[str],
) -> list[RetrievalResult]:
    return sorted(results, key=lambda result: _expanded_sort_key(result, terms))


def _expanded_sort_key(
    result: RetrievalResult,
    terms: set[str],
) -> tuple[int, float, int]:
    row_rank = 0 if result.is_table and _table_has_row_match(result.text, terms) else 1
    index = result.chunk_index if result.chunk_index is not None else 10**9

    return row_rank, -result.score, index


def _within_context_caps(
    result: RetrievalResult,
    relation: str | None,
    counts: _SelectCounts,
) -> bool:
    if relation and counts.expanded >= LLMConfig.ANSWER_EXPANDED_CONTEXT_LIMIT:
        return False
    if result.is_table and counts.tables >= LLMConfig.ANSWER_TABLE_CONTEXT_LIMIT:
        return False

    return True


def _increment_counts(
    result: RetrievalResult,
    relation: str | None,
    counts: _SelectCounts,
) -> None:
    if relation:
        counts.expanded += 1
    else:
        counts.primary += 1

    if result.is_table:
        counts.tables += 1


def _split_direct(
    results: list[RetrievalResult],
    terms: set[str],
) -> tuple[list[RetrievalResult], list[RetrievalResult]]:
    if not terms:
        return results, []

    direct: list[RetrievalResult] = []
    related: list[RetrievalResult] = []
    threshold = 0.45 if len(terms) >= 3 else 0.5

    for result in results:
        target = direct if _term_coverage(result, terms) >= threshold else related
        target.append(result)

    return direct, related


def _diverse_first(results: list[RetrievalResult]) -> list[RetrievalResult]:
    first: list[RetrievalResult] = []
    rest: list[RetrievalResult] = []
    seen_docs: set[str] = set()

    for result in sorted(results, key=_candidate_sort_key):
        doc_key = result.filename
        if doc_key not in seen_docs:
            first.append(result)
            seen_docs.add(doc_key)
        else:
            rest.append(result)

    return first + rest


def _preserve_diverse_first(results: list[RetrievalResult]) -> list[RetrievalResult]:
    first: list[RetrievalResult] = []
    rest: list[RetrievalResult] = []
    seen_docs: set[str] = set()

    for result in results:
        doc_key = result.filename
        if doc_key not in seen_docs:
            first.append(result)
            seen_docs.add(doc_key)
        else:
            rest.append(result)

    return first + rest


def _candidate_sort_key(result: RetrievalResult) -> tuple[float, int]:
    index = result.chunk_index if result.chunk_index is not None else 10**9

    return -result.score, index


def _evidence_by_id(items: list[EvidenceItem] | None) -> dict[str, EvidenceItem]:
    if not items:
        return {}

    return {item.chunk_id: item for item in items}


def _evidence_sort(
    result: RetrievalResult,
    expanded_relations: dict[str, str],
    evidence_by_id: dict[str, EvidenceItem],
) -> tuple[int, int, float, int]:
    stage = 0 if result.id not in expanded_relations else 1
    index = result.chunk_index if result.chunk_index is not None else 10**9

    return _evidence_rank(result, evidence_by_id), stage, -result.score, index


def _evidence_rank(
    result: RetrievalResult,
    evidence_by_id: dict[str, EvidenceItem],
) -> int:
    item = evidence_by_id.get(result.id)
    if item is None:
        return 4

    return _EVIDENCE_PRIORITY.get(item.evidence_role, 4)


def _drop_noise(
    result: RetrievalResult,
    evidence_by_id: dict[str, EvidenceItem],
) -> bool:
    if not any(item.evidence_role != "noise" for item in evidence_by_id.values()):
        return False

    item = evidence_by_id.get(result.id)
    return bool(item and item.evidence_role == "noise")


def _term_coverage(result: RetrievalResult, terms: set[str]) -> float:
    return term_coverage(terms, _result_search_text(result))


def _result_search_text(result: RetrievalResult) -> str:
    return " ".join(
        [
            result.text,
            result.filename,
            result.section_path,
            result.parent_heading or "",
            result.leaf_heading or "",
            result.table_caption or "",
            result.table_id or "",
        ]
    )


def _allow_candidate(
    result: RetrievalResult,
    primary_count: int,
    terms: set[str],
    expanded_relations: dict[str, str],
    allow_norm_refs: bool,
    evidence_by_id: dict[str, EvidenceItem],
) -> bool:
    if is_norm_refs(result) and not allow_norm_refs:
        return False

    if _drop_noise(result, evidence_by_id):
        return False

    if terms and _is_heading_only(result):
        return False

    if terms and not _covers_query_terms(result, terms, expanded_relations):
        return False

    if result.is_table:
        return _allow_table_candidate(result, terms, expanded_relations)

    if _is_req_ref_result(result, expanded_relations):
        return True

    return primary_count < LLMConfig.ANSWER_CONTEXT_LIMIT


def _allow_table_candidate(
    result: RetrievalResult,
    terms: set[str],
    expanded_relations: dict[str, str],
) -> bool:
    if _table_has_row_match(result.text, terms):
        return True

    if result.id not in expanded_relations:
        return True

    return (result.table_window_index or 1) == 1


def _is_heading_only(result: RetrievalResult) -> bool:
    if result.is_table:
        return False

    words = result.text.strip().split()
    return len(words) <= 12 and bool(result.section_path or result.anchor_refs)


def _covers_query_terms(
    result: RetrievalResult,
    terms: set[str],
    expanded_relations: dict[str, str],
) -> bool:
    if len(terms) < 2:
        return True

    relation = expanded_relations.get(result.id, "")
    if relation.startswith("table"):
        return _table_has_row_match(result.text, terms) or _query_match(result, terms)

    return _query_match(result, terms)


def _query_match(result: RetrievalResult, terms: set[str]) -> bool:
    return _query_coverage(result, terms) >= _query_threshold(terms)


def _query_coverage(result: RetrievalResult, terms: set[str]) -> float:
    text = _result_search_text(result).lower()
    hits = sum(term_hit(term, text) for term in terms)
    if hits < min(2, len(terms)):
        return 0.0

    return hits / len(terms)


def _query_threshold(terms: set[str]) -> float:
    return 0.5 if len(terms) <= 3 else 0.4


def _is_req_ref_result(
    result: RetrievalResult,
    expanded_relations: dict[str, str],
) -> bool:
    relation = expanded_relations.get(result.id)
    if not relation:
        return False

    return relation.startswith(
        ("table:", "table_id:", "section:", "appendix:", "neighbor:")
    )


def _allow_source_group(
    result: RetrievalResult,
    group_counts: dict[str, int],
) -> bool:
    return group_counts.get(source_group(result), 0) < 1


def _increment_group(
    result: RetrievalResult,
    group_counts: dict[str, int],
) -> None:
    key = source_group(result)
    group_counts[key] = group_counts.get(key, 0) + 1


def _pack_candidates(
    candidates: list[RetrievalResult],
    selection_excluded: list[RetrievalResult],
    terms: set[str],
    budget: _Budget,
    expanded_relations: dict[str, str],
    fixed_tokens: int,
) -> PackedContext:
    blocks, included, excluded, used = _fit_candidates(
        candidates, terms, budget, expanded_relations
    )
    input_tokens = fixed_tokens + used

    return _packed_context(
        candidates,
        blocks,
        included,
        excluded,
        selection_excluded,
        used,
        input_tokens,
        budget,
    )


def _packed_context(
    candidates: list[RetrievalResult],
    blocks: list[str],
    included: list[RetrievalResult],
    excluded: list[RetrievalResult],
    selection_excluded: list[RetrievalResult],
    used: int,
    input_tokens: int,
    budget: _Budget,
) -> PackedContext:
    dropped = len(excluded)
    text = _append_budget_note("\n\n---\n\n".join(blocks), dropped, budget, used)

    return PackedContext(
        text=text,
        included_count=len(blocks),
        dropped_count=dropped + len(selection_excluded),
        used_tokens=used,
        budget_tokens=budget.context_tokens,
        max_output_tokens=budget.output_tokens,
        input_tokens=input_tokens,
        model_max_tokens=LLMConfig.ANSWER_MODEL_MAX_LEN,
        dropped_by_budget=dropped,
        dropped_by_relevance=len(selection_excluded),
        candidates=tuple(candidates),
        included=tuple(included),
        excluded=tuple(excluded),
        selection_excluded=tuple(selection_excluded),
    )


def _fit_candidates(
    candidates: list[RetrievalResult],
    terms: set[str],
    budget: _Budget,
    expanded_relations: dict[str, str],
) -> tuple[list[str], list[RetrievalResult], list[RetrievalResult], int]:
    blocks: list[str] = []
    included: list[RetrievalResult] = []
    excluded: list[RetrievalResult] = []
    used = 0

    for result in candidates:
        block = _format_block(
            len(blocks) + 1, result, terms, expanded_relations.get(result.id)
        )
        cost = _estimate_tokens(block)

        if used + cost > budget.context_tokens:
            excluded.append(result)
            continue

        blocks.append(block)
        included.append(result)
        used += cost

    return blocks, included, excluded, used


def _format_block(
    index: int,
    result: RetrievalResult,
    terms: set[str],
    relation: str | None,
) -> str:
    text = _compact_result_text(result, terms)
    fields = [
        f"Фрагмент {index}",
        f"Документ: {result.filename}",
        f"Связь: {_relation_line(relation)}",
        f"Группа источника: {source_group(result)}",
        f"Раздел: {result.section_path or '—'}",
        f"Заголовки: {_headings_line(result)}",
        f"Тип: {'таблица' if result.is_table else 'текст'}",
        _table_line(result, relation),
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

    return bool(terms and any(term_hit(term, lowered) for term in terms))


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


def _relation_line(relation: str | None) -> str:
    if relation:
        return f"подтянут по внутренней ссылке {relation}"

    return "основной результат поиска"


def _refs_line(result: RetrievalResult) -> str:
    refs = [f"external:{ref}" for ref in result.man_refs]
    refs.extend(result.cross_refs)
    refs.extend(f"anchor:{ref}" for ref in result.anchor_refs)

    return ", ".join(refs) or "—"


def _headings_line(result: RetrievalResult) -> str:
    return " > ".join(result.headings) or "—"


def _table_line(result: RetrievalResult, relation: str | None) -> str:
    if not result.is_table:
        return "Таблица: —"

    caption = result.table_caption or result.leaf_heading or "—"
    part = _format_index(result.table_part_index, result.table_part_total)
    window = _format_index(result.table_window_index, result.table_window_total)
    required = "; обязательный контекст по ссылке" if relation else ""

    return f"Таблица: {caption}; часть: {part}; окно: {window}{required}"


def _format_index(index: int | None, total: int | None) -> str:
    if index is None or total is None:
        return "—"

    return f"{index}/{total}"
