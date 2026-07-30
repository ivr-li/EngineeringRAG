import hashlib
import re
from dataclasses import dataclass, field

from app.pipeline.schemas import CanonicalGroup, ExpandedChunk
from app.pipeline.services.text_terms import query_terms, term_coverage
from app.schemas import RetrievalResult

_SECTION_ANCHOR_RE = re.compile(r"^section:(?P<num>\d+(?:\.\d+){1,4})$")
_INLINE_SECTION_RE = re.compile(r"\b(?P<num>\d+(?:\.\d+){1,4})\b")
_TOKEN_RE = re.compile(r"[а-яёa-z0-9]+", re.IGNORECASE)
_GENERIC_TABLE_IDS = {"", "table", "table_"}


@dataclass(frozen=True)
class CanonicalizedResults:
    results: list[RetrievalResult]
    groups: list[CanonicalGroup]


@dataclass
class _SourceGroup:
    key: str
    first_index: int
    items: list[RetrievalResult] = field(default_factory=list)


def canonicalize_results(
    results: list[RetrievalResult],
    expanded_chunks: list[ExpandedChunk],
    query: str,
) -> CanonicalizedResults:
    relations = {item.chunk.id: item.relation for item in expanded_chunks}
    groups = _collect_groups(results)
    selected = [_select_group(group, relations, query) for group in groups]
    trace_groups = [
        _trace_group(group, selected_result)
        for group, selected_result in zip(groups, selected, strict=False)
    ]

    return CanonicalizedResults(results=selected, groups=trace_groups)


def canonical_source_group(result: RetrievalResult) -> str:
    if result.is_table:
        return _table_key(result)

    section_num = _specific_section_num(result)
    if section_num:
        return f"{result.filename}|section|{section_num}"

    section = _clean_key(result.section_path or result.leaf_heading or "")
    fingerprint = _text_fingerprint(result.text)

    return f"{result.filename}|text|{section}|{fingerprint}"


def _collect_groups(results: list[RetrievalResult]) -> list[_SourceGroup]:
    groups: dict[str, _SourceGroup] = {}

    for index, result in enumerate(results):
        key = canonical_source_group(result)
        group = groups.setdefault(key, _SourceGroup(key=key, first_index=index))
        group.items.append(result)

    return sorted(groups.values(), key=lambda group: group.first_index)


def _select_group(
    group: _SourceGroup,
    relations: dict[str, str],
    query: str,
) -> RetrievalResult:
    return min(group.items, key=lambda result: _representative_key(result, relations, query))


def _trace_group(group: _SourceGroup, selected: RetrievalResult) -> CanonicalGroup:
    merged_ids = [item.id for item in group.items]
    reason = "canonical_unit_duplicate" if len(merged_ids) > 1 else ""

    return CanonicalGroup(
        source_group=group.key,
        kept_chunk_id=selected.id,
        merged_chunk_ids=merged_ids,
        dedup_reason=reason,
    )


def _representative_key(
    result: RetrievalResult,
    relations: dict[str, str],
    query: str,
) -> tuple[float, int, int, int, float, int]:
    coverage = _query_coverage(result, query)
    stage = 0 if result.id not in relations else 1
    table_rank = 0 if _is_relevant_table(result, query) else 1
    overlap = 1 if result.is_overlap_window else 0
    index = result.chunk_index if result.chunk_index is not None else 10**9

    return (-coverage, stage, table_rank, overlap, -result.score, index)


def _query_coverage(result: RetrievalResult, query: str) -> float:
    terms = query_terms(query)
    if not terms:
        return 1.0

    return term_coverage(terms, _searchable_text(result))


def _is_relevant_table(result: RetrievalResult, query: str) -> bool:
    if not result.is_table:
        return False

    terms = query_terms(query)
    return bool(terms and term_coverage(terms, result.text) > 0)


def _table_key(result: RetrievalResult) -> str:
    table_id = _clean_key(result.table_id or "")
    if table_id not in _GENERIC_TABLE_IDS:
        return f"{result.filename}|table|{table_id}"

    table_label = _clean_key(
        result.table_caption or result.leaf_heading or " ".join(result.anchor_refs)
    )
    if table_label:
        return f"{result.filename}|table|{table_label}"

    return f"{result.filename}|table|{_text_fingerprint(result.text)}"


def _specific_section_num(result: RetrievalResult) -> str:
    for ref in result.anchor_refs:
        match = _SECTION_ANCHOR_RE.match(ref)
        if match:
            return match.group("num")

    match = _INLINE_SECTION_RE.search(result.text[:500])
    return match.group("num") if match else ""


def _text_fingerprint(text: str) -> str:
    tokens = _TOKEN_RE.findall(text.lower())
    normalized = " ".join(tokens[:80])
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()

    return digest[:12]


def _searchable_text(result: RetrievalResult) -> str:
    return " ".join(
        [
            result.text,
            result.filename,
            result.section_path,
            result.parent_heading or "",
            result.leaf_heading or "",
            result.table_caption or "",
        ]
    )


def _clean_key(text: str) -> str:
    return " ".join(text.lower().split())
