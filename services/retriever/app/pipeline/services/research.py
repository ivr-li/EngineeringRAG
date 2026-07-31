import re
from dataclasses import dataclass

from app.pipeline.schemas import EvidenceGroup, EvidenceItem, ExpandedChunk, QueryAspect
from app.pipeline.services.canonical import canonical_source_group
from app.pipeline.services.text_terms import query_terms, term_coverage, term_hit
from app.schemas import RetrievalResult

_UNIT_VALUE_RE = re.compile(
    r"\b\d+(?:[,.]\d+)?\s*"
    r"(?:мм|см|м|%|°C|°С|кПа|МПа|Па|сут|дн|кН|Н|т)\b",
    re.IGNORECASE,
)
_DECIMAL_VALUE_RE = re.compile(r"(?<![\w])\d+[,.]\d+(?![\w])")
_FORMULA_VALUE_RE = re.compile(
    r"\b[а-яёa-z][а-яёa-z0-9_ '\-]{0,18}\s*"
    r"(?:=|≤|>=|<=|<|>)\s*[^.;\n]{1,48}",
    re.IGNORECASE,
)
_SEGMENT_RE = re.compile(r"(?<=[.!?;])\s+|\n+")
DIRECT_ROLE = "direct_rule"
CALC_ROLE = "calculation_table"
APPLICABILITY_ROLE = "applicability"
BACKGROUND_ROLE = "background"
NOISE_ROLE = "noise"
_SUPPORT_ROLES = {DIRECT_ROLE, CALC_ROLE, APPLICABILITY_ROLE}
_ROLE_PRIORITY = {
    DIRECT_ROLE: 5,
    CALC_ROLE: 4,
    APPLICABILITY_ROLE: 3,
    BACKGROUND_ROLE: 1,
    NOISE_ROLE: 0,
}
_STRUCT_REF_PREFIXES = ("table:", "section:", "appendix:")
_CONDITION_OPERATORS = (
    "при ",
    "если ",
    "в случае",
    "для ",
    "в зависимости",
    "допускается",
    "не допускается",
)
_NORMATIVE_OPERATORS = (
    "следует",
    "должен",
    "необходимо",
    "допускается",
    "не допускается",
    "принимают",
    "принимается",
    "определяют",
    "устанавливают",
    "выполняют",
    "производят",
)


@dataclass(frozen=True)
class _EvidenceSignal:
    text: str
    coverage: float


def build_q_plan(query: str, effective: str) -> list[QueryAspect]:
    plan: list[QueryAspect] = []
    seen: set[str] = set()

    _add_plan(plan, seen, query, "original")
    _add_plan(plan, seen, effective, "rewritten")
    _add_plan(plan, seen, f"требования {query}", "requirements")
    _add_plan(plan, seen, f"условия применения {query}", "conditions")
    _add_plan(plan, seen, f"таблица значения {query}", "tables")
    _add_masonry_plan(plan, seen, query)

    return plan[:6]


def build_evidence(
    results: list[RetrievalResult],
    query: str,
    expanded_chunks: list[ExpandedChunk] | None = None,
) -> list[EvidenceItem]:
    items_by_group: dict[str, EvidenceItem] = {}
    relations = _expanded_relations(expanded_chunks)

    for result in results:
        item = _build_evidence_item(result, query, relations.get(result.id, ""))
        if not item:
            continue

        current = items_by_group.get(item.source_group)
        if current is None or _better_evidence(item, current):
            items_by_group[item.source_group] = item

    return sorted(items_by_group.values(), key=_evidence_sort_key)


def build_follow_up_plan(
    query: str,
    effective: str,
    results: list[RetrievalResult],
    existing: list[QueryAspect],
    evidence_items: list[EvidenceItem],
) -> list[QueryAspect]:
    plan: list[QueryAspect] = []
    seen = {_norm_key(item.query) for item in existing}
    seed = effective if _norm_key(effective) != _norm_key(query) else query

    for ref in _unresolved_struct_refs(results):
        _add_plan(
            plan,
            seen,
            f"{_ref_query(ref)} {seed}",
            _ref_aspect(ref),
            "unresolved_structural_ref",
        )

    return plan[:2]


def pick_ans_mode(items: list[EvidenceItem], query: str) -> str:
    direct = _direct_items(items)
    primary_direct = _primary_items(direct)
    related = _related_items(items)

    if not direct and related:
        return "partial_supported"
    if not direct:
        return "not_found"
    if not primary_direct:
        return "partial_supported"
    if _needs_summary(primary_direct, direct):
        return "multi_path"

    return "direct_supported"


def answer_mode_reason(items: list[EvidenceItem], query: str, mode: str) -> str:
    direct = [item for item in items if item.supports_intent]
    primary_direct = _primary_items(direct)
    expanded_direct = _expanded_items(direct)

    if mode == "not_found":
        return "Нет прямых или смежных evidence для исходного вопроса."
    if mode == "partial_supported":
        return _partial_reason(direct, expanded_direct)
    if mode == "multi_path":
        return _summary_reason(primary_direct, direct)

    return "Найден один согласованный primary-evidence для прямого ответа."


def build_evidence_groups(items: list[EvidenceItem]) -> list[EvidenceGroup]:
    groups: list[EvidenceGroup] = []
    _add_group(groups, DIRECT_ROLE, "Прямые нормативные правила", items)
    _add_group(groups, CALC_ROLE, "Расчетные таблицы, коэффициенты и значения", items)
    _add_group(groups, APPLICABILITY_ROLE, "Условия применимости и ограничения", items)
    _add_group(groups, BACKGROUND_ROLE, "Фоновый нормативный контекст", items)

    return groups


def plan_block(plan: list[QueryAspect]) -> str:
    if not plan:
        return "План поиска не сформирован."

    lines = [_plan_line(item) for item in plan]

    return "\n".join(lines)


def evidence_block(items: list[EvidenceItem], mode: str) -> str:
    if not items:
        return "Evidence не извлечены."

    lines = [f"Выбранная стратегия ответа: {mode}."]
    groups = (
        ("Прямые правила", _role_items(items, DIRECT_ROLE)),
        ("Расчетные таблицы и коэффициенты", _role_items(items, CALC_ROLE)),
        ("Условия применимости", _role_items(items, APPLICABILITY_ROLE)),
        ("Фоновый контекст", _role_items(items, BACKGROUND_ROLE)),
    )
    for title, group_items in groups:
        if group_items:
            lines.append(f"\n{title}:")
            for index, item in enumerate(group_items[:8], start=1):
                lines.append(_evidence_line(index, item))

    return "\n".join(lines)


def basis_block(items: list[EvidenceItem]) -> str:
    selected = [item for item in items if item.evidence_role in _SUPPORT_ROLES]
    if not selected:
        return "Прямые уникальные основания не выделены."

    lines = ["Документ | Раздел | Роль | Что подтверждает | source_group"]
    seen: set[str] = set()
    for item in selected:
        if item.source_group in seen:
            continue

        seen.add(item.source_group)
        lines.append(_basis_line(item))

    return "\n".join(lines[:10])


def coverage_gaps(items: list[EvidenceItem], query: str) -> list[str]:
    roles = {item.evidence_role for item in items if item.evidence_role != NOISE_ROLE}
    gaps: list[str] = []

    if not roles & _SUPPORT_ROLES:
        gaps.append("direct_evidence_missing")

    return gaps


def evidence_role(result: RetrievalResult, query: str, relation: str = "") -> str:
    return _role_for(result, query, relation, _best_signal(result, query))


def source_group(result: RetrievalResult) -> str:
    return canonical_source_group(result)


def asks_norm_refs(query: str) -> bool:
    lowered = query.lower()
    markers = (
        "нормативные ссылки",
        "список документов",
        "перечень документов",
        "какие документы",
        "на какие нормы",
        "ссылки на нормы",
    )

    return any(marker in lowered for marker in markers)


def is_norm_refs(result: RetrievalResult) -> bool:
    path = _section(result).lower()
    heading = " ".join(result.headings).lower()

    return "нормативные ссылки" in path or "нормативные ссылки" in heading


def _expanded_relations(
    expanded_chunks: list[ExpandedChunk] | None,
) -> dict[str, str]:
    if not expanded_chunks:
        return {}

    return {item.chunk.id: item.relation for item in expanded_chunks}


def _unresolved_struct_refs(results: list[RetrievalResult]) -> list[str]:
    present = _present_struct_refs(results)
    refs: list[str] = []
    seen: set[str] = set()

    for result in results:
        for ref in result.cross_refs:
            normalized = _norm_struct_ref(ref)
            if normalized and normalized not in present and normalized not in seen:
                refs.append(normalized)
                seen.add(normalized)

    return refs


def _present_struct_refs(results: list[RetrievalResult]) -> set[str]:
    refs: set[str] = set()

    for result in results:
        refs.update(_anchored_struct_refs(result))

    return refs


def _anchored_struct_refs(result: RetrievalResult) -> set[str]:
    refs = {_norm_struct_ref(ref) for ref in result.anchor_refs}
    refs.discard("")

    if result.is_table and result.table_id:
        refs.add(_norm_struct_ref(f"table:{result.table_id}"))

    return refs


def _norm_struct_ref(ref: object) -> str:
    text = str(ref).strip().lower()
    if not text.startswith(_STRUCT_REF_PREFIXES) or ":" not in text:
        return ""

    prefix, value = text.split(":", 1)
    normalized = _norm_ref_value(value)

    return f"{prefix}:{normalized}" if normalized else ""


def _norm_ref_value(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _ref_query(ref: str) -> str:
    prefix, value = ref.split(":", 1)
    labels = {"table": "таблица", "section": "пункт", "appendix": "приложение"}

    return f"{labels.get(prefix, prefix)} {value}".strip()


def _ref_aspect(ref: str) -> str:
    prefix = ref.split(":", 1)[0]

    return f"{prefix}_ref"


def _role_for(
    result: RetrievalResult,
    query: str,
    relation: str,
    signal: _EvidenceSignal,
) -> str:
    support_score = signal.coverage
    if support_score <= 0.0 or _is_heading_result(result):
        return NOISE_ROLE
    if is_norm_refs(result) and not asks_norm_refs(query):
        return BACKGROUND_ROLE
    if result.is_table:
        return _table_role(result, support_score)
    if _is_general_section(result):
        return BACKGROUND_ROLE
    if support_score >= 0.55 and _has_normative_text(signal.text):
        return DIRECT_ROLE
    if support_score >= 0.35 and _has_condition_text(signal.text):
        return APPLICABILITY_ROLE
    if relation and support_score >= 0.25:
        return APPLICABILITY_ROLE if _has_condition_text(signal.text) else BACKGROUND_ROLE

    return BACKGROUND_ROLE if support_score >= 0.2 else NOISE_ROLE


def _table_role(result: RetrievalResult, support_score: float) -> str:
    if support_score >= 0.3 and _has_table_evidence(result):
        return CALC_ROLE

    return BACKGROUND_ROLE if support_score >= 0.2 else NOISE_ROLE


def _has_table_evidence(result: RetrievalResult) -> bool:
    return bool(result.table_id or result.table_caption or _values_in(result.text))


def _has_normative_text(text: str) -> bool:
    return any(operator in text for operator in _NORMATIVE_OPERATORS)


def _has_condition_text(text: str) -> bool:
    return any(operator in text for operator in _CONDITION_OPERATORS)


def _is_general_section(result: RetrievalResult) -> bool:
    section = _section(result).lower()
    markers = ("область применения", "термины и определения", "общие положения")

    return any(marker in section for marker in markers)


def _is_heading_result(result: RetrievalResult) -> bool:
    if result.is_table:
        return False

    return len(result.text.strip().split()) <= 12 and bool(result.section_path)


def _better_evidence(candidate: EvidenceItem, current: EvidenceItem) -> bool:
    return _evidence_sort_key(candidate) < _evidence_sort_key(current)


def _evidence_sort_key(item: EvidenceItem) -> tuple[int, int, float]:
    stage_priority = 0 if item.source_stage == "primary" else 1

    return (-_ROLE_PRIORITY[item.evidence_role], stage_priority, -item.support_score)


def _build_evidence_item(
    result: RetrievalResult,
    query: str,
    relation: str,
) -> EvidenceItem | None:
    claim = _claim_text(result)
    if not claim:
        return None

    signal = _best_signal(result, query)
    role = _role_for(result, query, relation, signal)

    return EvidenceItem(
        claim=claim,
        document=result.filename,
        section=_section(result),
        condition=_condition(result, query),
        value=", ".join(_num_values(result.text, query)),
        interpretation=role,
        supports_intent=role in _SUPPORT_ROLES,
        chunk_id=result.id,
        source_stage="expanded" if relation else "primary",
        relation=relation,
        support_score=signal.coverage,
        evidence_role=role,
        source_group=source_group(result),
    )


def _add_plan(
    plan: list[QueryAspect],
    seen: set[str],
    query: str,
    aspect: str,
    reason: str = "",
) -> None:
    normalized = " ".join(query.split()).lower()
    if not normalized or normalized in seen:
        return

    seen.add(normalized)
    plan.append(QueryAspect(query=query.strip(), aspect=aspect, reason=reason))


def _add_masonry_plan(
    plan: list[QueryAspect],
    seen: set[str],
    query: str,
) -> None:
    lowered = query.lower()
    if not ("шв" in lowered and "клад" in lowered):
        return

    _add_plan(plan, seen, f"перевязка вертикальных швов кладки {query}", "joint_bond")
    _add_plan(plan, seen, f"швы кладки перевязка расстояние {query}", "masonry_joint")


def _evidence_line(index: int, item: EvidenceItem) -> str:
    value = f"; значения: {item.value}" if item.value else ""
    section = item.section or "—"
    relation = f"; relation={item.relation}" if item.relation else ""

    return (
        f"{index}. [{item.evidence_role}; {item.source_stage}; "
        f"score={item.support_score:.2f}{relation}; group={item.source_group}] "
        f"{item.document}, {section}: "
        f"{item.claim}{value} (chunk_id={item.chunk_id})"
    )


def _plan_line(item: QueryAspect) -> str:
    reason = f" ({item.reason})" if item.reason else ""

    return f"- {item.aspect}: {item.query}{reason}"


def _basis_line(item: EvidenceItem) -> str:
    section = item.section or "—"
    claim = _clean_condition(item.claim)

    return (
        f"{item.document} | {section} | {item.evidence_role} | "
        f"{claim} | {item.source_group}"
    )


def _claim_text(result: RetrievalResult) -> str:
    text = " ".join(result.text.strip().split())
    if not text:
        return ""

    limit = 420 if result.is_table else 320

    return text[:limit].strip()


def _num_values(text: str, query: str) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()

    for snippet in _value_snippets(text, query):
        for raw in _values_in(snippet):
            value = _norm_value(raw)
            if value not in seen:
                values.append(value)
                seen.add(value)

    return values[:10]


def _value_snippets(text: str, query: str) -> list[str]:
    segments = [
        segment.strip() for segment in _SEGMENT_RE.split(text) if segment.strip()
    ]
    terms = query_terms(query)
    if not segments or not terms:
        return segments[:4]

    matched = [
        segment
        for segment in segments
        if any(term_hit(term, segment.lower()) for term in terms)
    ]

    return matched or segments[:4]


def _values_in(text: str) -> list[str]:
    values: list[str] = []
    values.extend(_UNIT_VALUE_RE.findall(text))
    values.extend(_DECIMAL_VALUE_RE.findall(text))
    values.extend(match.group(0) for match in _FORMULA_VALUE_RE.finditer(text))

    return values


def _norm_value(raw: str) -> str:
    value = " ".join(raw.split()).replace(",", ".")

    return value.strip(" ,;.")


def _best_signal(result: RetrievalResult, query: str) -> _EvidenceSignal:
    terms = query_terms(query)
    if not terms:
        return _EvidenceSignal(text=result.text.lower(), coverage=1.0)

    if _has_shift(query, _res_text(result)):
        return _EvidenceSignal(text="", coverage=0.0)

    windows = _text_windows(result.text)
    metadata = _metadata_text(result)

    return max(
        (_window_signal(window, metadata, terms) for window in windows),
        key=lambda signal: signal.coverage,
        default=_EvidenceSignal(text="", coverage=0.0),
    )


def _text_windows(text: str) -> list[str]:
    segments = _segments(text)
    windows = [*segments]

    for index, segment in enumerate(segments[:-1]):
        windows.append(f"{segment} {segments[index + 1]}")

    return windows or [text.strip()]


def _segments(text: str) -> list[str]:
    return [segment.strip() for segment in _SEGMENT_RE.split(text) if segment.strip()]


def _window_signal(
    window: str,
    metadata: str,
    terms: set[str],
) -> _EvidenceSignal:
    searchable = f"{window} {metadata}".lower()
    coverage = term_coverage(terms, searchable)

    return _EvidenceSignal(text=window.lower(), coverage=coverage)


def _has_shift(query: str, text: str) -> bool:
    query = query.lower()
    text = text.lower()

    if "шв" in query and "деформац" in text and "деформац" not in query:
        return True

    return False


def _res_text(result: RetrievalResult) -> str:
    return " ".join(
        [
            result.text,
            result.filename,
            result.section_path,
            result.parent_heading or "",
            result.leaf_heading or "",
        ]
    ).lower()


def _metadata_text(result: RetrievalResult) -> str:
    return " ".join(
        [
            result.filename,
            result.section_path,
            result.parent_heading or "",
            result.leaf_heading or "",
            result.table_caption or "",
        ]
    )


def _section(result: RetrievalResult) -> str:
    if result.is_table:
        return result.table_caption or result.section_path or ""

    return result.section_path or result.leaf_heading or result.parent_heading or ""


def _condition(result: RetrievalResult, query: str) -> str:
    heading = result.leaf_heading or result.parent_heading or ""
    snippet = _first_value_snippet(result.text, query)
    if not snippet:
        return heading

    if heading and heading not in snippet:
        return f"{heading}: {snippet}"

    return snippet


def _first_value_snippet(text: str, query: str) -> str:
    for snippet in _value_snippets(text, query):
        if _values_in(snippet):
            return _clean_condition(snippet)

    return ""


def _clean_condition(text: str) -> str:
    text = " ".join(text.split())
    if len(text) <= 260:
        return text

    return text[:260].rsplit(" ", 1)[0].strip()


def _doc_count(items: list[EvidenceItem]) -> int:
    return len({item.document for item in items})


def _direct_items(items: list[EvidenceItem]) -> list[EvidenceItem]:
    return [item for item in items if item.evidence_role in _SUPPORT_ROLES]


def _related_items(items: list[EvidenceItem]) -> list[EvidenceItem]:
    return [item for item in items if item.evidence_role == BACKGROUND_ROLE]


def _role_items(items: list[EvidenceItem], role: str) -> list[EvidenceItem]:
    return [item for item in items if item.evidence_role == role]


def _primary_items(items: list[EvidenceItem]) -> list[EvidenceItem]:
    return [item for item in items if item.source_stage == "primary"]


def _expanded_items(items: list[EvidenceItem]) -> list[EvidenceItem]:
    return [item for item in items if item.source_stage == "expanded"]


def _needs_summary(
    primary_direct: list[EvidenceItem],
    direct: list[EvidenceItem],
) -> bool:
    if _doc_count(primary_direct) > 1:
        return True
    if len({item.evidence_role for item in direct}) > 1:
        return True

    return _condition_count(primary_direct) > 1 or _value_count(direct) > 1


def _condition_count(items: list[EvidenceItem]) -> int:
    values = {_norm_key(item.condition) for item in items if item.condition}

    return len(values)


def _value_count(items: list[EvidenceItem]) -> int:
    values = {_norm_key(item.value) for item in items if item.value}

    return len(values)


def _norm_key(text: str) -> str:
    return " ".join(text.lower().split())


def _partial_reason(
    direct: list[EvidenceItem],
    expanded_direct: list[EvidenceItem],
) -> str:
    if direct and len(direct) == len(expanded_direct):
        return "Прямые совпадения найдены только в expansion, без primary-подтверждения."

    return "Есть смежные evidence, но они не покрывают исходный вопрос полностью."


def _summary_reason(
    primary_direct: list[EvidenceItem],
    direct: list[EvidenceItem],
) -> str:
    reasons: list[str] = []
    role_count = len({item.evidence_role for item in direct})
    if _doc_count(primary_direct) > 1:
        reasons.append("несколько primary-документов")
    if role_count > 1:
        reasons.append("несколько типов нормативных оснований")
    if _condition_count(primary_direct) > 1:
        reasons.append("несколько условий применения")
    if _value_count(direct) > 1:
        reasons.append("несколько наборов значений")

    return ", ".join(reasons) or "нужно сравнить несколько нормативных путей"


def _add_group(
    groups: list[EvidenceGroup],
    name: str,
    description: str,
    items: list[EvidenceItem],
) -> None:
    chunk_ids = [item.chunk_id for item in _role_items(items, name)]
    if chunk_ids:
        groups.append(
            EvidenceGroup(name=name, description=description, chunk_ids=chunk_ids)
        )
