import re

from app.pipeline.schemas import EvidenceItem, QueryAspect
from app.schemas import RetrievalResult

_WORD_RE = re.compile(r"[а-яёa-z0-9][а-яёa-z0-9.\-]*", re.IGNORECASE)
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
_STOP = {
    "какие",
    "какой",
    "какая",
    "какое",
    "между",
    "нужно",
    "нужны",
    "должен",
    "должна",
    "должно",
    "должны",
    "можно",
    "норма",
    "нормы",
    "расстояние",
    "расстояния",
    "требования",
}
_SUFFIXES = (
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
    "а",
    "я",
    "ы",
    "и",
    "е",
    "у",
    "ю",
)


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
) -> list[EvidenceItem]:
    items: list[EvidenceItem] = []

    for result in results:
        claim = _claim_text(result)
        if not claim:
            continue

        items.append(
            EvidenceItem(
                claim=claim,
                document=result.filename,
                section=_section(result),
                condition=_condition(result, query),
                value=", ".join(_num_values(result.text, query)),
                interpretation=_interp(result, query),
                supports_intent=_supports(result, query),
                chunk_id=result.id,
            )
        )

    return items


def pick_ans_mode(items: list[EvidenceItem], query: str) -> str:
    direct = [item for item in items if item.supports_intent]
    related = [item for item in items if not item.supports_intent]

    if not direct and related:
        return "partial_supported"
    if not direct:
        return "not_found"
    if _doc_count(direct) > 1 or _is_broad(query):
        return "multi_path"

    return "direct_supported"


def plan_block(plan: list[QueryAspect]) -> str:
    if not plan:
        return "План поиска не сформирован."

    lines = [f"- {item.aspect}: {item.query}" for item in plan]

    return "\n".join(lines)


def evidence_block(items: list[EvidenceItem], mode: str) -> str:
    if not items:
        return "Evidence не извлечены."

    lines = [f"Выбранная стратегия ответа: {mode}."]
    for index, item in enumerate(items[:16], start=1):
        lines.append(_evidence_line(index, item))

    return "\n".join(lines)


def _add_plan(
    plan: list[QueryAspect],
    seen: set[str],
    query: str,
    aspect: str,
) -> None:
    normalized = " ".join(query.split()).lower()
    if not normalized or normalized in seen:
        return

    seen.add(normalized)
    plan.append(QueryAspect(query=query.strip(), aspect=aspect))


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
    support = "direct" if item.supports_intent else "related"
    value = f"; значения: {item.value}" if item.value else ""
    section = item.section or "—"

    return (
        f"{index}. [{support}] {item.document}, {section}: "
        f"{item.claim}{value} (chunk_id={item.chunk_id})"
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
    terms = _term_set(query)
    if not segments or not terms:
        return segments[:4]

    matched = [
        segment
        for segment in segments
        if any(_term_hit(term, segment.lower()) for term in terms)
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


def _supports(result: RetrievalResult, query: str) -> bool:
    text = _res_text(result)
    if _has_shift(query, text):
        return False

    terms = _term_set(query)
    if not terms:
        return True

    covered = [term for term in terms if _term_hit(term, text)]

    return len(covered) / len(terms) >= 0.5


def _has_shift(query: str, text: str) -> bool:
    query = query.lower()
    text = text.lower()

    if "шв" in query and "деформац" in text and "деформац" not in query:
        return True

    return False


def _term_set(text: str) -> set[str]:
    terms: set[str] = set()

    for raw in _WORD_RE.findall(text.lower()):
        term = raw.strip(".-")
        key = _term_key(term)
        if _term_ok(term, key):
            terms.add(key)

    return terms


def _term_ok(term: str, key: str) -> bool:
    if not key or term in _STOP:
        return False

    return any(ch.isdigit() for ch in term) or len(key) >= 4


def _term_key(term: str) -> str:
    if any(ch.isdigit() for ch in term):
        return term

    for suffix in _SUFFIXES:
        if len(term) > len(suffix) + 3 and term.endswith(suffix):
            return term[: -len(suffix)]

    return term


def _term_hit(term: str, text: str) -> bool:
    if term in text:
        return True

    if len(term) < 5:
        return False

    return term[:5] in text


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


def _interp(result: RetrievalResult, query: str) -> str:
    if _supports(result, query):
        return "direct"

    return "related"


def _section(result: RetrievalResult) -> str:
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


def _is_broad(query: str) -> bool:
    lowered = query.lower()
    markers = ("как", "какие", "нужно", "можно", "требован", "норм", "услов")

    return any(marker in lowered for marker in markers)
