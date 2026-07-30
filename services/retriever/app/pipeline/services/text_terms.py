from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

WORD_RE = re.compile(r"[а-яёa-z0-9][а-яёa-z0-9.\-]*", re.IGNORECASE)
DEFAULT_STOP_TERMS = frozenset(
    {
        "какие",
        "какой",
        "какая",
        "какое",
        "какого",
        "каких",
        "каким",
        "какими",
        "между",
        "можно",
        "нужно",
        "нужны",
        "принимать",
        "должен",
        "должна",
        "должно",
        "должны",
        "предъявляется",
        "предъявляются",
        "предъявлять",
    }
)
SUFFIXES = (
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


@dataclass(frozen=True)
class TextTerm:
    raw: str
    key: str


def query_terms(text: str) -> set[str]:
    return term_set(text, min_len=4)


def expansion_terms(text: str) -> set[str]:
    return term_set(text, min_len=5)


def term_set(text: str, min_len: int = 4) -> set[str]:
    return {term.key for term in term_items(text, min_len=min_len)}


def term_items(text: str, min_len: int = 4) -> tuple[TextTerm, ...]:
    result: list[TextTerm] = []
    seen: set[str] = set()

    for raw in _words(text):
        key = term_key(raw)
        if key not in seen and _keep_term(raw, key, min_len):
            result.append(TextTerm(raw=raw, key=key))
            seen.add(key)

    return tuple(result)


def term_keys(text: str, min_len: int = 4) -> set[str]:
    return {term_key(raw) for raw in _words(text)}


def term_key(term: str) -> str:
    if any(ch.isdigit() for ch in term):
        return term

    for suffix in SUFFIXES:
        if len(term) > len(suffix) + 2 and term.endswith(suffix):
            return term[: -len(suffix)]

    return term


def term_hit(term: str, text: str) -> bool:
    if term in text:
        return True

    return len(term) >= 5 and term[:5] in text


def term_coverage(terms: Iterable[str], text: str) -> float:
    terms = tuple(terms)
    if not terms:
        return 0.0

    target = text.lower()
    hits = sum(term_hit(term, target) for term in terms)

    return hits / len(terms)


def term_covered(key: str, context_keys: set[str]) -> bool:
    if key in context_keys:
        return True

    return len(key) >= 5 and any(term.startswith(key[:5]) for term in context_keys)


def _words(text: str) -> tuple[str, ...]:
    return tuple(raw.strip(".-") for raw in WORD_RE.findall(text.lower()))


def _keep_term(term: str, key: str, min_len: int) -> bool:
    if not key or term in DEFAULT_STOP_TERMS:
        return False

    return any(ch.isdigit() for ch in term) or len(key) >= min_len or _is_short_stem(
        term, key
    )


def _is_short_stem(term: str, key: str) -> bool:
    return len(key) == 3 and len(term) >= 4 and bool(re.search(r"[а-яё]", term))
