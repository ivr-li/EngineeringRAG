import logging
import re
from collections.abc import Iterable
from functools import lru_cache

from common.txt_feature.table_repair import (
    extract_table_metadata,
    split_html_table_text,
)

# BGE-M3 hard limit is 8192 tokens, but for retrieval quality 380 is optimal.
# Overlap of 2 sentences ≈ 40–60 tokens re-indexed per split boundary.
TARGET_TOKENS: int = 380  # soft target for merged / split chunks
MAX_TOKENS: int = 420  # hard ceiling before forced split
MIN_WORDS: int = 8  # noise filter: too-short chunks
MIN_WORDS_MERGE: int = 15  # merge gate: don't keep buffer below this
OVERLAP_SENTENCES: int = 2  # sentences carried over to next window chunk

_EXCLUDED_SECTION = re.compile(
    r"исключен|утратил\s+силу|не\s+применяется",
    re.IGNORECASE,
)

_NOISE_HEADINGS = re.compile(
    r"сведения о (стандарте|своде правил|нормативном документе|документе)|"
    r"предисловие|foreword|"
    r"библиография|bibliography|"
    r"^приложение\s*[а-яёa-z]?$|"
    r"дата введения|"
    r"термины и определения",
    re.IGNORECASE,
)
_FIGURE_CAPTION = re.compile(r"^\d+\s*[-–-]\s+\S+")
_TECHEXPERT_WATERMARKS = [
    r"Внимание!\s*Документ\s*имеет\s*особый\s*порядок\s*вступления\s*в\s*силу\.[^\n]*\n?",
    r"Внимание!\s*Документ\s*включен\s*в\s*доказательную\s*базу\s*технического\s*регламента\.[^\n]*\n?",
    r"Дополнительную\s*информацию\s*см\.\s*в\s*ярлыке\s*[«\"]Примечания[»\"][^\n]*\n?",
    r"ИС\s*«Техэксперт:[^»]*»\s*Интранет[^\n]*\n?",
    r"См\s*ярлык\s*[\"«]Примечания[\"»][^\n]*\n?",
]

_MANDATORY_PATTERNS: list[str] = [
    # СНиП, ГОСТ
    r"[СсГг][НнОо][ИиСс][ПпТт]\s*[\d\.\-]+\.[\d\.]+(?:\s*\((?:пункт|п\.?|таблиц[аеу]|табл\.?)\s*[\d\.]+\))?",
    r"[СсГг][НнОо][ИиСс][ПпТт]\s*[\d\.\-]+",
    # СП — отдельно, иначе CROSS_PATTERNS съедает «П» как п.3.45
    r"(?:(?:пункт[аеу]?|п\.)\s*[\d]+(?:\.[\d]+)*\s+)?СП\s*[\d]+(?:\.[\d]+)*(?:\s*\((?:пункт[аеу]?|п\.?|таблиц[аеу]|табл\.?)\s*[\d\.]+\))?",
    r"СП\s*[\d]+(?:\.[\d]+)*",
    # СанПиН
    r"СанПи[нН]\s*[\d]+(?:\.[\d\-]+)*",
]

_TABLE_REF_RE = re.compile(
    r"\bтабл(?:\.|иц[а-яё]*)?\s*№?\s*"
    r"(?P<numbers>\d+(?:\.\d+)*(?:\s*,\s*\d+(?:\.\d+)*)*)",
    re.IGNORECASE,
)
_SECTION_REF_RE = re.compile(
    r"\b(?:п\.|пункт[а-яё]*|раздел[а-яё]*|подраздел[а-яё]*)\s*№?\s*"
    r"(?P<number>\d+(?:\.\d+)*(?:\.?[дД])?)",
    re.IGNORECASE,
)
_APPENDIX_REF_RE = re.compile(r"\bприложени[еяй]\s*(?P<code>[А-ЯA-Z\d]+)", re.IGNORECASE)
_SECTION_ANCHOR_RE = re.compile(r"(?m)^\s*(?P<number>\d+(?:\.\d+)+)(?=\s)")
_TABLE_ID_NUMBER_RE = re.compile(r"table[_-](?P<number>\d+(?:[_\.]\d+)*)", re.IGNORECASE)
_TABLE_CONTROL_MARKER_RE = re.compile(r"\[/?TABLE_(?:BEGIN|END)\]", re.IGNORECASE)
_TABLE_CONTROL_FIELDS = (
    re.compile(r"\bTABLE_ID\s*=\s*[a-z0-9_\-]+\s*[;|]?\s*", re.IGNORECASE),
    re.compile(r"\bTABLE_PART\s*=\s*\d+\s*/\s*\d+\s*[;|]?\s*", re.IGNORECASE),
    re.compile(r"\bTABLE_WINDOW\s*=\s*\d+\s*/\s*\d+\s*[;|]?\s*", re.IGNORECASE),
    re.compile(r"\bTABLE_ORIENTATION\s*=\s*[a-z0-9_\-]+\s*[;|]?\s*", re.IGNORECASE),
    re.compile(r"\bTABLE_CAPTION\s*=\s*[^;|\n<]*\s*[;|]?\s*", re.IGNORECASE),
)


def attach_table_captions(text: str) -> str:
    """
    Before:
        Таблица 33\n\n| col1 | col2 |
    After:
        | <!-- Таблица 33 --> col1 | col2 |
    """
    text = re.sub(
        r"((?:Таблица|Рисунок)\s+\d+[^\n]*)\n{2,}(\|)",
        r"\1\n\2",
        text,
        flags=re.IGNORECASE,
    )
    return text


@lru_cache(maxsize=1)
def _get_tokenizer():
    try:
        from transformers import AutoTokenizer

        tok = AutoTokenizer.from_pretrained("BAAI/bge-m3")
        logging.info("⨠⨠⨠ChunkCleaner: using BGE-M3 tokenizer for token counting")
        return tok
    except Exception as exc:  # noqa: BLE001
        logging.warning(
            f"⨉⨉⨉ChunkCleaner: transformers not available ({exc}), falling back to words*1.6 heuristic"
        )
        return None


def _normalize(s: str) -> str:
    """Normalise dash variants and non-breaking spaces."""
    return s.replace("–", "-").replace("—", "-").replace("\xa0", " ")


def _enrich_metadata(chunk: dict) -> dict:
    """
    Add hierarchical section metadata derived from the *headings* list.

    NEW-2: Qdrant payload fields added:
        section_level  – depth in the document tree (1 = top section)
        section_path   – last two heading levels joined with ' > '
                        (useful for keyword filtering and BM25 queries)
        parent_heading – heading one level above the current section
        leaf_heading   – the most specific (deepest) heading

    These fields are indexed as KEYWORD in Qdrant, enabling efficient
    filter queries like:
        Filter(must=[FieldCondition("section_path",
                                    MatchText(text="6 Расчёт"))])
    """
    headings = chunk.get("headings", [])
    section_nums = [re.match(r"^[\d\.]+", h) for h in headings]
    chunk["section_id"] = " > ".join(m.group(0) for m in section_nums if m)
    chunk["section_level"] = len(headings)
    chunk["section_path"] = (
        " > ".join(headings[-2:])
        if len(headings) >= 2
        else (headings[0] if headings else "")
    )
    chunk["parent_heading"] = headings[-2] if len(headings) > 1 else None
    chunk["leaf_heading"] = headings[-1] if headings else None
    return chunk


def _split_table_chunk(chunk: dict, max_tokens: int = MAX_TOKENS) -> list[dict]:
    chunk = _enrich_table_metadata(chunk)
    text = chunk.get("text", "")

    html_parts = split_html_table_text(text, max_tokens, _count_tokens)
    if html_parts:
        return _table_parts_from_texts(chunk, html_parts)

    if _count_tokens(text) <= max_tokens:
        return [chunk]

    markdown_parts = _split_markdown_table_text(text, max_tokens)
    if markdown_parts:
        return _table_parts_from_texts(chunk, markdown_parts)

    plain_parts = _split_plain_table_text(text, max_tokens)
    if plain_parts:
        return _table_parts_from_texts(chunk, plain_parts)
    return [chunk]


def _enrich_table_metadata(chunk: dict) -> dict:
    metadata = extract_table_metadata(chunk.get("text", ""))
    if metadata.get("table_id"):
        chunk["is_table"] = True
    for key, value in metadata.items():
        if not chunk.get(key):
            chunk[key] = value
    return chunk


def _table_parts_from_texts(chunk: dict, texts: list[str]) -> list[dict]:
    total = len(texts)
    result: list[dict] = []
    for index, part_text in enumerate(texts):
        new_chunk = _build_table_part(chunk, part_text, index, total)
        result.append(new_chunk)
        logging.debug(
            f"⨠ Table split: part {index + 1}/{total}, "
            f"tokens={_count_tokens(new_chunk['text'])}"
        )
    logging.info(
        f"⨠⨠⨠ Table chunk split into {total} parts "
        f"(original {_count_tokens(chunk['text'])} tokens)"
    )
    return result


def _build_table_part(chunk: dict, part_text: str, index: int, total: int) -> dict:
    labelled_text = _table_window_label(chunk, part_text, index, total)
    new_chunk = {
        **chunk,
        "text": labelled_text,
        "window_index": index,
        "is_overlap_window": False,
        "table_window_index": index + 1,
        "table_window_total": total,
        "doc_items": list(chunk.get("doc_items", [])),
        "man_refs": _extract_mandatory_refs(labelled_text),
        "cross_refs": _extract_cross_refs(labelled_text),
    }
    return _refresh_reference_metadata(_enrich_table_metadata(new_chunk))


def _table_window_label(chunk: dict, text: str, index: int, total: int) -> str:
    caption = chunk.get("table_caption") or chunk.get("leaf_heading") or ""
    table_id = chunk.get("table_id") or ""
    label = f"[ТАБЛИЦА окно {index + 1}/{total}]"
    if caption or table_id:
        label = f"{label} {caption} {table_id}".strip()
    return f"{label}\n{text.strip()}"


def _split_markdown_table_text(text: str, max_tokens: int) -> list[str]:
    header_lines, data_lines = _markdown_table_sections(text.splitlines())
    if not data_lines:
        return []
    row_groups = _window_markdown_rows(header_lines, data_lines, max_tokens)
    return ["\n".join(header_lines + rows) for rows in row_groups]


def _markdown_table_sections(lines: list[str]) -> tuple[list[str], list[str]]:
    sep_re = re.compile(r"^\|[\s\-:|]+\|")
    header: list[str] = []
    data: list[str] = []
    header_done = False
    for line in lines:
        if not header_done:
            header.append(line)
            header_done = bool(sep_re.match(line))
        elif line.strip():
            data.append(line)
    if header_done:
        return header, data
    return lines[:1], lines[1:]


def _window_markdown_rows(
    header_lines: list[str],
    data_lines: list[str],
    max_tokens: int,
) -> list[list[str]]:
    parts: list[list[str]] = []
    current: list[str] = []
    header_text = "\n".join(header_lines)
    for row in data_lines:
        candidate = "\n".join([header_text, *current, row])
        if current and _count_tokens(candidate) > max_tokens:
            parts.append(current)
            current = [row]
        else:
            current.append(row)
    if current:
        parts.append(current)
    return parts


def _split_plain_table_text(text: str, max_tokens: int) -> list[str]:
    header, rows, footer = _plain_table_sections(text.splitlines())
    if not rows:
        return []
    row_groups = _window_plain_rows(header, rows, footer, max_tokens)
    return ["\n".join(header + group + footer) for group in row_groups]


def _plain_table_sections(lines: list[str]) -> tuple[list[str], list[str], list[str]]:
    row_marker = _find_line(lines, "Строки таблицы:")
    end_marker = _find_line(lines, "[TABLE_END]")
    if row_marker is None:
        return [], [], []
    data_end = end_marker if end_marker is not None else len(lines)
    return lines[: row_marker + 1], lines[row_marker + 1 : data_end], lines[data_end:]


def _window_plain_rows(
    header: list[str],
    rows: list[str],
    footer: list[str],
    max_tokens: int,
) -> list[list[str]]:
    parts: list[list[str]] = []
    current: list[str] = []
    for row in rows:
        candidate = "\n".join(header + current + [row] + footer)
        if current and _count_tokens(candidate) > max_tokens:
            parts.append(current)
            current = [row]
        else:
            current.append(row)
    if current:
        parts.append(current)
    return parts


def _find_line(lines: list[str], target: str) -> int | None:
    for index, line in enumerate(lines):
        if line.strip() == target:
            return index
    return None


def _split_with_overlap(
    chunks: list[dict],
    max_tokens: int = MAX_TOKENS,
    overlap_sentences: int = OVERLAP_SENTENCES,
) -> list[dict]:
    """
    Split any chunk that exceeds *max_tokens* into smaller windows,
    carrying *overlap_sentences* from the end of one window into the
    start of the next.

    Tables (is_table=True) are handled by _split_table_chunk instead of
    being passed through unchanged — oversized tables are split row-by-row
    with the header repeated in each part.
    """
    result: list[dict] = []

    # Simple Russian-aware sentence splitter
    _SENT_RE = re.compile(r"(?<=[.!?])\s+")

    for chunk in chunks:
        if chunk.get("is_table"):
            result.extend(_split_table_chunk(chunk, max_tokens))
            continue

        text = chunk.get("text", "")
        if _count_tokens(text) <= max_tokens:
            result.append(chunk)
            continue

        # Sentence-level sliding window
        sentences = _SENT_RE.split(text.strip())
        windows: list[list[str]] = []
        current: list[str] = []
        overlap_flag: list[bool] = []  # True = sentence is overlap from prev window

        for sent in sentences:
            if not sent.strip():
                continue
            sent_toks = _count_tokens(sent)
            cur_toks = _count_tokens(" ".join(current)) if current else 0

            if current and (cur_toks + sent_toks) > max_tokens:
                windows.append((list(current), list(overlap_flag)))
                # Seed next window with tail overlap
                tail = current[-overlap_sentences:] if overlap_sentences else []
                tail_flags = [True] * len(tail)
                current = tail + [sent]
                overlap_flag = tail_flags + [False]
            else:
                current.append(sent)
                overlap_flag.append(False)

        if current:
            windows.append((current, overlap_flag))

        for sub_idx, (sents, flags) in enumerate(windows):
            sub_text = " ".join(sents)
            has_overlap = any(flags)
            new_chunk = {
                **chunk,
                "text": sub_text,
                "is_overlap_window": has_overlap,
                "window_index": sub_idx,
                "doc_items": list(chunk.get("doc_items", [])),
                "refs": list(chunk.get("refs", [])),
                "man_refs": _extract_mandatory_refs(sub_text),
                "cross_refs": _extract_cross_refs(sub_text),
            }
            result.append(_refresh_reference_metadata(new_chunk))

    return result


def _count_tokens(text: str) -> int:
    """
    Return the number of subword tokens for *text* using the BGE-M3
    tokenizer.  Falls back to ``words * 1.6`` when the tokenizer is
    unavailable (e.g. in lightweight test environments).
    """
    tok = _get_tokenizer()
    if tok is not None:
        return len(tok.encode(text, add_special_tokens=False))
    return int(len(text.split()) * 1.6)


def strip_watermarks(text: str) -> str:
    for pat in _TECHEXPERT_WATERMARKS:
        text = re.sub(pat, "", text, flags=re.IGNORECASE)

    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _clean_text(text: str, headings: list[str]) -> str:
    """
    Remove common OCR artefacts from chunk text before vectorisation.

    Normalises excess whitespace, collapses repeated newlines, fixes
    hyphenated number ranges, and strips repeated pipe characters.
    """
    if not text:
        return text

    text = _normalize(text)
    for pat in _TECHEXPERT_WATERMARKS:
        text = re.sub(pat, "", text, flags=re.IGNORECASE)

    if headings:
        for h in headings:
            nh = re.escape(_normalize(h))
            text = re.sub(rf"^{nh}\s*\n?", "", text, flags=re.MULTILINE)

    text = "\n".join(line.strip() for line in text.splitlines() if line.strip())
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"(\d)\s*[-–]\s*(\d)", r"\1–\2", text)
    text = re.sub(r"[|]{2,}", "", text)
    text = re.sub(r"\bгНС\b|\bгнС\b|\bгНс\b", "ГНС", text)
    text = re.sub(r"\b([мМ])з\b", r"\1³", text)
    text = re.sub(r"\bмЗ\b", "м³", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<=\|)\s*т\s*(?=\|)", " — ", text)
    return text.strip()


def _strip_table_control_text(text: str) -> str:
    text = _TABLE_CONTROL_MARKER_RE.sub("", text)
    for pattern in _TABLE_CONTROL_FIELDS:
        text = pattern.sub("", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip(" ;|\n")


def _extract_mandatory_refs(text: str) -> list[str]:
    refs: list[str] = []
    for pat in _MANDATORY_PATTERNS:
        refs.extend(re.findall(pat, text))
    return _unique_refs(ref.strip() for ref in refs if ref.strip())


def _extract_cross_refs(text: str) -> list[str]:
    """Extract internal document references as stable keys."""
    refs: list[str] = []
    refs.extend(_extract_table_refs(text))
    refs.extend(_extract_section_refs(text))
    refs.extend(_extract_appendix_refs(text))
    return _unique_refs(refs)


def _refresh_reference_metadata(chunk: dict) -> dict:
    text = chunk.get("text", "")
    anchor_refs = _extract_anchor_refs(chunk)
    cross_refs = [
        ref for ref in _extract_cross_refs(text) if ref not in set(anchor_refs)
    ]
    chunk["anchor_refs"] = anchor_refs
    chunk["cross_refs"] = _unique_refs(cross_refs)
    chunk["man_refs"] = _extract_mandatory_refs(text)
    chunk["refs"] = _unique_refs(chunk["man_refs"] + chunk["cross_refs"])
    return chunk


def _extract_anchor_refs(chunk: dict) -> list[str]:
    refs: list[str] = []
    refs.extend(_extract_table_anchor_refs(chunk))
    refs.extend(_extract_section_anchor_refs(chunk))
    return _unique_refs(refs)


def _extract_table_anchor_refs(chunk: dict) -> list[str]:
    if not (chunk.get("is_table") or chunk.get("table_id") or chunk.get("table_caption")):
        return []
    refs = _extract_table_refs(str(chunk.get("table_caption") or ""))
    table_id_ref = _table_ref_from_id(str(chunk.get("table_id") or ""))
    if table_id_ref:
        refs.append(table_id_ref)
    return _unique_refs(refs)


def _extract_section_anchor_refs(chunk: dict) -> list[str]:
    refs: list[str] = []
    refs.extend(_section_refs_from_section_id(str(chunk.get("section_id") or "")))
    refs.extend(_section_refs_from_text_starts(chunk.get("text", "")))
    return _unique_refs(refs)


def _extract_table_refs(text: str) -> list[str]:
    refs: list[str] = []
    for match in _TABLE_REF_RE.finditer(text):
        refs.extend(f"table:{number}" for number in _split_ref_numbers(match))
    return refs


def _extract_section_refs(text: str) -> list[str]:
    return [
        f"section:{_normalize_ref_number(match.group('number'))}"
        for match in _SECTION_REF_RE.finditer(text)
    ]


def _extract_appendix_refs(text: str) -> list[str]:
    return [
        f"appendix:{match.group('code').upper()}"
        for match in _APPENDIX_REF_RE.finditer(text)
    ]


def _split_ref_numbers(match: re.Match[str]) -> list[str]:
    raw_numbers = re.split(r"\s*,\s*", match.group("numbers"))
    return [_normalize_ref_number(number) for number in raw_numbers if number.strip()]


def _section_refs_from_section_id(section_id: str) -> list[str]:
    refs = re.findall(r"\d+(?:\.\d+)*", section_id)
    return [f"section:{_normalize_ref_number(ref)}" for ref in refs]


def _section_refs_from_text_starts(text: str) -> list[str]:
    return [
        f"section:{_normalize_ref_number(match.group('number'))}"
        for match in _SECTION_ANCHOR_RE.finditer(text)
    ]


def _table_ref_from_id(table_id: str) -> str | None:
    match = _TABLE_ID_NUMBER_RE.search(table_id)
    if not match:
        return None
    number = match.group("number").replace("_", ".")
    return f"table:{_normalize_ref_number(number)}"


def _normalize_ref_number(number: str) -> str:
    return number.strip().replace(" ", "").replace("Д", "д").rstrip(".")


def _unique_refs(refs: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(ref for ref in refs if ref))


def _is_noise(chunk: dict) -> bool:
    """
    True if the chunk is garbage.

    Criteria:
    1. Too short (< MIN_WORDS words after removing the heading-prefix)
    2. Header section (preface, bibliography, etc.)
    3. Formula fragment: >60% of tokens are Latin/Cyrillic
       variables with a length of ≤2 characters (E s 0, R s w)
    """
    headings = chunk.get("headings", [])
    text = chunk.get("text", "")
    words = text.split()

    if chunk.get("is_table"):
        return False

    if len(words) < MIN_WORDS:
        return True

    for h in headings:
        if _NOISE_HEADINGS.search(h):
            return True
        if _FIGURE_CAPTION.match(h):
            return True

    alpha_words = [re.sub(r"[^а-яёa-z]", "", w.lower()) for w in words]
    short = sum(1 for w in alpha_words if len(w) <= 2)
    if words and short / len(words) > 0.6:
        return True

    leaf = chunk.get("leaf_heading", "") or ""
    if _EXCLUDED_SECTION.search(leaf):
        return True
    return False


def _merge_by_section(
    chunks: list[dict],
    max_tokens: int = MAX_TOKENS,
    min_words: int = MIN_WORDS_MERGE,
) -> list[dict]:
    """
    Объединяет соседние чанки одного раздела (одинаковые headings)
    пока суммарный размер < max_tokens токенов.

    Таблицы (is_table=True) никогда не мержатся - идут отдельно.
    """
    result: list[dict] = []
    buffer: dict | None = None

    for chunk in chunks:
        if chunk.get("is_table"):
            if buffer is not None:
                result.append(buffer)
                buffer = None
            result.append(chunk)
            continue

        if buffer is None:
            buffer = {**chunk, "doc_items": list(chunk.get("doc_items", []))}
            continue

        same_section = _normalize(str(buffer.get("headings", []))) == _normalize(
            str(chunk.get("headings", []))
        )
        buf_tokens = _count_tokens(buffer.get("text", ""))
        new_tokens = _count_tokens(chunk.get("text", ""))

        if same_section and (buf_tokens + new_tokens) < max_tokens:
            buffer["text"] += "\n" + chunk.get("text", "")
            buffer["doc_items"].extend(chunk.get("doc_items", []))
            buffer["refs"] = list(set(buffer.get("refs", []) + chunk.get("refs", [])))
        else:
            if len(buffer.get("text", "").split()) >= min_words:
                result.append(buffer)
            buffer = {
                **chunk,
                "doc_items": list(chunk.get("doc_items", [])),
                "refs": list(chunk.get("refs", [])),
                "man_refs": list(chunk.get("man_refs", [])),
                "cross_refs": list(chunk.get("cross_refs", [])),
            }

    if buffer and len(buffer.get("text", "").split()) >= min_words:
        result.append(buffer)
    return result


def _prepare_chunk(chunk: dict) -> dict:
    chunk["text"] = _clean_text(chunk.get("text", ""), chunk.get("headings", []))
    _enrich_table_metadata(chunk)
    if chunk.get("is_table"):
        chunk["text"] = _strip_table_control_text(chunk.get("text", ""))
    chunk["headings"] = chunk.get("headings", [])
    chunk["doc_items"] = chunk.get("doc_items", [])
    return _refresh_reference_metadata(chunk)


def _finalize_chunk(chunk: dict, index: int) -> dict:
    _refresh_reference_metadata(chunk)
    chunk["chunk_index"] = index
    chunk["num_tokens"] = _count_tokens(chunk.get("text", ""))
    return chunk


def process_chunks(chunks: list[dict]) -> list[dict]:
    """
    A pipeline for cleaning chanks by a single document.

    Steps
    -----
    1. clean_text      – strip OCR artefacts
    2. extract_refs    – (re)compute man_refs / cross_refs
    3. merge_by_section – pack micro-chunks within section budget
    4. split_with_overlap – break oversized chunks with sentence overlap
    5. enrich_metadata – add section_path / section_level / etc.
    6. filter noise    – drop garbage (tables are immune)
    7. reindex         – recalculate chunk_index / num_tokens

    Returns
    -------
    list[dict]
        A cleaned list of chunks ready for indexing.
    """
    before = len(chunks)

    # Step 1 - 2
    chunks = [_prepare_chunk(c) for c in chunks]
    # Step 3
    chunks = _merge_by_section(chunks)
    # Step 4
    chunks = _split_with_overlap(chunks)
    # Step 5
    chunks = [_enrich_metadata(c) for c in chunks]
    # Step 6
    chunks = [c for c in chunks if not _is_noise(c)]
    # Step 7
    chunks = [_finalize_chunk(c, idx) for idx, c in enumerate(chunks)]

    logging.info(
        f"⨠⨠⨠ChunkCleaner.process: {before} → {len(chunks)} chunks "
        f"({before - len(chunks)} removed/merged/split)"
    )
    return chunks
