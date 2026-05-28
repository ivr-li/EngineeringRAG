import logging
import re
from functools import lru_cache

from transformers import AutoTokenizer

# BGE-M3 hard limit is 8192 tokens, but for retrieval quality 380 is optimal.
# Overlap of 2 sentences ≈ 40–60 tokens re-indexed per split boundary.
TARGET_TOKENS: int = 380  # soft target for merged / split chunks
MAX_TOKENS: int = 420  # hard ceiling before forced split
MIN_WORDS: int = 8  # noise filter: too-short chunks
MIN_WORDS_MERGE: int = 15  # merge gate: don't keep buffer below this
OVERLAP_SENTENCES: int = 2  # sentences carried over to next window chunk

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

_CROSS_PATTERNS: list[str] = [
    r"[Тт]абл(?:иц[аеу]|(?:иц)?\.)\s*\d+(?:\.\d+)*(?:\s*,\s*\d+(?:\.\d+)*)*\b",
    r"[пП]\.\s*\d+(?:\.\d+)*(?:\.?[дД])?",
    # r"[Рр]ис(?:унок|\.)\s*\d+(?:\.\d+)*",  # рисунок 3
    r"[Пп]риложени[еяй]\s*[А-ЯA-Z\d]+",
]
_TOKENIZER = AutoTokenizer.from_pretrained("BAAI/bge-m3")


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
    chunk["section_level"] = len(headings)
    chunk["section_path"] = (
        " > ".join(headings[-2:])
        if len(headings) >= 2
        else (headings[0] if headings else "")
    )
    chunk["parent_heading"] = headings[-2] if len(headings) > 1 else None
    chunk["leaf_heading"] = headings[-1] if headings else None
    return chunk


def _split_with_overlap(
    chunks: list[dict],
    max_tokens: int = MAX_TOKENS,
    overlap_sentences: int = OVERLAP_SENTENCES,
) -> list[dict]:
    """
    Split any chunk that exceeds *max_tokens* into smaller windows,
    carrying *overlap_sentences* from the end of one window into the
    start of the next.

    NEW-1: This step runs *after* merge_by_section.  Merged sections that
    are too large for BGE-M3's effective context are split here rather than
    at the merge stage, so that the merge phase can still pack as much
    coherent content as possible.

    Tables (is_table=True) are never split — they are passed through as-is
    regardless of size, because splitting a table destroys its semantics.

    The ``is_overlap`` flag in payload marks sentences that were copied
    from the previous chunk.  This is useful for deduplication at query
    time (MMR / diversity re-ranking).

    Algorithm
    ---------
    1. Split text into sentences on [.!?] boundaries.
    2. Walk sentences, accumulating a window until adding the next
       sentence would exceed max_tokens.
    3. Emit the window as a new chunk.
    4. Seed the next window with the last *overlap_sentences* sentences.
    """
    result: list[dict] = []

    # Simple Russian-aware sentence splitter
    _SENT_RE = re.compile(r"(?<=[.!?])\s+")

    for chunk in chunks:
        # Tables pass through unchanged
        if chunk.get("is_table"):
            result.append(chunk)
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
            result.append(new_chunk)

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
    return text.strip()


def _extract_mandatory_refs(text: str) -> list[str]:
    """
    Extract all mandatoryreferences from chunk text
    По обязательным будет вестиcь углубленный поиск для углубдения контекста
    """
    refs: list[str] = []
    for pat in _MANDATORY_PATTERNS + _CROSS_PATTERNS:
        refs.extend(re.findall(pat, text))
    return list(set(refs))


def _extract_cross_refs(text: str) -> list[str]:
    """Extract cross-references (tables, paragraphs, appendices) (СП, ГОСТ, СНиП, СанПиН)"""
    refs: list[str] = []
    for pat in _CROSS_PATTERNS:
        refs.extend(re.findall(pat, text))
    return list(set(refs))


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
    for c in chunks:
        c["text"] = _clean_text(c.get("text", ""), c.get("headings", []))
        c["man_refs"] = _extract_mandatory_refs(c["text"])
        c["cross_refs"] = _extract_cross_refs(c["text"])
        c["headings"] = c.get("headings", [])
        c["doc_items"] = c.get("doc_items", [])
    # Step 3
    chunks = _merge_by_section(chunks)
    # Step 4
    chunks = _split_with_overlap(chunks)
    # Step 5
    chunks = [c for c in chunks if not _is_noise(c)]
    # Step 6
    chunks = [_enrich_metadata(c) for c in chunks]
    # Step 7
    for idx, c in enumerate(chunks):
        c["chunk_index"] = idx
        c["num_tokens"] = _count_tokens(c.get("text", ""))

    logging.info(
        f"⨠⨠⨠ChunkCleaner.process: {before} → {len(chunks)} chunks "
        f"({before - len(chunks)} removed/merged/split)"
    )
    return chunks
