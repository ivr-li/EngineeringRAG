import hashlib
import html
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from html.parser import HTMLParser

_CAPTION_RE = re.compile(r"^\s*(Таблица\s+\d+(?:\.\d+)?[^\n]*)\s*$", re.IGNORECASE)
_FRAGMENT_RE = re.compile(r"фрагмент\s+таблицы|част[ьи]\s+\d+", re.IGNORECASE)
_PAGE_RE = re.compile(r"Страница\s+\d+", re.IGNORECASE)
_TABLE_RE = re.compile(r"<table\b.*?</table>", re.IGNORECASE | re.DOTALL)
_TABLE_ID_RE = re.compile(r"TABLE_ID\s*=\s*([a-z0-9_\-]+)", re.IGNORECASE)
_TABLE_PART_RE = re.compile(r"TABLE_PART\s*=\s*(\d+)\s*/\s*(\d+)", re.IGNORECASE)
_TABLE_WINDOW_RE = re.compile(r"TABLE_WINDOW\s*=\s*(\d+)\s*/\s*(\d+)", re.IGNORECASE)
_TABLE_CAPTION_RE = re.compile(r"TABLE_CAPTION\s*=\s*([^;|\n<]+)", re.IGNORECASE)
_TABLE_ORIENTATION_RE = re.compile(
    r"TABLE_ORIENTATION\s*=\s*([a-z0-9_\-]+)",
    re.IGNORECASE,
)
_PRE_DOCLING_TABLE_MAX_TOKENS = 340


@dataclass
class TableGroup:
    caption: str | None
    fragments: list[str] = field(default_factory=list)


class HTMLTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._cell is not None and self._row is not None:
            self._row.append(_clean_cell("".join(self._cell)))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if any(cell for cell in self._row):
                self.rows.append(self._row)
            self._row = None


def repair_split_tables(markdown: str) -> str:
    output: list[str] = []
    pending_caption: str | None = None
    group: TableGroup | None = None
    table_number = 0

    for kind, value in _iter_markdown_units(markdown):
        if kind == "table":
            if group is None:
                table_number += 1
                group = TableGroup(caption=pending_caption)
                pending_caption = None
            group.fragments.append(value)
            continue

        group, pending_caption = _handle_text_unit(
            value, group, pending_caption, output, table_number
        )

    _flush_group(output, group, table_number)
    if pending_caption:
        output.append(f"{pending_caption}\n")
    return "".join(output)


def expand_tables_for_docling(
    markdown: str,
    max_tokens: int = _PRE_DOCLING_TABLE_MAX_TOKENS,
) -> str:
    return _TABLE_RE.sub(
        lambda match: "\n\n".join(_table_html_to_text_windows(match.group(0), max_tokens)),
        markdown,
    )


def html_table_to_rows(table_html: str) -> list[list[str]]:
    parser = HTMLTableParser()
    parser.feed(table_html)
    return parser.rows


def rows_to_html(rows: list[list[str]], attrs: dict[str, str] | None = None) -> str:
    attr_text = _format_attrs(attrs or {})
    body = "".join(_row_to_html(row) for row in rows)
    return f"<table{attr_text}>{body}</table>"


def extract_table_metadata(text: str) -> dict[str, object]:
    text = html.unescape(text)
    result: dict[str, object] = {}
    _extract_match(result, "table_id", _TABLE_ID_RE, text)
    _extract_match(result, "table_caption", _TABLE_CAPTION_RE, text)
    _extract_match(result, "table_orientation", _TABLE_ORIENTATION_RE, text)

    part = _TABLE_PART_RE.search(text)
    if part:
        result["table_part_index"] = int(part.group(1))
        result["table_part_total"] = int(part.group(2))
    window = _TABLE_WINDOW_RE.search(text)
    if window:
        result["table_window_index"] = int(window.group(1))
        result["table_window_total"] = int(window.group(2))
    return result


def split_html_table_text(
    text: str,
    max_tokens: int,
    token_counter: Callable[[str], int],
) -> list[str]:
    match = _TABLE_RE.search(text)
    if not match:
        return []

    prefix = text[: match.start()].strip()
    suffix = text[match.end() :].strip()
    rows = html_table_to_rows(match.group(0))

    if len(rows) < 2:
        return []

    header_rows, data_rows = _split_header_rows(rows)
    return _build_html_table_windows(
        prefix, header_rows, data_rows, suffix, max_tokens, token_counter
    )


def _table_html_to_text_windows(table_html: str, max_tokens: int) -> list[str]:
    rows = html_table_to_rows(table_html)
    if not rows:
        return [table_html]
    metadata = extract_table_metadata(_rows_to_text(rows[:1]))
    header_rows, data_rows = _split_header_rows(rows)
    header_rows = _drop_metadata_rows(header_rows)
    data_windows = _window_table_rows(metadata, header_rows, data_rows, max_tokens)
    total = len(data_windows)
    return [
        _format_docling_table_window(metadata, header_rows, rows, index, total)
        for index, rows in enumerate(data_windows, start=1)
    ]


def _window_table_rows(
    metadata: dict[str, object],
    header_rows: list[list[str]],
    data_rows: list[list[str]],
    max_tokens: int,
) -> list[list[list[str]]]:
    windows: list[list[list[str]]] = []
    current: list[list[str]] = []
    for row in data_rows:
        candidate = _format_docling_table_window(metadata, header_rows, current + [row], 1, 1)
        if current and _estimate_tokens(candidate) > max_tokens:
            windows.append(current)
            current = [row]
        else:
            current.append(row)
    if current:
        windows.append(current)
    return windows or [[]]


def _format_docling_table_window(
    metadata: dict[str, object],
    header_rows: list[list[str]],
    data_rows: list[list[str]],
    window_index: int,
    window_total: int,
) -> str:
    blocks = [
        "[TABLE_BEGIN]",
        _metadata_text(metadata, window_index, window_total),
        "Заголовки таблицы:",
        _rows_to_text(header_rows),
        "Строки таблицы:",
        _rows_to_text(data_rows),
        "[TABLE_END]",
    ]
    return "\n".join(block for block in blocks if block).strip()


def _metadata_text(
    metadata: dict[str, object],
    window_index: int,
    window_total: int,
) -> str:
    return (
        f"TABLE_ID={metadata.get('table_id', '')}; "
        f"TABLE_CAPTION={metadata.get('table_caption', '')}; "
        f"TABLE_PART={metadata.get('table_part_index', 1)}/{metadata.get('table_part_total', 1)}; "
        f"TABLE_WINDOW={window_index}/{window_total}; "
        f"TABLE_ORIENTATION={metadata.get('table_orientation', '')};"
    )


def _iter_markdown_units(markdown: str) -> Iterator[tuple[str, str]]:
    lines = markdown.splitlines(keepends=True)
    i = 0
    while i < len(lines):
        if "<table" not in lines[i].lower():
            yield "text", lines[i]
            i += 1
            continue
        block = [lines[i]]
        i += 1
        while i < len(lines) and "</table>" not in block[-1].lower():
            block.append(lines[i])
            i += 1
        yield "table", "".join(block)


def _handle_text_unit(
    text: str,
    group: TableGroup | None,
    caption: str | None,
    output: list[str],
    table_number: int,
) -> tuple[TableGroup | None, str | None]:
    found_caption = _caption_from_line(text)
    if found_caption:
        _flush_group(output, group, table_number)
        return None, found_caption

    if group and _is_table_separator(text):
        return group, caption

    if caption and _is_table_separator(text):
        return group, caption

    _flush_group(output, group, table_number)
    if caption:
        output.append(f"{caption}\n")

    output.append(text)
    return None, None


def _flush_group(
    output: list[str],
    group: TableGroup | None,
    table_number: int,
) -> None:
    if group is None:
        return
    output.extend(_format_table_group(group, table_number))


def _format_table_group(group: TableGroup, table_number: int) -> list[str]:
    table_id = _make_table_id(group.caption, table_number)
    tables = _merge_vertical_fragments(group.fragments)
    total = len(tables)
    result = [f"{group.caption}\n"] if group.caption else []

    for index, rows in enumerate(tables, start=1):
        orientation = "vertical_merged" if len(group.fragments) > total else "part"
        result.append(
            _annotated_table(rows, table_id, group.caption, index, total, orientation)
        )
        result.append("\n\n")
    return result


def _merge_vertical_fragments(html_fragments: list[str]) -> list[list[list[str]]]:
    tables: list[list[list[str]]] = []
    for html_fragment in html_fragments:
        rows = html_table_to_rows(html_fragment)
        if not rows:
            continue
        if tables and _can_merge_vertically(tables[-1], rows):
            tables[-1].extend(_drop_repeated_header(tables[-1], rows))
        else:
            tables.append(rows)
    return tables


def _annotated_table(
    rows: list[list[str]],
    table_id: str,
    caption: str | None,
    part_index: int,
    part_total: int,
    orientation: str,
) -> str:
    meta = _metadata_row(table_id, caption, part_index, part_total, orientation)
    attrs = {"data-table-id": table_id, "data-table-part": str(part_index)}
    return rows_to_html([meta] + rows, attrs)


def _metadata_row(
    table_id: str,
    caption: str | None,
    part_index: int,
    part_total: int,
    orientation: str,
) -> list[str]:
    caption_text = caption or ""
    return [
        f"TABLE_ID={table_id} | TABLE_PART={part_index}/{part_total} | "
        f"TABLE_CAPTION={caption_text} | TABLE_ORIENTATION={orientation}"
    ]


def _build_html_table_windows(
    prefix: str,
    header_rows: list[list[str]],
    data_rows: list[list[str]],
    suffix: str,
    max_tokens: int,
    token_counter: Callable[[str], int],
) -> list[str]:
    parts: list[list[list[str]]] = []
    current: list[list[str]] = []
    for row in data_rows:
        candidate = _format_table_text(prefix, header_rows, current + [row], suffix)
        if current and token_counter(candidate) > max_tokens:
            parts.append(current)
            current = [row]
        else:
            current.append(row)
    if current:
        parts.append(current)
    return [_format_table_text(prefix, header_rows, rows, suffix) for rows in parts]


def _format_table_text(
    prefix: str,
    header_rows: list[list[str]],
    data_rows: list[list[str]],
    suffix: str,
) -> str:
    blocks = [prefix, "Заголовки таблицы:", _rows_to_text(header_rows)]
    blocks.extend(["Строки таблицы:", _rows_to_text(data_rows), suffix])
    return "\n".join(block for block in blocks if block).strip()


def _split_header_rows(rows: list[list[str]]) -> tuple[list[list[str]], list[list[str]]]:
    meta_rows = 1 if rows and _has_table_meta(rows[0]) else 0
    header_count = min(len(rows), meta_rows + 4)
    if header_count == len(rows):
        header_count = max(1, len(rows) - 1)
    return rows[:header_count], rows[header_count:]


def _can_merge_vertically(base: list[list[str]], rows: list[list[str]]) -> bool:
    base_width = _dominant_width(base)
    rows_width = _dominant_width(rows)
    return base_width > 0 and base_width == rows_width


def _drop_repeated_header(
    base: list[list[str]], rows: list[list[str]]
) -> list[list[str]]:
    base_heads = {_normalise_row(row) for row in base[:5]}
    index = 0
    while index < min(5, len(rows)) and _normalise_row(rows[index]) in base_heads:
        index += 1
    return rows[index:]


def _dominant_width(rows: list[list[str]]) -> int:
    widths = [len(row) for row in rows if row]
    return max(set(widths), key=widths.count) if widths else 0


def _rows_to_text(rows: list[list[str]]) -> str:
    return "\n".join(" | ".join(cell for cell in row if cell) for row in rows)


def _drop_metadata_rows(rows: list[list[str]]) -> list[list[str]]:
    return [row for row in rows if not _has_table_meta(row)]


def _estimate_tokens(text: str) -> int:
    return int(len(text.split()) * 1.6)


def _caption_from_line(line: str) -> str | None:
    match = _CAPTION_RE.match(line.strip())
    return match.group(1).strip() if match else None


def _is_table_separator(line: str) -> bool:
    text = line.strip()
    return not text or bool(_PAGE_RE.search(text) or _FRAGMENT_RE.search(text))


def _make_table_id(caption: str | None, table_number: int) -> str:
    source = caption or f"table-{table_number}"
    digest = hashlib.sha1(source.encode("utf-8")).hexdigest()[:10]
    number = re.search(r"\d+(?:\.\d+)*", source)
    if not number:
        return f"table_{table_number}_{digest}"
    return f"table_{number.group(0).replace('.', '_')}_{digest}"


def _row_to_html(row: list[str]) -> str:
    cells = "".join(f"<td>{html.escape(cell)}</td>" for cell in row)
    return f"<tr>{cells}</tr>"


def _format_attrs(attrs: dict[str, str]) -> str:
    return "".join(
        f' {key}="{html.escape(value, quote=True)}"' for key, value in attrs.items()
    )


def _clean_cell(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _normalise_row(row: list[str]) -> tuple[str, ...]:
    return tuple(_clean_cell(cell).lower() for cell in row)


def _has_table_meta(row: list[str]) -> bool:
    return any(_TABLE_ID_RE.search(cell) for cell in row)


def _extract_match(
    target: dict[str, object],
    key: str,
    pattern: re.Pattern[str],
    text: str,
) -> None:
    match = pattern.search(text)
    if match:
        target[key] = match.group(1).strip()
