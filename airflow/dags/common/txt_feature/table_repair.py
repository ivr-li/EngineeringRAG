import hashlib
import html
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from html.parser import HTMLParser

_CAPTION_RE = re.compile(
    r"\b(Таблица\s+(?:[А-ЯA-Z]\.?\d+(?:\.\d+)*|\d+(?:\.\d+)*)"
    r"(?:\s*[-–—]\s*[^\n]+)?)",
    re.IGNORECASE,
)
_FRAGMENT_RE = re.compile(r"фрагмент\s+таблицы|част[ьи]\s+\d+", re.IGNORECASE)
_PAGE_RE = re.compile(r"Страница\s+\d+", re.IGNORECASE)
_TABLE_BOILERPLATE_RE = re.compile(
    r"ИС\s+«Техэксперт|Внимание!|Примечание\s+изготовителя|"
    r"^\*Вероятно,\s+ошибка\s+оригинала",
    re.IGNORECASE,
)
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
        self._cell_colspan = 1
        self._cell_rowspan = 1
        self._col_index = 0
        self._spans: dict[int, tuple[int, str]] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._row = []
            self._col_index = 0
        elif tag in {"td", "th"} and self._row is not None:
            self._fill_spans_to_next_cell()
            self._cell = []
            self._cell_colspan = _span_attr(attrs, "colspan")
            self._cell_rowspan = _span_attr(attrs, "rowspan")

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._cell is not None and self._row is not None:
            self._append_cell(_clean_cell("".join(self._cell)))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self._fill_spans_to_row_end()
            if any(cell for cell in self._row):
                self.rows.append(self._row)
            self._row = None

    def _append_cell(self, value: str) -> None:
        start = self._col_index
        for _ in range(self._cell_colspan):
            self._row.append(value)
            self._col_index += 1

        if self._cell_rowspan > 1:
            for col in range(start, start + self._cell_colspan):
                self._spans[col] = (self._cell_rowspan - 1, value)

    def _fill_spans_to_next_cell(self) -> None:
        while self._col_index in self._spans:
            self._append_span_cell(self._col_index)

    def _fill_spans_to_row_end(self) -> None:
        while self._spans and self._col_index <= max(self._spans):
            if self._col_index in self._spans:
                self._append_span_cell(self._col_index)
            else:
                self._row.append("")
                self._col_index += 1

    def _append_span_cell(self, col: int) -> None:
        remaining, value = self._spans[col]
        self._row.append(value)
        self._col_index += 1

        if remaining > 1:
            self._spans[col] = (remaining - 1, value)
        else:
            del self._spans[col]


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
    data_rows = [row for row in data_rows if not _is_boilerplate_row(row)]
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
        _format_header_rows(header_rows),
        "Строки таблицы:",
        _format_data_rows(header_rows, data_rows),
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
            tables[-1], rows = _align_fragment_widths(tables[-1], rows)
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
    blocks = [prefix, "Заголовки таблицы:", _format_header_rows(header_rows)]
    blocks.extend(["Строки таблицы:", _format_data_rows(header_rows, data_rows), suffix])
    return "\n".join(block for block in blocks if block).strip()


def _split_header_rows(rows: list[list[str]]) -> tuple[list[list[str]], list[list[str]]]:
    meta_rows = 1 if rows and _has_table_meta(rows[0]) else 0
    header_count = meta_rows + _header_body_count(rows[meta_rows:])

    if header_count >= len(rows):
        header_count = max(meta_rows + 1, len(rows) - 1)
    return rows[:header_count], rows[header_count:]


def _header_body_count(rows: list[list[str]]) -> int:
    if len(rows) <= 1:
        return len(rows)

    width = _dominant_width(rows)
    saw_numbers = False
    for index, row in enumerate(rows[: min(len(rows) - 1, 30)]):
        next_row = rows[index + 1] if index + 1 < len(rows) else []
        if _is_column_number_row(row):
            saw_numbers = True
            continue

        if _starts_data_block(row, next_row, width, saw_numbers):
            return max(1, index)

    return min(4, len(rows) - 1)


def _starts_data_block(
    row: list[str],
    next_row: list[str],
    width: int,
    saw_numbers: bool,
) -> bool:
    if _is_data_row(row, width, saw_numbers):
        return True

    return _is_group_row(row) and _is_data_row(next_row, width, saw_numbers)


def _can_merge_vertically(base: list[list[str]], rows: list[list[str]]) -> bool:
    base_width = _dominant_width(base)
    rows_width = _dominant_width(rows)
    if base_width <= 0 or rows_width <= 0:
        return False

    if base_width == rows_width:
        return True

    return _can_merge_header_data(base, rows, base_width, rows_width)


def _can_merge_header_data(
    base: list[list[str]],
    rows: list[list[str]],
    base_width: int,
    rows_width: int,
) -> bool:
    if rows_width <= base_width or rows_width - base_width > 4:
        return False

    return _looks_header_fragment(base, base_width) and _is_data_row(
        rows[0], rows_width, False
    )


def _looks_header_fragment(rows: list[list[str]], width: int) -> bool:
    data_like = sum(_is_data_row(row, width, False) for row in rows)

    return data_like <= max(1, len(rows) // 5)


def _align_fragment_widths(
    base: list[list[str]],
    rows: list[list[str]],
) -> tuple[list[list[str]], list[list[str]]]:
    target_width = max(_dominant_width(base), _dominant_width(rows))

    return _pad_rows(base, target_width), _pad_rows(rows, target_width)


def _pad_rows(rows: list[list[str]], target_width: int) -> list[list[str]]:
    return [row + [""] * max(0, target_width - len(row)) for row in rows]


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


def _format_data_rows(
    header_rows: list[list[str]],
    data_rows: list[list[str]],
) -> str:
    headers = _flatten_headers(header_rows)
    if not _use_key_value_rows(headers, data_rows):
        return _rows_to_text(data_rows)

    return "\n".join(_format_key_value_row(headers, row) for row in data_rows)


def _format_header_rows(header_rows: list[list[str]]) -> str:
    width = max((len(row) for row in header_rows), default=0)
    if width >= 8 and len(header_rows) > 2:
        return " | ".join(_flatten_headers(header_rows))

    return _rows_to_text(header_rows)


def _flatten_headers(header_rows: list[list[str]]) -> list[str]:
    width = max((len(row) for row in header_rows), default=0)
    headers: list[str] = []

    for col in range(width):
        parts = _header_col_parts(header_rows, col)
        headers.append(_short_header(" ".join(parts)) or f"колонка {col + 1}")

    return headers


def _header_col_parts(header_rows: list[list[str]], col: int) -> list[str]:
    parts: list[str] = []
    for row in header_rows:
        cell = row[col] if col < len(row) else ""
        if cell and cell not in parts and not _looks_column_number_cell(cell):
            parts.append(cell)

    return parts


def _use_key_value_rows(headers: list[str], data_rows: list[list[str]]) -> bool:
    if len(headers) < 5:
        return False

    widths = [len(row) for row in data_rows if len(row) >= 4]
    return bool(widths and max(widths) >= min(len(headers), 5))


def _format_key_value_row(headers: list[str], row: list[str]) -> str:
    if _is_group_row(row):
        return _group_row_text(row)

    if len([cell for cell in row if cell]) <= 1:
        return " | ".join(cell for cell in row if cell)

    pairs = [
        f"{headers[index]}={cell}"
        for index, cell in enumerate(row[: len(headers)])
        if cell
    ]
    return "; ".join(pairs)


def _short_header(value: str, limit: int = 42) -> str:
    value = _clean_cell(value)
    if len(value) <= limit:
        return value

    return value[:limit].rsplit(" ", 1)[0].strip() or value[:limit]


def _is_data_row(row: list[str], width: int, saw_numbers: bool) -> bool:
    nonempty = [cell for cell in row if cell]
    if len(nonempty) < 2 or _is_column_number_row(row):
        return False

    if saw_numbers and len(row) >= max(2, width // 2):
        return True

    value_count = _numeric_cell_count(row[1:])
    min_values = max(2, len(nonempty) // 2)

    return len(row) >= max(2, width // 2) and value_count >= min_values


def _is_group_row(row: list[str]) -> bool:
    nonempty = [cell for cell in row if cell]
    if len(nonempty) == 1:
        return _is_group_cell(nonempty[0])

    unique = {_clean_cell(cell).lower() for cell in nonempty}
    return len(unique) == 1 and _is_group_cell(nonempty[0])


def _is_group_cell(cell: str) -> bool:
    return not _looks_column_number_cell(cell) and not re.search(
        r"\d|≤|>=|<=|>|<",
        cell,
    )


def _group_row_text(row: list[str]) -> str:
    for cell in row:
        if cell:
            return cell

    return ""


def _is_column_number_row(row: list[str]) -> bool:
    nonempty = [cell for cell in row if cell]
    return bool(nonempty) and all(_looks_column_number_cell(cell) for cell in nonempty)


def _looks_column_number_cell(cell: str) -> bool:
    value = _clean_cell(cell).strip(".")
    return bool(re.fullmatch(r"\d+|[IVXLCDM]+", value, re.IGNORECASE))


def _numeric_cell_count(row: list[str]) -> int:
    return sum(bool(re.search(r"\d|≤|>=|<=|>|<", cell)) for cell in row)


def _is_boilerplate_row(row: list[str]) -> bool:
    return bool(_TABLE_BOILERPLATE_RE.search(" ".join(row)))


def _drop_metadata_rows(rows: list[list[str]]) -> list[list[str]]:
    return [row for row in rows if not _has_table_meta(row)]


def _estimate_tokens(text: str) -> int:
    return int(len(text.split()) * 1.6)


def _caption_from_line(line: str) -> str | None:
    text = line.strip()
    match = _CAPTION_RE.search(text)
    if not match:
        return None

    prefix = text[: match.start()].strip()
    if prefix and not re.match(r"^\d+(?:\.\d+)*\s+\S+", prefix):
        return None

    return match.group(1).strip()


def _is_table_separator(line: str) -> bool:
    text = line.strip()
    return not text or bool(
        _PAGE_RE.search(text)
        or _FRAGMENT_RE.search(text)
        or _TABLE_BOILERPLATE_RE.search(text)
    )


def _make_table_id(caption: str | None, table_number: int) -> str:
    source = caption or f"table-{table_number}"
    digest = hashlib.sha1(source.encode("utf-8")).hexdigest()[:10]
    key = _table_key(source)
    if not key:
        return f"table_{table_number}_{digest}"

    return f"table_{key}_{digest}"


def _table_key(source: str) -> str | None:
    match = re.search(
        r"Таблица\s+([А-ЯA-Z]\.?\d+(?:\.\d+)*|\d+(?:\.\d+)*)",
        source,
        re.IGNORECASE,
    )
    if not match:
        return None

    return re.sub(r"[^0-9a-zа-яё]+", "_", match.group(1).lower()).strip("_")


def _span_attr(attrs: list[tuple[str, str | None]], name: str) -> int:
    values = {key.lower(): value for key, value in attrs if key}
    raw_value = values.get(name)
    if not raw_value:
        return 1

    try:
        return max(1, int(raw_value))
    except ValueError:
        return 1


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
