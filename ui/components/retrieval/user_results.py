import base64
import re
import textwrap

import streamlit as st
from shared.logging.feedback_logger import log_feedback

SOURCE_EXCERPT_LINE_WIDTH = 110
SOURCE_TABLE_MAX_ROWS = 24
FEEDBACK_TOAST_KEY = "user_ui_feedback_toast"

COPY_BUTTON_HTML = """
<div class="answer-header">
    <h4>Ответ</h4>
    <button id="{button_id}" title="Копировать ответ">⧉ Копировать ответ</button>
</div>
<script>
const button = document.getElementById("{button_id}");
const bytes = Uint8Array.from(atob("{encoded_text}"), c => c.charCodeAt(0));
const text = new TextDecoder().decode(bytes);
button.onclick = async () => {{
    try {{
        await navigator.clipboard.writeText(text);
        button.textContent = "✓ Скопировано";
    }} catch {{
        button.textContent = "Копирование недоступно";
    }}
}};
</script>
<style>
.answer-header {{
    align-items: center;
    border-bottom: 1px solid var(--app-border);
    display: flex;
    gap: 1rem;
    justify-content: space-between;
    margin: -0.85rem -0.95rem 0.85rem;
    padding: 1rem 1.25rem;
}}
.answer-header h4 {{margin: 0; color: var(--app-text);}}
.answer-header button {{
    border: 0; padding: 0.15rem 0;
    background: transparent; color: var(--app-muted); cursor: pointer;
    font-size: 0.75rem;
    margin-left: auto;
}}
.answer-header button:hover {{color: var(--app-accent);}}
</style>
"""


def render_search(search: dict) -> str | None:
    _render_pending_feedback_toast()

    response = search["response"]
    with st.chat_message("user", avatar=_user_avatar()):
        _render_question(search)

    with st.chat_message("assistant", avatar="📋"):
        _render_answer(search)
        _render_quality_notice(response.get("results", []))
        _render_sources(search["id"], response.get("results", []))

    return _render_response_actions(search)


def _render_answer(search: dict) -> None:
    response = search["response"]
    answer = response.get("answer")
    if answer:
        answer_without_heading = _without_answer_heading(answer)
        with st.container(key=f"answer_card_{search['id']}"):
            _render_answer_header(answer_without_heading, search["id"])
            st.markdown(_with_answer_section_dividers(answer_without_heading))
    elif response.get("results"):
        st.warning("Ответ не сформирован. Изучите найденные источники ниже.")
    else:
        st.info("По запросу ничего не найдено.")


def _render_quality_notice(results: list[dict]) -> None:
    if not results:
        return

    max_score = max(result.get("score", 0) for result in results)
    if max_score < 8:
        st.warning(
            "Найдены источники с низкой релевантностью. Проверьте основания ответа."
        )
    elif len(results) == 1:
        st.info("Ответ основан только на одном найденном фрагменте.")


def _render_sources(search_id: str, results: list[dict]) -> None:
    if not results:
        return

    with st.container(key=f"sources_panel_{search_id}"):
        with st.expander(f"Источники и найденные фрагменты ({len(results)})"):
            for index, result in enumerate(results, start=1):
                with st.container(key=f"source_fragment_{search_id}_{index}"):
                    _render_source(index, result)
                if index < len(results):
                    st.divider()


def _render_source(index: int, result: dict) -> None:
    section = result.get("section_path") or "Раздел не указан"
    st.markdown(f"**[{index}] {result['filename']}**")
    st.caption(f"{section} · {'Таблица' if result.get('is_table') else 'Текст'}")
    _render_source_excerpt(result.get("text", ""))


def _render_source_excerpt(text: str) -> None:
    table = _pipe_table_data(text)
    if table is not None:
        st.table(table)
        return

    st.markdown(_source_excerpt(text))


def _source_excerpt(text: str, limit: int = 900) -> str:
    clean_text = " ".join(text.split())
    if len(clean_text) <= limit:
        return textwrap.fill(clean_text, width=SOURCE_EXCERPT_LINE_WIDTH)

    trimmed = clean_text[:limit].rsplit(" ", 1)[0]
    return f"{textwrap.fill(trimmed, width=SOURCE_EXCERPT_LINE_WIDTH)}..."


def _pipe_table_data(text: str) -> list[dict[str, str]] | None:
    rows = _pipe_rows(text)
    if len(rows) < 2:
        return None

    header, body = rows[0], rows[1:]
    if body and _is_separator_row(body[0]):
        body = body[1:]
    if not body:
        return None

    headers = _unique_headers(header)
    return [dict(zip(headers, row, strict=True)) for row in body[:SOURCE_TABLE_MAX_ROWS]]


def _pipe_rows(text: str) -> list[list[str]]:
    rows = [_pipe_cells(line) for line in text.splitlines() if _is_pipe_row(line)]
    if not rows:
        return []

    width = max(len(row) for row in rows)
    return [row + [""] * (width - len(row)) for row in rows]


def _is_pipe_row(line: str) -> bool:
    return line.count("|") >= 2


def _pipe_cells(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _is_separator_row(row: list[str]) -> bool:
    return all(re.fullmatch(r":?-{3,}:?", cell.replace(" ", "")) for cell in row)


def _unique_headers(headers: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    unique_headers = []
    for index, header in enumerate(headers, start=1):
        name = header or f"Колонка {index}"
        counts[name] = counts.get(name, 0) + 1
        unique_headers.append(name if counts[name] == 1 else f"{name} {counts[name]}")

    return unique_headers


def _render_question(search: dict) -> None:
    with st.container(key=f"question_card_{search['id']}"):
        st.markdown(search["query"])


def _render_answer_header(answer: str, search_id: str) -> None:
    _render_copy_button(answer, search_id)


def _render_copy_button(text: str, search_id: str) -> None:
    encoded_text = base64.b64encode(text.encode("utf-8")).decode("ascii")
    button_id = f"copy-answer-{search_id}"
    st.html(
        COPY_BUTTON_HTML.format(button_id=button_id, encoded_text=encoded_text),
        unsafe_allow_javascript=True,
    )


def _without_answer_heading(answer: str) -> str:
    return re.sub(r"^\s*#{1,6}\s*Ответ\s*\n+", "", answer, count=1)


def _with_answer_section_dividers(answer: str) -> str:
    pattern = r"(?m)^(#{2,6}\s+(?:Что удалось найти|Основание|Ограничения)\s*)$"
    return re.sub(pattern, r"---" "\n\n" r"\1", answer)


def _user_avatar() -> str:
    return "👤"


def _render_feedback(search: dict) -> None:
    selected = search.get("feedback")
    positive = st.button(
        "Полезно",
        key=f"positive_{search['id']}",
        help="Ответ полезен",
        icon=":material/thumb_up:"
        if selected == "positive"
        else ":material/thumb_up_off_alt:",
        type="primary" if selected == "positive" else "tertiary",
    )
    negative = st.button(
        "Есть ошибка",
        key=f"negative_{search['id']}",
        help="В ответе есть ошибка",
        icon=":material/thumb_down:"
        if selected == "negative"
        else ":material/thumb_down_off_alt:",
        type="primary" if selected == "negative" else "tertiary",
    )

    if positive:
        _toggle_rating(search, "positive")
    elif negative:
        _toggle_rating(search, "negative")


def _render_response_actions(search: dict) -> str | None:
    actions = st.container(
        key="response-actions",
        horizontal=True,
        horizontal_alignment="left",
        vertical_alignment="center",
        gap="small",
    )
    with actions:
        refined_query = _render_refine_popover(search)
        feedback = st.container(horizontal=True, gap="small")
        with feedback:
            _render_feedback(search)

    if search.get("feedback") == "negative":
        _render_feedback_comment(search)
    return refined_query


def _toggle_rating(search: dict, rating: str) -> None:
    if search.get("feedback") == rating:
        search["feedback"] = None
        log_feedback(search, "cleared")
        _queue_feedback_toast("Оценка сброшена")
        st.rerun()
        return

    previous_rating = search.get("feedback")
    search["feedback"] = rating
    if log_feedback(search, rating):
        _queue_feedback_toast("Спасибо за оценку!")
        st.rerun()
    else:
        search["feedback"] = previous_rating
        st.error("Не удалось сохранить оценку.")


def _queue_feedback_toast(message: str) -> None:
    st.session_state[FEEDBACK_TOAST_KEY] = message


def _render_pending_feedback_toast() -> None:
    message = st.session_state.pop(FEEDBACK_TOAST_KEY, None)
    if message:
        st.toast(message, icon="✅")


def _render_feedback_comment(search: dict) -> None:
    with st.form(f"feedback_comment_{search['id']}"):
        comment = st.text_area(
            "Что нужно исправить?",
            value=search.get("feedback_comment", ""),
            placeholder="Например: неверно указан пункт документа",
        )
        submitted = st.form_submit_button("Отправить комментарий")

    if submitted and comment.strip():
        search["feedback_comment"] = comment.strip()
        log_feedback(search, "negative", comment.strip())
        st.toast("Комментарий сохранён")


def _render_refine_popover(search: dict) -> str | None:
    with st.popover("Уточнить вопрос", icon=":material/edit:"):
        with st.form(f"refine_{search['id']}", border=False):
            query = st.text_area("Новый вопрос", value=search["query"], height=180)
            submitted = st.form_submit_button(
                "Обновить ответ",
                icon=":material/search:",
                type="primary",
            )
        if submitted and query.strip():
            st.toast("Обновляю вопрос...", icon="🔎")
            return query.strip()
    return None
