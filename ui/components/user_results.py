import base64
import re

import streamlit as st
from components.feedback_logger import log_feedback

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
    display: flex; align-items: baseline; gap: 0.6rem;
    margin: 0 0 0.5rem;
}}
h4 {{margin: 0;}}
button {{
    border: 0; padding: 0.15rem 0;
    background: transparent; color: #888; cursor: pointer;
    font-size: 0.75rem;
}}
button:hover {{color: #444;}}
</style>
"""


def render_search(search: dict) -> str | None:
    response = search["response"]
    with st.chat_message("user"):
        _render_question(search)

    with st.chat_message("assistant"):
        _render_answer(search)
        _render_quality_notice(response.get("results", []))
        _render_sources(response.get("results", []))

    return _render_response_actions(search)


def _render_answer(search: dict) -> None:
    response = search["response"]
    answer = response.get("answer")
    if answer:
        answer_without_heading = _without_answer_heading(answer)
        _render_answer_header(answer_without_heading, search["id"])
        st.markdown(answer_without_heading)
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


def _render_sources(results: list[dict]) -> None:
    if not results:
        return

    with st.expander(f"Источники и найденные фрагменты ({len(results)})"):
        for index, result in enumerate(results, start=1):
            _render_source(index, result)
            if index < len(results):
                st.divider()


def _render_source(index: int, result: dict) -> None:
    section = result.get("section_path") or "Раздел не указан"
    st.markdown(f"**[{index}] {result['filename']}**")
    st.caption(f"{section} · {'Таблица' if result.get('is_table') else 'Текст'}")
    st.code(_source_excerpt(result.get("text", "")), language=None)


def _source_excerpt(text: str, limit: int = 900) -> str:
    clean_text = " ".join(text.split())
    if len(clean_text) <= limit:
        return clean_text
    return f"{clean_text[:limit].rsplit(' ', 1)[0]}..."


def _render_question(search: dict) -> None:
    st.markdown(search["query"])


def _render_answer_header(answer: str, search_id: str) -> None:
    _render_copy_button(answer, search_id)


def _render_copy_button(text: str, search_id: str) -> None:
    encoded_text = base64.b64encode(text.encode("utf-8")).decode("ascii")
    button_id = f"copy-question-{search_id}"
    st.html(
        COPY_BUTTON_HTML.format(button_id=button_id, encoded_text=encoded_text),
        unsafe_allow_javascript=True,
    )


def _without_answer_heading(answer: str) -> str:
    return re.sub(r"^\s*#{1,6}\s*Ответ\s*\n+", "", answer, count=1)


def _render_feedback(search: dict) -> None:
    selected = search.get("feedback")
    positive = st.button(
        "Полезно",
        key=f"positive_{search['id']}",
        help="Ответ полезен",
        icon=":material/thumb_up:" if selected == "positive" else ":material/thumb_up_off_alt:",
        type="primary" if selected == "positive" else "tertiary",
    )
    negative = st.button(
        "Есть ошибка",
        key=f"negative_{search['id']}",
        help="В ответе есть ошибка",
        icon=":material/thumb_down:" if selected == "negative" else ":material/thumb_down_off_alt:",
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
        st.rerun()
        return

    search["feedback"] = rating
    if log_feedback(search, rating):
        st.toast("Спасибо, оценка сохранена")
    else:
        st.error("Не удалось сохранить оценку.")


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
            return query.strip()
    return None
