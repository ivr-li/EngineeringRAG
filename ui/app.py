from datetime import datetime
from uuid import uuid4

import requests
import streamlit as st
from components import RetrieverClient
from components.user_history import (
    add_search,
    get_selected_search,
    initialize_history,
    render_history_sidebar,
)
from components.user_results import render_search
from config import UIConfig

ERROR_KEY = "user_ui_search_error"
EXAMPLE_QUERIES = (
    "Как располагать стыки рабочей арматуры внахлестку",
    "Из каких материалов можно сделать вертикальный шумозащитный экран",
    "Какой ширины должна быть противопожарная зона на складе лесоматериалов площадью более 18 га",
    "Какие грунты нужно динамически испытывать при изысканиях в сейсмическом районе",
)
# EXAMPLE_QUERIES = (
#     "Как стыковать арматуры внахлестку",
#     "Из чего можно сделать вертикальный шумозащитный экран",
#     "Ширина противопожарной зоны на складе лесоматериалов площадью более 18 га",
#     "Какие грунты нужно динамически испытывать в сейсмическом районе",
# )


def main() -> None:
    st.set_page_config(
        page_title="Поиск по нормативной документации",
        page_icon="🏗️",
        layout="wide",
    )
    _apply_styles()
    initialize_history()
    render_history_sidebar()

    selected_search = get_selected_search()
    requested_query = _render_content(selected_search)
    chat_query = st.chat_input("Задайте вопрос по нормативной документации")

    if chat_query or requested_query:
        _search(chat_query or requested_query)


def _render_content(selected_search: dict | None) -> str | None:
    if selected_search:
        _render_compact_header()
        return render_search(selected_search)

    error_query = _render_search_error()
    return error_query or _render_start_screen()


def _render_compact_header() -> None:
    st.markdown(
        '<p class="compact-title">Поиск по нормативной документации</p>',
        unsafe_allow_html=True,
    )


def _render_start_screen() -> str | None:
    st.title("Поиск по нормативной документации")
    st.markdown(
        "Получите ответ по строительным нормам с указанием найденных документов "
        "и разделов. Каждый запрос создаёт отдельную запись в истории."
    )
    st.markdown("#### Примеры вопросов")

    for row_start in range(0, len(EXAMPLE_QUERIES), 2):
        selected = _render_example_row(EXAMPLE_QUERIES[row_start : row_start + 2])
        if selected:
            return selected

    st.info(
        "Старайтесь формулировать вопрос конкретно: укажите конструкцию, материал, параметр и т.п. На общий вопрос вы получите общий ответ."
    )
    return None


def _render_example_row(examples: tuple[str, ...]) -> str | None:
    columns = st.columns(len(examples))
    for column, example in zip(columns, examples, strict=True):
        if column.button(example, key=f"example_{example}", use_container_width=True):
            return example
    return None


def _render_search_error() -> str | None:
    error = st.session_state.get(ERROR_KEY)
    if not error:
        return None

    st.error(
        "Не удалось получить ответ (╯°□°）╯︵ ┻━┻. Проверьте доступность сервиса и повторите запрос. "
    )
    with st.expander("Техническая информация"):
        st.code(error["details"], language=None)
    if st.button("Повторить запрос", type="primary"):
        return error["query"]
    return None


def _search(query: str) -> None:
    try:
        response = _request_search(query)
    except requests.RequestException as error:
        st.session_state[ERROR_KEY] = {"query": query, "details": str(error)}
        st.rerun()
        return

    search = {
        "id": str(uuid4()),
        "query": query,
        "created_at": datetime.now().astimezone().isoformat(),
        "response": response,
    }
    st.session_state.pop(ERROR_KEY, None)
    add_search(search)
    st.rerun()


def _request_search(query: str) -> dict:
    with st.status("Обрабатываем запрос...", expanded=True) as status:
        st.write("Передали вопрос поисковому сервису")
        response = RetrieverClient().search(
            query=query,
            rewrite_system_prompt=UIConfig.REWRITE_SYSTEM_PROMPT,
            compose_system_prompt=UIConfig.ANSWER_SYSTEM_PROMPT,
            top_k=4,
            prefetch_k=40,
            mode="hybrid",
        )

        st.write("Проверяем полученные источники")
        status.update(label="Ответ готов", state="complete", expanded=False)
    return response


def _apply_styles() -> None:
    st.markdown(
        """
        <style>
        :root {--refine-popover-width: 900px;}
        .main .block-container {max-width: 600px; padding-top: 2.2rem;}
        .compact-title {color: #777; font-size: 0.9rem; margin-bottom: 1.5rem;}
        .st-key-response-actions {
            width: 100%;
            margin-top: 0.25rem;
            gap: 0.6rem;
        }
        .st-key-response-actions [data-testid="stHorizontalBlock"] {
            width: auto !important;
            flex: 0 0 auto !important;
            justify-content: flex-start !important;
        }
        .st-key-response-actions [data-testid="stHorizontalBlock"] > div,
        .st-key-response-actions [data-testid="stElementContainer"] {
            width: auto !important;
            flex: 0 0 auto !important;
        }
        .st-key-response-actions [data-testid="stMarkdownContainer"] p {
            margin: 0;
            white-space: nowrap;
        }
        .st-key-response-actions button[kind="tertiary"],
        .st-key-response-actions button[kind="primary"] {
            min-width: 1.75rem;
            padding: 0.2rem;
        }
        .st-key-response-actions button[kind="tertiary"] p,
        .st-key-response-actions button[kind="primary"] p {
            display: none;
        }
        [data-testid="stSidebar"] .stButton > button {
            min-height: 2rem; padding: 0.2rem 0.4rem; border: 0;
            background: transparent; color: rgba(128, 128, 128, 0.9);
            font-size: 0.8rem; font-weight: 400; text-align: left;
        }
        [data-testid="stSidebar"] .stButton > button:hover {
            background: rgba(128, 128, 128, 0.1); color: inherit;
        }
        [data-testid="stSidebar"] .stButton > button[kind="tertiary"],
        [data-testid="stSidebar"] .stButton > button[kind="tertiary"]:hover {
            min-width: 1.5rem; padding: 0; background: transparent;
            color: #999; opacity: 0.7;
        }
        [data-testid="stSidebar"] [data-testid="stPopover"] button {
            border: 0; background: transparent; color: rgba(128, 128, 128, 0.9);
            font-size: 0.8rem;
        }
        div[data-baseweb="popover"]:has([data-testid="stForm"]) {
            width: min(var(--refine-popover-width), calc(100vw - 2rem)) !important;
            max-width: min(var(--refine-popover-width), calc(100vw - 2rem)) !important;
            max-height: none !important;
            overflow: visible !important;
        }
        div[data-baseweb="popover"]:has([data-testid="stForm"]) > div,
        div[data-baseweb="popover"]:has([data-testid="stForm"]) > div > div {
            width: 100% !important;
            max-width: 100% !important;
            max-height: none !important;
            overflow: visible !important;
            box-sizing: border-box;
        }
        div[data-baseweb="popover"]:has([data-testid="stForm"])
        [data-testid="stForm"],
        div[data-baseweb="popover"]:has([data-testid="stForm"])
        [data-testid="stTextArea"],
        div[data-baseweb="popover"]:has([data-testid="stForm"])
        [data-testid="stTextArea"] > div {
            width: 100% !important;
            max-width: 100% !important;
            box-sizing: border-box;
        }
        div[data-baseweb="popover"]:has([data-testid="stForm"]) textarea {
            width: 100%;
            min-height: 4rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
