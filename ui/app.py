from datetime import datetime
from uuid import uuid4

import requests
import streamlit as st
from auth import get_current_user
from components import RetrieverClient
from components.auth_panel import render_auth_panel
from components.user_history import (
    add_search,
    get_selected_search,
    initialize_history,
    is_sidebar_compact,
    render_history_sidebar,
    update_search,
)
from components.user_results import render_search
from config import UIConfig
from theme import (
    APP_UI_STYLES,
    build_sidebar_layout_css,
    build_theme_css,
    initialize_theme,
)

ERROR_KEY = "user_ui_search_error"
EXAMPLE_QUERIES = (
    "Как располагать стыки рабочей арматуры внахлестку",
    "Из каких материалов можно сделать вертикальный шумозащитный экран",
    "Какой ширины должна быть противопожарная зона "
    "на складе лесоматериалов площадью более 18 га",
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
        initial_sidebar_state="expanded",
    )
    user = get_current_user()
    theme_key = initialize_theme(user.user_id)
    _apply_styles(theme_key, is_sidebar_compact())
    initialize_history(user.user_id)
    render_history_sidebar(user.user_id)
    _render_sidebar_footer(user.user_id)

    selected_search = get_selected_search()
    requested_query = _render_content(selected_search)
    chat_query = st.chat_input("Задайте вопрос по нормативной документации")

    if chat_query:
        _search(chat_query)
    elif requested_query:
        _search(requested_query, selected_search["id"] if selected_search else None)


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
    with st.container(key="start_screen"):
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
            "Старайтесь формулировать вопрос конкретно: укажите конструкцию, "
            "материал, параметр и т.п. На общий вопрос вы получите общий ответ."
        )
    return None


def _render_example_row(examples: tuple[str, ...]) -> str | None:
    columns = st.columns(len(examples))
    for column, example in zip(columns, examples, strict=True):
        if column.button(example, key=f"example_{example}", width="stretch"):
            return example
    return None


def _render_search_error() -> str | None:
    error = st.session_state.get(ERROR_KEY)
    if not error:
        return None

    st.error(
        "Не удалось получить ответ (╯°□°）╯︵ ┻━┻. "
        "Проверьте доступность сервиса и повторите запрос. "
    )
    with st.expander("Техническая информация"):
        st.code(error["details"], language=None)
    if st.button("Повторить запрос", type="primary"):
        return error["query"]
    return None


def _search(query: str, search_id: str | None = None) -> None:
    try:
        response = _request_search(query)
    except requests.RequestException as error:
        st.session_state[ERROR_KEY] = {"query": query, "details": str(error)}
        st.rerun()
        return

    st.session_state.pop(ERROR_KEY, None)
    if search_id:
        update_search(search_id, query, response)
        st.rerun()
        return

    search = {
        "id": str(uuid4()),
        "query": query,
        "created_at": datetime.now().astimezone().isoformat(),
        "response": response,
    }
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


def _render_sidebar_footer(user_id: str) -> None:
    with st.sidebar:
        with st.container(key="sidebar_footer"):
            render_auth_panel(user_id)


def _apply_styles(theme_key: str, is_compact_sidebar: bool) -> None:
    st.markdown(build_theme_css(theme_key), unsafe_allow_html=True)
    st.markdown(APP_UI_STYLES, unsafe_allow_html=True)
    st.markdown(build_sidebar_layout_css(is_compact_sidebar), unsafe_allow_html=True)


if __name__ == "__main__":
    main()
