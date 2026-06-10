from uuid import uuid4

import requests
import streamlit as st
from components import RetrieverClient
from config import UIConfig

HISTORY_KEY = "user_ui_history"
SELECTED_SEARCH_KEY = "user_ui_selected_search"


def main() -> None:
    st.set_page_config(
        page_title="Поиск по нормативной документации",
        page_icon="🏗️",
        layout="wide",
    )
    _initialize_state()
    _render_sidebar()

    st.title("🏗️ Поиск по нормативной документации")
    selected_search = _get_selected_search()
    if selected_search:
        _render_search(selected_search)
    else:
        st.info("Задайте вопрос по нормативной документации.")

    query = st.chat_input("Введите вопрос")
    if query:
        _search(query)


def _initialize_state() -> None:
    st.session_state.setdefault(HISTORY_KEY, [])
    st.session_state.setdefault(SELECTED_SEARCH_KEY, None)


def _render_sidebar() -> None:
    with st.sidebar:
        _apply_sidebar_styles()
        _render_history_actions()
        st.caption("ИСТОРИЯ ПОИСКА")
        _render_history_items()


def _render_history_actions() -> None:
    new_search, clear_history = st.columns([1, 1])
    if new_search.button("＋ Новый", use_container_width=True):
        st.session_state[SELECTED_SEARCH_KEY] = None
        st.rerun()

    if clear_history.button(
        "Очистить",
        disabled=not st.session_state[HISTORY_KEY],
        use_container_width=True,
    ):
        st.session_state[HISTORY_KEY] = []
        st.session_state[SELECTED_SEARCH_KEY] = None
        st.rerun()


def _render_history_items() -> None:
    if not st.session_state[HISTORY_KEY]:
        st.caption("Здесь появятся ваши запросы")
        return

    for item in reversed(st.session_state[HISTORY_KEY]):
        is_selected = item["id"] == st.session_state[SELECTED_SEARCH_KEY]
        if st.button(
            _history_label(item["query"], is_selected),
            key=f"history_{item['id']}",
            use_container_width=True,
        ):
            st.session_state[SELECTED_SEARCH_KEY] = item["id"]
            st.rerun()


def _apply_sidebar_styles() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] .stButton > button {
            min-height: 2rem;
            padding: 0.25rem 0.5rem;
            border: 0;
            border-radius: 0.4rem;
            background: transparent;
            color: rgba(128, 128, 128, 0.9);
            font-size: 0.82rem;
            font-weight: 400;
            text-align: left;
        }
        [data-testid="stSidebar"] .stButton > button:hover {
            background: rgba(128, 128, 128, 0.12);
            color: inherit;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _history_label(query: str, is_selected: bool) -> str:
    label = query if len(query) <= 42 else f"{query[:39]}..."
    return f"› {label}" if is_selected else label


def _get_selected_search() -> dict | None:
    selected_id = st.session_state[SELECTED_SEARCH_KEY]
    return next(
        (item for item in st.session_state[HISTORY_KEY] if item["id"] == selected_id),
        None,
    )


def _render_search(search: dict) -> None:
    response = search["response"]
    with st.chat_message("user"):
        st.markdown(search["query"])

    with st.chat_message("assistant"):
        answer = response.get("answer")
        if answer:
            st.markdown(answer)
        elif response.get("results"):
            st.warning("Ответ не сформирован. Найденные источники доступны ниже.")
        else:
            st.info("По запросу ничего не найдено.")

        _render_sources(response.get("results", []))


def _render_sources(results: list[dict]) -> None:
    if not results:
        return

    with st.expander(f"Источники ({len(results)})"):
        for index, result in enumerate(results, start=1):
            st.markdown(f"**{index}. {result['filename']}**")
            if result.get("section_path"):
                st.caption(result["section_path"])
            if index < len(results):
                st.divider()


def _search(query: str) -> None:
    try:
        with st.spinner("Ищу ответ в нормативной документации..."):
            response = RetrieverClient().search(
                query=query,
                rewrite_system_prompt=UIConfig.REWRITE_SYSTEM_PROMPT,
                compose_system_prompt=UIConfig.ANSWER_SYSTEM_PROMPT,
                top_k=4,
                prefetch_k=40,
                mode="hybrid",
            )
    except requests.RequestException as error:
        st.error(f"Не удалось выполнить поиск: {error}")
        return

    search = {"id": str(uuid4()), "query": query, "response": response}
    st.session_state[HISTORY_KEY].append(search)
    st.session_state[SELECTED_SEARCH_KEY] = search["id"]
    st.rerun()


if __name__ == "__main__":
    main()
