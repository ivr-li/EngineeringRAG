import hmac
import os

import streamlit as st
from components import (
    ResultsView,
    RetrieverClient,
    SearchBar,
    SidebarParams,
)
from dotenv import load_dotenv

AUTH_KEY = "developer_ui_authorized"
PASSWORD_ENV = "DEVELOPER_UI_PASSWORD"

load_dotenv()


def main() -> None:
    st.set_page_config(
        page_title="Интерфейс разработчика",
        page_icon="🏗️",
        layout="wide",
    )
    _require_password()
    st.title("Интерфейс разработчика")

    params = SidebarParams()
    search_bar = SearchBar()

    if search_bar.should_search:
        _run_search(search_bar, params)
    else:
        ResultsView().render()


def _require_password() -> None:
    if st.session_state.get(AUTH_KEY):
        if st.sidebar.button("Выйти из режима разработчика"):
            st.session_state[AUTH_KEY] = False
            st.rerun()
        return

    expected_password = os.getenv(PASSWORD_ENV)
    if not expected_password:
        st.error(f"Страница закрыта. Настройте переменную окружения `{PASSWORD_ENV}`.")
        st.stop()

    with st.form("developer_login"):
        password = st.text_input("Пароль разработчика", type="password")
        submitted = st.form_submit_button("Войти", type="primary")

    if submitted and hmac.compare_digest(password, expected_password):
        st.session_state[AUTH_KEY] = True
        st.rerun()
    if submitted:
        st.error("Неверный пароль.")
    st.stop()


def _run_search(search_bar: SearchBar, params: SidebarParams) -> None:
    with st.spinner("Обработка запроса…"):
        response = RetrieverClient().search(
            query=search_bar.query,
            rewrite_system_prompt=params.rewrite_system_prompt,
            compose_system_prompt=params.compose_system_prompt,
            top_k=params.top_k,
            prefetch_k=params.prefetch_k,
            mode=params.mode,
            only_tables=params.only_tables,
            filename_filter=params.filename_filter,
            section_filter=params.section_filter,
        )

    _store_response(response, search_bar, params)
    st.rerun()


def _store_response(response: dict, search_bar: SearchBar, params: SidebarParams) -> None:
    st.session_state["search_response"] = response
    st.session_state["meta"] = dict(
        query=search_bar.query,
        effective_query=response.get("effective_query", search_bar.query),
        was_rewritten=response.get("was_rewritten", False),
        mode=params.mode,
        top_k=params.top_k,
        prefetch_k=params.prefetch_k if params.mode == "hybrid" else None,
        only_tables=params.only_tables,
        filename_filter=params.filename_filter,
        section_filter=params.section_filter,
        generate_user_answer=params.generate_user_answer,
    )


if __name__ == "__main__":
    main()
