import streamlit as st

from core.auth import get_current_user, login, logout, register
from core.user_api_client import UserApiError
from components.theme import render_theme_selector


def render_auth_panel(user_id: str) -> None:
    user = get_current_user()

    with st.popover(
        user.display_name,
        icon=":material/account_circle:",
        width="stretch",
        key="auth_panel",
    ):
        with st.container(key="auth_panel_content"):
            render_theme_selector(user_id)

            if user.is_authenticated:
                _render_profile_state(user.display_name)
                return

            if st.button("Войти", type="primary", width="stretch"):
                _render_login_dialog()


def _render_profile_state(display_name: str) -> None:
    st.caption(f"Пользователь: {display_name}")

    if st.button("Выйти", width="stretch"):
        try:
            logout()
        except UserApiError as error:
            st.warning(f"Сессия закрыта локально: {error}")
        st.rerun()


@st.dialog("Вход", width="small")
def _render_login_dialog() -> None:
    with st.container(key="auth_dialog"):
        _render_login_form()
        st.divider()
        _render_register_form()


def _render_login_form() -> None:
    with st.form("auth_login_dialog", border=False):
        email = st.text_input("Email", placeholder="user@example.com")
        password = st.text_input("Пароль", type="password")
        submitted = st.form_submit_button("Войти", type="primary", width="stretch")

    if submitted:
        _submit_login(email or "", password or "")


def _render_register_form() -> None:
    with st.expander("Создать аккаунт"):
        with st.form("auth_register_dialog", border=False):
            display_name = st.text_input("Имя", key="register_display_name")
            email = st.text_input("Email", key="register_email")
            password = st.text_input("Пароль", type="password", key="register_password")
            submitted = st.form_submit_button("Создать", width="stretch")

    if submitted:
        _submit_register(email or "", password or "", display_name or None)


def _submit_login(email: str, password: str) -> None:
    try:
        user = login(email, password)
    except UserApiError as error:
        st.error(f"Не удалось войти: {error}")
        return

    if user:
        st.rerun()
        return

    st.error("Введите email и пароль.")


def _submit_register(email: str, password: str, display_name: str | None) -> None:
    try:
        user = register(email, password, display_name)
    except UserApiError as error:
        st.error(f"Не удалось создать аккаунт: {error}")
        return

    if user:
        st.rerun()
        return

    st.error("Введите email и пароль.")
