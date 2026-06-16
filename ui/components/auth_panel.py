import streamlit as st

from auth import AUTH_USER_KEY, get_current_user, login, logout
from theme import render_theme_selector


def render_auth_panel(user_id: str) -> None:
    user = get_current_user()

    with st.popover(
        user.display_name,
        icon=":material/account_circle:",
        width="stretch",
        key="auth_panel",
    ):
        render_theme_selector(user_id)

        if user.is_authenticated:
            _render_profile_state(user.display_name)
            return

        if st.button("Войти", type="primary", width="stretch"):
            _render_login_dialog()


def _render_profile_state(display_name: str) -> None:
    st.caption(f"Пользователь: {display_name}")

    if st.button("Выйти", width="stretch"):
        logout()
        st.rerun()


@st.dialog("Вход", width="small")
def _render_login_dialog() -> None:
    with st.form("auth_login_dialog", border=False):
        email = st.text_input("Email", placeholder="user@example.com")
        password = st.text_input("Пароль", type="password")
        submitted = st.form_submit_button("Войти", type="primary", width="stretch")

    if submitted:
        user = login(email or "", password or "")
        if user:
            st.session_state[AUTH_USER_KEY] = user.__dict__
            st.rerun()
        st.info("Авторизация будет подключена позже.")

    if st.button("Создать аккаунт", width="stretch"):
        st.info("Регистрация будет подключена позже.")
