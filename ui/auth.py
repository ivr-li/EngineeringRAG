from dataclasses import dataclass

import streamlit as st

AUTH_USER_KEY = "user_ui_auth_user"
ANONYMOUS_USER_ID = "anonymous_user"


@dataclass(frozen=True)
class UserContext:
    user_id: str
    display_name: str
    is_authenticated: bool = False


def get_current_user() -> UserContext:
    user = st.session_state.get(AUTH_USER_KEY)
    if isinstance(user, dict):
        return UserContext(**user)

    return UserContext(
        user_id=ANONYMOUS_USER_ID,
        display_name="Гость",
        is_authenticated=False,
    )


def login(email: str, password: str) -> UserContext | None:
    if not email.strip() or not password:
        return None

    return None


def logout() -> None:
    st.session_state.pop(AUTH_USER_KEY, None)
