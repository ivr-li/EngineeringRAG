from dataclasses import dataclass

import streamlit as st
from core.auth_cookies import (
    has_pending_auth_cookie_clear,
    read_auth_cookie,
    render_auth_cookie_sync as _render_auth_cookie_sync,
    schedule_auth_cookie,
    schedule_auth_cookie_clear,
)
from core.user_api_client import UserApiClient, UserApiError

AUTH_USER_KEY = "user_ui_auth_user"
AUTH_TOKEN_KEY = "user_ui_auth_token"
AUTH_SESSION_ID_KEY = "user_ui_auth_session_id"
AUTH_EXPIRES_AT_KEY = "user_ui_auth_expires_at"
AUTH_RESTORE_ATTEMPTED_KEY = "user_ui_auth_restore_attempted"
AUTH_RESTORE_ERROR_KEY = "user_ui_auth_restore_error"
ANONYMOUS_USER_ID = "anonymous_user"


@dataclass(frozen=True)
class UserContext:
    user_id: str
    display_name: str
    email: str | None = None
    is_authenticated: bool = False


def initialize_auth() -> None:
    if _should_skip_cookie_restore():
        return

    st.session_state[AUTH_RESTORE_ATTEMPTED_KEY] = True
    cookie = read_auth_cookie()
    if cookie is None:
        return

    try:
        user = UserApiClient().get_me(cookie["token"])
    except UserApiError as error:
        _handle_restore_error(error)
        return

    _store_user_session(
        user,
        cookie["token"],
        cookie["session_id"],
        cookie["expires_at"],
    )
    st.session_state.pop(AUTH_RESTORE_ERROR_KEY, None)


def render_auth_cookie_sync() -> None:
    _render_auth_cookie_sync()


def _should_skip_cookie_restore() -> bool:
    return (
        bool(get_auth_token())
        or bool(st.session_state.get(AUTH_RESTORE_ATTEMPTED_KEY))
        or has_pending_auth_cookie_clear()
    )


def get_current_user() -> UserContext:
    initialize_auth()
    user = st.session_state.get(AUTH_USER_KEY)
    if isinstance(user, dict):
        return UserContext(**user)

    return UserContext(
        user_id=ANONYMOUS_USER_ID,
        display_name="Гость",
        is_authenticated=False,
    )


def get_auth_token() -> str | None:
    token = st.session_state.get(AUTH_TOKEN_KEY)

    return token if isinstance(token, str) else None


def get_auth_session_id() -> str | None:
    session_id = st.session_state.get(AUTH_SESSION_ID_KEY)

    return session_id if isinstance(session_id, str) else None


def login(email: str, password: str) -> UserContext | None:
    if not email.strip() or not password:
        return None

    payload = UserApiClient().login(email.strip(), password)
    return _store_auth_payload(payload)


def register(email: str, password: str, display_name: str | None) -> UserContext | None:
    if not email.strip() or not password:
        return None

    payload = UserApiClient().register(email.strip(), password, display_name)
    return _store_auth_payload(payload)


def logout() -> None:
    token = get_auth_token()

    try:
        if token:
            UserApiClient().logout(token)
    finally:
        _clear_auth_state()


def _store_auth_payload(payload: dict) -> UserContext:
    session_id = str(payload["session_id"])
    expires_at = str(payload["expires_at"])
    context = _store_user_session(
        payload["user"],
        payload["token"],
        session_id,
        expires_at,
    )

    schedule_auth_cookie(payload["token"], session_id, expires_at)

    return context


def _store_user_session(
    user: dict,
    token: str,
    session_id: str,
    expires_at: str,
) -> UserContext:
    context = UserContext(
        user_id=str(user["id"]),
        display_name=user["display_name"],
        email=user["email"],
        is_authenticated=True,
    )

    st.session_state[AUTH_USER_KEY] = context.__dict__
    st.session_state[AUTH_TOKEN_KEY] = token
    st.session_state[AUTH_SESSION_ID_KEY] = session_id
    st.session_state[AUTH_EXPIRES_AT_KEY] = expires_at

    return context


def _clear_auth_state() -> None:
    st.session_state.pop(AUTH_USER_KEY, None)
    st.session_state.pop(AUTH_TOKEN_KEY, None)
    st.session_state.pop(AUTH_SESSION_ID_KEY, None)
    st.session_state.pop(AUTH_EXPIRES_AT_KEY, None)
    st.session_state.pop(AUTH_RESTORE_ERROR_KEY, None)
    schedule_auth_cookie_clear()


def _handle_restore_error(error: UserApiError) -> None:
    if error.status_code == 401:
        _clear_auth_state()
        return

    st.session_state[AUTH_RESTORE_ERROR_KEY] = str(error)
