import json
from datetime import UTC, datetime
from urllib.parse import quote, unquote

import streamlit as st

AUTH_COOKIE_NAME = "engineeringrag_auth"
AUTH_COOKIE_PATH = "/"
AUTH_COOKIE_PENDING_KEY = "user_ui_auth_cookie_pending"

_AUTH_COOKIE_SYNC_COMPONENT = st.components.v2.component(
    "auth_cookie_sync",
    html="<div></div>",
    js="""
export default function (component) {
  const { data } = component
  if (!data || typeof data.cookie !== "string") return

  document.cookie = data.cookie
}
""",
)


def read_auth_cookie() -> dict[str, str] | None:
    raw_cookie = _read_cookie(AUTH_COOKIE_NAME)
    if not raw_cookie:
        return None

    payload = _decode_cookie_payload(raw_cookie)
    if payload is None or _is_expired(payload["expires_at"]):
        return None

    return payload


def schedule_auth_cookie(token: str, session_id: str, expires_at: str) -> None:
    st.session_state[AUTH_COOKIE_PENDING_KEY] = {
        "action": "set",
        "token": token,
        "session_id": session_id,
        "expires_at": expires_at,
    }


def schedule_auth_cookie_clear() -> None:
    st.session_state[AUTH_COOKIE_PENDING_KEY] = {"action": "clear"}


def has_pending_auth_cookie_clear() -> bool:
    action = st.session_state.get(AUTH_COOKIE_PENDING_KEY)

    return isinstance(action, dict) and action.get("action") == "clear"


def render_auth_cookie_sync() -> None:
    action = st.session_state.pop(AUTH_COOKIE_PENDING_KEY, None)
    if not isinstance(action, dict):
        return

    if action.get("action") == "set":
        _render_cookie(_set_cookie_value(action))
        return

    if action.get("action") == "clear":
        _render_cookie(_clear_cookie_value(AUTH_COOKIE_NAME))


def _read_cookie(name: str) -> str | None:
    context = getattr(st, "context", None)
    cookies = getattr(context, "cookies", None)
    if not cookies:
        return None

    value = cookies.get(name)

    return value if isinstance(value, str) else None


def _decode_cookie_payload(raw_cookie: str) -> dict[str, str] | None:
    try:
        payload = json.loads(unquote(raw_cookie))
    except (TypeError, ValueError):
        return None

    if not isinstance(payload, dict):
        return None

    return _valid_cookie_payload(payload)


def _valid_cookie_payload(payload: dict) -> dict[str, str] | None:
    token = payload.get("token")
    session_id = payload.get("session_id")
    expires_at = payload.get("expires_at")
    values = (token, session_id, expires_at)
    if not all(isinstance(value, str) and value for value in values):
        return None

    return {"token": token, "session_id": session_id, "expires_at": expires_at}


def _set_cookie_value(action: dict) -> str:
    expires_at = str(action["expires_at"])
    max_age = _seconds_until(expires_at)
    if max_age <= 0:
        return _clear_cookie_value(AUTH_COOKIE_NAME)

    payload = _encoded_cookie_payload(action)

    return _cookie_assignment(AUTH_COOKIE_NAME, payload, max_age)


def _encoded_cookie_payload(action: dict) -> str:
    payload = {
        "token": str(action["token"]),
        "session_id": str(action["session_id"]),
        "expires_at": str(action["expires_at"]),
    }
    raw_payload = json.dumps(payload, separators=(",", ":"))

    return quote(raw_payload, safe="")


def _clear_cookie_value(name: str) -> str:
    return (
        f"{name}=; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT; "
        f"Path={AUTH_COOKIE_PATH}; SameSite=Lax"
    )


def _cookie_assignment(name: str, value: str, max_age: int) -> str:
    return (
        f"{name}={value}; Max-Age={max_age}; "
        f"Path={AUTH_COOKIE_PATH}; SameSite=Lax"
    )


def _render_cookie(cookie: str) -> None:
    _AUTH_COOKIE_SYNC_COMPONENT(
        data={"cookie": cookie},
        key=AUTH_COOKIE_PENDING_KEY,
        height="content",
    )


def _seconds_until(expires_at: str) -> int:
    expires_at_value = _parse_expires_at(expires_at)
    if expires_at_value is None:
        return 0

    return max(0, int((expires_at_value - datetime.now(UTC)).total_seconds()))


def _is_expired(expires_at: str) -> bool:
    expires_at_value = _parse_expires_at(expires_at)
    if expires_at_value is None:
        return True

    return expires_at_value <= datetime.now(UTC)


def _parse_expires_at(expires_at: str) -> datetime | None:
    try:
        expires_at_value = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except ValueError:
        return None

    if expires_at_value.tzinfo is None:
        return expires_at_value.replace(tzinfo=UTC)

    return expires_at_value.astimezone(UTC)
