import streamlit as st
from core.auth import ANONYMOUS_USER_ID, get_auth_token
from core.user_api_client import UserApiClient, UserApiError

from .models import DEFAULT_THEME_KEY, THEME_ORDER, THEMES

THEME_BY_USER_KEY = "user_ui_theme_by_user"
THEME_SELECT_KEY = "user_ui_theme_select"
THEME_LOADED_USER_KEY = "user_ui_theme_loaded_user"
THEME_ERROR_KEY = "user_ui_theme_error"


def initialize_theme(user_id: str) -> str:
    themes = st.session_state.setdefault(THEME_BY_USER_KEY, {})
    if _is_authenticated_user(user_id):
        _load_remote_theme(user_id, themes)

    return themes.setdefault(user_id, DEFAULT_THEME_KEY)


def render_theme_selector(user_id: str) -> None:
    theme_key = initialize_theme(user_id)
    theme_keys = _theme_keys()
    select_key = f"{THEME_SELECT_KEY}_{user_id}"

    _drop_invalid_selected_theme(select_key)
    st.selectbox(
        "Тема",
        options=theme_keys,
        index=theme_keys.index(theme_key),
        format_func=lambda key: THEMES[key].label,
        key=select_key,
        on_change=_save_selected_theme,
        args=(user_id, select_key),
        width="stretch",
    )

    _render_theme_error()


def build_theme_css(theme_key: str) -> str:
    theme = THEMES.get(theme_key, THEMES[DEFAULT_THEME_KEY])
    return f"""
<style>
:root {{
    --app-bg: {theme.background};
    --app-surface: {theme.surface};
    --app-sidebar: {theme.sidebar};
    --app-border: {theme.border};
    --app-text: {theme.text};
    --app-muted: {theme.muted};
    --app-accent: {theme.accent};
    --app-accent-soft: {theme.accent_soft};
    --app-primary-text: {theme.primary_text};
    --app-input-bg: {theme.input_bg};
}}
</style>
"""


def _load_remote_theme(user_id: str, themes: dict) -> None:
    if st.session_state.get(THEME_LOADED_USER_KEY) == user_id:
        return

    token = get_auth_token()
    if not token:
        return

    try:
        preferences = UserApiClient().get_preferences(token)
    except UserApiError as error:
        st.session_state[THEME_ERROR_KEY] = str(error)
        return

    themes[user_id] = _valid_theme(preferences.get("theme_key"))
    st.session_state[THEME_LOADED_USER_KEY] = user_id
    st.session_state.pop(THEME_ERROR_KEY, None)


def _save_theme(user_id: str, theme_key: str) -> None:
    themes = st.session_state.setdefault(THEME_BY_USER_KEY, {})
    if themes.get(user_id) == theme_key:
        return

    themes[user_id] = theme_key
    _save_remote_theme(user_id, theme_key)


def _save_selected_theme(user_id: str, select_key: str) -> None:
    selected = st.session_state.get(select_key)
    if not isinstance(selected, str):
        return

    _save_theme(user_id, _valid_theme(selected))


def _save_remote_theme(user_id: str, theme_key: str) -> None:
    token = get_auth_token()
    if not token or not _is_authenticated_user(user_id):
        return

    try:
        UserApiClient().update_preferences(token, theme_key)
    except UserApiError as error:
        st.session_state[THEME_ERROR_KEY] = str(error)


def _render_theme_error() -> None:
    if st.session_state.get(THEME_ERROR_KEY):
        st.caption("Тема временно не синхронизирована.")


def _valid_theme(theme_key: str | None) -> str:
    return theme_key if theme_key in THEMES else DEFAULT_THEME_KEY


def _drop_invalid_selected_theme(select_key: str) -> None:
    if st.session_state.get(select_key) in THEMES:
        return

    st.session_state.pop(select_key, None)


def _theme_keys() -> list[str]:
    return [theme_key for theme_key in THEME_ORDER if theme_key in THEMES]


def _is_authenticated_user(user_id: str) -> bool:
    return user_id != ANONYMOUS_USER_ID


def build_sidebar_layout_css(is_compact: bool) -> str:
    width = "4.25rem" if is_compact else "15rem"
    compact_css = SIDEBAR_COMPACT_CSS if is_compact else ""
    return f"""
<style>
:root {{
    --sidebar-width: {width};
}}
{compact_css}
</style>
"""


SIDEBAR_COMPACT_CSS = """
[data-testid="stSidebar"] [data-testid="stCaptionContainer"],
[data-testid="stSidebar"] [data-testid="stExpander"],
[data-testid="stSidebar"] [class*="st-key-history_item_"],
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"] {
    display: none !important;
}
[data-testid="stSidebar"] .stButton > button,
[data-testid="stSidebar"] [data-testid="stPopover"] button {
    justify-content: center !important;
    padding-left: 0 !important;
    padding-right: 0 !important;
}
[data-testid="stSidebar"] .stButton > button p,
[data-testid="stSidebar"] [data-testid="stPopover"] button p {
    display: none !important;
}
[data-testid="stSidebar"] [class*="st-key-history_actions"]
[data-testid="stColumn"]:first-child {
    flex: 1 1 100% !important;
    width: 100% !important;
}
[data-testid="stSidebar"] [class*="st-key-history_actions"]
[data-testid="stColumn"]:nth-child(2) {
    display: none !important;
}
[data-testid="stSidebar"] [class*="st-key-sidebar_footer"] {
    padding-left: 0.75rem;
    padding-right: 0.75rem;
}
"""
