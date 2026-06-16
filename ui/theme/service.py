import streamlit as st

from .models import DEFAULT_THEME_KEY, THEMES

THEME_BY_USER_KEY = "user_ui_theme_by_user"
THEME_SELECT_KEY = "user_ui_theme_select"


def initialize_theme(user_id: str) -> str:
    themes = st.session_state.setdefault(THEME_BY_USER_KEY, {})
    widget_theme = st.session_state.get(THEME_SELECT_KEY)
    selected_theme = widget_theme if widget_theme in THEMES else None

    if selected_theme:
        themes[user_id] = selected_theme

    return themes.setdefault(user_id, DEFAULT_THEME_KEY)


def render_theme_selector(user_id: str) -> None:
    theme_key = initialize_theme(user_id)
    theme_keys = list(THEMES)
    selected = st.selectbox(
        "Тема",
        options=theme_keys,
        index=theme_keys.index(theme_key),
        format_func=lambda key: THEMES[key].label,
        key=THEME_SELECT_KEY,
        width="stretch",
    )

    st.session_state[THEME_BY_USER_KEY][user_id] = selected


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
