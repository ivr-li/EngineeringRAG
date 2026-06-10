from datetime import datetime, timedelta

import streamlit as st

HISTORY_KEY = "user_ui_history"
SELECTED_SEARCH_KEY = "user_ui_selected_search"


def initialize_history() -> None:
    st.session_state.setdefault(HISTORY_KEY, [])
    st.session_state.setdefault(SELECTED_SEARCH_KEY, None)

    for item in st.session_state[HISTORY_KEY]:
        item.setdefault("created_at", datetime.now().astimezone().isoformat())


def render_history_sidebar() -> None:
    with st.sidebar:
        _render_history_actions()
        st.caption("ИСТОРИЯ ПОИСКА")
        _render_history_groups()


def get_selected_search() -> dict | None:
    selected_id = st.session_state[SELECTED_SEARCH_KEY]
    return next(
        (item for item in st.session_state[HISTORY_KEY] if item["id"] == selected_id),
        None,
    )


def add_search(search: dict) -> None:
    st.session_state[HISTORY_KEY].append(search)
    st.session_state[SELECTED_SEARCH_KEY] = search["id"]


def _render_history_actions() -> None:
    new_search, clear_history = st.columns([1, 1])
    if new_search.button("＋ Новый", use_container_width=True):
        st.session_state[SELECTED_SEARCH_KEY] = None
        st.rerun()

    with clear_history.popover(
        "Очистить",
        disabled=not st.session_state[HISTORY_KEY],
        use_container_width=True,
    ):
        st.caption("Удалить всю историю текущей сессии?")
        if st.button("Удалить историю", type="primary", use_container_width=True):
            st.session_state[HISTORY_KEY] = []
            st.session_state[SELECTED_SEARCH_KEY] = None
            st.rerun()


def _render_history_groups() -> None:
    history = st.session_state[HISTORY_KEY]
    if not history:
        st.caption("Здесь появятся ваши запросы")
        return

    current_group = None
    for item in reversed(history):
        group = _date_group(item["created_at"])
        if group != current_group:
            st.caption(group)
            current_group = group
        _render_history_item(item)


def _render_history_item(item: dict) -> None:
    search_button, delete_button = st.columns([0.88, 0.12], gap="small")
    is_selected = item["id"] == st.session_state[SELECTED_SEARCH_KEY]
    if search_button.button(
        _history_label(item["query"], item["created_at"], is_selected),
        key=f"history_{item['id']}",
        use_container_width=True,
    ):
        st.session_state[SELECTED_SEARCH_KEY] = item["id"]
        st.rerun()

    if delete_button.button(
        "🗑",
        key=f"delete_{item['id']}",
        help="Удалить запрос",
        type="tertiary",
    ):
        _delete_search(item["id"])
        st.rerun()


def _delete_search(search_id: str) -> None:
    history = st.session_state[HISTORY_KEY]
    st.session_state[HISTORY_KEY] = [item for item in history if item["id"] != search_id]
    if st.session_state[SELECTED_SEARCH_KEY] == search_id:
        st.session_state[SELECTED_SEARCH_KEY] = None


def _history_label(query: str, created_at: str, is_selected: bool) -> str:
    label = query if len(query) <= 30 else f"{query[:27]}..."
    time = datetime.fromisoformat(created_at).astimezone().strftime("%H:%M")
    prefix = "› " if is_selected else ""
    return f"{prefix}{time} · {label}"


def _date_group(created_at: str) -> str:
    created_date = datetime.fromisoformat(created_at).astimezone().date()
    today = datetime.now().astimezone().date()

    if created_date == today:
        return "Сегодня"
    if created_date == today - timedelta(days=1):
        return "Вчера"
    return "Ранее"
