from collections import defaultdict
from datetime import date, datetime, timedelta

import streamlit as st
from core.auth import ANONYMOUS_USER_ID, get_auth_token
from core.user_api_client import UserApiClient, UserApiError

HISTORY_KEY = "user_ui_history"
HISTORY_BY_USER_KEY = "user_ui_history_by_user"
SELECTED_SEARCH_KEY = "user_ui_selected_search"
CURRENT_USER_KEY = "user_ui_current_user_id"
SIDEBAR_COMPACT_KEY = "user_ui_sidebar_compact"
HISTORY_LOADED_USER_KEY = "user_ui_history_loaded_user"
HISTORY_ERROR_KEY = "user_ui_history_error"


def is_sidebar_compact() -> bool:
    return bool(st.session_state.get(SIDEBAR_COMPACT_KEY, False))


def initialize_history(user_id: str = "anonymous_user") -> None:
    histories = st.session_state.setdefault(HISTORY_BY_USER_KEY, {})
    if _uses_remote_history(user_id):
        _load_remote_history(histories, user_id)
    else:
        _migrate_current_history(histories, user_id)

    if st.session_state.get(CURRENT_USER_KEY) != user_id:
        st.session_state[SELECTED_SEARCH_KEY] = None

    st.session_state[CURRENT_USER_KEY] = user_id
    st.session_state[HISTORY_KEY] = histories.setdefault(user_id, [])
    st.session_state.setdefault(SELECTED_SEARCH_KEY, None)

    for item in st.session_state[HISTORY_KEY]:
        item.setdefault("created_at", datetime.now().astimezone().isoformat())


def render_history_sidebar(user_id: str = "anonymous_user") -> None:
    with st.sidebar:
        _render_history_actions()

        st.caption("История")
        _render_history_error()
        _render_history_groups()


def get_selected_search() -> dict | None:
    selected_id = st.session_state[SELECTED_SEARCH_KEY]
    return next(
        (item for item in st.session_state[HISTORY_KEY] if item["id"] == selected_id),
        None,
    )


def clear_selected_search() -> None:
    st.session_state[SELECTED_SEARCH_KEY] = None


def add_search(search: dict) -> None:
    stored_search = _create_remote_search(search) or search

    st.session_state[HISTORY_KEY].append(stored_search)
    st.session_state[SELECTED_SEARCH_KEY] = stored_search["id"]


def update_search(search_id: str, query: str, response: dict) -> None:
    remote_search = _update_remote_search(search_id, query, response)

    for index, item in enumerate(st.session_state[HISTORY_KEY]):
        if item["id"] != search_id:
            continue

        st.session_state[HISTORY_KEY][index] = remote_search or _updated_search(
            item,
            query,
            response,
        )
        st.session_state[SELECTED_SEARCH_KEY] = search_id
        return


def _render_history_actions() -> None:
    with st.container(key="history_actions"):
        toggle_column, search_column = st.columns([0.26, 0.74], gap="small")
        with toggle_column:
            toggled = st.button(
                "",
                icon=_sidebar_toggle_icon(),
                key="sidebar_toggle",
                help="Развернуть панель" if is_sidebar_compact() else "Свернуть панель",
                width="stretch",
            )
        with search_column:
            new_search = st.button(
                "Новый",
                icon=":material/add:",
                key="new_search",
                width="stretch",
            )

    if toggled:
        st.session_state[SIDEBAR_COMPACT_KEY] = not is_sidebar_compact()
        st.rerun()

    if new_search:
        clear_selected_search()
        st.rerun()


def _sidebar_toggle_icon() -> str:
    if is_sidebar_compact():
        return ":material/keyboard_double_arrow_right:"
    return ":material/keyboard_double_arrow_left:"


def _render_history_groups() -> None:
    history = st.session_state[HISTORY_KEY]
    if not history:
        st.caption("Здесь появятся ваши запросы")
        return

    for group_date, items in _group_history_by_date(history):
        with st.expander(
            _date_group_label(group_date),
            expanded=_is_today(group_date),
            icon=":material/calendar_today:",
        ):
            for item in items:
                _render_history_item(item)


def _render_history_item(item: dict) -> None:
    is_selected = item["id"] == st.session_state[SELECTED_SEARCH_KEY]
    selected, deleted = _render_history_item_buttons(item, is_selected)

    if selected:
        st.session_state[SELECTED_SEARCH_KEY] = item["id"]
        st.rerun()

    if deleted:
        _delete_search(item["id"])
        st.rerun()


def _render_history_item_buttons(item: dict, is_selected: bool) -> tuple[bool, bool]:
    item_key = "history_item_selected" if is_selected else "history_item"
    with st.container(key=f"{item_key}_{item['id']}"):
        selected = st.button(
            _history_label(item["query"], item["created_at"]),
            key=f"history_{item['id']}",
            help=item["query"],
            width="stretch",
        )
        deleted = _render_history_item_menu(item)

    return selected, deleted


def _render_history_item_menu(item: dict) -> bool:
    with st.popover(
        "⋯",
        key=f"chat_menu_{item['id']}",
        help="Действия с чатом",
        type="tertiary",
        width="content",
    ):
        st.caption("Удалить этот чат?")
        return st.button(
            "Удалить чат",
            key=f"delete_{item['id']}",
            type="primary",
            width="stretch",
        )


def _history_label(query: str, created_at: str) -> str:
    time = datetime.fromisoformat(created_at).astimezone().strftime("%H:%M")

    return f"{time} · {query}"


def _delete_search(search_id: str) -> None:
    if not _delete_remote_search(search_id):
        return

    history = st.session_state[HISTORY_KEY]
    st.session_state[HISTORY_KEY] = [item for item in history if item["id"] != search_id]
    if st.session_state[SELECTED_SEARCH_KEY] == search_id:
        st.session_state[SELECTED_SEARCH_KEY] = None


def _load_remote_history(histories: dict, user_id: str) -> None:
    if st.session_state.get(HISTORY_LOADED_USER_KEY) == user_id:
        return

    token = get_auth_token()
    if not token:
        return

    try:
        histories[user_id] = UserApiClient().list_searches(token)
    except UserApiError as error:
        st.session_state[HISTORY_ERROR_KEY] = str(error)
        histories.setdefault(user_id, [])
        return

    st.session_state[HISTORY_LOADED_USER_KEY] = user_id
    st.session_state.pop(HISTORY_ERROR_KEY, None)


def _create_remote_search(search: dict) -> dict | None:
    token = get_auth_token()
    if not token:
        return None

    try:
        return UserApiClient().create_search(token, search["query"], search["response"])
    except UserApiError as error:
        st.session_state[HISTORY_ERROR_KEY] = str(error)
        return None


def _update_remote_search(search_id: str, query: str, response: dict) -> dict | None:
    token = get_auth_token()
    if not token:
        return None

    try:
        return UserApiClient().update_search(token, search_id, query, response)
    except UserApiError as error:
        st.session_state[HISTORY_ERROR_KEY] = str(error)
        return None


def _delete_remote_search(search_id: str) -> bool:
    token = get_auth_token()
    if not token:
        return True

    try:
        UserApiClient().delete_search(token, search_id)
    except UserApiError as error:
        st.session_state[HISTORY_ERROR_KEY] = str(error)
        return False

    return True


def _updated_search(item: dict, query: str, response: dict) -> dict:
    updated_item = {**item}
    updated_item["query"] = query
    updated_item["response"] = response
    updated_item["updated_at"] = datetime.now().astimezone().isoformat()

    return updated_item


def _render_history_error() -> None:
    if st.session_state.get(HISTORY_ERROR_KEY):
        st.caption("История временно не синхронизирована.")


def _uses_remote_history(user_id: str) -> bool:
    return user_id != ANONYMOUS_USER_ID and get_auth_token() is not None


def _migrate_current_history(histories: dict, user_id: str) -> None:
    current_history = st.session_state.get(HISTORY_KEY)
    if not current_history or user_id in histories:
        return

    histories[user_id] = current_history


def _group_history_by_date(history: list[dict]) -> list[tuple[date, list[dict]]]:
    groups = defaultdict(list)
    for item in sorted(history, key=_created_at, reverse=True):
        groups[_created_at(item).date()].append(item)

    return sorted(groups.items(), key=lambda group: group[0], reverse=True)


def _created_at(item: dict) -> datetime:
    return datetime.fromisoformat(item["created_at"]).astimezone()


def _date_group_label(group_date: date) -> str:
    today = datetime.now().astimezone().date()

    if group_date == today:
        return f"Сегодня · {group_date:%d.%m.%Y}"
    if group_date == today - timedelta(days=1):
        return f"Вчера · {group_date:%d.%m.%Y}"
    return f"{group_date:%d.%m.%Y}"


def _is_today(group_date: date) -> bool:
    return group_date == datetime.now().astimezone().date()
