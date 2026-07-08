import streamlit as st
from config import UIConfig

SearchMode = UIConfig.SearchMode


class SearchBar:
    """The query input field and control buttons"""

    def __init__(self) -> None:
        self.query: str = st.text_input(
            "Запрос",
            placeholder="арматура для железобетона",
            label_visibility="collapsed",
        )
        col_search, col_clear, _ = st.columns([1, 1, 6])
        with col_search:
            self.search_clicked = st.button(
                "Найти", type="primary", width="stretch"
            )
        with col_clear:
            if st.button("Сбросить", width="stretch"):
                st.session_state.pop("results", None)
                st.session_state.pop("meta", None)
                st.rerun()

    @property
    def should_search(self) -> bool:
        return self.search_clicked and bool(self.query.strip())
