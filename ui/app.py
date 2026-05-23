import streamlit as st

from ui.components.results_view import ResultsView
from ui.components.search_bar import SearchBar
from ui.components.sidebar import SidebarParams
from ui.services import search_runner


def main() -> None:
    st.set_page_config(
        page_title="Construction RAG",
        page_icon="🏗️",
        layout="wide",
    )
    st.title("🏗️ Поиск по нормативной документации")

    params = SidebarParams()
    search_bar = SearchBar()
    client = RetrieverClient()

    if search_bar.should_search:
        search_runner.run(search_bar.query, params)

    ResultsView().render()


if __name__ == "__main__":
    main()
