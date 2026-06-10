import streamlit as st
from components import (
    ResultsView,
    RetrieverClient,
    SearchBar,
    SidebarParams,
)


def main() -> None:
    st.set_page_config(
        page_title="Интерфейс разработчика",
        page_icon="🏗️",
        layout="wide",
    )
    st.title("Интерфейс разработчика")

    params = SidebarParams()
    search_bar = SearchBar()
    client = RetrieverClient()

    if search_bar.should_search:
        with st.spinner("Обработка запроса…"):
            response = client.search(
                query=search_bar.query,
                rewrite_system_prompt=params.rewrite_system_prompt,
                compose_system_prompt=params.compose_system_prompt,
                top_k=params.top_k,
                prefetch_k=params.prefetch_k,
                mode=params.mode,
                only_tables=params.only_tables,
                filename_filter=params.filename_filter,
                section_filter=params.section_filter,
            )
            # print(response)
            st.session_state["search_response"] = response
            st.session_state["meta"] = dict(
                query=search_bar.query,
                effective_query=response.get("effective_query", search_bar.query),
                was_rewritten=response.get("was_rewritten", False),
                mode=params.mode,
                top_k=params.top_k,
                prefetch_k=params.prefetch_k if params.mode == "hybrid" else None,
                only_tables=params.only_tables,
                filename_filter=params.filename_filter,
                section_filter=params.section_filter,
                generate_user_answer=params.generate_user_answer,
            )
            st.rerun()
    else:
        ResultsView().render()


if __name__ == "__main__":
    main()
