import streamlit as st

from ui.components.sidebar import SidebarParams
from ui.config import LLMData
from ui.dataclasses import RetrievalResult
from ui.services.answer_composer import AnswerComposer
from ui.services.query_logger import QueryLogger
from ui.services.query_rewriter import QueryRewriter
from ui.services.retriever_client import RetrieverClient

class SearchRunner:
    """
    Опционально переформулирует запрос через QueryRewriter,
    затем выполняет поиск и кладёт результаты в session_state.
    """

    def __init__(self) -> None:
        self.client = RetrieverClient()

        # self._rewriter = QueryRewriter()
        # self._answer_composer = AnswerComposer()
        # self._logger = QueryLogger()

    def run(self, query: str, params: SidebarParams) -> None:
        effective_query, rewritten = self._maybe_rewrite(query, params)
        results = self._fetch_results(effective_query, params)
        user_answer_md = self._build_user_answer(query, effective_query, results, params)
        self._validate_and_store(
            results,
            query,
            effective_query,
            params,
            rewritten,
            user_answer_md,
            self._logger,
        )

    def _maybe_rewrite(self, query: str, params: SidebarParams) -> tuple[str, bool]:
        if not params.use_rewriter:
            return query, False
        with st.spinner("Переформулирование запроса…"):
            return self.client.rewrite()

    def _build_user_answer(
        self,
        query: str,
        effective_query: str,
        results: list[RetrievalResult],
        params: SidebarParams,
    ) -> str | None:
        if not params.generate_user_answer:
            return None
        with st.spinner("Формирование пользовательского ответа…"):
            return self._answer_composer.compose(query, effective_query, results)

    @staticmethod
    def _show_rewrite_info(original: str, effective: str, was_rewritten: bool) -> None:
        if was_rewritten:
            st.info(
                f"**Исходный запрос:** {original}\n\n**Переформулированный:** {effective}"
            )

    def _fetch_results(self, query: str, params: SidebarParams) -> list[RetrievalResult]:
        label = (
            f"hybrid + ColBERT rerank (prefetch_k={params.prefetch_k})"
            if params.mode == "hybrid"
            else params.mode
        )
        with st.spinner(f"Режим: {label}, top_k={params.top_k}…"):
            self.client.search(
                            #     query=query,
            #     top_k=params.top_k,
            #     prefetch_k=params.prefetch_k,
            #     mode=params.mode,
            #     only_tables=params.only_tables,
            #     filename_filter=params.filename_filter,
            #     section_filter=params.section_filter,
            )
            # return self._retriever.search(
            #     query=query,
            #     top_k=params.top_k,
            #     prefetch_k=params.prefetch_k,
            #     mode=params.mode,
            #     only_tables=params.only_tables,
            #     filename_filter=params.filename_filter,
            #     section_filter=params.section_filter,
            # )
    def _rewrite(self):

    @staticmethod
    def _validate_and_store(
        results: list[RetrievalResult],
        original_query: str,
        effective_query: str,
        params: SidebarParams,
        rewritten: bool,
        user_answer_md: str | None,
        logger: QueryLogger,
    ) -> None:
        logger.log(
            query=original_query,
            effective_query=effective_query,
            was_rewritten=rewritten,
            mode=params.mode,
            top_k=params.top_k,
            results=results,
            user_answer_md=user_answer_md,
        )

        st.session_state["results"] = results
        st.session_state["user_answer_md"] = user_answer_md
        st.session_state["meta"] = dict(
            query=original_query,
            effective_query=effective_query,
            was_rewritten=rewritten,
            mode=params.mode,
            top_k=params.top_k,
            prefetch_k=params.prefetch_k if params.mode == "hybrid" else None,
            only_tables=params.only_tables,
            filename_filter=params.filename_filter,
            section_filter=params.section_filter,
            generate_user_answer=params.generate_user_answer,
        )
