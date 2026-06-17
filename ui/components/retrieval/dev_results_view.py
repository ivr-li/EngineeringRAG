import streamlit as st
from config import RetrievalResult, UIConfig

SCORE_THRESHOLD = 0


def _score_dot(score: float, mode: UIConfig.SearchMode) -> str:
    if mode == "hybrid":
        return "🟢" if score >= 15 else ("🟡" if score >= 8 else "🔴")
    return "🟢" if score >= 0.02 else ("🟡" if score >= 0.01 else "🔴")


class ResultsView:
    """A user response and debug view of results"""

    def render(self) -> None:
        response = st.session_state.get("search_response", {})
        if not response:
            self._render_empty_prompt()
            return

        results: list[RetrievalResult] = [
            RetrievalResult(**r) for r in response.get("results", [])
        ]
        meta: dict = st.session_state.get("meta", {})
        user_answer_md: str | None = response.get("answer")

        tab_user, tab_dev = st.tabs(["Ответ", "Отладка"])

        with tab_user:
            self._render_user_tab(results, meta, user_answer_md)

        with tab_dev:
            self._render_dev_tab(results, meta)

    def _render_user_tab(
        self,
        results: list[RetrievalResult],
        meta: dict,
        user_answer_md: str | None,
    ) -> None:
        # st.markdown("#### Ответ для пользователя")
        if user_answer_md:
            st.markdown(user_answer_md)
        elif not meta.get("generate_user_answer", True):
            st.info("Генерация пользовательского ответа отключена в боковой панели.")
        elif not results:
            st.info("Ничего не найдено.")
        else:
            st.warning(
                "Пользовательский ответ пока недоступен, но найденные фрагменты доступны во вкладке «Отладка»."
            )

        if results and results[0].score < SCORE_THRESHOLD:
            st.caption(
                "Релевантность низкая: ответ стоит перепроверить по исходным фрагментам."
            )

    def _render_dev_tab(self, results: list[RetrievalResult], meta: dict) -> None:
        st.markdown("#### Отладочное представление")
        st.divider()
        self._render_metrics(results, meta)

        # Show rewrite info from meta
        if meta.get("was_rewritten"):
            st.markdown(
                f"**Исходный запрос:** {meta.get('query', '')}\n\n"
                f"**Переформулированный:** {meta.get('effective_query', '')}"
            )
        else:
            st.markdown(f"**Исходный запрос:** {meta.get('query', '')}")

        if not results:
            st.info("Ничего не найдено.")
            return

        for idx, result in enumerate(results, start=1):
            self._render_chunk(idx, result, meta.get("mode", "hybrid"))

    def _render_metrics(self, results: list[RetrievalResult], meta: dict) -> None:
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Найдено", len(results))
        m2.metric("Режим", meta.get("mode", "—").upper())
        m3.metric(
            "prefetch_k",
            meta.get("prefetch_k", "—") if meta.get("mode") == "hybrid" else "—",
        )
        m4.metric("Таблиц", sum(1 for r in results if r.is_table))
        m5.metric("Макс. score", f"{max((r.score for r in results), default=0):.4f}")

    def _render_chunk(
        self, idx: int, result: RetrievalResult, mode: UIConfig.SearchMode
    ) -> None:
        dot = _score_dot(result.score, mode)
        kind = "Таблица" if result.is_table else "Текст"
        overlap_badge = " 🔁" if result.is_overlap_window else ""
        expanded_info = (
            f" · ref `{result.expanded_from}`" if result.expanded_from else ""
        )
        section_info = f" · `{result.section_path}`" if result.section_path else ""
        table_info = _table_label(result)

        label = (
            f"{dot} **#{idx}** `score: {result.score:.4f}` · "
            f"{kind}{table_info}{overlap_badge}{expanded_info}{section_info} · "
            f"`{result.filename}` · chunk #{result.chunk_index}"
        )

        with st.expander(label, expanded=(idx <= 3)):
            self._render_headings(result)
            self._render_text(result)
            self._render_refs(result)
            self._render_metadata_popover(result)

    def _render_headings(self, result: RetrievalResult) -> None:
        if result.headings:
            st.markdown(" › ".join(f"**{h}**" for h in result.headings))

    def _render_text(self, result: RetrievalResult) -> None:
        st.markdown("**Текст чанка**")
        if result.is_table:
            st.code(result.text, language=None)
        else:
            st.markdown(result.text)

    def _render_refs(self, result: RetrievalResult) -> None:
        if result.man_refs:
            badges_man = " ".join(
                f'<span style="background:#1a6b4a;color:#fff;padding:2px 6px;border-radius:4px;font-size:0.75em">{ref}</span>'
                for ref in result.man_refs
            )
            st.markdown(f"**Нормативные:** {badges_man}", unsafe_allow_html=True)

        if result.cross_refs:
            badges_cross = " ".join(
                f'<span style="background:#1a3a6b;color:#fff;padding:2px 6px;border-radius:4px;font-size:0.75em">{ref}</span>'
                for ref in result.cross_refs
            )
            st.markdown(f"**Ссылки:** {badges_cross}", unsafe_allow_html=True)

        if result.anchor_refs:
            badges_anchor = " ".join(
                f'<span style="background:#5a4b13;color:#fff;padding:2px 6px;border-radius:4px;font-size:0.75em">{ref}</span>'
                for ref in result.anchor_refs
            )
            st.markdown(f"**Якоря:** {badges_anchor}", unsafe_allow_html=True)

    def _render_metadata_popover(self, result: RetrievalResult) -> None:
        with st.popover("Метаданные (JSON)"):
            st.json(
                {
                    "id": result.id,
                    "score": result.score,
                    "filename": result.filename,
                    "chunk_index": result.chunk_index,
                    "is_table": result.is_table,
                    "headings": result.headings,
                    "section_path": result.section_path,
                    "section_level": result.section_level,
                    "parent_heading": result.parent_heading,
                    "leaf_heading": result.leaf_heading,
                    "is_overlap_window": result.is_overlap_window,
                    "window_index": result.window_index,
                    "table_id": result.table_id,
                    "table_caption": result.table_caption,
                    "table_part_index": result.table_part_index,
                    "table_part_total": result.table_part_total,
                    "table_window_index": result.table_window_index,
                    "table_window_total": result.table_window_total,
                    "table_orientation": result.table_orientation,
                    "man_refs": result.man_refs,
                    "cross_refs": result.cross_refs,
                    "anchor_refs": result.anchor_refs,
                    "expanded_from": result.expanded_from,
                }
            )

    @staticmethod
    def _render_empty_prompt() -> None:
        st.markdown("Введите запрос и нажмите **Найти**")


def _table_label(result: RetrievalResult) -> str:
    if not result.is_table:
        return ""
    part = _index_label(result.table_part_index, result.table_part_total)
    window = _index_label(result.table_window_index, result.table_window_total)
    details = " ".join(value for value in [part, window] if value)
    return f" `{details}`" if details else ""


def _index_label(index: int | None, total: int | None) -> str:
    if index is None or total is None or total <= 1:
        return ""
    return f"{index}/{total}"
