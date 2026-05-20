from typing import Literal

import streamlit as st
import structlog
from openai import OpenAI
from retriever.retriever import QdrantRetriever, RetrievalResult

SearchMode = Literal["hybrid", "dense", "sparse"]

SCORE_THRESHOLD = 0.0
log = structlog.get_logger(__name__)


# ========= Singleton retriever =========
@st.cache_resource(show_spinner="Подключение к Qdrant и загрузка моделей…")
def _load_retriever() -> QdrantRetriever:
    return QdrantRetriever()


# ========= Score indicator =========
def _score_dot(score: float, mode: SearchMode) -> str:
    if mode == "hybrid":
        return "🟢" if score >= 15 else ("🟡" if score >= 8 else "🔴")
    return "🟢" if score >= 0.02 else ("🟡" if score >= 0.01 else "🔴")


class QueryRewriter:
    """
    Переформулирует запрос через vllm-light.
    При недоступности модели возвращает оригинальный запрос.
    """

    REWRITE_SYSTEM_PROMPT = """
    Ты — ассистент по переформулированию поисковых запросов для системы RAG.
    Твоя задача: преобразовать вопрос пользователя в точный поисковый запрос,
    пригодный для векторного поиска по нормативным документам (СП, ГОСТ, СНиП).

    Правила:
    - Верни ТОЛЬКО переформулированный запрос, без пояснений и кавычек.
    - Убери разговорные обороты («расскажи мне», «хочу узнать» и т.п.).
    - Сохрани все технические термины, номера стандартов, классы материалов.
    - Добавь релевантные синонимы и уточнения области применения, если очевидны.
    - Длина ответа — не более двух предложений.
    """
    REWRITER_BASE_URL = "http://localhost:8020/v1"
    REWRITER_MODEL = "query-rewriter"

    def __init__(self, timeout: float = 30.0) -> None:
        self.client = OpenAI(
            base_url=self.REWRITER_BASE_URL,
            api_key="",
            timeout=timeout,
        )

    def rewrite(self, query: str) -> tuple[str, bool]:
        try:
            resp = self.client.chat.completions.create(
                model=self.REWRITER_MODEL,
                messages=[
                    {"role": "system", "content": self.REWRITE_SYSTEM_PROMPT},
                    {"role": "user", "content": query},
                ],
                temperature=0.2,
                max_tokens=256,
            )
            rewritten = (resp.choices[0].message.content or "").strip()
            if not rewritten:
                return query, False
            return rewritten, True
        except Exception as e:
            log.error("rewriter_error", query=query, error=str(e))
            return query, False


class SidebarParams:
    """Считывает все параметры поиска из боковой панели."""

    def __init__(self) -> None:
        with st.sidebar:
            st.title("Параметры поиска")

            self.mode: SearchMode = st.selectbox(
                "Режим поиска",
                options=["hybrid", "dense", "sparse"],
                index=0,
                help=(
                    "**hybrid** — dense + sparse Prefetch → ColBERT rerank (рекомендуется)\n\n"
                    "**dense** — только BGE-M3 dense (ANN).\n\n"
                    "**sparse** — только BGE-M3 BM25 (ключевые слова)."
                ),
            )
            self.top_k: int = st.slider("top_k — финальных результатов", 1, 20, 10)
            self.prefetch_k: int = self._render_prefetch_k()

            st.divider()
            self.only_tables, self.filename_filter, self.section_filter = self._render_filters()

            st.divider()
            self.use_rewriter: bool = st.toggle(
                "Переформулировать запрос",
                value=True,
                help="Использует vllm-light для преобразования вопроса в поисковый запрос.",
            )

            st.divider()
            st.caption(f"Коллекция: `{_load_retriever().collection}`\n\nBGE-M3 · BM25 · ColBERTv2")

    def _render_prefetch_k(self) -> int:
        if self.mode != "hybrid":
            return self.top_k * 4
        return st.slider(
            "prefetch_k — кандидатов для ColBERT rerank",
            min_value=self.top_k,
            max_value=100,
            value=min(self.top_k * 4, 100),
            help="Кандидатов из dense+sparse перед rerank. Больше = точнее, медленнее.",
        )

    def _render_filters(self) -> tuple[bool | None, str | None, str | None]:
        table_filter = st.radio(
            "Тип чанков",
            options=["Все", "Только текст", "Только таблицы"],
            index=0,
            horizontal=True,
        )
        only_tables: bool | None = {
            "Все": None,
            "Только текст": False,
            "Только таблицы": True,
        }[table_filter]

        filename_filter = (
            st.text_input("Фильтр по файлу (filename)", placeholder="sp_63_13330") or None
        )
        # NEW: section filter
        section_filter = (
            st.text_input(
                "Фильтр по разделу (section_path)",
                placeholder="6 Расчёт или 6.3",
                help=(
                    "Подстрока в поле section_path.\n\n"
                    "Пример: **6.3** вернёт только чанки из раздела 6.3.x"
                ),
            )
            or None
        )
        return only_tables, filename_filter, section_filter


class SearchBar:
    """Поле ввода запроса и кнопки управления."""

    def __init__(self) -> None:
        self.query: str = st.text_input(
            "Запрос",
            placeholder="арматура для железобетона",
            label_visibility="collapsed",
        )
        col_search, col_clear, _ = st.columns([1, 1, 6])
        with col_search:
            self.search_clicked = st.button("Найти", type="primary", use_container_width=True)
        with col_clear:
            if st.button("Сбросить", use_container_width=True):
                st.session_state.pop("results", None)
                st.session_state.pop("meta", None)
                st.rerun()

    @property
    def should_search(self) -> bool:
        return self.search_clicked and bool(self.query.strip())


class SearchRunner:
    """
    Опционально переформулирует запрос через QueryRewriter,
    затем выполняет поиск и кладёт результаты в session_state.
    """

    def __init__(self, retriever: QdrantRetriever) -> None:
        self._retriever = retriever
        self._rewriter = QueryRewriter()

    def run(self, query: str, params: SidebarParams) -> None:
        effective_query, rewritten = self._maybe_rewrite(query, params)
        results = self._fetch_results(effective_query, params)
        self._validate_and_store(results, query, effective_query, params, rewritten)

    def _maybe_rewrite(self, query: str, params: SidebarParams) -> tuple[str, bool]:
        if not params.use_rewriter:
            return query, False
        with st.spinner("Переформулирование запроса…"):
            return self._rewriter.rewrite(query)

    @staticmethod
    def _show_rewrite_info(original: str, effective: str, was_rewritten: bool) -> None:
        if was_rewritten:
            st.info(f"**Исходный запрос:** {original}\n\n**Переформулированный:** {effective}")

    def _fetch_results(self, query: str, params: SidebarParams) -> list[RetrievalResult]:
        label = (
            f"hybrid + ColBERT rerank (prefetch_k={params.prefetch_k})"
            if params.mode == "hybrid"
            else params.mode
        )
        with st.spinner(f"Режим: {label}, top_k={params.top_k}…"):
            return self._retriever.search(
                query=query,
                top_k=params.top_k,
                prefetch_k=params.prefetch_k,
                mode=params.mode,
                only_tables=params.only_tables,
                filename_filter=params.filename_filter,
                section_filter=params.section_filter,  # NEW
            )

    @staticmethod
    def _validate_and_store(
        results: list[RetrievalResult],
        original_query: str,
        effective_query: str,
        params: SidebarParams,
        rewritten: bool,
    ) -> None:
        # if not results or results[0].score < SCORE_THRESHOLD:
        #     st.warning("Документ по этой теме отсутствует в базе")
        if results:
            st.caption(f"Лучший скор: {results[0].score:.4f} (порог: {SCORE_THRESHOLD})")
        # st.stop()

        st.session_state["results"] = results
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
        )


class ResultsView:
    """Отображает метрики и список найденных чанков."""

    def render(self) -> None:
        if "results" not in st.session_state:
            self._render_empty_prompt()
            return

        results: list[RetrievalResult] = st.session_state["results"]
        meta: dict = st.session_state.get("meta", {})

        st.divider()
        self._render_metrics(results, meta)

        # label_query = meta.get("effective_query") or meta.get("query", "")
        st.markdown("#### Результат:\n")
        print(meta.get("effective_query", ""))
        if meta.get("was_rewritten"):
            st.markdown(
                f"**Исходный запрос:** {meta.get('query', '')}\n\n"
                f"**Переформулированный:** {meta.get('effective_query', '')}"
            )
        else:
            st.markdown(f"**Исходный запрос:** {meta.get('query', '')}")
        # st.markdown(f"*Перефраз: *«{meta.get('effective_query

        if not results:
            st.info("Ничего не найдено.")
        else:
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
        self,
        idx: int,
        result: RetrievalResult,
        mode: SearchMode,
    ) -> None:
        dot = _score_dot(result.score, mode)
        kind = "Таблица" if result.is_table else "Текст"

        overlap_badge = "🔁" if result.is_overlap_window else ""
        section_info = f" · `{result.section_path}`" if result.section_path else ""

        label = (
            f"{dot} **#{idx}** `score: {result.score:.4f}` · "
            f"{kind}{overlap_badge}{section_info} · "
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
        # Нормативные ссылки (man_refs)
        if result.man_refs:
            badges_man = " ".join(
                f'<span style="background:#1a6b4a;color:#fff;'
                f'padding:2px 6px;border-radius:4px;font-size:0.75em">'
                f"{ref}</span>"
                for ref in result.man_refs
            )
            st.markdown(f"**Нормативные:** {badges_man}", unsafe_allow_html=True)

        refs = result.cross_refs
        if refs:
            badges_cross = " ".join(
                f'<span style="background:#1a3a6b;color:#fff;'
                f'padding:2px 6px;border-radius:4px;font-size:0.75em">'
                f"{ref}</span>"
                for ref in refs
            )
            st.markdown(f"**Ссылки:** {badges_cross}", unsafe_allow_html=True)

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
                    # NEW
                    "section_path": result.section_path,
                    "section_level": result.section_level,
                    "parent_heading": result.parent_heading,
                    "leaf_heading": result.leaf_heading,
                    "is_overlap_window": result.is_overlap_window,
                    "window_index": result.window_index,
                    "man_refs": result.man_refs,
                    "cross_refs": result.cross_refs,
                }
            )

    @staticmethod
    def _render_empty_prompt() -> None:
        st.markdown(
            "Введите запрос и нажмите **Найти**\n\n"
            "hybrid (dense + sparse → ColBERT rerank) · dense · sparse"
        )


def main() -> None:
    st.set_page_config(
        page_title="Construction RAG",
        page_icon="🏗️",
        layout="wide",
    )
    st.title("🏗️ Поиск по нормативной документации")

    retriever = _load_retriever()
    params = SidebarParams()
    search_bar = SearchBar()

    if search_bar.should_search:
        SearchRunner(retriever).run(search_bar.query, params)

    ResultsView().render()


if __name__ == "__main__":
    main()
