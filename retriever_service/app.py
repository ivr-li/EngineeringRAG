# import os
# import warnings

# os.environ["TRANSFORMERS_VERBOSITY"] = "error"
# warnings.filterwarnings("ignore", category=UserWarning, module="transformers")


import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
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


class LLMData:
    REWRITER_BASE_URL = "http://localhost:8020/v1"
    REWRITER_MODEL = "query-rewriter"

    ANSTER_BASE_URL = REWRITER_BASE_URL
    ANSTER_MODEL = REWRITER_MODEL
    ANSWER_CONTEXT_LIMIT = 6


class QueryLogger:
    def __init__(
        self, log_path: str | Path = "retriever_service/logs/user_queries_logs.json"
    ) -> None:
        self._path = Path(log_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict:
        if self._path.exists():
            try:
                return json.loads(self._path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                log.warning("query_log_corrupted", path=str(self._path))
        return {}

    def log(
        self,
        query: str,
        effective_query: str,
        was_rewritten: bool,
        mode: str,
        top_k: int,
        results: list[RetrievalResult],
        user_answer_md: str | None,
    ) -> None:
        data = self._load()
        data[f"{query}_{uuid.uuid4()}"] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "effective_query": effective_query,
            "was_rewritten": was_rewritten,
            "mode": mode,
            "top_k": top_k,
            "answer": user_answer_md,
            "chunks": [
                {
                    "rank": idx,
                    "score": result.score,
                    "filename": result.filename,
                    "chunk_index": result.chunk_index,
                    "section_path": result.section_path,
                    "headings": result.headings,
                    "is_table": result.is_table,
                    "man_refs": result.man_refs,
                    "cross_refs": result.cross_refs,
                    "text": result.text.strip(),
                }
                for idx, result in enumerate(results, start=1)
            ],
        }
        try:
            self._path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            log.error("query_logger_error", error=str(e))


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
    - Добавь релевантные синонимы которые могут улучшить векторный поиск.
    - Если в ответе есть номры их не трогай, если нет, но добовлять нормы запрещено.
    - Длина ответа — не более двух предложений.
    """
    # - Если мало контекста для поиска то добавь несколько ключивых слов которые отсутствуют в вопросе. Но не бельше чем 2 слова.

    def __init__(self, timeout: float = 120.0) -> None:
        self.client = OpenAI(
            base_url=LLMData.REWRITER_BASE_URL,
            api_key="",
            timeout=timeout,
        )

    def rewrite(self, query: str) -> tuple[str, bool]:
        try:
            resp = self.client.chat.completions.create(
                model=LLMData.REWRITER_MODEL,
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


class AnswerComposer:
    """Генерирует пользовательский markdown-ответ по найденным чанкам."""

    ANSWER_SYSTEM_PROMPT = """
    Ты — ассистент по нормативной документации в строительстве.
    Твоя задача: на основе найденных фрагментов подготовить понятный ответ для пользователя.

    Правила:
    - Отвечай только по переданному контексту.
    - Ничего не выдумывай и не добавляй от себя.
    - Если данных недостаточно или они противоречивы, прямо скажи об этом.
    - Форматируй ответ в Markdown.
    - Пиши на русском языке.
    - Используй структуру:
      1. Короткий но развернутый вывод в 1-3 предложениях максимально по контексту.
      2. Раздел "Что удалось найти" со списком.
      3. Раздел "Ограничения" только если это действительно нужно.
      4. Раздел "Основание" с коротким списком использованных документов/разделов.
    - Не показывай служебные поля вроде score, chunk_index, id.
    - Не пиши, что ты модель или ИИ.

    Шаблон формирования ответа?
    #### Ответ
    <1–3 предложения: прямой ответ>

    #### Что удалось найти
    - факт 1 (п. X.X.X)
    - факт 2
    - ...

    #### Основание
    | Документ | Раздел |
    |---|---|
    | СП 63.13330.2018 | 10.3 Армирование |


    #### Ограничения        ← только если есть оговорки
    > текст оговорки
    """

    def __init__(self, timeout: float = 1200) -> None:
        self.client = OpenAI(
            base_url=LLMData.ANSTER_BASE_URL,
            api_key="",
            timeout=timeout,
        )
        self.model = LLMData.ANSTER_MODEL

    def compose(self, query: str, effective_query: str, results: list[RetrievalResult]) -> str:
        if not results:
            return (
                "## Ответ\n\n"
                "По этому запросу ничего не найдено в базе.\n\n"
                "Попробуйте уточнить формулировку, номер документа или нужный раздел."
            )

        context = self._build_context(results)
        user_prompt = (
            f"Исходный запрос пользователя:\n{query}\n\n"
            f"Поисковый запрос после переформулирования:\n{effective_query}\n\n"
            f"Контекст из ретривера:\n{context}"
        )
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.ANSWER_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.15,
                max_tokens=900,
            )
            answer = (resp.choices[0].message.content or "").strip()
            return answer or self._fallback_answer(results)
        except Exception as e:
            log.error("answer_composer_error", query=query, error=str(e))
            return self._fallback_answer(results)

    def _build_context(self, results: list[RetrievalResult]) -> str:
        parts: list[str] = []
        for idx, result in enumerate(results[: LLMData.ANSWER_CONTEXT_LIMIT], start=1):
            headings = " > ".join(result.headings or []) or "—"
            section_path = result.section_path or "—"
            refs = ", ".join(result.man_refs or result.cross_refs or []) or "—"
            parts.append(
                "\n".join(
                    [
                        f"Фрагмент {idx}",
                        f"Документ: {result.filename}",
                        f"Раздел: {section_path}",
                        f"Заголовки: {headings}",
                        f"Тип: {'таблица' if result.is_table else 'текст'}",
                        f"Ссылки: {refs}",
                        "Текст:",
                        result.text.strip(),
                    ]
                )
            )
        return "\n\n---\n\n".join(parts)

    def _fallback_answer(self, results: list[RetrievalResult]) -> str:
        bullets: list[str] = []
        basis: list[str] = []
        for result in results[:3]:
            snippet = " ".join(result.text.strip().split())[:280]
            if snippet:
                bullets.append(f"- {snippet}...")
            label = result.filename
            if result.section_path:
                label += f", раздел {result.section_path}"
            basis.append(f"- {label}")

        answer_parts = [
            "## Ответ:\n",
            "\tНе удалось сформировать полноценный ответ через модель, поэтому ниже показана краткая сводка по найденным фрагментам.",
            "## Что удалось найти\n",
            *(
                bullets
                or ["- В релевантных фрагментах нет достаточного объёма данных для краткой сводки."]
            ),
            "## Основание",
            *(basis or ["- Подходящие фрагменты не найдены."]),
        ]
        return "\n".join(answer_parts)


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
                help="Использует vllm-light для преобразования вопроса в поисковый запрос",
            )
            self.generate_user_answer: bool = st.toggle(
                "Генерировать ответ для пользователя",
                value=True,
                help="Использует ту же модель для подготовки ответа по найденным фрагментам",
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
        self._answer_composer = AnswerComposer()
        self._logger = QueryLogger()

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
            return self._rewriter.rewrite(query)

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
        user_answer_md: str | None,
        logger: "QueryLogger",
    ) -> None:
        # --- logs ---
        logger.log(
            query=original_query,
            effective_query=effective_query,
            was_rewritten=rewritten,
            mode=params.mode,
            top_k=params.top_k,
            results=results,
            user_answer_md=user_answer_md,
        )

        # if results:
        #     st.caption(f"Лучший скор: {results[0].score:.4f} (порог: {SCORE_THRESHOLD})")

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


class ResultsView:
    """Отображает пользовательский ответ и отладочное представление результатов."""

    def render(self) -> None:
        if "results" not in st.session_state:
            self._render_empty_prompt()
            return

        results: list[RetrievalResult] = st.session_state["results"]
        meta: dict = st.session_state.get("meta", {})
        user_answer_md: str | None = st.session_state.get("user_answer_md")

        # st.divider()
        # self._render_metrics(results, meta)

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
            st.caption("Релевантность низкая: ответ стоит перепроверить по исходным фрагментам.")

    def _render_dev_tab(self, results: list[RetrievalResult], meta: dict) -> None:
        st.markdown("#### Отладочное представление")
        st.divider()
        self._render_metrics(results, meta)
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

    def _render_chunk(self, idx: int, result: RetrievalResult, mode: SearchMode) -> None:
        dot = _score_dot(result.score, mode)
        kind = "Таблица" if result.is_table else "Текст"
        overlap_badge = " 🔁" if result.is_overlap_window else ""
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
                    "man_refs": result.man_refs,
                    "cross_refs": result.cross_refs,
                }
            )

    @staticmethod
    def _render_empty_prompt() -> None:
        st.markdown(
            "Введите запрос и нажмите **Найти**.\n\n"
            "После поиска появятся две вкладки: **Ответ** для пользователя и **Отладка** для разработчика."
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
