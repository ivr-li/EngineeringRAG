import streamlit as st
from config import UIConfig


class SidebarParams:
    """All sidebar parameters"""

    def __init__(self) -> None:
        with st.sidebar:
            st.title("Параметры поиска")
            self._render_search_params()
            self._render_generation_params()
            self._render_prompts()

    def _render_search_params(self) -> None:
        self.mode: UIConfig.SearchMode = st.selectbox(
            "Режим поиска",
            options=["hybrid", "dense", "sparse"],
            index=0,
            help=(
                "**hybrid** — dense + sparse Prefetch → ColBERT rerank (рекомендуется)\n\n"
                "**dense** — только BGE-M3 dense (ANN).\n\n"
                "**sparse** — только BGE-M3 BM25 (ключевые слова)."
            ),
        )
        self.top_k: int = st.slider("top_k — финальных результатов", 1, 20, 4)
        self.prefetch_k: int = self._render_prefetch_k()
        self.only_tables, self.filename_filter, self.section_filter = (
            self._render_filters()
        )

    def _render_generation_params(self) -> None:
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

    def _render_prompts(self) -> None:
        self.rewrite_system_prompt = st.text_area(
            "Промт предобработки",
            value=UIConfig.REWRITE_SYSTEM_PROMPT,
            height=80,
        )
        self.compose_system_prompt = st.text_area(
            "Промт ответа",
            value=UIConfig.ANSWER_SYSTEM_PROMPT,
            height=80,
        )

    def _render_prefetch_k(self) -> int:
        if self.mode != "hybrid":
            return self.top_k * 4
        return st.slider(
            "prefetch_k — кандидатов для ColBERT rerank",
            min_value=self.top_k,
            max_value=100,
            value=min(40, 100),
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
