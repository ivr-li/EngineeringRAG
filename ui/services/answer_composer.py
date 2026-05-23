import structlog
from openai import OpenAI

from ui.config import LLMData
from ui.dataclasses import RetrievalResult

log = structlog.get_logger(__name__)


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
