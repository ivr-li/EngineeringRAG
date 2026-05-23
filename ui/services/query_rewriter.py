import structlog
from openai import OpenAI

from ui.config import LLMData

log = structlog.get_logger(__name__)


class QueryRewriter:
    """
    Переформулирует запрос через vllm-light.
    При недоступности модели возвращает оригинальный запрос.
    """

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
