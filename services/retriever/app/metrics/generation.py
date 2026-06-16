import json

from openai import OpenAI
from pydantic import ValidationError
from starlette.concurrency import run_in_threadpool

from app.metrics.schemas import GenerationMetrics, JudgeResponse
from app.pipeline.schemas import PipelineResult

JUDGE_SYSTEM_PROMPT = """
Ты оцениваешь ответ RAG-системы. Верни только JSON:
{
  "faithfulness_score": число от 0 до 1,
  "answer_relevance_score": число от 0 до 1,
  "unsupported_claims": ["неподтвержденные утверждения"],
  "explanation": "краткое объяснение"
}
Faithfulness оценивает подтвержденность ответа контекстом.
Answer relevance оценивает ответ на исходный вопрос по существу.
""".strip()


class GenerationMetricCalculator:
    def __init__(self, client: OpenAI, model: str, max_attempts: int = 2) -> None:
        self.client = client
        self.model = model
        self.max_attempts = max_attempts

    async def calculate(self, result: PipelineResult) -> GenerationMetrics:
        if not result.answer:
            return GenerationMetrics(error="Answer is empty")

        prompt = _judge_prompt(result)
        last_error = "Judge did not return a valid response"

        for _ in range(self.max_attempts):
            try:
                response = await self._call_judge(prompt)
                return GenerationMetrics(**response.model_dump())
            except (ValueError, ValidationError, json.JSONDecodeError) as ex:
                last_error = str(ex)
            except Exception as ex:
                last_error = str(ex)

        return GenerationMetrics(error=last_error)

    async def _call_judge(self, prompt: str) -> JudgeResponse:
        response = await run_in_threadpool(
            self.client.chat.completions.create,
            model=self.model,
            messages=[
                {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            max_tokens=700,
        )
        content = response.choices[0].message.content or ""
        payload = json.loads(_extract_json(content))

        return JudgeResponse.model_validate(payload)


def _judge_prompt(result: PipelineResult) -> str:
    return (
        f"Вопрос:\n{result.question}\n\n"
        f"Контекст:\n{result.context_text}\n\n"
        f"Ответ:\n{result.answer}"
    )


def _extract_json(content: str) -> str:
    start = content.find("{")
    end = content.rfind("}")

    if start < 0 or end < start:
        raise ValueError("Judge response does not contain JSON")

    return content[start : end + 1]
