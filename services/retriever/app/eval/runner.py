import argparse
import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from openai import OpenAI

from app.eval.dataset import EvalRunConfig, load_dataset, load_run_config
from app.eval.report import build_summary, render_markdown
from app.metrics.generation import GenerationMetricCalculator
from app.metrics.retrieval import calculate_retrieval_metrics
from app.metrics.schemas import EvalQuestion
from app.pipeline.search_pipeline import SearchPipeline
from app.schemas import LLMConfig, SearchRequest
from app.services import QdrantRetriever, compose_answer, rewrite_query


class EvalRunner:
    def __init__(
        self,
        pipeline: SearchPipeline,
        config: EvalRunConfig,
        generation_metrics: GenerationMetricCalculator | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.config = config
        self.generation_metrics = generation_metrics

    async def run(self, questions: list[EvalQuestion]) -> list[dict]:
        records: list[dict] = []
        total = len(questions)
        print(f"Running evaluation for {total} questions", flush=True)

        for index, question in enumerate(questions, start=1):
            print(f"[{index}/{total}] {question.id}: running", flush=True)
            try:
                records.append(await self._run_question(question))
            except Exception as ex:
                records.append(_error_record(question, ex))
                print(f"[{index}/{total}] {question.id}: error: {ex}", flush=True)
            else:
                print(f"[{index}/{total}] {question.id}: done", flush=True)

        return records

    async def _run_question(self, question: EvalQuestion) -> dict:
        pipeline_result = await self.pipeline.run(
            self._search_request(question),
            index_version=self.config.index_version,
        )
        retrieval = calculate_retrieval_metrics(pipeline_result, question)
        generation = None

        if self.generation_metrics:
            generation = await self.generation_metrics.calculate(pipeline_result)

        return {
            "question_id": question.id,
            "pipeline_result": pipeline_result.model_dump(mode="json"),
            "retrieval_metrics": retrieval.model_dump(mode="json"),
            "generation_metrics": generation.model_dump(mode="json")
            if generation
            else {},
        }

    def _search_request(self, question: EvalQuestion) -> SearchRequest:
        return SearchRequest(
            query=question.question,
            index_version=self.config.index_version,
            top_k=self.config.top_k,
            prefetch_k=self.config.prefetch_k,
            mode=self.config.search_mode,
            use_rewriter=self.config.use_rewriter,
            expand_refs=self.config.expand_refs,
            ref_depth=self.config.ref_depth,
            rewrite_system_prompt=self.config.rewrite_system_prompt,
            compose_system_prompt=self.config.compose_system_prompt,
        )


def _error_record(question: EvalQuestion, error: Exception) -> dict:
    return {
        "question_id": question.id,
        "pipeline_result": {},
        "retrieval_metrics": {},
        "generation_metrics": {},
        "error": str(error),
    }


async def _run_cli(args: argparse.Namespace) -> Path:
    config = load_run_config(args.config)
    api_key = os.getenv("OPENAI_API_KEY") or "EMPTY"
    client = OpenAI(
        base_url=LLMConfig.REWRITER_BASE_URL,
        api_key=api_key,
        timeout=120,
    )
    pipeline = SearchPipeline(QdrantRetriever(), client, rewrite_query, compose_answer)
    generation = _generation_calculator(client, config)
    runner = EvalRunner(pipeline, config, generation)
    records = await runner.run(load_dataset(args.dataset))

    return _write_results(args.output_dir, config, records)


def _generation_calculator(
    client: OpenAI,
    config: EvalRunConfig,
) -> GenerationMetricCalculator | None:
    if not config.judge_model:
        return None

    return GenerationMetricCalculator(client, config.judge_model)


def _write_results(output_dir: Path, config: EvalRunConfig, records: list[dict]) -> Path:
    created_at = datetime.now(UTC)
    run_id = created_at.strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    config_payload = config.model_dump(mode="json")
    run_summary = {
        "run_id": run_id,
        "created_at": created_at.isoformat(),
        "config_path": f"{run_id}/config.json",
        "config": config_payload,
        **build_summary(records),
    }

    _write_json(run_dir / "config.json", config_payload)
    _write_json(run_dir / "per_question.json", records)
    _update_history(output_dir, run_summary)

    return run_dir


def _update_history(output_dir: Path, run_summary: dict) -> None:
    summary_path = output_dir / "summary.json"
    history = _load_history(summary_path)
    history["runs"].append(run_summary)

    _write_json(summary_path, history)
    (output_dir / "report.md").write_text(
        render_markdown(history["runs"]),
        encoding="utf-8",
    )


def _load_history(path: Path) -> dict:
    if not path.exists():
        return {"runs": []}

    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict | list[dict]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run retriever offline evaluation")
    parser.add_argument(
        "--dataset",
        type=Path,
        required=True,
        default=Path("app/eval/data/questions.jsonl"),
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        default=Path("app/eval/configs/retrieval_baseline.json"),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("app/eval/results"))
    return parser.parse_args()


def main() -> None:
    run_dir = asyncio.run(_run_cli(_parse_args()))
    print(run_dir)


# ../../venv/bin/python -m app.eval.runner
if __name__ == "__main__":
    main()
