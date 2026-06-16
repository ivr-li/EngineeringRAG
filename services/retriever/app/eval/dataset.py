import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from app.metrics.schemas import EvalQuestion


class EvalRunConfig(BaseModel):
    index_version: str = "current"
    search_mode: Literal["hybrid", "dense", "sparse"] = "hybrid"
    top_k: int = 10
    prefetch_k: int = 40
    use_rewriter: bool = True
    expand_refs: bool = True
    ref_depth: int = 1
    rewrite_system_prompt: str = ""
    compose_system_prompt: str = ""
    judge_model: str | None = None


def load_dataset(path: Path) -> list[EvalQuestion]:
    questions: list[EvalQuestion] = []

    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if line.strip():
                questions.append(_parse_question(line, path, line_number))

    return questions


def load_run_config(path: Path) -> EvalRunConfig:
    with path.open(encoding="utf-8") as source:
        return EvalRunConfig.model_validate(json.load(source))


def _parse_question(line: str, path: Path, line_number: int) -> EvalQuestion:
    try:
        return EvalQuestion.model_validate_json(line)
    except ValueError as ex:
        raise ValueError(f"Invalid eval record at {path}:{line_number}") from ex
