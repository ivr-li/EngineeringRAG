import argparse
import re
from pathlib import Path

from app.metrics.schemas import EvalQuestion, EvidenceGroup, EvidenceSource

_QUESTION_HEADING = re.compile(r"^## (?P<id>(?:АР|АС|КЖ)-\d{3})\s*$", re.MULTILINE)
_FIELDS = {
    "question": re.compile(r"\*\*Вопрос:\*\*\s*(.+?)(?:\n\n|$)", re.DOTALL),
    "reference_answer": re.compile(
        r"\*\*Предложенный ответ:\*\*\s*(.+?)(?:\n\n|$)", re.DOTALL
    ),
    "document_id": re.compile(r"- \*\*Документ:\*\*\s*(.+?)(?:\n|$)"),
    "anchor": re.compile(r"- \*\*Раздел или таблица:\*\*\s*(.+?)(?:\n|$)"),
    "quote": re.compile(r"\*\*Фрагмент источника:\*\*\s*(.+)\Z", re.DOTALL),
}


def convert_directory(source_dir: Path) -> list[EvalQuestion]:
    questions: list[EvalQuestion] = []

    for path in sorted(source_dir.glob("*.md")):
        questions.extend(parse_markdown(path))

    _validate_unique_ids(questions)
    return questions


def parse_markdown(path: Path) -> list[EvalQuestion]:
    text = path.read_text(encoding="utf-8")
    matches = list(_QUESTION_HEADING.finditer(text))
    questions: list[EvalQuestion] = []

    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        questions.append(_parse_block(match.group("id"), text[match.end() : end], path))

    return questions


def write_jsonl(path: Path, questions: list[EvalQuestion]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(question.model_dump_json() for question in questions)
    path.write_text(f"{payload}\n", encoding="utf-8")


def _parse_block(question_id: str, block: str, path: Path) -> EvalQuestion:
    fields = {
        name: _extract(pattern, block, name, question_id)
        for name, pattern in _FIELDS.items()
    }
    quote = _clean_quote(fields["quote"])

    return EvalQuestion(
        id=question_id,
        question=_clean_text(fields["question"]),
        reference_answer=_clean_text(fields["reference_answer"]),
        evidence_groups=[
            EvidenceGroup(
                name="primary_evidence",
                acceptable_sources=[
                    EvidenceSource(
                        document_id=_clean_text(fields["document_id"]),
                        anchor=_clean_text(fields["anchor"]),
                        quote=quote,
                    )
                ],
            )
        ],
        metadata={
            "discipline": question_id.split("-", maxsplit=1)[0],
            "source": "expert_markdown",
            "source_file": path.name,
        },
    )


def _extract(
    pattern: re.Pattern,
    block: str,
    field: str,
    question_id: str,
) -> str:
    match = pattern.search(block)
    if not match:
        raise ValueError(f"{question_id}: missing field {field}")

    return match.group(1)


def _clean_quote(value: str) -> str:
    lines = []

    for line in value.strip().splitlines():
        if line.strip() == "---":
            continue
        lines.append(re.sub(r"^>\s?", "", line).rstrip())

    return "\n".join(lines).strip()


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _validate_unique_ids(questions: list[EvalQuestion]) -> None:
    ids = [question.id for question in questions]
    duplicates = sorted(
        {question_id for question_id in ids if ids.count(question_id) > 1}
    )

    if duplicates:
        raise ValueError(f"Duplicate question IDs: {duplicates}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert expert Markdown to eval JSONL")
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    questions = convert_directory(args.source_dir)
    write_jsonl(args.output, questions)
    print(f"Written {len(questions)} questions to {args.output}")


# cd services/retriever

# ../../venv/bin/python -m app.eval.markdown_dataset \
#   --source-dir tests/data \
#   --output app/eval/data/questions.jsonl
if __name__ == "__main__":
    main()
