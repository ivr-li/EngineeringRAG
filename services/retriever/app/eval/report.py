import json
from collections.abc import Iterable
from statistics import mean

RETRIEVAL_METRICS = (
    "de_recall_at_5",
    "de_recall_at_10",
    "ee_recall",
    "ce_recall",
    "cploss_rate",
    "rrs_rate",
    "reciprocal_rank",
)
GENERATION_METRICS = ("faithfulness_score", "answer_relevance_score")


def build_summary(records: list[dict]) -> dict:
    retrieval = _aggregate_section(records, "retrieval_metrics", RETRIEVAL_METRICS)
    retrieval["mrr"] = retrieval.pop("reciprocal_rank")

    return {
        "questions": len(records),
        "retrieval": retrieval,
        "generation": _aggregate_section(
            records, "generation_metrics", GENERATION_METRICS
        ),
    }


def render_markdown(runs: list[dict]) -> str:
    lines = ["# Eval report", "", f"Runs: {len(runs)}", ""]

    for run in reversed(runs):
        lines.extend(_run_lines(run))

    return "\n".join(lines).rstrip() + "\n"


def _run_lines(run: dict) -> list[str]:
    lines = [
        f"## {run['run_id']}",
        "",
        f"- Created at: `{run['created_at']}`",
        f"- Questions: `{run['questions']}`",
        f"- Config: `{run['config_path']}`",
        "",
    ]
    lines.extend(_config_lines(run["config"]))
    lines.extend(_section_lines("Retrieval metrics", run["retrieval"], level=3))
    lines.extend(_section_lines("Generation metrics", run["generation"], level=3))

    return lines


def _config_lines(config: dict) -> list[str]:
    lines = ["### Configuration", ""]

    for name, value in config.items():
        formatted = json.dumps(value, ensure_ascii=False)
        lines.append(f"- `{name}`: `{formatted}`")

    lines.append("")
    return lines


def _aggregate_section(
    records: list[dict],
    section: str,
    metric_names: Iterable[str],
) -> dict[str, float | None]:
    return {
        name: _mean_not_none(record.get(section, {}).get(name) for record in records)
        for name in metric_names
    }


def _mean_not_none(values: Iterable[float | None]) -> float | None:
    available = [value for value in values if value is not None]
    return mean(available) if available else None


def _section_lines(
    title: str,
    metrics: dict[str, float | None],
    level: int = 2,
) -> list[str]:
    lines = [f"{'#' * level} {title}", ""]

    for name, value in metrics.items():
        formatted = "n/a" if value is None else f"{value:.4f}"
        lines.append(f"- `{name}`: {formatted}")

    lines.append("")
    return lines
