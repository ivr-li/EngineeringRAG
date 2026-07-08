import json
from datetime import UTC, datetime
from pathlib import Path

FEEDBACK_PATH = Path("data/ui_feedback.jsonl")


def log_feedback(search: dict, rating: str, comment: str | None = None) -> bool:
    record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "search_id": search["id"],
        "query": search["query"],
        "rating": rating,
        "comment": comment,
        "effective_query": search["response"].get("effective_query"),
        "result_ids": [
            result.get("id") for result in search["response"].get("results", [])
        ],
    }

    try:
        FEEDBACK_PATH.parent.mkdir(parents=True, exist_ok=True)
        with FEEDBACK_PATH.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError:
        return False

    return True
