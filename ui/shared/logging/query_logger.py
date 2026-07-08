import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import structlog
from config import RetrievalResult

log = structlog.get_logger(__name__)


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
        data[f"{query}_{uuid.uuid4()}"] = _query_record(
            effective_query,
            was_rewritten,
            mode,
            top_k,
            results,
            user_answer_md,
        )
        try:
            self._path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            log.error("query_logger_error", error=str(e))


def _query_record(
    effective_query: str,
    was_rewritten: bool,
    mode: str,
    top_k: int,
    results: list[RetrievalResult],
    user_answer_md: str | None,
) -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "effective_query": effective_query,
        "was_rewritten": was_rewritten,
        "mode": mode,
        "top_k": top_k,
        "answer": user_answer_md,
        "chunks": [
            _chunk_record(idx, result)
            for idx, result in enumerate(results, start=1)
        ],
    }


def _chunk_record(idx: int, result: RetrievalResult) -> dict:
    return {
        "rank": idx,
        "score": result.score,
        "filename": result.filename,
        "chunk_index": result.chunk_index,
        "section_path": result.section_path,
        "headings": result.headings,
        "is_table": result.is_table,
        "man_refs": result.man_refs,
        "cross_refs": result.cross_refs,
        "anchor_refs": result.anchor_refs,
        "expanded_from": result.expanded_from,
        "text": result.text.strip(),
    }
