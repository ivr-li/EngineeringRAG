from dataclasses import dataclass, field
from typing import Literal


class LLMConfig:
    REWRITER_BASE_URL = "http://localhost:8020/v1"
    REWRITER_MODEL = "query-rewriter"

    ANSTER_BASE_URL = REWRITER_BASE_URL
    ANSTER_MODEL = REWRITER_MODEL
    ANSWER_CONTEXT_LIMIT = 6
    SearchMode = Literal["hybrid", "dense", "sparse"]
    SCORE_THRESHOLD = 0.0


@dataclass
class RetrievalResult:
    id: str
    score: float
    text: str
    filename: str
    headings: list[str]
    is_table: bool
    chunk_index: int | None = None

    # references
    man_refs: list[str] = field(default_factory=list)
    cross_refs: list[str] = field(default_factory=list)

    # hierarchy metadata
    section_path: str = ""
    section_level: int = 0
    parent_heading: str | None = None
    leaf_heading: str | None = None

    # sliding window markers
    is_overlap_window: bool = False
    window_index: int = 0
