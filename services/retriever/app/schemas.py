from contextlib import contextmanager
from datetime import UTC, datetime
from time import perf_counter
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


class LLMConfig:
    REWRITER_BASE_URL = "http://localhost:8020/v1"
    REWRITER_MODEL = "query-rewriter"

    ANSWER_BASE_URL = REWRITER_BASE_URL
    ANSWER_MODEL = REWRITER_MODEL
    ANSWER_CONTEXT_LIMIT = 6
    ANSWER_CONTEXT_HARD_LIMIT = 24
    ANSWER_MODEL_CONTEXT_TOKENS = 10000
    ANSWER_MAX_TOKENS = 1600
    ANSWER_MIN_TOKENS = 512
    ANSWER_TOKEN_SAFETY_MARGIN = 700
    ANSWER_MIN_CONTEXT_TOKENS = 1200
    ANSWER_MAX_TEXT_BLOCK_TOKENS = 700
    ANSWER_MAX_TABLE_HEADER_TOKENS = 260
    ANSWER_MAX_TABLE_ROW_TOKENS = 650
    ANSWER_MAX_TABLE_SNIPPETS = 4
    ANSWER_TABLE_SNIPPET_CHARS = 900
    SearchMode = Literal["hybrid", "dense", "sparse"]
    SCORE_THRESHOLD = 0.0


class SearchRequest(BaseModel):
    query: str
    user_id: str | None = None
    session_id: str | None = None
    client_metadata: dict | None = None
    index_version: str = "current"
    experiment_id: str | None = None
    variant: str | None = None
    top_k: int = 10
    prefetch_k: int = 40
    mode: Literal["hybrid", "dense", "sparse"] = "hybrid"
    only_tables: bool | None = None
    use_rewriter: bool = True
    expand_refs: bool = True
    ref_depth: int = 1
    filename_filter: str | None = None
    section_filter: str | None = None
    rewrite_system_prompt: str
    compose_system_prompt: str


class RetrievalResult(BaseModel):
    id: str
    score: float
    text: str
    filename: str
    headings: list[str]
    is_table: bool
    chunk_index: int | None = None

    # references
    man_refs: list[str]
    cross_refs: list[str]
    anchor_refs: list[str] = Field(default_factory=list)

    # hierarchy metadata
    section_path: str = ""
    section_level: int = 0
    parent_heading: str | None = None
    leaf_heading: str | None = None

    # sliding window markers
    is_overlap_window: bool = False
    window_index: int = 0

    # table continuation metadata
    table_id: str | None = None
    table_caption: str | None = None
    table_part_index: int | None = None
    table_part_total: int | None = None
    table_window_index: int | None = None
    table_window_total: int | None = None
    table_orientation: str | None = None


class SearchResponse(BaseModel):
    answer: str | None = Field(None)
    effective_query: str = Field(...)
    was_rewritten: bool = Field(False)
    results: list[RetrievalResult]


class RetrievedChunkTrace(BaseModel):
    chunk_id: str
    filename: str
    rank: int
    score: float | None = None
    text: str


class QueryTrace(BaseModel):
    model_config = {"arbitrary_types_allowed": True, "frozen": False}

    query_id: str = Field(default_factory=lambda: str(uuid4()))
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    query: str
    rewritten_query: str | None = None
    search_mode: str
    top_k: int
    prefetch_k: int
    retrieved: list[RetrievedChunkTrace] = Field(default_factory=list)
    context_chunks: list[str] = Field(default_factory=list)
    answer: str | None = None
    pipeline_result: dict | None = None
    latency_ms: int | None = None
    rewrite_latency_ms: int | None = None
    retrieval_latency_ms: int | None = None
    generation_latency_ms: int | None = None
    error: str | None = None

    @contextmanager
    def measure(self, name: str):
        st = perf_counter()
        try:
            yield
        finally:
            setattr(self, name, int((perf_counter() - st) * 1000))
