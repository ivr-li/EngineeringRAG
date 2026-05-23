from typing import Literal

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    query: str
    top_k: int = 10
    prefetch_k: int = 40
    mode: Literal["hybrid", "dense", "sparse"] = "hybrid"
    only_tables: bool | None = None
    use_rewriter: bool = True
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

    # hierarchy metadata
    section_path: str = ""
    section_level: int = 0
    parent_heading: str | None = None
    leaf_heading: str | None = None

    # sliding window markers
    is_overlap_window: bool = False
    window_index: int = 0


class SearchResponse(BaseModel):
    results: list[RetrievalResult]
    answer: str | None = Field(None)
    effective_query: str = Field(...)
    was_rewritten: bool = Field(False)
