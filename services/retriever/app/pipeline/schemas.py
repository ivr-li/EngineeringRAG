from contextlib import contextmanager
from time import perf_counter

from pydantic import BaseModel, Field

from app.schemas import RetrievalResult


class PipelineConfiguration(BaseModel):
    index_version: str = "current"
    search_mode: str
    top_k: int
    prefetch_k: int
    use_rewriter: bool
    expand_refs: bool
    ref_depth: int
    answer_strategy: str = "auto"
    experiment_id: str | None = None
    variant: str | None = None


class QueryAspect(BaseModel):
    query: str
    aspect: str


class ExpandedChunk(BaseModel):
    chunk: RetrievalResult
    expanded_from_chunk_id: str
    relation: str
    depth: int = 1
    path: list[str] = Field(default_factory=list)


class EvidenceItem(BaseModel):
    claim: str
    document: str
    section: str
    condition: str = ""
    value: str = ""
    interpretation: str
    supports_intent: bool
    chunk_id: str


class ContextExclusion(BaseModel):
    chunk: RetrievalResult
    reason: str


class PipelineTimings(BaseModel):
    latency_ms: int | None = None
    rewrite_latency_ms: int | None = None
    retrieval_latency_ms: int | None = None
    generation_latency_ms: int | None = None

    @contextmanager
    def measure(self, name: str):
        started = perf_counter()
        try:
            yield
        finally:
            setattr(self, name, int((perf_counter() - started) * 1000))


class PipelineResult(BaseModel):
    query_id: str
    question: str
    effective_question: str
    was_rewritten: bool
    configuration: PipelineConfiguration
    retrieved: list[RetrievalResult] = Field(default_factory=list)
    expanded: list[ExpandedChunk] = Field(default_factory=list)
    results: list[RetrievalResult] = Field(default_factory=list)
    query_plan: list[QueryAspect] = Field(default_factory=list)
    answer_mode: str = "direct_supported"
    evidence_items: list[EvidenceItem] = Field(default_factory=list)
    context_candidates: list[RetrievalResult] = Field(default_factory=list)
    context_included: list[RetrievalResult] = Field(default_factory=list)
    context_excluded: list[ContextExclusion] = Field(default_factory=list)
    context_text: str = ""
    answer: str | None = None
    timings: PipelineTimings = Field(default_factory=PipelineTimings)
    error: str | None = None
