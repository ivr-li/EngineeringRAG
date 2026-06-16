from pydantic import BaseModel, Field


class EvidenceSource(BaseModel):
    document_id: str
    document_version: str | None = None
    anchor: str | None = None
    quote: str | None = None


class EvidenceGroup(BaseModel):
    name: str
    required: bool = True
    acceptable_sources: list[EvidenceSource] = Field(default_factory=list)
    resolved_chunk_ids: dict[str, list[str]] = Field(default_factory=dict)


class BridgeSource(BaseModel):
    target_evidence_group: str
    document_id: str | None = None
    anchor: str | None = None
    quote: str | None = None
    resolved_chunk_ids: dict[str, list[str]] = Field(default_factory=dict)


class EvalQuestion(BaseModel):
    id: str
    question: str
    answerable: bool = True
    reference_answer: str | None = None
    evidence_groups: list[EvidenceGroup] = Field(default_factory=list)
    bridge_sources: list[BridgeSource] = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)


class RetrievalMetrics(BaseModel):
    de_recall_at_5: float | None = None
    de_recall_at_10: float | None = None
    ee_recall: float | None = None
    ce_recall: float | None = None
    cploss_rate: float | None = None
    rrs_rate: float | None = None
    reciprocal_rank: float = 0.0


class JudgeResponse(BaseModel):
    faithfulness_score: float = Field(ge=0, le=1)
    answer_relevance_score: float = Field(ge=0, le=1)
    unsupported_claims: list[str] = Field(default_factory=list)
    explanation: str


class GenerationMetrics(BaseModel):
    faithfulness_score: float | None = Field(default=None, ge=0, le=1)
    answer_relevance_score: float | None = Field(default=None, ge=0, le=1)
    unsupported_claims: list[str] = Field(default_factory=list)
    explanation: str | None = None
    error: str | None = None
