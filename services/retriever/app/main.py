from contextlib import asynccontextmanager
import os

import structlog
from dotenv import load_dotenv
from fastapi import Body, FastAPI
from minio import Minio
from openai import OpenAI
from sqlalchemy.ext.asyncio import (
    create_async_engine,
)

from app.pipeline.schemas import PipelineResult
from app.pipeline.search_pipeline import SearchPipeline
from app.pipeline.services import (
    MinioTraceLogger,
    PGTraceLogger,
    QdrantRetriever,
    TraceLogger,
    compose_answer,
    get_bge_m3,
    rewrite_query,
)
from app.schemas import (
    LLMConfig,
    QueryTrace,
    RetrievalResult,
    RetrievedChunkTrace,
    SearchRequest,
    SearchResponse,
)

log = structlog.get_logger(__name__)
load_dotenv()
retriever = QdrantRetriever()
OPENAI_COMPATIBLE_API_KEY = os.getenv("OPENAI_API_KEY") or "EMPTY"
llm_client = OpenAI(
    base_url=LLMConfig.REWRITER_BASE_URL,
    api_key=OPENAI_COMPATIBLE_API_KEY,
    timeout=120,
)

trace_logger: TraceLogger

RETRIEVER_DATABASE_URL = os.getenv(
    "RETRIEVER_DATABASE_URL",
    "postgresql+asyncpg://app_user:app_password@localhost:5432/app_db",
)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
TRACE_BUCKET_NAME = os.getenv("TRACE_BUCKET_NAME", "ragfiles")
TRACE_PREFIX = os.getenv("TRACE_PREFIX", "dev_data/logs/query-traces")


def _apply_pipeline_trace(trace: QueryTrace, result: PipelineResult) -> None:
    trace.rewritten_query = result.effective_question if result.was_rewritten else None
    trace.answer = result.answer
    trace.answer_mode = result.answer_mode
    trace.latency_ms = result.timings.latency_ms
    trace.rewrite_latency_ms = result.timings.rewrite_latency_ms
    trace.retrieval_latency_ms = result.timings.retrieval_latency_ms
    trace.generation_latency_ms = result.timings.generation_latency_ms
    trace.pipeline_result = result.model_dump(mode="json")
    trace.context_chunks = [chunk.id for chunk in result.context_included]
    trace.retrieved = [
        RetrievedChunkTrace(
            chunk_id=chunk.id,
            filename=chunk.filename,
            rank=rank,
            score=chunk.score,
            text=chunk.text,
        )
        for rank, chunk in enumerate(result.retrieved, start=1)
    ]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global trace_logger
    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    engine = create_async_engine(RETRIEVER_DATABASE_URL, pool_pre_ping=True)

    trace_logger = TraceLogger(
        minio_logger=MinioTraceLogger(
            client=minio_client,
            bucket_name=TRACE_BUCKET_NAME,
            prefix=TRACE_PREFIX,
        ),
        trace_repository=PGTraceLogger(engine),
    )

    await trace_logger.ensure_storage()
    get_bge_m3()

    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(title="Construction RAG API", lifespan=lifespan)


# ==============================================================
# Endpoints
# ==============================================================


@app.get("/he")
async def root():
    return {"message": "Hello World"}


@app.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest = Body(..., description="Search parameters")):
    trace = QueryTrace(
        query=request.query,
        user_id=request.user_id,
        session_id=request.session_id,
        search_mode=request.mode,
        top_k=request.top_k,
        prefetch_k=request.prefetch_k,
    )

    try:
        pipeline = SearchPipeline(retriever, llm_client, rewrite_query, compose_answer)
        result = await pipeline.run(request, query_id=trace.query_id)
        _apply_pipeline_trace(trace, result)

        return SearchResponse(
            results=result.results,
            answer=result.answer,
            effective_query=result.effective_question,
            was_rewritten=result.was_rewritten,
            answer_mode=result.answer_mode,
        )
    except Exception as ex:
        trace.error = str(ex)
        raise
    finally:
        await trace_logger.log(trace)


@app.post("/rewrite_query")
async def rewrite_query_endpoint(
    query: str = Body(..., description="User query"),
    system_prompt: str = Body(..., description="System prompt"),
):
    rewritten, was_rewritten = await rewrite_query(
        client=llm_client,
        query=query,
        system_prompt=system_prompt,
    )
    return {"rewritten": rewritten, "was_rewritten": was_rewritten}


@app.post("/compose_answer")
async def compose_answer_endpoint(
    query: str = Body(..., description="User query"),
    effective_query: str = Body(..., description="Rewritten queru"),
    system_prompt: str = Body(..., description="System prompt"),
    results: list[RetrievalResult] = Body(..., description="/search result"),
):
    answer = await compose_answer(
        client=llm_client,
        query=query,
        effective_query=effective_query,
        system_prompt=system_prompt,
        results=results,
    )
    return {"answer": answer}


# python -m uvicorn app.main:app --host 0.0.0.0 --port 9123 --reload
# if __name__ == "__main__":
# uvicorn.run("app.main:app", host="0.0.0.0", port=9123, reload=True)
