from contextlib import asynccontextmanager

import structlog
from dotenv import load_dotenv
from fastapi import Body, FastAPI
from openai import OpenAI
from starlette.concurrency import run_in_threadpool

from app.schemas import (
    LLMConfig,
    QueryTrace,
    RetrievalResult,
    RetrievedChunkTrace,
    SearchRequest,
    SearchResponse,
)
from app.services import (
    MinioTraceLogger,
    QdrantRetriever,
    compose_answer,
    get_bge_m3,
    rewrite_query,
)

log = structlog.get_logger(__name__)
load_dotenv()
retriever = QdrantRetriever()
llm_client = OpenAI(
    base_url=LLMConfig.REWRITER_BASE_URL,
    api_key="",
    timeout=120,
)
trace_logger = MinioTraceLogger(
    endpoint="localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    bucket_name="ragfiles",
    secure=False,
    prefix="/dev_data/logs/query-traces",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await trace_logger.ensure_bucket()
    get_bge_m3()
    yield


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
        search_mode=request.mode,
        top_k=request.top_k,
        prefetch_k=request.prefetch_k,
    )

    with trace.measure("latency_ms"):
        try:
            effective_query = request.query
            was_rewritten = False

            if request.use_rewriter and request.rewrite_system_prompt:
                with trace.measure("rewrite_latency_ms"):
                    effective_query, was_rewritten = await rewrite_query(
                        client=llm_client,
                        query=request.query,
                        system_prompt=request.rewrite_system_prompt,
                    )
                trace.rewritten_query = effective_query

            with trace.measure("retrieval_latency_ms"):
                results = await run_in_threadpool(
                    retriever.search,
                    query=effective_query,
                    top_k=request.top_k,
                    prefetch_k=request.prefetch_k,
                    mode=request.mode,
                    only_tables=request.only_tables,
                    expand_refs=request.expand_refs,
                    ref_depth=request.ref_depth,
                    filename_filter=request.filename_filter,
                    section_filter=request.section_filter,
                )

            for index, result in enumerate(results, start=1):
                trace.retrieved.append(
                    RetrievedChunkTrace(
                        chunk_id=result.id,
                        filename=result.filename,
                        rank=index,
                        score=result.score,
                        text=result.text,
                    )
                )
                trace.context_chunks.append(result.id)

            answer: str | None = None
            if request.compose_system_prompt and results:
                with trace.measure("generation_latency_ms"):
                    answer = await compose_answer(
                        client=llm_client,
                        query=request.query,
                        effective_query=effective_query,
                        system_prompt=request.compose_system_prompt,
                        results=results,
                    )

            trace.answer = answer
            return SearchResponse(
                results=results,
                answer=answer,
                effective_query=effective_query,
                was_rewritten=was_rewritten,
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
