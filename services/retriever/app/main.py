import structlog
import uvicorn
from fastapi import Body, FastAPI
from openai import OpenAI
from starlette.concurrency import run_in_threadpool

from app.schemas import LLMConfig, RetrievalResult, SearchRequest, SearchResponse
from app.services import QdrantRetriever, compose_answer, rewrite_query

app = FastAPI(title="Construction RAG API")
log = structlog.get_logger(__name__)
retriever = QdrantRetriever()
llm_client = OpenAI(
    base_url=LLMConfig.REWRITER_BASE_URL,
    api_key="",
    timeout=120,
)


# ==============================================================
# Endpoints
# ==============================================================


@app.get("/he")
async def root():
    return {"message": "Hello World"}


@app.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest = Body(..., description="Search parameters")):
    effective_query = request.query
    was_rewritten = False

    if request.use_rewriter and request.rewrite_system_prompt:
        effective_query, was_rewritten = await rewrite_query(
            client=llm_client,
            query=request.query,
            system_prompt=request.rewrite_system_prompt,
        )
        log.info(
            "query_rewritten",
            original=request.query,
            effective=effective_query,
            was_rewritten=was_rewritten,
        )

    results = await run_in_threadpool(
        retriever.search,
        query=effective_query,
        top_k=request.top_k,
        prefetch_k=request.prefetch_k,
        mode=request.mode,
        only_tables=request.only_tables,
        filename_filter=request.filename_filter,
        section_filter=request.section_filter,
    )

    answer: str | None = None
    if request.compose_system_prompt and results:
        answer = await compose_answer(
            client=llm_client,
            query=request.query,
            effective_query=effective_query,
            system_prompt=request.compose_system_prompt,
            results=results,
        )
    log.info("SearchResponse is finished")
    return SearchResponse(
        results=results,
        answer=answer,
        effective_query=effective_query,
        was_rewritten=was_rewritten,
    )


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
