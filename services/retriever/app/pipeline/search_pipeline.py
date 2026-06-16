from collections.abc import Awaitable, Callable
from uuid import uuid4

from openai import OpenAI
from starlette.concurrency import run_in_threadpool

from app.pipeline.schemas import (
    ContextExclusion,
    PipelineConfiguration,
    PipelineResult,
)
from app.schemas import SearchRequest
from app.services import QdrantRetriever, prepare_answer_context
from app.services.context_packer import PackedContext

Rewriter = Callable[[OpenAI, str, str], Awaitable[tuple[str, bool]]]
Composer = Callable[..., Awaitable[str]]


class SearchPipeline:
    def __init__(
        self,
        retriever: QdrantRetriever,
        llm_client: OpenAI,
        rewriter: Rewriter,
        composer: Composer,
    ) -> None:
        self.retriever = retriever
        self.llm_client = llm_client
        self.rewriter = rewriter
        self.composer = composer

    async def run(
        self,
        request: SearchRequest,
        query_id: str | None = None,
        index_version: str | None = None,
        experiment_id: str | None = None,
        variant: str | None = None,
    ) -> PipelineResult:
        result = self._new_result(
            request,
            query_id or str(uuid4()),
            index_version or request.index_version,
            experiment_id or request.experiment_id,
            variant or request.variant,
        )

        with result.timings.measure("latency_ms"):
            await self._rewrite(request, result)
            await self._retrieve(request, result)
            await self._pack_and_generate(request, result)

        return result

    def _new_result(
        self,
        request: SearchRequest,
        query_id: str,
        index_version: str,
        experiment_id: str | None,
        variant: str | None,
    ) -> PipelineResult:
        configuration = PipelineConfiguration(
            index_version=index_version,
            search_mode=request.mode,
            top_k=request.top_k,
            prefetch_k=request.prefetch_k,
            use_rewriter=request.use_rewriter,
            expand_refs=request.expand_refs,
            ref_depth=request.ref_depth,
            experiment_id=experiment_id,
            variant=variant,
        )

        return PipelineResult(
            query_id=query_id,
            question=request.query,
            effective_question=request.query,
            was_rewritten=False,
            configuration=configuration,
        )

    async def _rewrite(self, request: SearchRequest, result: PipelineResult) -> None:
        if not request.use_rewriter or not request.rewrite_system_prompt:
            return

        with result.timings.measure("rewrite_latency_ms"):
            rewritten, was_rewritten = await self.rewriter(
                self.llm_client, request.query, request.rewrite_system_prompt
            )

        result.effective_question = rewritten
        result.was_rewritten = was_rewritten

    async def _retrieve(self, request: SearchRequest, result: PipelineResult) -> None:
        with result.timings.measure("retrieval_latency_ms"):
            retrieved, expanded = await run_in_threadpool(
                self.retriever.search_stages,
                query=result.effective_question,
                top_k=request.top_k,
                prefetch_k=request.prefetch_k,
                mode=request.mode,
                only_tables=request.only_tables,
                expand_refs=request.expand_refs,
                ref_depth=request.ref_depth,
                filename_filter=request.filename_filter,
                section_filter=request.section_filter,
            )

        result.retrieved = retrieved
        result.expanded = expanded
        result.results = self.retriever.merge_stages(retrieved, expanded)

    async def _pack_and_generate(
        self,
        request: SearchRequest,
        result: PipelineResult,
    ) -> None:
        if not result.results:
            return

        static_prompt, packed = prepare_answer_context(
            results=result.results,
            query=request.query,
            effective_query=result.effective_question,
            system_prompt=request.compose_system_prompt,
            expanded_chunks=result.expanded,
        )
        _apply_packed_context(result, packed)

        if request.compose_system_prompt:
            await self._generate(request, result, packed, static_prompt)

    async def _generate(
        self,
        request: SearchRequest,
        result: PipelineResult,
        packed: PackedContext,
        static_prompt: str,
    ) -> None:
        with result.timings.measure("generation_latency_ms"):
            result.answer = await self.composer(
                client=self.llm_client,
                query=request.query,
                effective_query=result.effective_question,
                system_prompt=request.compose_system_prompt,
                results=result.results,
                packed_context=packed,
                static_prompt=static_prompt,
                expanded_chunks=result.expanded,
            )


def _apply_packed_context(result: PipelineResult, packed: PackedContext) -> None:
    result.context_candidates = list(packed.candidates)
    result.context_included = list(packed.included)
    token_excluded = [
        ContextExclusion(chunk=chunk, reason="token_budget") for chunk in packed.excluded
    ]
    selection_excluded = [
        ContextExclusion(chunk=chunk, reason="context_selection")
        for chunk in packed.selection_excluded
    ]
    result.context_excluded = selection_excluded + token_excluded
    result.context_text = packed.text
