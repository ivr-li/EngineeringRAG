from collections.abc import Awaitable, Callable
from uuid import uuid4

from openai import OpenAI
from starlette.concurrency import run_in_threadpool

from app.pipeline.schemas import (
    ContextExclusion,
    ExpandedChunk,
    PipelineConfiguration,
    PipelineResult,
    QueryAspect,
)
from app.pipeline.services import QdrantRetriever, prepare_answer_context
from app.pipeline.services.canonical import canonicalize_results
from app.pipeline.services.context_packer import PackedContext
from app.pipeline.services.research import (
    answer_mode_reason,
    build_evidence,
    build_evidence_groups,
    build_follow_up_plan,
    build_q_plan,
    coverage_gaps,
    pick_ans_mode,
)
from app.schemas import SearchRequest

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
            answer_strategy=request.answer_strategy,
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
        plan = build_q_plan(request.query, result.effective_question)

        with result.timings.measure("retrieval_latency_ms"):
            retrieved, expanded, result.query_plan = await self._retrieve_plan(
                request, plan, result.effective_question
            )

        result.retrieved = retrieved
        result.expanded = expanded
        canonicalized = canonicalize_results(
            self.retriever.merge_stages(retrieved, expanded),
            expanded,
            request.query,
        )
        result.results = canonicalized.results
        result.canonical_groups = canonicalized.groups

    async def _retrieve_plan(
        self,
        request: SearchRequest,
        plan: list[QueryAspect],
        effective_query: str,
    ) -> tuple[list, list, list[QueryAspect]]:
        retrieved: list = []
        expanded: list = []
        seen: set[str] = set()

        for aspect in plan:
            batch, refs = await self._retrieve_one(request, aspect.query)
            _add_results(retrieved, batch, seen)
            _add_expanded(expanded, refs, seen)

        merged = self.retriever.merge_stages(retrieved, expanded)
        canonicalized = canonicalize_results(merged, expanded, request.query)
        base_evidence = build_evidence(
            canonicalized.results,
            request.query,
            expanded,
        )
        follow_ups = build_follow_up_plan(
            request.query,
            effective_query,
            canonicalized.results,
            plan,
            base_evidence,
        )
        for aspect in follow_ups:
            batch, refs = await self._retrieve_one(request, aspect.query)
            _add_results(retrieved, batch, seen)
            _add_expanded(expanded, refs, seen)

        return retrieved, expanded, plan + follow_ups

    async def _retrieve_one(
        self,
        request: SearchRequest,
        query: str,
    ) -> tuple[list, list[ExpandedChunk]]:
        return await run_in_threadpool(
            self.retriever.search_stages,
            query=query,
            top_k=request.top_k,
            prefetch_k=request.prefetch_k,
            mode=request.mode,
            only_tables=request.only_tables,
            expand_refs=request.expand_refs,
            ref_depth=request.ref_depth,
            filename_filter=request.filename_filter,
            section_filter=request.section_filter,
        )

    async def _pack_and_generate(
        self,
        request: SearchRequest,
        result: PipelineResult,
    ) -> None:
        if not result.results:
            return

        self._build_answer_metadata(request, result)
        static_prompt, packed = self._prepare_context(request, result)
        _apply_packed_context(result, packed)

        if request.compose_system_prompt:
            await self._generate(request, result, packed, static_prompt)

    def _build_answer_metadata(
        self,
        request: SearchRequest,
        result: PipelineResult,
    ) -> None:
        result.evidence_items = build_evidence(
            result.results,
            request.query,
            result.expanded,
        )
        result.answer_mode = pick_ans_mode(result.evidence_items, request.query)
        result.answer_mode_reason = answer_mode_reason(
            result.evidence_items,
            request.query,
            result.answer_mode,
        )
        result.coverage_gaps = coverage_gaps(result.evidence_items, request.query)
        result.evidence_groups = build_evidence_groups(result.evidence_items)

    def _prepare_context(
        self,
        request: SearchRequest,
        result: PipelineResult,
    ) -> tuple[str, PackedContext]:
        return prepare_answer_context(
            results=result.results,
            query=request.query,
            effective_query=result.effective_question,
            system_prompt=request.compose_system_prompt,
            expanded_chunks=result.expanded,
            answer_mode=result.answer_mode,
            evidence_items=result.evidence_items,
            query_plan=result.query_plan,
        )

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
                answer_mode=result.answer_mode,
                evidence_items=result.evidence_items,
                query_plan=result.query_plan,
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
    result.context_stats = {
        "input_tokens": packed.input_tokens,
        "max_output_tokens": packed.max_output_tokens,
        "model_max_len": packed.model_max_tokens,
        "budget_tokens": packed.budget_tokens,
        "used_tokens": packed.used_tokens,
        "dropped_by_budget": packed.dropped_by_budget,
        "dropped_by_relevance": packed.dropped_by_relevance,
    }


def _add_results(target: list, batch: list, seen: set[str]) -> None:
    for chunk in batch:
        if chunk.id in seen:
            continue

        target.append(chunk)
        seen.add(chunk.id)


def _add_expanded(
    target: list[ExpandedChunk],
    batch: list[ExpandedChunk],
    seen: set[str],
) -> None:
    for item in batch:
        if item.chunk.id in seen:
            continue

        target.append(item)
        seen.add(item.chunk.id)
