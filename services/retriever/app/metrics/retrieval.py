from app.metrics.evidence import matches_bridge, matches_evidence_group
from app.metrics.schemas import (
    BridgeSource,
    EvalQuestion,
    EvidenceGroup,
    RetrievalMetrics,
)
from app.pipeline.schemas import PipelineResult
from app.schemas import RetrievalResult


def calculate_retrieval_metrics(
    pipeline_result: PipelineResult,
    ground_truth: EvalQuestion,
) -> RetrievalMetrics:
    groups = [group for group in ground_truth.evidence_groups if group.required]
    index_version = pipeline_result.configuration.index_version
    retrieved = pipeline_result.retrieved
    expanded = [item.chunk for item in pipeline_result.expanded]
    before_packing = [*retrieved, *expanded]
    context = pipeline_result.context_included

    return RetrievalMetrics(
        de_recall_at_5=_recall(groups, retrieved[:5], index_version),
        de_recall_at_10=_recall(groups, retrieved[:10], index_version),
        ee_recall=_recall(groups, before_packing, index_version),
        ce_recall=_recall(groups, context, index_version),
        cploss_rate=_packing_loss(groups, before_packing, context, index_version),
        rrs_rate=_reference_resolution_rate(pipeline_result, ground_truth, index_version),
        reciprocal_rank=_reciprocal_rank(groups, retrieved, index_version),
    )


def _recall(
    groups: list[EvidenceGroup],
    chunks: list[RetrievalResult],
    index_version: str,
) -> float | None:
    if not groups:
        return None

    found = sum(_group_found(group, chunks, index_version) for group in groups)
    return found / len(groups)


def _packing_loss(
    groups: list[EvidenceGroup],
    before_packing: list[RetrievalResult],
    context: list[RetrievalResult],
    index_version: str,
) -> float | None:
    found_before = [
        group for group in groups if _group_found(group, before_packing, index_version)
    ]
    if not found_before:
        return None

    lost = sum(not _group_found(group, context, index_version) for group in found_before)
    return lost / len(found_before)


def _reciprocal_rank(
    groups: list[EvidenceGroup],
    retrieved: list[RetrievalResult],
    index_version: str,
) -> float:
    for rank, chunk in enumerate(retrieved, start=1):
        if any(matches_evidence_group(group, chunk, index_version) for group in groups):
            return 1 / rank

    return 0.0


def _reference_resolution_rate(
    result: PipelineResult,
    ground_truth: EvalQuestion,
    index_version: str,
) -> float | None:
    targets = {group.name: group for group in ground_truth.evidence_groups}
    found_bridges = [
        (bridge, chunk)
        for bridge in ground_truth.bridge_sources
        for chunk in result.retrieved
        if matches_bridge(bridge, chunk, index_version)
    ]

    if not found_bridges:
        return None

    successes = _resolved_bridge_count(result, found_bridges, targets, index_version)
    return successes / len(found_bridges)


def _resolved_bridge_count(
    result: PipelineResult,
    found_bridges: list[tuple[BridgeSource, RetrievalResult]],
    targets: dict[str, EvidenceGroup],
    index_version: str,
) -> int:
    successes = 0

    for bridge, bridge_chunk in found_bridges:
        target = targets.get(bridge.target_evidence_group)
        if target is None:
            continue

        successes += _bridge_resolved(result, bridge_chunk.id, target, index_version)

    return successes


def _bridge_resolved(
    result: PipelineResult,
    bridge_id: str,
    target: EvidenceGroup,
    index_version: str,
) -> bool:
    return any(
        bridge_id in item.path
        and matches_evidence_group(target, item.chunk, index_version)
        for item in result.expanded
    )


def _group_found(
    group: EvidenceGroup,
    chunks: list[RetrievalResult],
    index_version: str,
) -> bool:
    return any(matches_evidence_group(group, chunk, index_version) for chunk in chunks)
