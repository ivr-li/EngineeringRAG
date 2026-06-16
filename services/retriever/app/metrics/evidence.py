import re

from app.metrics.schemas import BridgeSource, EvidenceGroup, EvidenceSource
from app.schemas import RetrievalResult


def matches_evidence_group(
    group: EvidenceGroup,
    chunk: RetrievalResult,
    index_version: str,
) -> bool:
    resolved_ids = set(group.resolved_chunk_ids.get(index_version, []))
    if resolved_ids:
        return chunk.id in resolved_ids

    return any(_matches_source(source, chunk) for source in group.acceptable_sources)


def matches_bridge(
    bridge: BridgeSource,
    chunk: RetrievalResult,
    index_version: str,
) -> bool:
    resolved_ids = set(bridge.resolved_chunk_ids.get(index_version, []))
    if resolved_ids:
        return chunk.id in resolved_ids

    source = EvidenceSource(
        document_id=bridge.document_id or "",
        anchor=bridge.anchor,
        quote=bridge.quote,
    )
    return _matches_source(source, chunk)


def _matches_source(source: EvidenceSource, chunk: RetrievalResult) -> bool:
    if not _document_matches(source.document_id, chunk.filename):
        return False

    if source.quote:
        return _text_contains(_normalize(source.quote), _normalize(chunk.text))

    return _anchor_matches(source.anchor, chunk)


def _document_matches(document_id: str, filename: str) -> bool:
    document = _normalize(document_id)
    chunk_filename = _normalize(filename)

    return bool(
        document
        and chunk_filename
        and (document in chunk_filename or chunk_filename in document)
    )


def _anchor_matches(anchor: str | None, chunk: RetrievalResult) -> bool:
    if not anchor:
        return False

    metadata = " ".join(
        [
            *chunk.anchor_refs,
            chunk.section_path,
            chunk.table_caption or "",
            chunk.leaf_heading or "",
        ]
    )
    return _normalize(anchor) in _normalize(metadata)


def _text_contains(expected: str, actual: str) -> bool:
    if not expected or not actual:
        return False

    if expected in actual:
        return True

    return len(actual) >= 80 and actual in expected


def _normalize(value: str) -> str:
    return re.sub(r"[^а-яёa-z0-9]+", " ", value.lower()).strip()
