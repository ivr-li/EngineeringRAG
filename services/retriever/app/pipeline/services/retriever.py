from __future__ import annotations

import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal

import torch
from FlagEmbedding import BGEM3FlagModel
from qdrant_client import QdrantClient
from qdrant_client.models import (
    FieldCondition,
    Filter,
    MatchText,
    MatchValue,
    Prefetch,
    Range,
    SparseVector,
)

from app.pipeline.schemas import ExpandedChunk
from app.pipeline.services.research import asks_norm_refs, is_norm_refs
from app.pipeline.services.text_terms import (
    expansion_terms,
    query_terms,
    term_coverage,
)
from app.schemas import RetrievalResult

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "construction_docs")
BGE_M3_MODEL = os.getenv("BGE_M3_MODEL", "BAAI/bge-m3")
BGE_M3_CACHE_DIR = Path("/root/.cache/huggingface/hub")
DENSE_SIZE = 1024
REFERENCE_EXPANSION_LIMIT = 64
REFERENCE_SCROLL_BATCH = 32
REFERENCE_EXPANSION_TOP_RESULTS = 5
REFERENCE_EXPANSION_MAX_DEPTH = 2
TABLE_FULL_EXPANSION_MAX_WINDOWS = 24
TABLE_NEIGHBOR_WINDOW_RADIUS = 2
SECTION_NEIGHBOR_RADIUS = 2
_TABLE_REF_RE = re.compile(
    r"\b(?:таблиц[аеиыу]?|табл\.)\s*(\d+(?:\.\d+)*)",
    re.IGNORECASE,
)
_SECTION_REF_RE = re.compile(
    r"\b(?:пункт[аеуы]?|п\.|подраздел[аеуы]?|раздел[аеуы]?)\s*"
    r"(\d+(?:\.\d+){1,3})",
    re.IGNORECASE,
)
_APPENDIX_REF_RE = re.compile(
    r"\bприложени[еяию]\s+([А-ЯA-Z])",
    re.IGNORECASE,
)
_EXPAND_MARKERS = (
    "таблиц",
    "табл.",
    "формул",
    "коэффициент",
    "$$",
    "пункт",
    "подраздел",
)


@lru_cache(maxsize=1)
def get_bge_m3() -> BGEM3FlagModel:
    return BGEM3FlagModel(
        str(_local_bge_m3_snapshot()),
        use_fp16=torch.cuda.is_available(),
        device="cuda" if torch.cuda.is_available() else "cpu",
    )


def _local_bge_m3_snapshot() -> Path:
    model_cache = BGE_M3_CACHE_DIR / f"models--{BGE_M3_MODEL.replace('/', '--')}"
    revision_file = model_cache / "refs/main"
    if not revision_file.is_file():
        raise FileNotFoundError(
            f"BGE-M3 revision not found at {revision_file}. "
            "Set BGE_M3_CACHE_DIR to the Hugging Face hub cache directory."
        )

    snapshot = model_cache / "snapshots" / revision_file.read_text().strip()
    if not snapshot.is_dir():
        raise FileNotFoundError(f"BGE-M3 snapshot not found at {snapshot}")

    return snapshot


PROVIDERS = (
    ["CUDAExecutionProvider"] if torch.cuda.is_available() else ["CPUExecutionProvider"]
)
SearchMode = Literal["hybrid", "dense", "sparse"]


@dataclass(frozen=True)
class _ExpansionContext:
    source: RetrievalResult
    depth: int
    path: list[str]


def _anchor_filter(filename: str, ref: str) -> Filter:
    return Filter(
        must=[
            FieldCondition(key="filename", match=MatchText(text=filename)),
            FieldCondition(key="anchor_refs", match=MatchValue(value=ref)),
        ]
    )


def _table_id_filter(filename: str, table_id: str) -> Filter:
    return Filter(
        must=[
            FieldCondition(key="filename", match=MatchText(text=filename)),
            FieldCondition(key="table_id", match=MatchValue(value=table_id)),
        ]
    )


def _table_window_filter(
    filename: str,
    table_id: str,
    start: int,
    end: int,
) -> Filter:
    return Filter(
        must=[
            FieldCondition(key="filename", match=MatchText(text=filename)),
            FieldCondition(key="table_id", match=MatchValue(value=table_id)),
            FieldCondition(key="table_window_index", range=Range(gte=start, lte=end)),
        ]
    )


def _chunk_window_filter(filename: str, start: int, end: int) -> Filter:
    return Filter(
        must=[
            FieldCondition(key="filename", match=MatchText(text=filename)),
            FieldCondition(key="chunk_index", range=Range(gte=start, lte=end)),
        ]
    )


def _result_sort_key(result: RetrievalResult) -> tuple[int, int, int]:
    return (
        result.chunk_index if result.chunk_index is not None else 10**9,
        result.table_part_index or 0,
        result.table_window_index or 0,
    )


def _hit_score(hit) -> float:
    score = getattr(hit, "score", None)

    return float(score) if score is not None else 0.0


def _table_relation(table_id: str) -> str:
    return f"table_id:{table_id}"


def _is_large_table_hit(result: RetrievalResult) -> bool:
    total = result.table_window_total or 0
    return bool(total > TABLE_FULL_EXPANSION_MAX_WINDOWS and result.table_window_index)


def _nearby_table_filter(result: RetrievalResult) -> Filter:
    index = result.table_window_index or 1
    start = max(1, index - TABLE_NEIGHBOR_WINDOW_RADIUS)
    end = index + TABLE_NEIGHBOR_WINDOW_RADIUS

    return _table_window_filter(result.filename, result.table_id or "", start, end)


def _allow_expanded_chunk(
    source: RetrievalResult,
    chunk: RetrievalResult,
    relation: str,
    query: str,
) -> bool:
    if _is_heading_only(chunk):
        return False

    if is_norm_refs(chunk) and not asks_norm_refs(query):
        return False

    if relation.startswith(("table:", "table_id:")):
        return _allow_table_ref(source, chunk, query)

    if relation.startswith("neighbor:"):
        return _allow_neighbor(source, chunk, query)

    if relation.startswith("section:"):
        return _term_coverage(source, chunk) >= 0.15

    return True


def _allow_table_ref(
    source: RetrievalResult,
    chunk: RetrievalResult,
    query: str,
) -> bool:
    terms = query_terms(query)
    if not terms:
        return _term_coverage(source, chunk) >= 0.15

    query_match = term_coverage(terms, _searchable_text(chunk)) >= 0.5
    source_match = _term_coverage(source, chunk) >= 0.2

    return query_match or source_match


def _allow_neighbor(
    source: RetrievalResult,
    chunk: RetrievalResult,
    query: str,
) -> bool:
    if not _same_section_area(source, chunk):
        return False
    if _term_coverage(source, chunk) >= 0.1:
        return True

    terms = query_terms(query)
    return bool(terms and term_coverage(terms, _searchable_text(chunk)) >= 0.35)


def _same_section_area(source: RetrievalResult, chunk: RetrievalResult) -> bool:
    if not source.section_path or not chunk.section_path:
        return True

    return source.section_path == chunk.section_path


def _allow_table_siblings(result: RetrievalResult, query: str) -> bool:
    terms = query_terms(query)
    if not terms:
        return True

    return term_coverage(terms, _searchable_text(result)) >= 0.5


def _is_heading_only(result: RetrievalResult) -> bool:
    text = result.text.strip()
    if result.is_table:
        return False

    words = text.split()
    anchors = [ref for ref in result.anchor_refs if str(ref).startswith("section:")]

    return len(words) <= 12 and bool(anchors or result.section_path)


def _term_coverage(source: RetrievalResult, chunk: RetrievalResult) -> float:
    terms = expansion_terms(_searchable_text(source))
    return term_coverage(terms, _searchable_text(chunk))


def _searchable_text(result: RetrievalResult) -> str:
    return " ".join(
        [
            result.text,
            result.filename,
            result.section_path,
            result.parent_heading or "",
            result.leaf_heading or "",
            result.table_caption or "",
        ]
    )


def _semantic_refs(result: RetrievalResult) -> list[str]:
    refs: list[str] = []
    seen: set[str] = set()

    for ref in [*result.cross_refs, *_parsed_refs(result.text)]:
        if ref in seen:
            continue

        refs.append(ref)
        seen.add(ref)

    return refs


def _parsed_refs(text: str) -> list[str]:
    refs: list[str] = []
    refs.extend(f"table:{match.group(1)}" for match in _TABLE_REF_RE.finditer(text))
    refs.extend(f"section:{match.group(1)}" for match in _SECTION_REF_RE.finditer(text))
    refs.extend(
        f"appendix:{match.group(1).upper()}" for match in _APPENDIX_REF_RE.finditer(text)
    )

    return refs


def _needs_neighbor_expansion(result: RetrievalResult) -> bool:
    if result.is_table:
        return False

    text = _searchable_text(result).lower()
    return bool(_semantic_refs(result)) or any(marker in text for marker in _EXPAND_MARKERS)


def _encode_query(
    query: str,
    dense: bool,
    sparse: bool,
    colbert: bool,
):
    model = get_bge_m3()

    return model.encode(
        [query],
        max_length=128,
        return_dense=dense,
        return_sparse=sparse,
        return_colbert_vecs=colbert,
    )


def _sparse_vector(weights) -> SparseVector:
    return SparseVector(
        indices=[int(key) for key in weights.keys()],
        values=[float(value) for value in weights.values()],
    )


def _hybrid_prefetch(
    dense_vec: list[float],
    weights,
    prefetch_k: int,
    qdrant_filter: Filter | None,
) -> list[Prefetch]:
    return [
        Prefetch(
            query=dense_vec,
            using="dense",
            limit=prefetch_k // 2,
            filter=qdrant_filter,
        ),
        Prefetch(
            query=_sparse_vector(weights),
            using="sparse",
            limit=prefetch_k,
            filter=qdrant_filter,
        ),
    ]


# ==================================
# Retriever
# ==================================


class QdrantRetriever:
    """
    Retriever для коллекции construction_docs.

    Режимы поиска
    -------------
    hybrid (рекомендуется)
        dense Prefetch + sparse Prefetch → Colbert rerank (MaxSim).
        Широкий recall от двух сигналов + точный rerank на токен-уровне.

    dense
        Только BGE-M3 dense (ANN по HNSW).

    sparse
        Только BGE-M3 BM25-sparse (точное вхождение терминов).

    Важно
    -----
    Colbert-вектор должен быть создан с hnsw_config.m=0 —
    он не используется для ANN-поиска, только для rerank через MaxSim.
    """

    def __init__(
        self,
        url: str = QDRANT_URL,
        collection: str = QDRANT_COLLECTION,
        timeout: int = 30,
    ) -> None:
        self.client = QdrantClient(url=url, timeout=timeout)
        self.collection = collection

    def search(
        self,
        query: str,
        top_k: int = 10,
        prefetch_k: int = 40,
        mode: SearchMode = "hybrid",
        only_tables: bool | None = None,
        expand_refs: bool = True,
        ref_depth: int = 1,
        filename_filter: str | None = None,
        section_filter: str | None = None,
    ) -> list[RetrievalResult]:
        retrieved, expanded = self.search_stages(
            query=query,
            top_k=top_k,
            prefetch_k=prefetch_k,
            mode=mode,
            only_tables=only_tables,
            expand_refs=expand_refs,
            ref_depth=ref_depth,
            filename_filter=filename_filter,
            section_filter=section_filter,
        )

        return self.merge_stages(retrieved, expanded)

    def search_stages(
        self,
        query: str,
        top_k: int = 10,
        prefetch_k: int = 40,
        mode: SearchMode = "hybrid",
        only_tables: bool | None = None,
        expand_refs: bool = True,
        ref_depth: int = 1,
        filename_filter: str | None = None,
        section_filter: str | None = None,
    ) -> tuple[list[RetrievalResult], list[ExpandedChunk]]:
        qdrant_filter = self._build_filter(only_tables, filename_filter, section_filter)
        retrieved = self._search(query, top_k, prefetch_k, mode, qdrant_filter)

        if expand_refs and ref_depth > 0:
            return retrieved, self._expand_by_refs(retrieved, ref_depth, query)

        return retrieved, []

    def _search(
        self,
        query: str,
        top_k: int,
        prefetch_k: int,
        mode: SearchMode,
        qdrant_filter: Filter | None,
    ) -> list[RetrievalResult]:
        if mode == "dense":
            return self._search_dense(query, top_k, qdrant_filter)
        if mode == "sparse":
            return self._search_sparse(query, top_k, qdrant_filter)

        return self._search_hybrid_rerank(query, top_k, prefetch_k, qdrant_filter)

    @staticmethod
    def merge_stages(
        retrieved: list[RetrievalResult],
        expanded: list[ExpandedChunk],
    ) -> list[RetrievalResult]:
        expanded_by_source: dict[str, list[ExpandedChunk]] = {}
        for item in expanded:
            expanded_by_source.setdefault(item.expanded_from_chunk_id, []).append(item)

        merged: list[RetrievalResult] = []
        for result in retrieved:
            merged.append(result)
            QdrantRetriever._append_expanded(result.id, expanded_by_source, merged)

        return merged

    @staticmethod
    def _append_expanded(
        source_id: str,
        expanded_by_source: dict[str, list[ExpandedChunk]],
        merged: list[RetrievalResult],
    ) -> None:
        for item in expanded_by_source.get(source_id, []):
            merged.append(item.chunk)
            QdrantRetriever._append_expanded(item.chunk.id, expanded_by_source, merged)

    @staticmethod
    def _build_filter(
        only_tables: bool | None,
        filename_filter: str | None,
        section_filter: str | None = None,
    ) -> Filter | None:
        conditions = []

        if only_tables is not None:
            conditions.append(
                FieldCondition(key="is_table", match=MatchValue(value=only_tables))
            )

        if filename_filter:
            conditions.append(
                FieldCondition(key="filename", match=MatchText(text=filename_filter))
            )

        # section_path substring match
        if section_filter:
            conditions.append(
                FieldCondition(key="section_path", match=MatchText(text=section_filter))
            )

        return Filter(must=conditions) if conditions else None

    @staticmethod
    def _hit_to_result(hit) -> RetrievalResult:
        p = hit.payload or {}

        return RetrievalResult(
            id=str(hit.id),
            score=_hit_score(hit),
            text=p.get("text", ""),
            filename=p.get("filename", ""),
            headings=p.get("headings", []),
            is_table=p.get("is_table", False),
            chunk_index=p.get("chunk_index"),
            man_refs=p.get("man_refs", []),
            cross_refs=p.get("cross_refs", []),
            anchor_refs=p.get("anchor_refs", []),
            section_path=p.get("section_path", ""),
            section_level=p.get("section_level", 0),
            parent_heading=p.get("parent_heading", ""),
            leaf_heading=p.get("leaf_heading", ""),
            is_overlap_window=p.get("is_overlap_window", False),
            window_index=p.get("window_index", 0),
            table_id=p.get("table_id"),
            table_caption=p.get("table_caption"),
            table_part_index=p.get("table_part_index"),
            table_part_total=p.get("table_part_total"),
            table_window_index=p.get("table_window_index"),
            table_window_total=p.get("table_window_total"),
            table_orientation=p.get("table_orientation"),
        )

    def _expand_by_refs(
        self,
        results: list[RetrievalResult],
        ref_depth: int,
        query: str,
    ) -> list[ExpandedChunk]:
        expanded: list[ExpandedChunk] = []
        seen_ids = {result.id for result in results}
        remaining_depth = min(ref_depth, REFERENCE_EXPANSION_MAX_DEPTH)

        for index, result in enumerate(results):
            if index >= REFERENCE_EXPANSION_TOP_RESULTS:
                continue

            context = _ExpansionContext(result, depth=1, path=[result.id])
            expanded.extend(self._table_sibling_results(context, seen_ids, query))
            expanded.extend(
                self._expanded_neighbors(
                    context,
                    seen_ids,
                    remaining_depth=remaining_depth,
                    query=query,
                )
            )
            expanded.extend(
                self._expand_result_refs(
                    context,
                    seen_ids,
                    remaining_depth=remaining_depth,
                    query=query,
                )
            )

        return expanded

    def _expanded_neighbors(
        self,
        context: _ExpansionContext,
        seen_ids: set[str],
        remaining_depth: int,
        query: str,
    ) -> list[ExpandedChunk]:
        neighbors = self._neighbor_results(context, seen_ids, query)
        if remaining_depth <= 1:
            return neighbors

        return self._with_nested_refs(neighbors, seen_ids, remaining_depth - 1, query)

    def _expand_result_refs(
        self,
        context: _ExpansionContext,
        seen_ids: set[str],
        remaining_depth: int,
        query: str,
    ) -> list[ExpandedChunk]:
        if remaining_depth <= 0:
            return []

        direct = self._related_results(context, seen_ids, query)

        if remaining_depth == 1:
            return direct

        return self._with_nested_refs(direct, seen_ids, remaining_depth - 1, query)

    def _with_nested_refs(
        self,
        expanded_chunks: list[ExpandedChunk],
        seen_ids: set[str],
        remaining_depth: int,
        query: str,
    ) -> list[ExpandedChunk]:
        expanded: list[ExpandedChunk] = []

        for item in expanded_chunks:
            expanded.append(item)
            context = _ExpansionContext(item.chunk, item.depth + 1, item.path)
            expanded.extend(
                self._expand_result_refs(context, seen_ids, remaining_depth, query)
            )

        return expanded

    def _related_results(
        self,
        context: _ExpansionContext,
        seen_ids: set[str],
        query: str,
    ) -> list[ExpandedChunk]:
        related: list[ExpandedChunk] = []

        for ref in _semantic_refs(context.source):
            related.extend(self._scroll_ref_results(context, ref, seen_ids, query))

        return sorted(related, key=lambda item: _result_sort_key(item.chunk))

    def _neighbor_results(
        self,
        context: _ExpansionContext,
        seen_ids: set[str],
        query: str,
    ) -> list[ExpandedChunk]:
        result = context.source
        if not _needs_neighbor_expansion(result) or result.chunk_index is None:
            return []

        start = max(0, result.chunk_index - SECTION_NEIGHBOR_RADIUS)
        end = result.chunk_index + SECTION_NEIGHBOR_RADIUS
        records = self._scroll_filter_records(
            _chunk_window_filter(result.filename, start, end),
            REFERENCE_EXPANSION_LIMIT,
        )

        related = self._records_to_expanded_chunks(
            records, "neighbor:section", seen_ids, context, query
        )

        return sorted(related, key=lambda item: _result_sort_key(item.chunk))

    def _table_sibling_results(
        self,
        context: _ExpansionContext,
        seen_ids: set[str],
        query: str,
    ) -> list[ExpandedChunk]:
        result = context.source
        if not result.is_table or not result.table_id:
            return []

        if not _allow_table_siblings(result, query):
            return []

        scroll_filter = _table_id_filter(result.filename, result.table_id)
        if _is_large_table_hit(result):
            scroll_filter = _nearby_table_filter(result)

        records = self._scroll_filter_records(
            scroll_filter,
            REFERENCE_EXPANSION_LIMIT,
        )
        related = self._records_to_expanded_chunks(
            records,
            _table_relation(result.table_id),
            seen_ids,
            context,
            query,
        )

        return sorted(related, key=lambda item: _result_sort_key(item.chunk))

    def _scroll_ref_results(
        self,
        context: _ExpansionContext,
        ref: str,
        seen_ids: set[str],
        query: str,
    ) -> list[ExpandedChunk]:
        records = self._scroll_filter_records(
            _anchor_filter(context.source.filename, ref),
            REFERENCE_EXPANSION_LIMIT,
        )

        return self._records_to_expanded_chunks(records, ref, seen_ids, context, query)

    def _scroll_filter_records(
        self,
        scroll_filter: Filter,
        limit: int,
    ):
        records = []
        next_offset = None

        while len(records) < limit:
            batch, next_offset = self.client.scroll(
                collection_name=self.collection,
                scroll_filter=scroll_filter,
                limit=min(REFERENCE_SCROLL_BATCH, limit - len(records)),
                offset=next_offset,
                with_payload=True,
                with_vectors=False,
            )
            records.extend(batch)

            if not batch or next_offset is None:
                break

        return records

    def _records_to_expanded_chunks(
        self,
        records,
        relation: str,
        seen_ids: set[str],
        context: _ExpansionContext,
        query: str,
    ) -> list[ExpandedChunk]:
        results: list[ExpandedChunk] = []

        for record in records:
            record_id = str(record.id)
            if record_id in seen_ids:
                continue

            item = self._expanded_record(record, relation, context, query)
            if item is None:
                continue

            seen_ids.add(record_id)
            results.append(item)

        return results

    def _expanded_record(
        self,
        record,
        relation: str,
        context: _ExpansionContext,
        query: str,
    ) -> ExpandedChunk | None:
        chunk = self._hit_to_result(record)
        if not _allow_expanded_chunk(context.source, chunk, relation, query):
            return None

        return ExpandedChunk(
            chunk=chunk,
            expanded_from_chunk_id=context.source.id,
            relation=relation,
            depth=context.depth,
            path=[*context.path, chunk.id],
        )

    def _search_hybrid_rerank(
        self,
        query: str,
        top_k: int,
        prefetch_k: int,
        qdrant_filter: Filter | None,
    ) -> list[RetrievalResult]:
        """
        Hybrid search: dense Prefetch + sparse Prefetch → Colbert MaxSim rerank.
        """
        output = _encode_query(query, dense=True, sparse=True, colbert=True)
        dense_vec = output["dense_vecs"][0].tolist()
        weights = output["lexical_weights"][0]
        colbert_vec = output["colbert_vecs"][0].tolist()

        hits = self.client.query_points(
            collection_name=self.collection,
            prefetch=_hybrid_prefetch(dense_vec, weights, prefetch_k, qdrant_filter),
            query=colbert_vec,
            using="colbert",
            limit=top_k,
            with_payload=True,
        ).points

        return [self._hit_to_result(h) for h in hits]

    def _search_dense(
        self,
        query: str,
        top_k: int,
        qdrant_filter: Filter | None,
    ) -> list[RetrievalResult]:
        """ANN search using BGE-M3 dense vector only."""
        output = _encode_query(query, dense=True, sparse=False, colbert=False)
        vec = output["dense_vecs"][0].tolist()

        result = self.client.query_points(
            collection_name=self.collection,
            query=vec,
            using="dense",
            limit=top_k,
            query_filter=qdrant_filter,
            with_payload=True,
        )
        return [self._hit_to_result(h) for h in result.points]

    def _search_sparse(
        self,
        query: str,
        top_k: int,
        qdrant_filter: Filter | None,
    ) -> list[RetrievalResult]:
        """BM25 keyword search using BGE-M3 sparse (lexical) weights."""
        output = _encode_query(query, dense=False, sparse=True, colbert=False)
        sv = _sparse_vector(output["lexical_weights"][0])

        result = self.client.query_points(
            collection_name=self.collection,
            query=sv,
            using="sparse",
            limit=top_k,
            query_filter=qdrant_filter,
            with_payload=True,
        )
        return [self._hit_to_result(h) for h in result.points]
