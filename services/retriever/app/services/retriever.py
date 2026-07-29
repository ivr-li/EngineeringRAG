from __future__ import annotations

import os
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
    SparseVector,
)

from app.pipeline.schemas import ExpandedChunk
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
            return retrieved, self._expand_by_refs(retrieved, ref_depth)

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
    ) -> list[ExpandedChunk]:
        expanded: list[ExpandedChunk] = []
        seen_ids = {result.id for result in results}
        remaining_depth = min(ref_depth, REFERENCE_EXPANSION_MAX_DEPTH)

        for index, result in enumerate(results):
            if index >= REFERENCE_EXPANSION_TOP_RESULTS:
                continue

            context = _ExpansionContext(result, depth=1, path=[result.id])
            expanded.extend(self._table_sibling_results(context, seen_ids))
            expanded.extend(
                self._expand_result_refs(
                    context,
                    seen_ids,
                    remaining_depth=remaining_depth,
                )
            )

        return expanded

    def _expand_result_refs(
        self,
        context: _ExpansionContext,
        seen_ids: set[str],
        remaining_depth: int,
    ) -> list[ExpandedChunk]:
        if remaining_depth <= 0:
            return []

        direct = self._related_results(context, seen_ids)

        if remaining_depth == 1:
            return direct

        return self._with_nested_refs(direct, seen_ids, remaining_depth - 1)

    def _with_nested_refs(
        self,
        expanded_chunks: list[ExpandedChunk],
        seen_ids: set[str],
        remaining_depth: int,
    ) -> list[ExpandedChunk]:
        expanded: list[ExpandedChunk] = []

        for item in expanded_chunks:
            expanded.append(item)
            context = _ExpansionContext(item.chunk, item.depth + 1, item.path)
            expanded.extend(self._expand_result_refs(context, seen_ids, remaining_depth))

        return expanded

    def _related_results(
        self,
        context: _ExpansionContext,
        seen_ids: set[str],
    ) -> list[ExpandedChunk]:
        related: list[ExpandedChunk] = []

        for ref in context.source.cross_refs:
            related.extend(self._scroll_ref_results(context, ref, seen_ids))

        return sorted(related, key=lambda item: _result_sort_key(item.chunk))

    def _table_sibling_results(
        self,
        context: _ExpansionContext,
        seen_ids: set[str],
    ) -> list[ExpandedChunk]:
        result = context.source
        if not result.is_table or not result.table_id:
            return []

        records = self._scroll_filter_records(
            _table_id_filter(result.filename, result.table_id),
            REFERENCE_EXPANSION_LIMIT,
        )
        related = self._records_to_expanded_chunks(
            records,
            _table_relation(result.table_id),
            seen_ids,
            context,
        )

        return sorted(related, key=lambda item: _result_sort_key(item.chunk))

    def _scroll_ref_results(
        self,
        context: _ExpansionContext,
        ref: str,
        seen_ids: set[str],
    ) -> list[ExpandedChunk]:
        records = self._scroll_filter_records(
            _anchor_filter(context.source.filename, ref),
            REFERENCE_EXPANSION_LIMIT,
        )

        return self._records_to_expanded_chunks(records, ref, seen_ids, context)

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
    ) -> list[ExpandedChunk]:
        results: list[ExpandedChunk] = []

        for record in records:
            record_id = str(record.id)
            if record_id in seen_ids:
                continue

            seen_ids.add(record_id)
            results.append(
                ExpandedChunk(
                    chunk=self._hit_to_result(record),
                    expanded_from_chunk_id=context.source.id,
                    relation=relation,
                    depth=context.depth,
                    path=[*context.path, record_id],
                )
            )

        return results

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
