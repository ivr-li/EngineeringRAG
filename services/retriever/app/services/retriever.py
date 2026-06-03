from __future__ import annotations

from functools import lru_cache
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

from app.schemas import RetrievalResult

QDRANT_URL = "http://localhost:6333"
QDRANT_COLLECTION = "construction_docs"
BGE_M3_MODEL = "BAAI/bge-m3"
DENSE_SIZE = 1024
REFERENCE_EXPANSION_LIMIT = 64
REFERENCE_SCROLL_BATCH = 32
REFERENCE_EXPANSION_TOP_RESULTS = 5
REFERENCE_EXPANSION_MAX_DEPTH = 2


@lru_cache(maxsize=1)
def get_bge_m3() -> BGEM3FlagModel:
    return BGEM3FlagModel(
        BGE_M3_MODEL,
        use_fp16=torch.cuda.is_available(),
        device="cuda" if torch.cuda.is_available() else "cpu",
    )


PROVIDERS = (
    ["CUDAExecutionProvider"] if torch.cuda.is_available() else ["CPUExecutionProvider"]
)
SearchMode = Literal["hybrid", "dense", "sparse"]


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
        qdrant_filter = self._build_filter(only_tables, filename_filter, section_filter)

        if mode == "dense":
            results = self._search_dense(query, top_k, qdrant_filter)
        elif mode == "sparse":
            results = self._search_sparse(query, top_k, qdrant_filter)
        else:
            results = self._search_hybrid_rerank(query, top_k, prefetch_k, qdrant_filter)

        if expand_refs and ref_depth > 0:
            return self._expand_by_refs(results, ref_depth)
        return results

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
            expanded_from=p.get("expanded_from"),
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
    ) -> list[RetrievalResult]:
        expanded: list[RetrievalResult] = []
        seen_ids = {result.id for result in results}
        depth = min(ref_depth, REFERENCE_EXPANSION_MAX_DEPTH)
        for index, result in enumerate(results):
            expanded.append(result)
            if index >= REFERENCE_EXPANSION_TOP_RESULTS:
                continue
            expanded.extend(self._table_sibling_results(result, seen_ids))
            expanded.extend(self._expand_result_refs(result, seen_ids, depth))
        return expanded

    def _expand_result_refs(
        self,
        result: RetrievalResult,
        seen_ids: set[str],
        depth: int,
    ) -> list[RetrievalResult]:
        if depth <= 0:
            return []
        direct = self._related_results(result, seen_ids)
        if depth == 1:
            return direct
        return self._with_nested_refs(direct, seen_ids, depth - 1)

    def _with_nested_refs(
        self,
        results: list[RetrievalResult],
        seen_ids: set[str],
        depth: int,
    ) -> list[RetrievalResult]:
        expanded: list[RetrievalResult] = []
        for result in results:
            expanded.append(result)
            expanded.extend(self._expand_result_refs(result, seen_ids, depth))
        return expanded

    def _related_results(
        self,
        result: RetrievalResult,
        seen_ids: set[str],
    ) -> list[RetrievalResult]:
        related: list[RetrievalResult] = []
        for ref in result.cross_refs:
            related.extend(self._scroll_ref_results(result, ref, seen_ids))
        return sorted(related, key=_result_sort_key)

    def _table_sibling_results(
        self,
        result: RetrievalResult,
        seen_ids: set[str],
    ) -> list[RetrievalResult]:
        if not result.is_table or not result.table_id:
            return []
        records = self._scroll_filter_records(
            _table_id_filter(result.filename, result.table_id),
            REFERENCE_EXPANSION_LIMIT,
        )
        related = self._records_to_related_results(
            records,
            _table_relation(result.table_id),
            seen_ids,
        )
        return sorted(related, key=_result_sort_key)

    def _scroll_ref_results(
        self,
        result: RetrievalResult,
        ref: str,
        seen_ids: set[str],
    ) -> list[RetrievalResult]:
        records = self._scroll_filter_records(
            _anchor_filter(result.filename, ref),
            REFERENCE_EXPANSION_LIMIT,
        )
        return self._records_to_related_results(records, ref, seen_ids)

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

    def _records_to_related_results(
        self,
        records,
        ref: str,
        seen_ids: set[str],
    ) -> list[RetrievalResult]:
        results: list[RetrievalResult] = []
        for record in records:
            record_id = str(record.id)
            if record_id in seen_ids:
                continue
            related = self._hit_to_result(record)
            related.expanded_from = ref
            seen_ids.add(record_id)
            results.append(related)
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
        model = get_bge_m3()
        output = model.encode(
            [query],
            max_length=128,  # короткий запрос — экономия памяти
            return_dense=True,
            return_sparse=True,
            return_colbert_vecs=True,
        )
        dense_vec = output["dense_vecs"][0].tolist()
        lw = output["lexical_weights"][0]
        colbert_vec = output["colbert_vecs"][0].tolist()

        hits = self.client.query_points(
            collection_name=self.collection,
            prefetch=[
                # Semantic recall
                Prefetch(
                    query=dense_vec,
                    using="dense",
                    limit=prefetch_k // 2,
                    filter=qdrant_filter,
                ),
                # Keyword recall
                Prefetch(
                    query=SparseVector(
                        indices=[int(k) for k in lw.keys()],
                        values=[float(v) for v in lw.values()],
                    ),
                    using="sparse",
                    limit=prefetch_k,
                    filter=qdrant_filter,
                ),
            ],
            # Colbert rerank (MaxSim) across both prefetch sets
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
        model = get_bge_m3()
        output = model.encode(
            [query],
            max_length=128,
            return_dense=True,
            return_sparse=False,
            return_colbert_vecs=False,
        )
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
        model = get_bge_m3()
        output = model.encode(
            [query],
            max_length=128,
            return_dense=False,
            return_sparse=True,
            return_colbert_vecs=False,
        )
        lw = output["lexical_weights"][0]
        sv = SparseVector(
            indices=[int(k) for k in lw.keys()],
            values=[float(v) for v in lw.values()],
        )

        result = self.client.query_points(
            collection_name=self.collection,
            query=sv,
            using="sparse",
            limit=top_k,
            query_filter=qdrant_filter,
            with_payload=True,
        )
        return [self._hit_to_result(h) for h in result.points]
