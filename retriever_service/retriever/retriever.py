from __future__ import annotations

from dataclasses import dataclass, field
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

QDRANT_URL = "http://localhost:6333"
QDRANT_COLLECTION = "construction_docs"
BGE_M3_MODEL = "BAAI/bge-m3"
DENSE_SIZE = 1024


@lru_cache(maxsize=1)
def get_bge_m3() -> BGEM3FlagModel:
    return BGEM3FlagModel(
        BGE_M3_MODEL,
        use_fp16=torch.cuda.is_available(),
        device="cuda" if torch.cuda.is_available() else "cpu",
    )


PROVIDERS = ["CUDAExecutionProvider"] if torch.cuda.is_available() else ["CPUExecutionProvider"]
SearchMode = Literal["hybrid", "dense", "sparse"]

# ==================================
# Singleton-models
# ==================================


@dataclass
class RetrievalResult:
    id: str
    score: float
    text: str
    filename: str
    headings: list[str]
    is_table: bool
    chunk_index: int | None = None

    # references
    man_refs: list[str] = field(default_factory=list)
    cross_refs: list[str] = field(default_factory=list)

    # hierarchy metadata
    section_path: str = ""
    section_level: int = 0
    parent_heading: str | None = None
    leaf_heading: str | None = None

    # sliding window markers
    is_overlap_window: bool = False
    window_index: int = 0


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
        filename_filter: str | None = None,
        section_filter: str | None = None,
    ) -> list[RetrievalResult]:
        qdrant_filter = self._build_filter(only_tables, filename_filter, section_filter)

        if mode == "dense":
            return self._search_dense(query, top_k, qdrant_filter)
        elif mode == "sparse":
            return self._search_sparse(query, top_k, qdrant_filter)
        else:
            return self._search_hybrid_rerank(query, top_k, prefetch_k, qdrant_filter)

    @staticmethod
    def _build_filter(
        only_tables: bool | None,
        filename_filter: str | None,
        section_filter: str | None = None,
    ) -> Filter | None:
        conditions = []

        if only_tables is not None:
            conditions.append(FieldCondition(key="is_table", match=MatchValue(value=only_tables)))
        if filename_filter:
            conditions.append(FieldCondition(key="filename", match=MatchText(text=filename_filter)))

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
            score=hit.score,
            text=p.get("text", ""),
            filename=p.get("filename", ""),
            headings=p.get("headings", []),
            is_table=p.get("is_table", False),
            chunk_index=p.get("chunk_index"),
            man_refs=p.get("man_refs", []),
            cross_refs=p.get("cross_refs", []),
            section_path=p.get("section_path", ""),
            section_level=p.get("section_level", 0),
            parent_heading=p.get("parent_heading", ""),
            leaf_heading=p.get("leaf_heading", ""),
            is_overlap_window=p.get("is_overlap_window", False),
            window_index=p.get("window_index", 0),
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
