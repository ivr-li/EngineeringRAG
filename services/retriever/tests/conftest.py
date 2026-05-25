"""Pytest fixtures and configuration for retriever tests."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.schemas import RetrievalResult


@pytest.fixture(autouse=True)
def mock_qdrant_client():
    """Mock QdrantClient for all tests."""
    with patch("app.services.retriever.QdrantClient") as mock:
        mock_instance = MagicMock()
        mock.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_bge_m3_encoder():
    """Mock BGE-M3 encoder."""
    with patch("app.services.retriever.get_bge_m3") as mock:
        model = MagicMock()

        def encode_side_effect(texts, **kwargs):
            result = {
                "dense_vecs": [[0.1] * 1024],
                "lexical_weights": [{"тест": 1.5, "запрос": 1.2}],
                "colbert_vecs": [[[0.1] * 1024]],
            }
            if not kwargs.get("return_dense", True):
                del result["dense_vecs"]
            if not kwargs.get("return_sparse", True):
                del result["lexical_weights"]
            if not kwargs.get("return_colbert_vecs", True):
                del result["colbert_vecs"]
            return result

        model.encode = MagicMock(side_effect=encode_side_effect)
        mock.return_value = model
        yield model


@pytest.fixture
def sample_hits():
    """Sample Qdrant hits for testing."""
    return [
        MagicMock(
            id="1",
            score=0.85,
            payload={
                "text": "Test text content",
                "filename": "SP_63_13330",
                "headings": ["Раздел 1", "Подраздел 1.1"],
                "is_table": False,
                "chunk_index": 5,
                "man_refs": ["ГОСТ 27751"],
                "cross_refs": ["СП 63.13330"],
                "section_path": "1.2.3",
                "section_level": 2,
                "parent_heading": "Раздел 1",
                "leaf_heading": "Подраздел 1.1",
                "is_overlap_window": False,
                "window_index": 0,
            },
        ),
        MagicMock(
            id="2",
            score=0.72,
            payload={
                "text": "Table content",
                "filename": "SP_63_13330",
                "headings": ["Таблица 1"],
                "is_table": True,
                "chunk_index": 12,
                "man_refs": [],
                "cross_refs": [],
                "section_path": "4.5",
                "section_level": 1,
                "parent_heading": None,
                "leaf_heading": "Таблица 1",
                "is_overlap_window": True,
                "window_index": 1,
            },
        ),
    ]


@pytest.fixture
def sample_results():
    """Sample RetrievalResult objects."""
    return [
        RetrievalResult(
            id="1",
            score=0.85,
            text="Test text content",
            filename="SP_63_13330",
            headings=["Раздел 1", "Подраздел 1.1"],
            is_table=False,
            chunk_index=5,
            man_refs=["ГОСТ 27751"],
            cross_refs=["СП 63.13330"],
            section_path="1.2.3",
            section_level=2,
            parent_heading="Раздел 1",
            leaf_heading="Подраздел 1.1",
            is_overlap_window=False,
            window_index=0,
        ),
        RetrievalResult(
            id="2",
            score=0.72,
            text="Table content",
            filename="SP_63_13330",
            headings=["Таблица 1"],
            is_table=True,
            chunk_index=12,
            man_refs=[],
            cross_refs=[],
            section_path="4.5",
            section_level=1,
            parent_heading=None,
            leaf_heading="Таблица 1",
            is_overlap_window=True,
            window_index=1,
        ),
    ]
