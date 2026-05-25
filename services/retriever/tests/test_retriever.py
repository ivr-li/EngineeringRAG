"""Tests for QdrantRetriever class."""

import pytest
from app.services.retriever import QdrantRetriever
from qdrant_client.models import FieldCondition, Filter, MatchText, MatchValue


class TestQdrantRetrieverBuildFilter:
    """Tests for _build_filter static method."""

    @pytest.mark.parametrize(
        "only_tables, filename_filter, section_filter, expected_conditions",
        [
            # All filters None
            (None, None, None, []),
            # Only only_tables
            (True, None, None, [("is_table", MatchValue)]),
            (False, None, None, [("is_table", MatchValue)]),
            # Only filename_filter
            (None, "SP_63_13330", None, [("filename", MatchText)]),
            # Only section_filter
            (None, None, "6.3", [("section_path", MatchText)]),
            # Combined filters
            (
                True,
                "SP_63",
                "6.3",
                [
                    ("is_table", MatchValue),
                    ("filename", MatchText),
                    ("section_path", MatchText),
                ],
            ),
        ],
    )
    def test_build_filter_combinations(
        self, only_tables, filename_filter, section_filter, expected_conditions
    ):
        """Test various filter combinations."""
        result = QdrantRetriever._build_filter(
            only_tables, filename_filter, section_filter
        )

        if not expected_conditions:
            assert result is None
            return

        assert result is not None
        assert isinstance(result, Filter)
        assert len(result.must) == len(expected_conditions)

        for condition, (key, condition_type) in zip(result.must, expected_conditions):
            assert isinstance(condition, FieldCondition)
            assert condition.key == key
            assert isinstance(condition.match, condition_type)

    def test_build_filter_only_tables_true(self):
        """Test building filter with only_tables=True."""
        result = QdrantRetriever._build_filter(
            only_tables=True, filename_filter=None, section_filter=None
        )

        assert result is not None
        assert len(result.must) == 1
        condition = result.must[0]
        assert condition.key == "is_table"
        assert condition.match.value is True

    def test_build_filter_only_tables_false(self):
        """Test building filter with only_tables=False."""
        result = QdrantRetriever._build_filter(
            only_tables=False, filename_filter=None, section_filter=None
        )

        assert result is not None
        assert len(result.must) == 1
        condition = result.must[0]
        assert condition.key == "is_table"
        assert condition.match.value is False

    def test_build_filter_filename(self):
        """Test building filter with filename_filter."""
        result = QdrantRetriever._build_filter(
            only_tables=None, filename_filter="SP_63_13330", section_filter=None
        )

        assert result is not None
        assert len(result.must) == 1
        condition = result.must[0]
        assert condition.key == "filename"
        assert isinstance(condition.match, MatchText)
        assert condition.match.text == "SP_63_13330"

    def test_build_filter_section_path(self):
        """Test building filter with section_filter."""
        result = QdrantRetriever._build_filter(
            only_tables=None, filename_filter=None, section_filter="6.3"
        )

        assert result is not None
        assert len(result.must) == 1
        condition = result.must[0]
        assert condition.key == "section_path"
        assert isinstance(condition.match, MatchText)
        assert condition.match.text == "6.3"


class TestQdrantRetrieverHitToResult:
    """Tests for _hit_to_result static method."""

    def test_hit_to_result_full_payload(self):
        """Test conversion with full payload."""
        hit = MagicMock()
        hit.id = "test-id-123"
        hit.score = 0.85
        hit.payload = {
            "text": "Тестовый текст",
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
        }

        result = QdrantRetriever._hit_to_result(hit)

        assert result.id == "test-id-123"
        assert result.score == 0.85
        assert result.text == "Тестовый текст"
        assert result.filename == "SP_63_13330"
        assert result.headings == ["Раздел 1", "Подраздел 1.1"]
        assert result.is_table is False
        assert result.chunk_index == 5
        assert result.man_refs == ["ГОСТ 27751"]
        assert result.cross_refs == ["СП 63.13330"]
        assert result.section_path == "1.2.3"
        assert result.section_level == 2
        assert result.parent_heading == "Раздел 1"
        assert result.leaf_heading == "Подраздел 1.1"
        assert result.is_overlap_window is False
        assert result.window_index == 0

    def test_hit_to_result_minimal_payload(self):
        """Test conversion with minimal payload (defaults)."""
        hit = MagicMock()
        hit.id = "test-id"
        hit.score = 0.5
        hit.payload = {"text": "Some text"}

        result = QdrantRetriever._hit_to_result(hit)

        assert result.id == "test-id"
        assert result.score == 0.5
        assert result.text == "Some text"
        assert result.filename == ""
        assert result.headings == []
        assert result.is_table is False
        assert result.chunk_index is None
        assert result.man_refs == []
        assert result.cross_refs == []
        assert result.section_path == ""
        assert result.section_level == 0
        assert result.parent_heading is None
        assert result.leaf_heading is None
        assert result.is_overlap_window is False
        assert result.window_index == 0

    def test_hit_to_result_empty_payload(self):
        """Test conversion with empty payload."""
        hit = MagicMock()
        hit.id = "test-id"
        hit.score = 0.5
        hit.payload = {}

        result = QdrantRetriever._hit_to_result(hit)

        assert result.id == "test-id"
        assert result.score == 0.5
        assert result.text == ""
        assert result.filename == ""
        assert result.headings == []

    def test_hit_to_result_none_payload(self):
        """Test conversion with None payload."""
        hit = MagicMock()
        hit.id = "test-id"
        hit.score = 0.5
        hit.payload = None

        result = QdrantRetriever._hit_to_result(hit)

        assert result.id == "test-id"
        assert result.score == 0.5
        assert result.text == ""
