"""Tests for LLM tools (rewrite_query, compose_answer)."""

import pytest
from app.schemas import RetrievalResult
from app.services.llm_tools import (
    _build_context,
    _fallback_answer,
    compose_answer,
    rewrite_query,
)
from openai import OpenAI


class TestRewriteQuery:
    """Tests for rewrite_query function."""

    @pytest.mark.asyncio
    async def test_rewrite_query_success(self):
        """Test successful query rewriting."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message = MagicMock()
        mock_response.choices[0].message.content = " переформулированный запрос "

        mock_client.chat.completions.create = MagicMock(return_value=mock_response)

        result = await rewrite_query(
            client=mock_client,
            query="как рассчитать фундамент?",
            system_prompt="Переформулируй запрос",
        )

        assert result == ("переформулированный запрос", True)
        mock_client.chat.completions.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_rewrite_query_empty_response(self):
        """Test handling of empty LLM response."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message = MagicMock()
        mock_response.choices[0].message.content = ""

        mock_client.chat.completions.create = MagicMock(return_value=mock_response)

        result = await rewrite_query(
            client=mock_client,
            query="исходный запрос",
            system_prompt="Переформулируй запрос",
        )

        assert result == ("исходный запрос", False)

    @pytest.mark.asyncio
    async def test_rewrite_query_none_response(self):
        """Test handling of None LLM response."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message = MagicMock()
        mock_response.choices[0].message.content = None

        mock_client.chat.completions.create = MagicMock(return_value=mock_response)

        result = await rewrite_query(
            client=mock_client,
            query="исходный запрос",
            system_prompt="Переформулируй запрос",
        )

        assert result == ("исходный запрос", False)

    @pytest.mark.asyncio
    async def test_rewrite_query_error_handling(self, caplog):
        """Test error handling in rewrite_query."""
        mock_client = MagicMock()
        mock_client.chat.completions.create = MagicMock(
            side_effect=Exception("API Error")
        )

        result = await rewrite_query(
            client=mock_client,
            query="исходный запрос",
            system_prompt="Переформулируй запрос",
        )

        assert result == ("исходный запрос", False)
        assert "llm_call_error" in caplog.text or "API Error" in caplog.text


class TestComposeAnswer:
    """Tests for compose_answer function."""

    @pytest.mark.asyncio
    async def test_compose_answer_with_results(self):
        """Test composing answer with valid results."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message = MagicMock()
        mock_response.choices[0].message.content = "**Ответ:** найдено в документе"

        mock_client.chat.completions.create = MagicMock(return_value=mock_response)

        results = [
            RetrievalResult(
                id="1",
                score=0.8,
                text="Текст из документа",
                filename="SP_63_13330",
                headings=["Раздел 1"],
                is_table=False,
                chunk_index=0,
                man_refs=[],
                cross_refs=[],
                section_path="1.1",
                section_level=1,
                parent_heading=None,
                leaf_heading="Раздел 1",
                is_overlap_window=False,
                window_index=0,
            )
        ]

        answer = await compose_answer(
            client=mock_client,
            query="исходный запрос",
            effective_query=" переформулированный запрос ",
            system_prompt="Ответь по контексту",
            results=results,
        )

        assert "Ответ:" in answer
        assert "Текст из документа" in answer
        mock_client.chat.completions.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_compose_answer_empty_results(self):
        """Test composing answer with no results."""
        answer = await compose_answer(
            client=MagicMock(),
            query="исходный запрос",
            effective_query="переформулированный запрос",
            system_prompt="Ответь по контексту",
            results=[],
        )

        assert "ничего не найдено" in answer.lower()

    @pytest.mark.asyncio
    async def test_compose_answer_llm_error_fallback(self):
        """Test fallback when LLM fails."""
        mock_client = MagicMock()
        mock_client.chat.completions.create = MagicMock(
            side_effect=Exception("API Error")
        )

        results = [
            RetrievalResult(
                id="1",
                score=0.8,
                text="Текст из документа",
                filename="SP_63_13330",
                headings=["Раздел 1"],
                is_table=False,
                chunk_index=0,
                man_refs=[],
                cross_refs=[],
                section_path="1.1",
                section_level=1,
                parent_heading=None,
                leaf_heading="Раздел 1",
                is_overlap_window=False,
                window_index=0,
            )
        ]

        answer = await compose_answer(
            client=mock_client,
            query="исходный запрос",
            effective_query="переформулированный запрос",
            system_prompt="Ответь по контексту",
            results=results,
        )

        assert "Ответ:" in answer
        assert "не удалось сформировать" in answer

    @pytest.mark.asyncio
    async def test_compose_answer_empty_llm_response_fallback(self):
        """Test fallback when LLM returns empty response."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message = MagicMock()
        mock_response.choices[0].message.content = ""

        mock_client.chat.completions.create = MagicMock(return_value=mock_response)

        results = [
            RetrievalResult(
                id="1",
                score=0.8,
                text="Текст из документа",
                filename="SP_63_13330",
                headings=["Раздел 1"],
                is_table=False,
                chunk_index=0,
                man_refs=[],
                cross_refs=[],
                section_path="1.1",
                section_level=1,
                parent_heading=None,
                leaf_heading="Раздел 1",
                is_overlap_window=False,
                window_index=0,
            )
        ]

        answer = await compose_answer(
            client=mock_client,
            query="исходный запрос",
            effective_query="переформулированный запрос",
            system_prompt="Ответь по контексту",
            results=results,
        )

        assert "Ответ:" in answer


class TestBuildContext:
    """Tests for _build_context function."""

    def test_build_context_single_result(self, sample_results):
        """Test building context from single result."""
        context = _build_context([sample_results[0]])

        assert "Фрагмент 1" in context
        assert "SP_63_13330" in context
        assert "1.2.3" in context
        assert "Раздел 1" in context
        assert "Текст" in context
        assert "Тестовый текст" in context

    def test_build_context_multiple_results(self, sample_results):
        """Test building context from multiple results."""
        context = _build_context(sample_results)

        assert "Фрагмент 1" in context
        assert "Фрагмент 2" in context
        assert "---" in context

    def test_build_context_empty_list(self):
        """Test building context from empty list."""
        context = _build_context([])

        assert context == ""


class TestFallbackAnswer:
    """Tests for _fallback_answer function."""

    def test_fallback_answer_with_results(self, sample_results):
        """Test fallback answer with results."""
        answer = _fallback_answer(sample_results)

        assert "Ответ:" in answer
        assert "Не удалось сформировать" in answer
        assert "Что удалось найти" in answer
        assert "SP_63_13330" in answer
        assert "Основание" in answer

    def test_fallback_answer_empty_list(self):
        """Test fallback answer with empty results."""
        answer = _fallback_answer([])

        assert "Ответ:" in answer
        assert "Не удалось сформировать" in answer
        assert "Подходящие фрагменты не найдены" in answer
