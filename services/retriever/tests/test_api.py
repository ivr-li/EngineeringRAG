"""Integration tests for main.py FastAPI endpoints."""

import pytest
from app.main import app
from app.schemas import RetrievalResult, SearchRequest, SearchResponse
from fastapi.testclient import TestClient

client = TestClient(app)


class TestHealthEndpoint:
    """Tests for /he endpoint."""

    def test_health_endpoint(self):
        """Test health check endpoint."""
        response = client.get("/he")
        assert response.status_code == 200
        data = response.json()
        assert data == {"message": "Hello World"}


class TestSearchEndpoint:
    """Tests for /search endpoint."""

    @pytest.fixture
    def search_request(self):
        """Default search request payload."""
        return {
            "query": "расчет фундамента",
            "top_k": 5,
            "prefetch_k": 20,
            "mode": "hybrid",
            "only_tables": None,
            "use_rewriter": True,
            "filename_filter": None,
            "section_filter": None,
            "rewrite_system_prompt": "Переформулируй запрос",
            "compose_system_prompt": "Ответь по контексту",
        }

    @pytest.mark.asyncio
    async def test_search_endpoint_basic(self, mocker, search_request):
        """Test basic search endpoint call."""
        # Mock the retriever
        mock_retriever = mocker.patch("app.main.retriever")
        mock_retriever.search.return_value = []

        # Mock LLM functions
        mocker.patch(
            "app.main.rewrite_query",
            new_callable=AsyncMock,
            return_value=("запрос", False),
        )
        mocker.patch(
            "app.main.compose_answer", new_callable=AsyncMock, return_value="Ответ"
        )

        response = client.post("/search", json=search_request)
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert "answer" in data
        assert "effective_query" in data
        assert "was_rewritten" in data

    @pytest.mark.asyncio
    async def test_search_endpoint_no_rewrite(self, mocker, search_request):
        """Test search endpoint with rewriter disabled."""
        search_request["use_rewriter"] = False

        mock_retriever = mocker.patch("app.main.retriever")
        mock_retriever.search.return_value = []

        mocker.patch(
            "app.main.rewrite_query",
            new_callable=AsyncMock,
            return_value=("запрос", False),
        )
        mocker.patch(
            "app.main.compose_answer", new_callable=AsyncMock, return_value="Ответ"
        )

        response = client.post("/search", json=search_request)
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_search_endpoint_no_compose(self, mocker, search_request):
        """Test search endpoint without answer composition."""
        search_request["compose_system_prompt"] = ""

        mock_retriever = mocker.patch("app.main.retriever")
        mock_retriever.search.return_value = []

        mocker.patch(
            "app.main.rewrite_query",
            new_callable=AsyncMock,
            return_value=("запрос", False),
        )

        response = client.post("/search", json=search_request)
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_search_endpoint_empty_results(self, mocker, search_request):
        """Test search with empty results."""
        mock_retriever = mocker.patch("app.main.retriever")
        mock_retriever.search.return_value = []

        mocker.patch(
            "app.main.rewrite_query",
            new_callable=AsyncMock,
            return_value=("запрос", False),
        )
        mocker.patch(
            "app.main.compose_answer",
            new_callable=AsyncMock,
            return_value="Ничего не найдено",
        )

        response = client.post("/search", json=search_request)
        assert response.status_code == 200
        data = response.json()
        assert data["results"] == []


class TestRewriteQueryEndpoint:
    """Tests for /rewrite_query endpoint."""

    @pytest.mark.asyncio
    async def test_rewrite_query_endpoint(self, mocker):
        """Test rewrite_query endpoint."""
        mocker.patch("app.main.llm_client")
        mocker.patch(
            "app.main.rewrite_query",
            new_callable=AsyncMock,
            return_value=("переформулированный", True),
        )

        response = client.post(
            "/rewrite_query",
            json={
                "query": "как рассчитать фундамент?",
                "system_prompt": "Переформулируй запрос",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["rewritten"] == "переформулированный"
        assert data["was_rewritten"] is True


class TestComposeAnswerEndpoint:
    """Tests for /compose_answer endpoint."""

    @pytest.mark.asyncio
    async def test_compose_answer_endpoint(self, mocker):
        """Test compose_answer endpoint."""
        mocker.patch("app.main.llm_client")
        mocker.patch(
            "app.main.compose_answer",
            new_callable=AsyncMock,
            return_value="**Ответ:** найдено",
        )

        results = [
            {
                "id": "1",
                "score": 0.8,
                "text": "Текст",
                "filename": "SP_63",
                "headings": ["Раздел"],
                "is_table": False,
                "chunk_index": 0,
                "man_refs": [],
                "cross_refs": [],
                "section_path": "1.1",
                "section_level": 1,
                "parent_heading": None,
                "leaf_heading": "Раздел",
                "is_overlap_window": False,
                "window_index": 0,
            }
        ]

        response = client.post(
            "/compose_answer",
            json={
                "query": "исходный",
                "effective_query": "переформулированный",
                "system_prompt": "Ответь по контексту",
                "results": results,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "Ответ:" in data["answer"]
