import os

import requests

DEFAULT_RETRIEVER_API_URL = "http://127.0.0.1:9123"


class RetrieverClient:
    def __init__(self):
        self.url = os.getenv(
            "RETRIEVER_API_URL",
            DEFAULT_RETRIEVER_API_URL,
        ).rstrip("/")

    def search(
        self,
        query: str,
        rewrite_system_prompt: str,
        compose_system_prompt: str,
        **kwargs,
    ):
        request_body = self._build_search_payload(
            query,
            rewrite_system_prompt,
            compose_system_prompt,
            kwargs,
        )
        response = self._post_search(request_body)

        return response.json()

    def _build_search_payload(
        self,
        query: str,
        rewrite_system_prompt: str,
        compose_system_prompt: str,
        options: dict,
    ) -> dict:
        return {
            "query": query,
            "user_id": options.get("user_id"),
            "session_id": options.get("session_id"),
            "rewrite_system_prompt": rewrite_system_prompt,
            "compose_system_prompt": compose_system_prompt,
            "top_k": options.get("top_k", 10),
            "prefetch_k": options.get("prefetch_k", 40),
            "mode": options.get("mode", "hybrid"),
            "only_tables": options.get("only_tables"),
            "use_rewriter": options.get("use_rewriter", True),
            "filename_filter": options.get("filename_filter"),
            "section_filter": options.get("section_filter"),
        }

    def _post_search(self, request_body: dict) -> requests.Response:
        response = requests.post(
            f"{self.url}/search",
            json=request_body,
            timeout=(5, 180),
        )
        response.raise_for_status()

        return response
