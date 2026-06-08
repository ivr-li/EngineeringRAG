import requests


class RetrieverClient:
    def __init__(self):
        self.url = "http://0.0.0.0:9123"

    def search(
        self, query: str, rewrite_system_prompt: str, compose_system_prompt: str, **kwargs
    ):
        """
        Search via retriever service.

        Parameters:
            query: User query
            rewrite_system_prompt: Prompt for query rewriting
            compose_system_prompt: Prompt for answer composition

        Args:
            top_k: Number of results (default: 10)
            prefetch_k: Candidates for ColBERT rerank (default: 40)
            mode: Search mode: hybrid, dense, sparse (default: hybrid)
            only_tables: Filter for tables only (default: None)
            use_rewriter: Rewrite query via LLM (default: True)
            filename_filter: Filter by filename (default: None)
            section_filter: Filter by section path (default: None)

        Returns:
            list[RetrievalResult]: Search results
        """
        request_body = {
            "query": query,
            "rewrite_system_prompt": rewrite_system_prompt,
            "compose_system_prompt": compose_system_prompt,
            "top_k": kwargs.get("top_k", 10),
            "prefetch_k": kwargs.get("prefetch_k", 40),
            "mode": kwargs.get("mode", "hybrid"),
            "only_tables": kwargs.get("only_tables"),
            "use_rewriter": kwargs.get("use_rewriter", True),
            "filename_filter": kwargs.get("filename_filter"),
            "section_filter": kwargs.get("section_filter"),
        }
        resp = requests.post(
            f"{self.url}/search",
            json=request_body,
            timeout=(5, 180),
        )
        resp.raise_for_status()
        return resp.json()
