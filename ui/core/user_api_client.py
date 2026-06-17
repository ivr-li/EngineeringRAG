import os
from typing import Any

import requests

DEFAULT_USER_API_URL = "http://127.0.0.1:9130"
DEFAULT_TIMEOUT = (3, 30)


class UserApiError(Exception):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class UserApiClient:
    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (
            base_url or os.getenv("USER_API_URL") or DEFAULT_USER_API_URL
        ).rstrip("/")

    def register(self, email: str, password: str, display_name: str | None) -> dict:
        return self._request(
            "POST",
            "/auth/register",
            json={
                "email": email,
                "password": password,
                "display_name": display_name,
            },
        )

    def login(self, email: str, password: str) -> dict:
        return self._request(
            "POST",
            "/auth/login",
            json={"email": email, "password": password},
        )

    def get_me(self, token: str) -> dict:
        return self._request("GET", "/me", token=token)

    def logout(self, token: str) -> None:
        self._request("POST", "/auth/logout", token=token)

    def get_preferences(self, token: str) -> dict:
        return self._request("GET", "/me/preferences", token=token)

    def update_preferences(self, token: str, theme_key: str) -> dict:
        return self._request(
            "PATCH",
            "/me/preferences",
            token=token,
            json={"theme_key": theme_key},
        )

    def list_searches(self, token: str, limit: int = 100) -> list[dict]:
        return self._request(
            "GET",
            "/me/searches",
            token=token,
            params={"limit": limit},
        )

    def create_search(self, token: str, query: str, response: dict) -> dict:
        return self._request(
            "POST",
            "/me/searches",
            token=token,
            json={"query": query, "response": response},
        )

    def update_search(
        self,
        token: str,
        search_id: str,
        query: str,
        response: dict,
    ) -> dict:
        return self._request(
            "PATCH",
            f"/me/searches/{search_id}",
            token=token,
            json={"query": query, "response": response},
        )

    def delete_search(self, token: str, search_id: str) -> None:
        self._request("DELETE", f"/me/searches/{search_id}", token=token)

    def _request(
        self,
        method: str,
        path: str,
        token: str | None = None,
        **kwargs: Any,
    ) -> Any:
        headers = kwargs.pop("headers", {})
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.request(
                method,
                f"{self.base_url}{path}",
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
                **kwargs,
            )
        except requests.RequestException as error:
            raise UserApiError(str(error)) from error

        if response.status_code >= 400:
            raise UserApiError(_error_message(response), response.status_code)
        if response.status_code == 204:
            return None

        return response.json()


def _error_message(response: requests.Response) -> str:
    try:
        detail = response.json().get("detail")
    except ValueError:
        detail = response.text

    return str(detail or "User API request failed")
