from fastapi import Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncEngine

from app.repositories import AuthContext, get_auth_context
from app.security import extract_bearer_token


def get_engine(request: Request) -> AsyncEngine:
    return request.app.state.engine


async def get_current_auth(
    request: Request,
    authorization: str | None = Header(default=None),
) -> AuthContext:
    token = extract_bearer_token(authorization)
    if not token:
        raise _unauthorized()

    auth_context = await get_auth_context(get_engine(request), token)
    if auth_context is None:
        raise _unauthorized()

    return auth_context


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required",
    )
