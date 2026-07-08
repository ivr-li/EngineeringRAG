from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncEngine

from app.config import get_settings
from app.database import build_engine, ensure_storage
from app.dependencies import get_current_auth, get_engine
from app.repositories import (
    AuthContext,
    DuplicateUserError,
    NotFoundError,
    authenticate_user,
    create_search,
    create_session,
    create_user,
    delete_search,
    get_preferences,
    list_searches,
    revoke_session,
    set_preferences,
    update_search,
)
from app.schemas import (
    AuthResponse,
    LoginRequest,
    PreferencesRead,
    PreferencesUpdate,
    RegisterRequest,
    SearchCreate,
    SearchRead,
    SearchUpdate,
    UserRead,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    engine = build_engine(settings.database_url)
    app.state.engine = engine

    await ensure_storage(engine)

    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(title="EngineeringRAG User API", lifespan=lifespan)
EngineDep = Annotated[AsyncEngine, Depends(get_engine)]
AuthDep = Annotated[AuthContext, Depends(get_current_auth)]


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/auth/register",
    response_model=AuthResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register(payload: RegisterRequest, engine: EngineDep) -> AuthResponse:
    try:
        user = await create_user(
            engine,
            payload.email,
            payload.password,
            payload.display_name,
        )
    except DuplicateUserError as error:
        raise HTTPException(status.HTTP_409_CONFLICT, "User already exists") from error

    session = await create_session(engine, user["id"], get_settings().session_ttl_days)
    return _auth_response(user, session)


@app.post("/auth/login", response_model=AuthResponse)
async def login(payload: LoginRequest, engine: EngineDep) -> AuthResponse:
    user = await authenticate_user(engine, payload.email, payload.password)
    if user is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid email or password")

    session = await create_session(engine, user["id"], get_settings().session_ttl_days)
    return _auth_response(user, session)


@app.post("/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(auth: AuthDep, engine: EngineDep) -> Response:
    await revoke_session(engine, auth.token_hash)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/me", response_model=UserRead)
async def me(auth: AuthDep) -> UserRead:
    return _user_response(auth.user)


@app.get("/me/preferences", response_model=PreferencesRead)
async def read_preferences(auth: AuthDep, engine: EngineDep) -> PreferencesRead:
    preferences = await get_preferences(engine, auth.user["id"])

    return PreferencesRead(theme_key=preferences["theme_key"])


@app.patch("/me/preferences", response_model=PreferencesRead)
async def patch_preferences(
    payload: PreferencesUpdate,
    auth: AuthDep,
    engine: EngineDep,
) -> PreferencesRead:
    preferences = await set_preferences(engine, auth.user["id"], payload.theme_key)

    return PreferencesRead(theme_key=preferences["theme_key"])


@app.get("/me/searches", response_model=list[SearchRead])
async def read_searches(
    auth: AuthDep,
    engine: EngineDep,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[dict]:
    return await list_searches(engine, auth.user["id"], limit)


@app.post("/me/searches", response_model=SearchRead, status_code=status.HTTP_201_CREATED)
async def post_search(
    payload: SearchCreate,
    auth: AuthDep,
    engine: EngineDep,
) -> dict:
    return await create_search(engine, auth.user["id"], payload.query, payload.response)


@app.patch("/me/searches/{search_id}", response_model=SearchRead)
async def patch_search(
    search_id: UUID,
    payload: SearchUpdate,
    auth: AuthDep,
    engine: EngineDep,
) -> dict:
    values = _search_update_values(payload)
    if not values:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    try:
        return await update_search(engine, auth.user["id"], search_id, values)
    except NotFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Search not found") from error


@app.delete("/me/searches/{search_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_search(search_id: UUID, auth: AuthDep, engine: EngineDep) -> Response:
    try:
        await delete_search(engine, auth.user["id"], search_id)
    except NotFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Search not found") from error

    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _auth_response(user: dict, session) -> AuthResponse:
    return AuthResponse(
        token=session.token,
        session_id=session.session_id,
        expires_at=session.expires_at,
        user=_user_response(user),
    )


def _user_response(user: dict) -> UserRead:
    return UserRead(
        id=user["id"],
        email=user["email"],
        display_name=user["display_name"],
    )


def _search_update_values(payload: SearchUpdate) -> dict:
    values = {}
    if payload.query is not None:
        values["query"] = payload.query
    if payload.response is not None:
        values["response_json"] = payload.response

    return values
