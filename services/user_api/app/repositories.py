from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import and_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.database import search_history, user_preferences, user_sessions, users
from app.security import create_session_token, hash_password, hash_token, verify_password

DEFAULT_THEME_KEY = "light"


class DuplicateUserError(Exception):
    pass


class NotFoundError(Exception):
    pass


@dataclass(frozen=True)
class CreatedSession:
    session_id: UUID
    token: str
    expires_at: datetime


@dataclass(frozen=True)
class AuthContext:
    user: dict[str, Any]
    session_id: UUID
    token_hash: str


async def create_user(
    engine: AsyncEngine,
    email: str,
    password: str,
    display_name: str | None,
) -> dict[str, Any]:
    now = _now()
    statement = users.insert().values(
        id=uuid4(),
        email=email,
        display_name=_display_name(email, display_name),
        password_hash=hash_password(password),
        is_active=True,
        created_at=now,
        updated_at=now,
    ).returning(*users.c)

    try:
        async with engine.begin() as connection:
            row = (await connection.execute(statement)).mappings().one()
    except IntegrityError as error:
        raise DuplicateUserError from error

    return dict(row)


async def authenticate_user(
    engine: AsyncEngine,
    email: str,
    password: str,
) -> dict[str, Any] | None:
    user = await get_user_by_email(engine, email)
    if not user or not verify_password(password, user["password_hash"]):
        return None

    return user if user["is_active"] else None


async def get_user_by_email(engine: AsyncEngine, email: str) -> dict[str, Any] | None:
    statement = select(*users.c).where(users.c.email == email)

    async with engine.begin() as connection:
        row = (await connection.execute(statement)).mappings().first()

    return dict(row) if row else None


async def create_session(
    engine: AsyncEngine,
    user_id: UUID,
    ttl_days: int,
) -> CreatedSession:
    token = create_session_token()
    expires_at = _now() + timedelta(days=ttl_days)
    session_id = uuid4()
    statement = user_sessions.insert().values(
        id=session_id,
        user_id=user_id,
        token_hash=hash_token(token),
        expires_at=expires_at,
        created_at=_now(),
    )

    async with engine.begin() as connection:
        await connection.execute(statement)

    return CreatedSession(session_id=session_id, token=token, expires_at=expires_at)


async def get_auth_context(engine: AsyncEngine, token: str) -> AuthContext | None:
    token_hash = hash_token(token)
    statement = _auth_context_statement(token_hash)

    async with engine.begin() as connection:
        row = (await connection.execute(statement)).mappings().first()

    if not row:
        return None

    return AuthContext(
        user=_user_from_auth_row(row),
        session_id=row["session_id"],
        token_hash=token_hash,
    )


async def revoke_session(engine: AsyncEngine, token_hash: str) -> None:
    statement = update(user_sessions).where(
        user_sessions.c.token_hash == token_hash,
        user_sessions.c.revoked_at.is_(None),
    ).values(revoked_at=_now())

    async with engine.begin() as connection:
        await connection.execute(statement)


async def get_preferences(engine: AsyncEngine, user_id: UUID) -> dict[str, Any]:
    statement = select(*user_preferences.c).where(user_preferences.c.user_id == user_id)

    async with engine.begin() as connection:
        row = (await connection.execute(statement)).mappings().first()

    return dict(row) if row else {"user_id": user_id, "theme_key": DEFAULT_THEME_KEY}


async def set_preferences(
    engine: AsyncEngine,
    user_id: UUID,
    theme_key: str,
) -> dict[str, Any]:
    now = _now()
    statement = pg_insert(user_preferences).values(
        user_id=user_id,
        theme_key=theme_key,
        created_at=now,
        updated_at=now,
    ).on_conflict_do_update(
        index_elements=[user_preferences.c.user_id],
        set_={"theme_key": theme_key, "updated_at": now},
    ).returning(*user_preferences.c)

    async with engine.begin() as connection:
        row = (await connection.execute(statement)).mappings().one()

    return dict(row)


async def list_searches(
    engine: AsyncEngine,
    user_id: UUID,
    limit: int,
) -> list[dict[str, Any]]:
    statement = select(*search_history.c).where(
        search_history.c.user_id == user_id,
        search_history.c.deleted_at.is_(None),
    ).order_by(search_history.c.created_at.desc()).limit(limit)

    async with engine.begin() as connection:
        rows = (await connection.execute(statement)).mappings().all()

    return [_search_from_row(row) for row in rows]


async def create_search(
    engine: AsyncEngine,
    user_id: UUID,
    query: str,
    response: dict[str, Any],
) -> dict[str, Any]:
    now = _now()
    statement = search_history.insert().values(
        id=uuid4(),
        user_id=user_id,
        query=query,
        response_json=response,
        created_at=now,
    ).returning(*search_history.c)

    async with engine.begin() as connection:
        row = (await connection.execute(statement)).mappings().one()

    return _search_from_row(row)


async def update_search(
    engine: AsyncEngine,
    user_id: UUID,
    search_id: UUID,
    values: dict[str, Any],
) -> dict[str, Any]:
    statement = _search_update_statement(user_id, search_id, values)

    async with engine.begin() as connection:
        row = (await connection.execute(statement)).mappings().first()

    if not row:
        raise NotFoundError

    return _search_from_row(row)


async def delete_search(engine: AsyncEngine, user_id: UUID, search_id: UUID) -> None:
    statement = update(search_history).where(
        search_history.c.id == search_id,
        search_history.c.user_id == user_id,
        search_history.c.deleted_at.is_(None),
    ).values(deleted_at=_now(), updated_at=_now())

    async with engine.begin() as connection:
        result = await connection.execute(statement)

    if result.rowcount == 0:
        raise NotFoundError


def _auth_context_statement(token_hash: str):
    return select(
        users.c.id,
        users.c.email,
        users.c.display_name,
        users.c.is_active,
        user_sessions.c.id.label("session_id"),
    ).select_from(
        user_sessions.join(users, users.c.id == user_sessions.c.user_id)
    ).where(
        user_sessions.c.token_hash == token_hash,
        user_sessions.c.expires_at > _now(),
        user_sessions.c.revoked_at.is_(None),
        users.c.is_active.is_(True),
    )


def _search_update_statement(user_id: UUID, search_id: UUID, values: dict[str, Any]):
    payload = {**values, "updated_at": _now()}

    return update(search_history).where(
        search_history.c.id == search_id,
        search_history.c.user_id == user_id,
        search_history.c.deleted_at.is_(None),
    ).values(**payload).returning(*search_history.c)


def _search_from_row(row: dict[str, Any]) -> dict[str, Any]:
    search = dict(row)
    search["response"] = search.pop("response_json")

    return search


def _user_from_auth_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "email": row["email"],
        "display_name": row["display_name"],
        "is_active": row["is_active"],
    }


def _display_name(email: str, display_name: str | None) -> str:
    if display_name and display_name.strip():
        return display_name.strip()

    return email.split("@", 1)[0]


def _now() -> datetime:
    return datetime.now(UTC)
