from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    MetaData,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

IDENTITY_SCHEMA = "identity"
UI_STATE_SCHEMA = "ui_state"
metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", PostgreSQLUUID(as_uuid=True), primary_key=True),
    Column("email", Text, nullable=False, unique=True),
    Column("display_name", Text, nullable=False),
    Column("password_hash", Text, nullable=False),
    Column("is_active", Boolean, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    schema=IDENTITY_SCHEMA,
)

user_sessions = Table(
    "user_sessions",
    metadata,
    Column("id", PostgreSQLUUID(as_uuid=True), primary_key=True),
    Column(
        "user_id",
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(f"{IDENTITY_SCHEMA}.users.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("token_hash", Text, nullable=False, unique=True),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("revoked_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=IDENTITY_SCHEMA,
)

user_preferences = Table(
    "user_preferences",
    metadata,
    Column(
        "user_id",
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(f"{IDENTITY_SCHEMA}.users.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("theme_key", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    schema=UI_STATE_SCHEMA,
)

search_history = Table(
    "search_history",
    metadata,
    Column("id", PostgreSQLUUID(as_uuid=True), primary_key=True),
    Column(
        "user_id",
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(f"{IDENTITY_SCHEMA}.users.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("query", Text, nullable=False),
    Column("response_json", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=True),
    Column("deleted_at", DateTime(timezone=True), nullable=True),
    schema=UI_STATE_SCHEMA,
)

Index("ix_user_sessions_token_hash", user_sessions.c.token_hash)
Index(
    "ix_search_history_user_created_at",
    search_history.c.user_id,
    search_history.c.created_at,
)


def build_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


async def ensure_storage(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{IDENTITY_SCHEMA}"'))
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{UI_STATE_SCHEMA}"'))

        await connection.run_sync(metadata.create_all)
