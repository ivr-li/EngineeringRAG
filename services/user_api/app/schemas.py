from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from app.security import is_valid_email, normalize_email


class UserRead(BaseModel):
    id: UUID
    email: str
    display_name: str
    is_authenticated: bool = True


class RegisterRequest(BaseModel):
    email: str
    password: str = Field(min_length=8)
    display_name: str | None = Field(default=None, max_length=120)

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        if not is_valid_email(value):
            raise ValueError("Invalid email")

        return normalize_email(value)


class LoginRequest(BaseModel):
    email: str
    password: str

    @field_validator("email")
    @classmethod
    def normalize_login_email(cls, value: str) -> str:
        return normalize_email(value)


class AuthResponse(BaseModel):
    token: str
    session_id: UUID
    expires_at: datetime
    user: UserRead


class PreferencesRead(BaseModel):
    theme_key: str


class PreferencesUpdate(BaseModel):
    theme_key: str = Field(min_length=1, max_length=64)


class SearchCreate(BaseModel):
    query: str = Field(min_length=1)
    response: dict[str, Any]


class SearchUpdate(BaseModel):
    query: str | None = Field(default=None, min_length=1)
    response: dict[str, Any] | None = None


class SearchRead(BaseModel):
    id: UUID
    query: str
    response: dict[str, Any]
    created_at: datetime
    updated_at: datetime | None = None
