from __future__ import annotations

from pydantic import BaseModel, Field


class QuestionOptionOut(BaseModel):
    id: int
    text: str

    model_config = {"from_attributes": True}


class QuestionCreate(BaseModel):
    text: str = Field(min_length=1)
    options: list[str] = Field(min_length=2)
    correct_option_index: int = Field(ge=0)


class QuestionUpdate(BaseModel):
    text: str | None = Field(default=None, min_length=1)
    # For simplicity, options are replaced as a whole when provided
    options: list[str] | None = None
    correct_option_index: int | None = Field(default=None, ge=0)


class QuestionOut(BaseModel):
    id: int
    text: str
    options: list[QuestionOptionOut]

    model_config = {"from_attributes": True}

