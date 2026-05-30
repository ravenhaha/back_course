from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import AdminUser
from app.schemas.questions import QuestionCreate, QuestionOut, QuestionUpdate
from app.services.questions import QuestionService

router = APIRouter(prefix="/questions", tags=["questions"])
DbDep = Annotated[AsyncSession, Depends(get_db)]


@router.get("", response_model=list[QuestionOut])
async def list_questions(db: DbDep, _: AdminUser, offset: int = 0, limit: int = 100):
    return await QuestionService(db).list(offset=offset, limit=limit)


@router.post("", response_model=QuestionOut, status_code=201)
async def create_question(db: DbDep, _: AdminUser, data: QuestionCreate):
    if data.correct_option_index >= len(data.options):
        raise HTTPException(status_code=400, detail="correct_option_index out of range")
    return await QuestionService(db).create(
        text=data.text, options=data.options, correct_option_index=data.correct_option_index
    )


@router.get("/{question_id}", response_model=QuestionOut)
async def get_question(db: DbDep, _: AdminUser, question_id: int):
    q = await QuestionService(db).get(question_id)
    if q is None:
        raise HTTPException(status_code=404, detail="Question not found")
    return q


@router.patch("/{question_id}", response_model=QuestionOut)
async def update_question(db: DbDep, _: AdminUser, question_id: int, data: QuestionUpdate):
    qs = QuestionService(db)
    q = await qs.get(question_id)
    if q is None:
        raise HTTPException(status_code=404, detail="Question not found")
    if data.correct_option_index is not None:
        opts_len = len(data.options) if data.options is not None else len(q.options)
        if data.correct_option_index >= opts_len:
            raise HTTPException(status_code=400, detail="correct_option_index out of range")
    try:
        return await qs.update(
            q,
            text=data.text,
            options=data.options,
            correct_option_index=data.correct_option_index,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.delete("/{question_id}", status_code=204)
async def delete_question(db: DbDep, _: AdminUser, question_id: int):
    ok = await QuestionService(db).delete(question_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Question not found")

