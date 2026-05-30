from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.question import Question, QuestionOption


class QuestionService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def list(self, *, offset: int = 0, limit: int = 100) -> list[Question]:
        res = await self.db.execute(
            select(Question).options(selectinload(Question.options)).offset(offset).limit(limit).order_by(Question.id)
        )
        return list(res.scalars().all())

    async def get(self, question_id: int) -> Question | None:
        res = await self.db.execute(
            select(Question).options(selectinload(Question.options)).where(Question.id == question_id)
        )
        return res.scalar_one_or_none()

    async def create(self, *, text: str, options: list[str], correct_option_index: int) -> Question:
        q = Question(text=text)
        q.options = [
            QuestionOption(text=opt, is_correct=(idx == correct_option_index))
            for idx, opt in enumerate(options)
        ]
        self.db.add(q)
        await self.db.commit()
        await self.db.refresh(q)
        return await self.get(q.id)  # with options

    async def update(
        self,
        question: Question,
        *,
        text: str | None = None,
        options: list[str] | None = None,
        correct_option_index: int | None = None,
    ) -> Question:
        if text is not None:
            question.text = text
        if options is not None:
            question.options = [
                QuestionOption(text=opt, is_correct=False) for opt in options
            ]
        if correct_option_index is not None:
            if not question.options or correct_option_index >= len(question.options):
                raise ValueError("correct_option_index out of range")
            for idx, opt in enumerate(question.options):
                opt.is_correct = idx == correct_option_index
        await self.db.commit()
        return await self.get(question.id)  # with options

    async def delete(self, question_id: int) -> bool:
        res = await self.db.execute(delete(Question).where(Question.id == question_id))
        await self.db.commit()
        return bool(res.rowcount)

