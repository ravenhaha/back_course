from pathlib import Path
from typing import Annotated, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from students_db import StudentRecord, StudentRepository

app = FastAPI(title="Students API")

CSV_PATH = Path(__file__).parent.parent / "students.csv"
DB_URL = "sqlite:///students.db"


# ─── Dependency ───────────────────────────────────────────────────────────────

def get_repo() -> StudentRepository:
    return StudentRepository(db_url=DB_URL)


RepoDep = Annotated[StudentRepository, Depends(get_repo)]


# ─── Pydantic-схемы ───────────────────────────────────────────────────────────

class StudentCreate(BaseModel):
    last_name: str = Field(min_length=1)
    first_name: str = Field(min_length=1)
    faculty: str = Field(min_length=1)
    course: str = Field(min_length=1)
    score: int = Field(ge=0, le=100)


class StudentUpdate(BaseModel):
    last_name: Optional[str] = Field(default=None, min_length=1)
    first_name: Optional[str] = Field(default=None, min_length=1)
    faculty: Optional[str] = Field(default=None, min_length=1)
    course: Optional[str] = Field(default=None, min_length=1)
    score: Optional[int] = Field(default=None, ge=0, le=100)


class StudentResponse(StudentCreate):
    id: int

    model_config = {"from_attributes": True}


# ─── Вспомогательная функция ──────────────────────────────────────────────────

def _to_response(record: StudentRecord) -> StudentResponse:
    return StudentResponse.model_validate(record)


def _get_or_404(repo: StudentRepository, record_id: int) -> StudentRecord:
    record = repo.get_by_id(record_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Запись с id={record_id} не найдена")
    return record


# ─── CRUD-эндпоинты ───────────────────────────────────────────────────────────

@app.post("/students/", response_model=StudentResponse, status_code=201,
          summary="Создать запись")
def create_student(data: StudentCreate, repo: RepoDep):
    record = StudentRecord(**data.model_dump())
    repo.insert(record)
    return _to_response(record)


@app.get("/students/", response_model=list[StudentResponse],
         summary="Получить все записи")
def read_students(repo: RepoDep):
    return [_to_response(r) for r in repo.select_all()]


@app.get("/students/{student_id}", response_model=StudentResponse,
         summary="Получить запись по id")
def read_student(student_id: int, repo: RepoDep):
    return _to_response(_get_or_404(repo, student_id))


@app.put("/students/{student_id}", response_model=StudentResponse,
         summary="Обновить запись по id")
def update_student(student_id: int, data: StudentUpdate, repo: RepoDep):
    _get_or_404(repo, student_id)
    fields = data.model_dump(exclude_none=True)
    updated = repo.update(student_id, fields)
    return _to_response(updated)


@app.delete("/students/{student_id}", status_code=204,
            summary="Удалить запись по id")
def delete_student(student_id: int, repo: RepoDep):
    _get_or_404(repo, student_id)
    repo.delete(student_id)


# ─── Дополнительные эндпоинты из предыдущего задания ─────────────────────────

@app.get("/students/faculty/{faculty}", response_model=list[dict],
         summary="Уникальные студенты по факультету")
def students_by_faculty(faculty: str, repo: RepoDep):
    rows = repo.get_students_by_faculty(faculty)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Факультет {faculty!r} не найден")
    return [{"last_name": last, "first_name": first} for last, first in rows]


@app.get("/courses/", response_model=list[str],
         summary="Список уникальных курсов")
def unique_courses(repo: RepoDep):
    return repo.get_unique_courses()


@app.get("/faculty/{faculty}/avg-score",
         summary="Средний балл по факультету")
def avg_score_by_faculty(faculty: str, repo: RepoDep):
    avg = repo.get_avg_score_by_faculty(faculty)
    if avg == 0.0 and not repo.get_students_by_faculty(faculty):
        raise HTTPException(status_code=404, detail=f"Факультет {faculty!r} не найден")
    return {"faculty": faculty, "avg_score": avg}


@app.get("/courses/{course}/below-score", response_model=list[StudentResponse],
         summary="Студенты с оценкой ниже порога на курсе")
def students_below_score(
    course: str,
    repo: RepoDep,
    threshold: Annotated[int, Query(ge=0, le=100)] = 30,
):
    rows = repo.get_students_below_score(course, threshold)
    return [_to_response(r) for r in rows]


# ─── Служебный эндпоинт: загрузка CSV ────────────────────────────────────────

@app.post("/load-csv/", summary="Загрузить данные из students.csv")
def load_csv(repo: RepoDep):
    if not CSV_PATH.exists():
        raise HTTPException(status_code=404, detail="Файл students.csv не найден")
    count = repo.load_from_csv(CSV_PATH)
    return {"loaded": count}