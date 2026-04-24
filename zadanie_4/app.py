from pathlib import Path
from typing import Annotated, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field

from auth import CurrentUser, router as auth_router
from cache import cache_or_compute, invalidate
from students_db import StudentRecord, StudentRepository

app = FastAPI(title="Students API")
app.include_router(auth_router)

CSV_PATH = Path(__file__).parent.parent / "students.csv"
DB_URL = "sqlite:///students.db"

CACHE_PREFIX = "students:"


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


class LoadCsvRequest(BaseModel):
    path: str = Field(min_length=1, description="Абсолютный путь к csv-файлу")


class BulkDeleteRequest(BaseModel):
    ids: list[int] = Field(min_length=1)


def _to_response(record: StudentRecord) -> StudentResponse:
    return StudentResponse.model_validate(record)


def _get_or_404(repo: StudentRepository, record_id: int) -> StudentRecord:
    record = repo.get_by_id(record_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Запись с id={record_id} не найдена")
    return record


# ─── CRUD-эндпоинты (доступны только авторизованным) ─────────────────────────

@app.post("/students/", response_model=StudentResponse, status_code=201,
          summary="Создать запись")
def create_student(data: StudentCreate, repo: RepoDep, user: CurrentUser):
    record = StudentRecord(**data.model_dump())
    repo.insert(record)
    invalidate(CACHE_PREFIX)
    return _to_response(record)


@app.get("/students/", response_model=list[StudentResponse],
         summary="Получить все записи (кешируется)")
def read_students(repo: RepoDep, user: CurrentUser):
    return cache_or_compute(
        f"{CACHE_PREFIX}all",
        lambda: [_to_response(r) for r in repo.select_all()],
    )


@app.get("/students/{student_id}", response_model=StudentResponse,
         summary="Получить запись по id (кешируется)")
def read_student(student_id: int, repo: RepoDep, user: CurrentUser):
    return cache_or_compute(
        f"{CACHE_PREFIX}id:{student_id}",
        lambda: _to_response(_get_or_404(repo, student_id)),
    )


@app.put("/students/{student_id}", response_model=StudentResponse,
         summary="Обновить запись по id")
def update_student(student_id: int, data: StudentUpdate, repo: RepoDep, user: CurrentUser):
    _get_or_404(repo, student_id)
    fields = data.model_dump(exclude_none=True)
    updated = repo.update(student_id, fields)
    invalidate(CACHE_PREFIX)
    return _to_response(updated)


@app.delete("/students/{student_id}", status_code=204,
            summary="Удалить запись по id")
def delete_student(student_id: int, repo: RepoDep, user: CurrentUser):
    _get_or_404(repo, student_id)
    repo.delete(student_id)
    invalidate(CACHE_PREFIX)


@app.get("/students/faculty/{faculty}", response_model=list[dict],
         summary="Уникальные студенты по факультету (кешируется)")
def students_by_faculty(faculty: str, repo: RepoDep, user: CurrentUser):
    def compute():
        rows = repo.get_students_by_faculty(faculty)
        if not rows:
            raise HTTPException(status_code=404, detail=f"Факультет {faculty!r} не найден")
        return [{"last_name": last, "first_name": first} for last, first in rows]

    return cache_or_compute(f"{CACHE_PREFIX}faculty:{faculty}", compute)


@app.get("/courses/", response_model=list[str],
         summary="Список уникальных курсов (кешируется)")
def unique_courses(repo: RepoDep, user: CurrentUser):
    return cache_or_compute(f"{CACHE_PREFIX}courses", repo.get_unique_courses)


@app.get("/faculty/{faculty}/avg-score",
         summary="Средний балл по факультету (кешируется)")
def avg_score_by_faculty(faculty: str, repo: RepoDep, user: CurrentUser):
    def compute():
        avg = repo.get_avg_score_by_faculty(faculty)
        if avg == 0.0 and not repo.get_students_by_faculty(faculty):
            raise HTTPException(status_code=404, detail=f"Факультет {faculty!r} не найден")
        return {"faculty": faculty, "avg_score": avg}

    return cache_or_compute(f"{CACHE_PREFIX}faculty_avg:{faculty}", compute)


@app.get("/courses/{course}/below-score", response_model=list[StudentResponse],
         summary="Студенты с оценкой ниже порога на курсе (кешируется)")
def students_below_score(
    course: str,
    repo: RepoDep,
    user: CurrentUser,
    threshold: Annotated[int, Query(ge=0, le=100)] = 30,
):
    return cache_or_compute(
        f"{CACHE_PREFIX}below:{course}:{threshold}",
        lambda: [_to_response(r) for r in repo.get_students_below_score(course, threshold)],
    )


# ─── Фоновые задачи ───────────────────────────────────────────────────────────

def _load_csv_task(path: str) -> None:
    repo = StudentRepository(db_url=DB_URL)
    repo.load_from_csv(path)
    invalidate(CACHE_PREFIX)


def _bulk_delete_task(ids: list[int]) -> None:
    repo = StudentRepository(db_url=DB_URL)
    repo.delete_many(ids)
    invalidate(CACHE_PREFIX)


@app.post("/load-csv-bg/", status_code=status.HTTP_202_ACCEPTED,
          summary="Фоновая загрузка данных из CSV")
def load_csv_bg(data: LoadCsvRequest, bg: BackgroundTasks, user: CurrentUser):
    if not Path(data.path).exists():
        raise HTTPException(status_code=404, detail=f"Файл {data.path!r} не найден")
    bg.add_task(_load_csv_task, data.path)
    return {"status": "scheduled", "path": data.path}


@app.post("/students/bulk-delete/", status_code=status.HTTP_202_ACCEPTED,
          summary="Фоновое удаление записей по списку id")
def bulk_delete(data: BulkDeleteRequest, bg: BackgroundTasks, user: CurrentUser):
    bg.add_task(_bulk_delete_task, data.ids)
    return {"status": "scheduled", "count": len(data.ids)}


# ─── Старый синхронный эндпоинт загрузки (оставлен для совместимости) ────────

@app.post("/load-csv/", summary="Загрузить students.csv (синхронно)")
def load_csv(repo: RepoDep, user: CurrentUser):
    if not CSV_PATH.exists():
        raise HTTPException(status_code=404, detail="Файл students.csv не найден")
    count = repo.load_from_csv(CSV_PATH)
    invalidate(CACHE_PREFIX)
    return {"loaded": count}